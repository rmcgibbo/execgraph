use anyhow::Context;
use bitvec::array::BitArray;
use pyo3::{
    exceptions::{PyIOError, PyIndexError, PyOSError, PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyTuple},
};
use std::convert::TryInto;
use std::ffi::OsString;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

use crate::{
    execgraph::{Cmd, ExecGraph, RemoteProvisionerSpec},
    logfile2::{self, LogEntry, LogFile, LogFileRW},
    logging,
};

/// Parallel execution of shell commands with DAG dependencies.
/// It's sort of like the core routine behind a build system like Make
/// or Ninja, except without most of the features.
///
/// This class manages a collection of shell commands, each of
/// which you register with `add_task`. Associated with each task
/// can be a list of dependencies, which are represented by the
/// indices of previously added tasks.
///
/// When `execute` is called, the commands get executed in parallel.
/// There's an argument to the constructor to control the degree of
/// parallelism.
///
/// Args:
///    num_parallel (int): Maximum number of local parallel processes
///      to run. [default=2 more than the number of CPU cores].
///    logfile (str): The path to the log file.
///    failures_allowed (int): keep going until N jobs fail (0 means infinity)
///      [default=1].
///
#[pyclass(name = "ExecGraph")]
pub struct PyExecGraph {
    g: ExecGraph,
    num_parallel: u32,
    failures_allowed: u32,
    key: String,
    rerun_failures: bool,
}

#[pymethods]
impl PyExecGraph {
    #[new]
    #[pyo3(signature=(
        num_parallel,
        logfile,
        storage_roots = vec![PathBuf::from("")],
        failures_allowed = 1,
        newkeyfn = None,
        rerun_failures=true
    ))]
    #[tracing::instrument(skip(py))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python,
        mut num_parallel: i32,
        logfile: PathBuf,
        storage_roots: Vec<PathBuf>,
        failures_allowed: u32,
        newkeyfn: Option<PyObject>,
        rerun_failures: bool,
    ) -> PyResult<PyExecGraph> {
        if num_parallel < 0 {
            num_parallel = std::thread::available_parallelism()?.get().try_into()?;
        }

        let mut log = LogFile::<LogFileRW>::new(&logfile).map_err(|e| match e {
            logfile2::LogfileError::MismatchedVersion { .. } => {
                PyRuntimeError::new_err(e.to_string())
            }
            _ => PyIOError::new_err(e.to_string()),
        })?;

        let stdout_mirror_to_file = logfile.parent().unwrap().join("log.txt");
        logging::init_logging(Some(&stdout_mirror_to_file))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        if log
            .header_version()
            .map(|v| v != logfile2::LOGFILE_VERSION)
            .unwrap_or(false)
        {
            return Err(PyRuntimeError::new_err(format!("This version of wrk uses the v{} logfile format. Cannot continue from a prior workflow using an older or newer format.", logfile2::LOGFILE_VERSION)));
        };

        let key = match log.workflow_key() {
            Some(key) => key,
            None => match newkeyfn {
                Some(newkeyfn) => newkeyfn.call(py, (), None)?.extract(py)?,
                None => "default-key-value".to_owned(),
            },
        };
        assert_eq!(std::str::from_utf8(key.as_bytes())?, key);
        debug!("Writing new log header key={}", key);
        log.write(LogEntry::new_header(
            &key,
            storage_roots
                .iter()
                .map(|s| PathBuf::from(s.as_os_str().to_string_lossy().replace("$KEY", &key)))
                .collect(),
        )?)?;

        Ok(PyExecGraph {
            g: ExecGraph::new(log),
            num_parallel: num_parallel as u32,
            failures_allowed: (if failures_allowed == 0 {
                u32::MAX
            } else {
                failures_allowed
            }),
            key,
            rerun_failures,
        })
    }

    fn all_storage_roots(&self) -> Vec<PathBuf> {
        self.g.logfile.storage_roots()
    }

    /// Get the number of tasks in the graph
    fn ntasks(&self) -> usize {
        self.g.ntasks()
    }

    /// Get the runcount that should be used for a task with this key.
    fn logfile_runcount(&self, key: &str) -> (u32, Option<PathBuf>) {
        match self.g.logfile.runcount(key) {
            // The task is new and has never been executed before.
            // obviously this calls for a runcount of zero, and we don't
            // know the task directory yet -- caller will be able to choose.
            None => (0, None),
            // During the last round, the task was added to the task graph,
            // became ready, but was not started. so no directory would have
            // been created, and we can reuse the prior run count.
            Some(logfile2::RuncountStatus::Ready { runcount, .. }) => (runcount, None),
            // The task was previously started, but not finished. this shouldn't
            // happen, because we should create a "fake" finished record with a
            // fake timeout_status, but maybe we got sigkilled or something. Anyways,
            // we're going to need a new directory. This is basically like a failure.
            Some(logfile2::RuncountStatus::Started { runcount, .. }) => (runcount + 1, None),
            // The task previously finished successfully. We reuse the old run count
            // because we're not going to actually run it again -- this lets us refer
            // to the assets in the correct directory.
            Some(logfile2::RuncountStatus::Finished {
                runcount,
                storage_root,
                success,
            }) if success => (
                runcount,
                Some(
                    self.g
                        .logfile
                        .storage_root(storage_root)
                        .with_context(|| {
                            format!("getting {}th entry from this logfile", storage_root)
                        })
                        .expect("Unable to find storage root")
                        .join(format!("{}.{}", key, runcount)),
                ),
            ),
            // New directory for the next run, as above.
            Some(logfile2::RuncountStatus::Finished { runcount, .. }) => (runcount + 1, None),
        }
    }

    /// Get the workflow-level key, created by the ``newkeyfn``
    /// callback passed into the constructor
    fn key(&self) -> String {
        self.key.clone()
    }

    /// Get a particular task in the graph.
    fn get_task<'p>(&mut self, py: Python<'p>, id: u32) -> PyResult<&'p PyTuple> {
        self.g
            .get_task(id)
            .ok_or_else(|| PyIndexError::new_err("index out of range"))
            .map(|c| {
                PyTuple::new(
                    py,
                    &[
                        c.cmdline.into_py(py),
                        c.key.into_py(py),
                        c.display.into_py(py),
                    ],
                )
            })
    }

    /// Get all of keys in the current network
    fn task_keys(&self) -> Vec<String> {
        self.g.task_keys()
    }

    /// Add a task to the graph.
    ///
    /// Each task is identified by a couple pieces of information:
    ///
    ///   1. First, there's the shell command to execute.
    ///   2. Second, there's "key". The idea is that this is a unique identifier
    ///      for the command -- it could be the hash of the cmdline, or the hash
    ///      of the cmdline and all of its inputs, or something like that. (We don't
    ///      do it for you). When we execute the command, we'll append the key to
    ///      a log file. That way when we rerun the graph at some later time, we'll
    ///      be able to skip executing any commands that have already been executed.
    ///   3. Next, there's the list of dependencies, identified by integer ids
    ///      that reference previous tasks. A task cannot depend on itself, and can only
    ///      depend on previous tasks tha have already been added. This enforces a DAG
    ///      structure.
    ///   4. Oh, actually there's one more thing: the display string. This is the
    ///      string associated with the command that will be printed to stdout when
    ///      we run the command. If not supplied, we'll just use the cmdline for these
    ///      purposes. But if the cmdline contains some boring wrapper scripts that you
    ///      want to hide from your users, this might make sense.
    ///   5. Then there's the concept of queue affinity. This us a u64 bitmask, which allows
    ///      us to address up to 64 "queues". Each task can have an affinity for one or more
    ///      of the queues. Queue 0 is the 'local' queue, and has up to n_parallel execution
    ///      slots. Queue 1 is the 'console' queue, and has either zero slots if n_parallel
    ///      is zero, or else 1 slot. If a task runs in the console queue, it will have
    ///      stdin/stdout/stderr hooked up. The remaining queues are not set up, but can be used
    ///      by remote executors -- when they connect they advertise what queue they are serving
    ///      and are given appropriate tasks.
    ///   6. Next is the 'preamble' and 'postamble'. These are an optional PyCapsule that's
    ///      supposed to contain a C function pointer inside and the name "Execgraph::Capsule".
    ///      The functions will be called (passing the capsule's `ctx` pointer as the only
    ///      argument, and it's expected to return a 32-bit signed integer) immediately
    ///      before and after the command is executed. This can be used if there is some
    ///      kind of setup or teardown you need to do before the task executes that you don't
    ///      want to do inside the task itself.
    ///
    /// Notes:
    ///   If key == "", then the task will never be skipped (i.e. it will always be
    ///   considered out of date).
    ///
    /// Args:
    ///     cmdline (List[str]): command to execute
    ///     key (str): unique identifier
    ///     dependencies (List[int], default=[]): dependencies for this task
    /// Returns:
    ///     taskid (int): integer id of this task
    #[pyo3(signature=(
        cmdline,
        key,
        dependencies = vec![],
        display = None,
        affinity = 1,
        fd_input = None,
        preamble = None,
        postamble = None,
        storage_root = 0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn add_task(
        &mut self,
        cmdline: Vec<OsString>,
        key: String,
        dependencies: Vec<u32>,
        display: Option<String>,
        affinity: u64,
        fd_input: Option<(i32, &PyBytes)>,
        preamble: Option<PyObject>,
        postamble: Option<PyObject>,
        storage_root: u32,
    ) -> PyResult<u32> {
        let runcount = self.logfile_runcount(&key as &str).0;
        let cmd = Cmd {
            cmdline,
            key,
            display,
            fd_input: fd_input.map(|(fd, buf)| (fd, buf.extract::<Vec<u8>>().unwrap())),
            storage_root,
            runcount,
            priority: 0,
            affinity: BitArray::<u64>::new(affinity),
            preamble: preamble.map(crate::execgraph::Capsule::new),
            postamble: postamble.map(crate::execgraph::Capsule::new),
        };
        self.g
            .add_task(cmd, dependencies)
            .map_err(|e| PyIndexError::new_err(e.to_string()))
    }

    /// Execute all the commands (in parallel to the extent possible while obeying
    /// the dependencies), and skipping any that the log file identifies as having
    /// previously completed successfully.
    ///
    /// Args:
    ///    target (Optional[int]): if you'd like to particularly execute up to a single
    ///      target, you can do this. in this case we'll only execute the tasks that
    ///      are required for this target. if not supplied we'll try to execute the
    ///      whole graph.
    ///    remote_provisioner_cmd (Optional[str]): Path to a remote provisioning script.
    ///      If supplied, we call this script with the url of an emphemeral server
    ///      as the first argument, and it can launch processes that can connect back
    ///      to this ExecGraph instance's http server to run tasks. Note: this package
    ///      includes a binary called ``execgraph-remote`` which implemenets the HTTP
    ///      protocol to "check out" tasks from the server, run them, and report their
    ///      status back. You'll need to write a provisioner script that arranges for
    ///      these execgraph-remote processes to be executed remotely using whatever
    ///      job queuing system you have though.
    ///   remote_provisioner_info (Optional[str]): If you have extra data that you
    ///     want to pass to the remote provisioner script, you can stash it here, and the
    ///     pick it up inside the remote provisioner by querying the /status endpoint.
    ///     Note that the version served on the /status endpoint can also be dynamically
    ///     updated during execution by POSTing to the admin unix socket.
    ///   ratelimit_per_second (u32): Rate limit command execution so that approximately
    ///     no more than this number of tasks are started per second. Set to zero to
    ///     disable.
    /// Returns:
    ///     num_failed (int): the number of tasks that failed. a failure is identified
    ///         when a task exits with a nonzero exit code.
    ///     execution_order (List[int]): the ids of the tasks that finished (success
    ///         or failure) in order of when they finished.
    ///
    #[pyo3(signature=(
        target = None,
        remote_provisioner_cmd = None,
        remote_provisioner_info = None,
        ratelimit_per_second = 0,
    ))]
    #[tracing::instrument(skip_all)]
    fn execute(
        &mut self,
        py: Python,
        target: Option<u32>,
        remote_provisioner_cmd: Option<String>,
        remote_provisioner_info: Option<String>,
        ratelimit_per_second: u32,
    ) -> PyResult<(u32, Vec<String>)> {
        // Create a new process group so that at shutdown time, we can send a
        // SIGTERM to this process group and kill of all child processes.
        unsafe {
            if libc::setpgid(libc::getpid(), libc::getpid()) != 0 {
                PyRuntimeError::new_err("Cannot setpgid");
            }
        }

        let x = remote_provisioner_cmd.map(|cmd| RemoteProvisionerSpec {
            cmd,
            info: remote_provisioner_info,
        });

        let result = py.allow_threads(move || {
            let rt = Runtime::new().expect("Failed to build tokio runtime");
            rt.block_on(async {
                self.g
                    .execute(
                        target,
                        self.num_parallel,
                        self.failures_allowed,
                        self.rerun_failures,
                        x,
                        ratelimit_per_second,
                    )
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        });

        logging::flush_logging().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        result
    }
}

impl From<logfile2::LogfileError> for PyErr {
    fn from(err: logfile2::LogfileError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

extern "C" fn test_callback(_ctx: *const std::ffi::c_void, foo: i32, bar: i32, ntasks: u32) -> i32 {
    println!("Hello from test_callback foo={} bar={} ntasks={}", foo, bar, ntasks);
    0
}

#[pyfunction]
fn test_make_capsule(py: Python) -> PyResult<PyObject> {
    const CAPSULE_NAME: &[u8] = b"Execgraph::Capsule-v3\0";
    let name: *const std::os::raw::c_char = CAPSULE_NAME.as_ptr() as *const i8;
    let obj = unsafe {
        let cb = test_callback as *const () as *mut std::ffi::c_void;
        let capsule = pyo3::ffi::PyCapsule_New(cb, name, None);
        PyObject::from_owned_ptr(py, capsule)
    };

    Ok(obj)
}

#[pyfunction]
#[tracing::instrument(skip(py))]
fn load_logfile(py: Python, path: std::path::PathBuf, mode: String) -> PyResult<PyObject> {
    let mut log = logfile2::LogFileSnapshotReader::open(path)?;
    let value = match &mode as &str {
        "current,outdated" => pythonize::pythonize(py, &log.read_current_and_outdated()?),
        "current" => pythonize::pythonize(py, &log.read_current_and_outdated()?.0),
        "outdated" => pythonize::pythonize(py, &log.read_current_and_outdated()?.1),
        "all" => pythonize::pythonize(py, &log.read()?),
        _ => return Err(PyValueError::new_err("Unrecognized mode")),
    };
    Ok(value?)
}

#[pyfunction]
#[tracing::instrument(skip(value))]
fn write_logfile(path: std::path::PathBuf, value: &PyAny) -> PyResult<()> {
    let mut log = logfile2::LogFile::<logfile2::LogFileRW>::new(path)?;
    let v: Vec<LogEntry> = pythonize::depythonize(value)?;
    for item in v {
        log.write(item)?;
    }
    log.flush()?;
    Ok(())
}

#[pymodule]
pub fn execgraph(_py: Python, m: &PyModule) -> PyResult<()> {
    unsafe {
        time::util::local_offset::set_soundness(time::util::local_offset::Soundness::Unsound);
    } // YOLO

    logging::init_logging(None).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    m.add_class::<PyExecGraph>()?;
    m.add_function(wrap_pyfunction!(test_make_capsule, m)?)?;
    m.add_function(wrap_pyfunction!(load_logfile, m)?)?;
    m.add_function(wrap_pyfunction!(write_logfile, m)?)?;
    Ok(())
}
