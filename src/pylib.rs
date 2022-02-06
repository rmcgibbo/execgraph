use pyo3::{
    exceptions::{PyIOError, PyIndexError, PyOSError, PyRuntimeError, PyValueError},
    prelude::*,
    types::PyTuple,
};
use std::ffi::OsString;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

use crate::{
    execgraph::{Cmd, ExecGraph, RemoteProvisionerSpec},
    runnercapabilities::{RunnerCapabilities, FeatureExpr},
    logfile2::{self, LogEntry, LogFile},
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
    #[args(
        num_parallel = -1,
        failures_allowed = 1,
        newkeyfn = "None",
        rerun_failures=true
    )]
    #[tracing::instrument(skip(py))]
    fn new(
        py: Python,
        mut num_parallel: i32,
        logfile: std::path::PathBuf,
        failures_allowed: u32,
        newkeyfn: Option<PyObject>,
        rerun_failures: bool,
    ) -> PyResult<PyExecGraph> {
        if num_parallel < 0 {
            num_parallel = num_cpus::get() as i32 + 2;
        }

        let mut log = LogFile::new(&logfile).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let key = match log.workflow_key() {
            Some(key) => key,
            None => match newkeyfn {
                Some(newkeyfn) => newkeyfn.call(py, (), None)?.extract(py)?,
                None => "default-key-value".to_owned(),
            },
        };
        debug!("Writing new log header key={}", key);
        log.write(LogEntry::new_header(&key)?)?;

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

    /// Get the number of tasks in the graph
    fn ntasks(&self) -> usize {
        self.g.ntasks()
    }

    /// Get the runcount that should be used for a task with this key.
    #[tracing::instrument(skip(self))]
    fn logfile_runcount(&self, key: &str) -> u32 {
        match self.g.logfile.runcount(key) {
            // the task is new and has never been executed before.
            // obviously this calls for a runcount of zero.
            None => 0,
            // during the last round, the task was added to the task graph,
            // became ready, but was not started. so no directory would have
            // been created, and we can reuse the prior run count.
            Some(logfile2::RuncountStatus::Ready(r)) => r,
            // the task was previously started, but not finished. this shouldn't
            // happen, because we should create a "fake" finished record with a
            // fake timeout_status, but maybe we got sigkilled or something. anyways,
            // we're going to need a new directory. this is basically like a failure.
            Some(logfile2::RuncountStatus::Started(r)) => {
                // log::error!("task has a started record but no finished record?");
                r + 1
            }
            // the task previously finished successfully. we reuse the old run count
            // because we're not going to actually run it again -- this lets us refer
            // to the assets in the correct directory.
            Some(logfile2::RuncountStatus::Finished(r, status)) if status == 0 => r,
            // new directory for the next run, as above.
            Some(logfile2::RuncountStatus::Finished(r, _status)) => r + 1,
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
    ///      Note: the key may not contain any tab characters.
    ///   3. Next, there's the list of dependencies, identified by integer ids
    ///      that reference previous tasks. A task cannot depend on itself, and can only
    ///      depend on previous tasks tha have already been added. This enforces a DAG
    ///      structure.
    ///   4. Oh, actually there's one more thing: the display string. This is the
    ///      string associated with the command that will be printed to stdout when
    ///      we run the command. If not supplied, we'll just use the cmdline for these
    ///      purposes. But if the cmdline contains some boring wrapper scripts that you
    ///      want to hide from your users, this might make sense.
    ///   5. Then there's the concept of a "queue name". You can associate each
    ///      job with a resource (arbitrary string), like "gpu", and then it will be
    ///      restricted and only run on remote runners that identify themselves as having
    ///      that resource.
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
    #[args(
        dependencies = "vec![]",
        display = "None",
        features = "vec![]",
        stdin = "vec![]",
        preamble = "None",
        postamble = "None"
    )]
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all)]
    fn add_task(
        &mut self,
        cmdline: Vec<OsString>,
        key: String,
        dependencies: Vec<u32>,
        display: Option<String>,
        features: Vec<String>,
        stdin: Vec<u8>,
        preamble: Option<PyObject>,
        postamble: Option<PyObject>,
    ) -> PyResult<u32> {
        let runcount = self.logfile_runcount(&key as &str);
        let cmd = Cmd {
            cmdline,
            key,
            display,
            stdin,
            runcount,
            priority: 0,
            features: FeatureExpr::new(features),
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
    ///    remote_provisioner (Optional[str]): Path to a remote provisioning script.
    ///      If supplied, we call this script with the url of an emphemeral server
    ///      as the first argument, and it can launch processes that can connect back
    ///      to this ExecGraph instance's http server to run tasks. Note: this package
    ///      includes a binary called ``execgraph-remote`` which implemenets the HTTP
    ///      protocol to "check out" tasks from the server, run them, and report their
    ///      status back. You'll need to write a provisioner script that arranges for
    ///      these execgraph-remote processes to be executed remotely using whatever
    ///      job queuing system you have though.
    ///   remote_provisioner_arg2 (Optional[str]): If you have extra data that you
    ///     want to pass to the remote provisioner script, you can use this. If supplied
    ///     it'll be passed as the second argument to remote_provisioner. The first
    ///     argument will be the url.
    ///
    /// Returns:
    ///     num_failed (int): the number of tasks that failed. a failure is identified
    ///         when a task exits with a nonzero exit code.
    ///     execution_order (List[int]): the ids of the tasks that finished (success
    ///         or failure) in order of when they finished.
    ///
    #[args(
        target = "None",
        remote_provisioner = "None",
        remote_provisioner_arg2 = "None",
        local_capabilities = "vec![\"local\".to_string()]",
        remote_capabilities = "vec![]"
    )]
    #[tracing::instrument(skip_all)]
    fn execute(
        &mut self,
        py: Python,
        target: Option<u32>,
        remote_provisioner: Option<String>,
        remote_provisioner_arg2: Option<String>,
        local_capabilities: Vec<String>,
        remote_capabilities: Vec<Vec<String>>,
    ) -> PyResult<(u32, Vec<String>)> {
        // Create a new process group so that at shutdown time, we can send a
        // SIGTERM to this process group and kill of all child processes.
        unsafe {
            if libc::setpgid(libc::getpid(), libc::getpid()) != 0 {
                PyRuntimeError::new_err("Cannot setpgid");
            }
        }

        let x = match remote_provisioner {
            Some(cmd) => Some(RemoteProvisionerSpec {cmd: cmd, arg2: remote_provisioner_arg2 }),
            None => None
        };
        let c = RunnerCapabilities {
            local: local_capabilities,
            remote: remote_capabilities,
        };

        py.allow_threads(move || {
            let rt = Runtime::new().expect("Failed to build tokio runtime");
            rt.block_on(async {
                self.g
                    .execute(
                        target,
                        self.num_parallel,
                        self.failures_allowed,
                        self.rerun_failures,
                        x,
                        c
                    )
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

impl From<logfile2::LogfileError> for PyErr {
    fn from(err: logfile2::LogfileError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

extern "C" fn test_callback(_ctx: *const std::ffi::c_void) -> i32 {
    println!("Hello from test_callback");
    0
}

#[pyfunction]
fn test_make_capsule(py: Python) -> PyResult<PyObject> {
    const CAPSULE_NAME: &[u8] = b"Execgraph::Capsule\0";
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
    let mut log = logfile2::LogFileReadOnly::open(path)?;
    let value = match &mode as &str {
        "current" => log.read_current()?,
        "all" => log.read()?,
        _ => return Err(PyValueError::new_err("Unrecognized mode")),
    };
    Ok(pythonize::pythonize(py, &value)?)
}

#[pyfunction]
#[tracing::instrument(skip(value))]
fn write_logfile(path: std::path::PathBuf, value: &PyAny) -> PyResult<()> {
    let mut log = logfile2::LogFile::new(path)?;
    let v: Vec<LogEntry> = pythonize::depythonize(value)?;
    for item in v {
        log.write(item)?;
    }
    log.flush()?;
    Ok(())
}

#[pymodule]
pub fn execgraph(_py: Python, m: &PyModule) -> PyResult<()> {
    tracing_subscriber::fmt::init();

    m.add_class::<PyExecGraph>()?;
    m.add_function(wrap_pyfunction!(test_make_capsule, m)?)?;
    m.add_function(wrap_pyfunction!(load_logfile, m)?)?;
    m.add_function(wrap_pyfunction!(write_logfile, m)?)?;
    Ok(())
}
