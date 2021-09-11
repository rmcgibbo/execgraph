mod execgraph;
mod graphtheory;
pub mod httpinterface;
mod logfile;
mod parser;
mod server;
pub mod sync;
mod unsafecode;
use std::{ffi::OsString, io::BufRead};

use pyo3::{
    exceptions::{PyIndexError, PyRuntimeError, PySyntaxError, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyTuple},
};
use std::io::Write;
use tokio::runtime::Runtime;

use crate::execgraph::{Cmd, ExecGraph};

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
///    keyfile (str): The path to the log file.
///    failures_allowed (int): keep going until N jobs fail (0 means infinity)
///      [default=1].
///
#[pyclass(name = "ExecGraph")]
pub struct PyExecGraph {
    g: ExecGraph,
    num_parallel: u32,
    failures_allowed: u32,
    key: String,
}

#[pymethods]
impl PyExecGraph {
    #[new]
    #[args(num_parallel = -1, failures_allowed = 1, newkeyfn = "None")]
    fn new(
        py: Python,
        mut num_parallel: i32,
        keyfile: String,
        failures_allowed: u32,
        newkeyfn: Option<PyObject>,
    ) -> PyResult<PyExecGraph> {
        if num_parallel < 0 {
            num_parallel = num_cpus::get() as i32 + 2;
        }

        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&keyfile)?;

        let key = match f.metadata()?.len() {
            // If we're at the start of the file, that means we just opened it.
            // So lets write the header.
            0 => {
                let key = match newkeyfn {
                    Some(newkeyfn) => newkeyfn.call(py, (), None)?.extract(py)?,
                    None => "default-key-value".to_owned(),
                };
                if !regex::Regex::new(r"^\w+").unwrap().is_match(&key) {
                    return Err(PyValueError::new_err(format!("Invalid key: {}", key)));
                }
                writeln!(f, "wrk v=2 key={}", key)?;
                key
            }
            _ => {
                // If we're not at the start of the file, we need to read the header
                // and check that it's what we expect
                let r = std::io::BufReader::new(&f);
                let line = r
                    .lines()
                    .next()
                    .ok_or_else(|| PyValueError::new_err("Unable to read file"))??;
                let parts: Vec<&str> = line.split(" ").collect();
                if !(parts.len() == 3
                    && parts[0] == "wrk"
                    && parts[1] == "v=2"
                    && parts[2].starts_with("key="))
                {
                    return Err(PyValueError::new_err(format!("Invalid header: {}", line)));
                }
                let key = parts[2].strip_prefix("key=").unwrap().to_string();
                key
            }
        };

        writeln!(f)?;
        f.flush()?;

        Ok(PyExecGraph {
            g: ExecGraph::new(keyfile).map_err(|e| PyValueError::new_err(e.to_string()))?,
            num_parallel: num_parallel as u32,
            failures_allowed: (if failures_allowed == 0 {
                u32::MAX
            } else {
                failures_allowed
            }),
            key,
        })
    }

    /// Get the number of tasks in the graph
    fn ntasks(&self) -> usize {
        self.g.ntasks()
    }

    fn key(&self) -> String {
        return self.key.clone();
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
        queuename = "None",
        stdin = "vec![]",
        preamble = "None",
        postamble = "None"
    )]
    fn add_task(
        &mut self,
        cmdline: Vec<OsString>,
        key: String,
        dependencies: Vec<u32>,
        display: Option<String>,
        queuename: Option<String>,
        stdin: Vec<u8>,
        preamble: Option<PyObject>,
        postamble: Option<PyObject>,
    ) -> PyResult<u32> {
        let cmd = Cmd {
            cmdline,
            key,
            display,
            queuename,
            stdin,
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
    /// Notes:
    ///     We currently do not do output buffering, so stdout goes directly to the
    ///     terminal, including the bad behavior of overlapping streams for stout of
    ///     commands executed in parallel.
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
        remote_provisioner_arg2 = "None"
    )]
    fn execute(
        &mut self,
        py: Python,
        target: Option<u32>,
        remote_provisioner: Option<String>,
        remote_provisioner_arg2: Option<String>,
    ) -> PyResult<(u32, Vec<u32>)> {
        // Create a new process group so that at shutdown time, we can send a
        // SIGTERM to this process group annd kill of all child processes.
        nix::unistd::setpgid(nix::unistd::Pid::this(), nix::unistd::Pid::this())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        py.allow_threads(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                self.g
                    .execute(
                        target,
                        self.num_parallel,
                        self.failures_allowed,
                        remote_provisioner,
                        remote_provisioner_arg2,
                    )
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}
#[pyfunction]
fn parse_fstringish<'a>(
    py: Python<'a>,
    input: &str,
    filename: &str,
    line_offset: u32,
) -> PyResult<(&'a PyAny, Vec<String>)> {
    let value = crate::parser::parse_fstringish(input).map_err(|e| {
        let spans = match e {
            nom::Err::Error(nom::error::VerboseError { errors: spans }) => spans,
            nom::Err::Failure(nom::error::VerboseError { errors: spans }) => spans,
            nom::Err::Incomplete(_) => {
                panic!("Unknown error modality")
            }
        };
        let span = spans[0].0;

        PySyntaxError::new_err((
            "f-string: invalid syntax",
            (
                filename.to_string(),
                line_offset + span.location_line(),
                span.get_utf8_column(),
                input
                    .split("\n")
                    .nth((span.location_line() - 1) as usize)
                    .unwrap()
                    .to_string(),
            ),
        ))
    })?;

    let ast = py.import("ast")?;
    let ast_walk = ast.getattr("walk")?;
    let ast_parse = ast.getattr("parse")?;
    let ast_constant = ast.getattr("Constant")?;
    let ast_formatted_value = ast.getattr("FormattedValue")?;

    let body = value
        .body
        .iter()
        .map(|s| match s {
            crate::parser::JoinedStringPart::Constant(span) => {
                let mut end_lineno = span.location_line();
                let mut end_col_offset = span.get_utf8_column();
                for chr in span.fragment().chars() {
                    if chr == '\n' {
                        end_lineno += 1;
                        end_col_offset = 1;
                    } else {
                        end_col_offset += 1;
                    }
                }

                let kwargs = vec![
                    ("lineno", (span.location_line() + line_offset) as usize),
                    ("end_lineno", (end_lineno + line_offset) as usize),
                    ("col_offset", span.get_utf8_column() - 1),
                    ("end_col_offset", end_col_offset - 1),
                ]
                .into_py_dict(py);
                ast_constant.call((*span.fragment(),), Some(kwargs))
            }
            crate::parser::JoinedStringPart::Expression(span) => {
                // parse the expression with ast.parse
                let expr = ast_parse
                    .call1((*span.fragment(), filename, "eval"))
                    .map_err(|e| {
                        let get_col_offset = || {
                            e.pvalue(py)
                                .getattr("args")?
                                .get_item(1)?
                                .get_item(2)?
                                .extract::<usize>()
                        };
                        let col_offset = match get_col_offset() {
                            Ok(o) => o,
                            Err(e) => {
                                return e;
                            }
                        };
                        PySyntaxError::new_err((
                            "f-string: invalid syntax",
                            (
                                filename.to_string(),
                                line_offset + span.location_line(),
                                span.get_utf8_column() + col_offset - 1,
                                input
                                    .split("\n")
                                    .nth((span.location_line() - 1) as usize)
                                    .unwrap()
                                    .to_string(),
                            ),
                        ))
                    })?
                    .getattr("body")?;

                // update the lineno and col_offset information for the body of the expression, since
                // ast.parse doesn't know what line/col in the file we were on
                for maybe_item in ast_walk.call1((expr,))?.iter()? {
                    let item = maybe_item?;

                    if let Ok(lineno) = item.getattr("lineno") {
                        item.setattr(
                            "lineno",
                            span.location_line() - 1 + lineno.extract::<u32>()? + line_offset,
                        )?;
                    }
                    if let Ok(end_lineno) = item.getattr("end_lineno") {
                        item.setattr(
                            "end_lineno",
                            span.location_line() - 1 + end_lineno.extract::<u32>()? + line_offset,
                        )?;
                    }
                    if let Ok(col_offset) = item.getattr("col_offset") {
                        item.setattr(
                            "col_offset",
                            span.get_utf8_column() - 1 + col_offset.extract::<usize>()?,
                        )?;
                    }
                    if let Ok(end_col_offset) = item.getattr("end_col_offset") {
                        if span.fragment().find("\n").is_none() {
                            item.setattr(
                                "end_col_offset",
                                span.get_utf8_column() - 1 + end_col_offset.extract::<usize>()?,
                            )?;
                        }
                    }
                }

                let kwargs = vec![
                    ("conversion", -1),
                    ("lineno", line_offset as i32 + span.location_line() as i32),
                    ("col_offset", span.get_utf8_column() as i32),
                    (
                        "end_lineno",
                        line_offset as i32
                            + input.matches("\n").count() as i32
                            + span.location_line() as i32,
                    ),
                    (
                        "end_col_offset",
                        (span.fragment().len() + span.get_utf8_column()) as i32,
                    ),
                ]
                .into_py_dict(py);
                ast_formatted_value.call((expr,), Some(kwargs))
            }
        })
        .collect::<PyResult<Vec<&PyAny>>>()?;

    // these end_col_offset values are not quite right for multi-line strings, but it doesn't really matter.
    let kwargs = vec![
        ("lineno", 1 + line_offset),
        (
            "end_lineno",
            1 + input.matches("\n").count() as u32 + line_offset,
        ),
        ("col_offset", 0),
        ("end_col_offset", input.len() as u32),
    ]
    .into_py_dict(py);
    let joinedstring = ast.getattr("JoinedStr")?.call((body,), Some(kwargs))?;
    let expr = ast
        .getattr("Expression")?
        .call((joinedstring,), Some(kwargs))?;

    Ok((
        expr,
        value
            .preamble_fragments()
            .iter()
            .map(|&s| s.to_string())
            .collect(),
    ))
}

const CAPSULE_NAME: &'static [u8] = b"Execgraph::Capsule\0";

extern "C" fn test_callback(_ctx: *const std::ffi::c_void) -> i32 {
    println!("Hello from test_callback");
    return 0;
}

#[pyfunction]
fn test_make_capsule<'a>(py: Python<'a>) -> PyResult<PyObject> {
    let obj = unsafe {
        let name: *const std::os::raw::c_char = std::mem::transmute(CAPSULE_NAME.as_ptr());
        let capsule =
            pyo3::ffi::PyCapsule_New(std::mem::transmute(test_callback as *const ()), name, None);
        PyObject::from_owned_ptr(py, capsule)
    };

    Ok(obj)
}

#[pymodule]
pub fn execgraph(_py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();
    m.add_class::<PyExecGraph>()?;
    m.add_function(wrap_pyfunction!(parse_fstringish, m)?)?;
    m.add_function(wrap_pyfunction!(test_make_capsule, m)?)?;
    Ok(())
}
