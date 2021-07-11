mod execgraph;
mod graphtheory;
mod logfile;
mod sync;

use num_cpus;
use pyo3::exceptions::{PyIndexError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::runtime::Runtime;

use execgraph::{Cmd, ExecGraph};

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
///    num_parallel (Optional[int]): Maximum number of parallel processes
///      to run. If not supplied, we'll use 2 more than the number of CPU cores.
///    keyfile (str): The path to the log file.
#[pyclass(name = "ExecGraph")]
pub struct PyExecGraph {
    g: ExecGraph,
    num_parallel: usize,
}

#[pymethods]
impl PyExecGraph {
    #[new]
    #[args(num_parallel = 0)]
    fn new(mut num_parallel: usize, keyfile: String) -> PyResult<PyExecGraph> {
        if num_parallel == 0 {
            num_parallel = num_cpus::get() + 2;
        }
        Ok(PyExecGraph {
            g: ExecGraph::new(keyfile),
            num_parallel: num_parallel,
        })
    }

    /// Compute the indices of all nodes in the task graph for which that nodes `key`
    /// is a substring of `s`.
    ///
    /// This is ancillary to the coree purpose of this class, but it is useful for
    /// dependencies if you happen to structure your nodes/keys in a certain way.
    fn scan_keys(&self, s: String) -> Vec<u32> {
        self.g.scan_keys(&s)
    }

    /// Get the number of tasks in the graph
    fn ntasks(&self) -> usize {
        self.g.ntasks()
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

    /// Add a task to the graph.
    ///
    /// Each task is identified by a couple pieces of information:
    ///
    ///     1. First, there's the shell command to execute. This is supplied
    ///        as `cmdline`. It is interpreted with "sh -c", which is why it's
    ///        just a string, rather than a list of strings to directly execve
    ///     2. Second, there's `key`. The idea is that this is a unique identifier
    ///        for the command -- it could be the hash of the cmdline, or the hash
    ///        of the cmdline and all of its inputs, or something like that. (We don't
    ///        do it for you). When we execute the command, we'll append the key to
    ///        a log file. That way when we rerun the graph at some later time, we'll
    ///        be able to skip executing any commands that have already been executed.
    ///        Note: the key may not contain any tab characters.
    ///     3. Finally, there's the list of dependencies, identified by integer ids
    ///        that reference previous tasks. A task cannot depend on itself, and can only
    ///        depend on previous tasks tha have already been added. This enforces a DAG
    ///        structure.
    ///     4. Oh, actually there's one more thing: the display string. This is the
    ///        string associated with the command that will be printed to stdout when
    ///        we run the command. If not supplied, we'll just use the cmdline for these
    ///        purposes. But if the cmdline contains some boring wrapper scripts that you
    ///        want to hide from your users, this might make sense.
    ///
    /// Notes:
    ///   If key == "", then the task will never be skipped (i.e. it will always be
    ///   considered out of date).
    ///
    /// Args:
    ///     cmdline (str): command to execute
    ///     key (str): unique identifier
    ///     dependencies (List[int], default=[]): dependencies for this task
    /// Returns:
    ///     taskid (int): integer id of this task
    #[args(display = "None", dependencies = "vec![]")]
    fn add_task(
        &mut self,
        cmdline: String,
        key: String,
        dependencies: Vec<u32>,
        display: Option<String>,
    ) -> PyResult<u32> {
        let cmd = Cmd {
            cmdline: cmdline,
            key: key,
            display: display,
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
    ///     target (Optional[int]): if you'd like to particularly execute up to a single
    ///         target, you can do this. in this case we'll only execute the tasks that
    ///         are required for this target. if not supplied we'll try to execute the
    ///         whole graph.
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
    #[args(target = "None")]
    fn execute(&mut self, target: Option<u32>) -> PyResult<(u32, Vec<u32>)> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async { self.g.execute(target, self.num_parallel).await })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[pymodule]
pub fn _execgraph(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyExecGraph>()?;
    Ok(())
}
