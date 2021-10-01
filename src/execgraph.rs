use crate::{
    graphtheory::transitive_closure_dag,
    logfile::LogfileSnapshot,
    server::{router, State as ServerState},
    sync::{ReadyTracker, StatusUpdater},
};
use anyhow::{anyhow, Result};
use async_channel::Receiver;
use derivative::Derivative;
use futures::future::join_all;
use hyper::Server;
use petgraph::prelude::*;
use pyo3::AsPyPointer;
use routerify::RouterService;
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    net::SocketAddr,
    os::unix::process::ExitStatusExt,
    process::Stdio,
    sync::Arc,
};
use tokio::{io::AsyncWriteExt, process::Command, sync::oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Derivative)]
#[derivative(Debug, Clone, Default, PartialEq, Hash)]
pub struct Cmd {
    pub cmdline: Vec<OsString>,
    pub key: String,
    pub display: Option<String>,
    pub queuename: Option<String>,
    pub runcount: u32,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub stdin: Vec<u8>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub preamble: Option<Capsule>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub postamble: Option<Capsule>,
}

#[derive(Debug, Clone)]
pub struct Capsule {
    capsule: pyo3::PyObject,
}

impl Capsule {
    pub fn new(capsule: pyo3::PyObject) -> Self {
        Capsule { capsule }
    }

    fn call(&self) -> Result<i32> {
        const CAPSULE_NAME: &'static [u8] = b"Execgraph::Capsule\0";
        unsafe {
            let pyobj = self.capsule.as_ptr();
            let capsule_name_ptr: *const std::os::raw::c_char =
                std::mem::transmute(CAPSULE_NAME.as_ptr());
            if (pyo3::ffi::PyCapsule_CheckExact(pyobj) > 0)
                && (pyo3::ffi::PyCapsule_IsValid(pyobj, capsule_name_ptr) > 0)
            {
                let ptr = pyo3::ffi::PyCapsule_GetPointer(pyobj, capsule_name_ptr);
                let ctx = pyo3::ffi::PyCapsule_GetContext(pyobj);
                assert!(!ptr.is_null()); // guarenteed by https://docs.python.org/3/c-api/capsule.html#c.PyCapsule_IsValid
                let f = std::mem::transmute::<
                    *mut std::ffi::c_void,
                    fn(*const std::ffi::c_void) -> i32,
                >(ptr);
                Ok(f(ctx))
            } else {
                Err(anyhow!("Not a capsule!"))
            }
        }
    }
}

impl Cmd {
    pub fn display(&self) -> String {
        match &self.display {
            Some(s) => s.to_string(),
            None => self
                .cmdline
                .iter()
                .map(|x| x.clone().into_string().unwrap())
                .collect::<Vec<String>>()
                .join(" ")
                .replace("\\\n", " ")
                .replace("\t", "\\t"),
        }
    }

    pub fn call_preamble(&self) {
        match &self.preamble {
            Some(preamble) => {
                match preamble.call() {
                    Ok(i) if i != 0 => {
                        panic!("Preamble failed with error code {}", i);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Invalid preamble in cmd `{}`: {}", self.display(), e);
                    }
                };
            }
            None => {}
        };
    }

    pub fn call_postamble(&self) {
        match &self.postamble {
            Some(postamble) => {
                match postamble.call() {
                    Ok(i) if i != 0 => {
                        panic!("Postamble failed with error code {}", i);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Invalid postamble in cmd `{}`: {}", self.display(), e);
                    }
                };
            }
            None => {}
        };
    }
}

pub struct ExecGraph {
    deps: Graph<Cmd, (), Directed>,
    key_to_nodeid: HashMap<String, NodeIndex<u32>>,
    logfile: String,
    logfile_snapshot: LogfileSnapshot,
    completed: HashSet<String>,
}

impl ExecGraph {
    pub fn new(logfile: String) -> Result<ExecGraph> {
        // Load prior the successful tasks from logfile.
        let logfile_snapshot = LogfileSnapshot::new(&logfile)?;

        Ok(ExecGraph {
            deps: Graph::new(),
            key_to_nodeid: HashMap::new(),
            logfile: logfile,
            logfile_snapshot,
            completed: HashSet::new(),
        })
    }

    pub fn logfile_runcount(&self, key: &str) -> i32 {
        self.logfile_snapshot
            .runcounts
            .get(key)
            .map(|x| x + 1)
            .or(Some(0))
            .unwrap()
    }

    pub fn ntasks(&self) -> usize {
        self.deps.raw_nodes().len()
    }

    pub fn get_task(&self, id: u32) -> Option<Cmd> {
        self.deps.node_weight(NodeIndex::from(id)).cloned()
    }

    pub fn task_keys(&self) -> Vec<String> {
        self.deps
            .raw_nodes()
            .iter()
            .map(|n| n.weight.key.clone())
            .collect()
    }

    pub fn add_task(&mut self, cmd: Cmd, dependencies: Vec<u32>) -> Result<u32> {
        if !cmd.key.is_empty() {
            match self.key_to_nodeid.get(&cmd.key) {
                Some(index) => return Ok(index.index() as u32),
                None => {}
            }
        }

        let key = cmd.key.clone();
        let new_node = self.deps.add_node(cmd);
        for dep in dependencies.iter().map(|&i| NodeIndex::from(i)) {
            if dep == new_node || self.deps.node_weight(dep).is_none() {
                self.deps.remove_node(new_node);
                return Err(anyhow!("Invalid dependency index"));
            }
            self.deps.add_edge(dep, new_node, ());
        }

        self.key_to_nodeid.insert(key, new_node);
        Ok(new_node.index() as u32)
    }

    fn get_subgraph(
        &mut self,
        target: Option<u32>,
        rerun_failures: bool,
    ) -> Result<Graph<(Cmd, NodeIndex), ()>> {
        // Get the subgraph containing all cmds in the transitive closure of target.
        // these are all the commands we need to resolve first. if target is none, then
        // just get the whole subraph (with an unnecessary copy unfortunately)
        let subgraph = match target {
            Some(target_index) => {
                let mut reversed = self.deps.clone();
                reversed.reverse();
                let tc = transitive_closure_dag(&reversed)?;

                let mut relevant = tc
                    .edges_directed(NodeIndex::from(target_index), Direction::Outgoing)
                    .map(|e| e.target())
                    .collect::<HashSet<NodeIndex>>();
                relevant.insert(NodeIndex::from(target_index));

                self.deps.filter_map(
                    |n, w| match relevant.contains(&n) {
                        true => Some((w, n)),
                        false => None,
                    },
                    |_e, _w| Some(()),
                )
            }
            None => self.deps.filter_map(|n, w| Some((w, n)), |_e, _w| Some(())),
        };

        let mut reused_old_keys = HashMap::new();

        // Now let's remove all edges from the dependency graph if the source has already finished
        // or if we've already exeuted the task in a previous call to execute
        let mut filtered_subgraph = subgraph.filter_map(
            |_n, w| {
                if self.completed.contains(&w.0.key) {
                    // if we already ran this command within this process, don't record it
                    // in reused_old_keys. that's only for stuff that was run in a previous
                    // session
                    return None; // returning none excludes it from filtered_subgraph
                }
                let has_success = self.logfile_snapshot.has_success(&w.0.key);
                if has_success {
                    reused_old_keys.insert(
                        w.0.key.clone(),
                        self.logfile_snapshot.get_record(&w.0.key).unwrap(),
                    );
                    return None;
                }

                return Some((w.0.clone(), w.1)); // returning some keeps it in filtered_subgraph
            },
            |_e, &w| Some(w),
        );

        if !rerun_failures {
            let tc = transitive_closure_dag(&filtered_subgraph)?;
            let failures = tc
                .node_weights()
                .filter_map(|n| match self.logfile_snapshot.has_failure(&n.0.key) {
                    true => Some(n.1),
                    false => None,
                })
                .collect::<HashSet<NodeIndex>>();

            filtered_subgraph = filtered_subgraph.filter_map(
                |n, w| {
                    if failures.contains(&w.1) {
                        reused_old_keys.insert(
                            w.0.key.clone(),
                            self.logfile_snapshot.get_record(&w.0.key).unwrap(),
                        );
                        return None;
                    }
                    for e in tc.edges_directed(n, Direction::Incoming) {
                        if failures.contains(&tc.node_weight(e.source()).unwrap().1) {
                            return None;
                        }
                    }

                    Some(w.clone())
                },
                |_e, &w| Some(w),
            );
            //return Ok(filtered_subgraph2);
        }

        // See test_copy_reused_keys_logfile.
        // This is not elegant at all. The point is that when a job is run in one execgraph invocation and
        // then requested in a new invocation but not rerun because we detected that it had already been
        // run, we want to copy the entries in the logfile. this ensures that the last entries in the logfile
        // since the final blank line describe the full set of jobs that the last invocation dependend on.
        // this way, when reading the log file, it's possible to figure out what jobs are garbage and what
        // jobs are still "in use".
        crate::logfile::copy_reused_keys(&self.logfile, &reused_old_keys)?;
        for key in reused_old_keys.keys() {
            self.logfile_snapshot.remove(key);
            self.completed.insert(key.to_owned());
        }

        Ok(filtered_subgraph)
    }

    pub async fn execute(
        &mut self,
        target: Option<u32>,
        num_parallel: u32,
        failures_allowed: u32,
        rerun_failures: bool,
        provisioner: Option<String>,
        provisioner_arg2: Option<String>,
    ) -> Result<(u32, Vec<u32>)> {
        let subgraph = Arc::new(self.get_subgraph(target, rerun_failures)?);
        if subgraph.raw_nodes().is_empty() {
            return Ok((0, vec![]));
        }

        let token = CancellationToken::new();
        let (mut servicer, tasks_ready, status_updater) = ReadyTracker::new(
            subgraph.clone(),
            self.completed.len() as u32,
            failures_allowed,
        );

        // Run local processes
        let handles = (0..num_parallel)
            .map(|_| {
                let token = token.clone();
                tokio::spawn(run_local_process_loop(
                    subgraph.clone(),
                    tasks_ready.get(&None).expect("No null queue").clone(),
                    status_updater.clone(),
                    token,
                ))
            })
            .collect::<Vec<tokio::task::JoinHandle<_>>>();

        //
        // Create the server that can manage farming off tasks to remote machines over http
        //
        let (provisioner_exited_tx, provisioner_exited_rx) = oneshot::channel();

        match provisioner {
            Some(provisioner) => {
                let subgraph1 = subgraph.clone();
                let tasks_ready1 = tasks_ready.clone();
                let status_updater1 = status_updater.clone();
                let p2 = provisioner_arg2.clone();
                let token1 = token.clone();
                let token2 = token.clone();
                let token3 = token.clone();

                tokio::spawn(async move {
                    let state = ServerState::new(subgraph1, tasks_ready1, status_updater1);
                    let router = router(state);
                    let service = RouterService::new(router).unwrap();
                    let addr = SocketAddr::from(([0, 0, 0, 0], 0));
                    let server = Server::bind(&addr).serve(service);
                    let bound_addr = server.local_addr();
                    let graceful = server.with_graceful_shutdown(token1.cancelled());
                    let (server_start_tx, server_start_rx) = oneshot::channel();

                    tokio::spawn(async move {
                        server_start_rx.await.expect("failed to recv");
                        match spawn_and_wait_for_provisioner(&provisioner, p2, bound_addr, token2).await {
                            Err(e) => {log::error!("{}", e)},
                            Ok(_) => {},
                        };
                        token3.cancel();
                        provisioner_exited_tx
                            .send(())
                            .expect("could not send to channel");
                    });

                    server_start_tx.send(()).expect("failed to send");
                    if let Err(err) = graceful.await {
                        log::error!("Server error: {}", err);
                    }
                });
            }
            None => {
                provisioner_exited_tx.send(()).expect("Could not send");
            }
        };

        // run the background service that will send commands to the ready channel to be picked up by the
        // tasks spawned above. background_serve should wait for sigint and exit when it hits a sigintt too.
        servicer
            .background_serve(&self.logfile, token.clone())
            .await
            .unwrap();
        token.cancel();
        join_all(handles).await;
        servicer.drain(&self.logfile).unwrap();

        provisioner_exited_rx
            .await
            .expect("failed to close provisioner");

        let n_failed = servicer.n_failed;
        // get indices of the tasks we executed, mapped back from the subgraph node ids
        // to the node ids in the deps graph
        let completed: Vec<u32> = servicer
            .finished_order
            .iter()
            .map(|&n| (subgraph[n].1.index() as u32))
            .collect();

        for n in servicer.finished_order.iter() {
            self.completed.insert(subgraph[*n].0.key.clone());
        }

        Ok((n_failed, completed))
    }
}

async fn run_local_process_loop(
    subgraph: Arc<DiGraph<(Cmd, NodeIndex), ()>>,
    tasks_ready: Receiver<NodeIndex>,
    status_updater: StatusUpdater,
    token: CancellationToken,
) -> Result<()> {
    while let Ok(subgraph_node_id) = tasks_ready.recv().await {
        let (cmd, _) = subgraph
            .node_weight(subgraph_node_id)
            .unwrap_or_else(|| panic!("failed to get node {:#?}", subgraph_node_id));

        cmd.call_preamble();
        let skip_execution_debugging_race_conditions = cmd.cmdline.is_empty();
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let (hostpid, status, stdout, stderr) = if skip_execution_debugging_race_conditions {
            let pid = 0;
            let fake_status = 0;
            let hostpid = format!("{}:{}", hostname, pid);
            status_updater
                .send_started(subgraph_node_id, &cmd, &hostpid)
                .await;

            (hostpid, fake_status, "".to_owned(), "".to_owned())
        } else {
            let maybe_child = Command::new(&cmd.cmdline[0])
                .args(&cmd.cmdline[1..])
                .kill_on_drop(true)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();
            let mut child = match maybe_child {
                Ok(child) => child,
                Err(_) => {
                    log::error!("failed to execute {:#?}", &cmd.cmdline[0]);
                    status_updater
                        .send_started(subgraph_node_id, &cmd, "")
                        .await;
                    status_updater
                        .send_finished(
                            subgraph_node_id,
                            &cmd,
                            "",
                            127,
                            "".to_owned(),
                            format!("No such command: {:#?}", &cmd.cmdline[0]),
                        )
                        .await;
                    continue;
                }
            };

            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(&cmd.stdin).await?;
            drop(stdin);

            let pid = child
                .id()
                .expect("hasn't been polled yet, so this id should exist");
            let hostpid = format!("{}:{}", hostname, pid);
            status_updater
                .send_started(subgraph_node_id, &cmd, &hostpid)
                .await;

            // Like `let output = child.await.expect("sh wasn't running");`, except
            // that we also wait for the cancellation token at the same time.
            let output = tokio::select! {
                _ = token.cancelled() => {
                    return Err(anyhow!("cancelled"));
                }
                wait_with_output = child.wait_with_output() => {
                    wait_with_output.expect("sh wasn't running")
                }
            };

            let code = match output.status.code() {
                Some(code) => code,
                None => output.status.signal().expect("No exit code and no signal?"),
            };
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (hostpid, code, stdout, stderr)
        };
        status_updater
            .send_finished(subgraph_node_id, &cmd, &hostpid, status, stdout, stderr)
            .await;
    }

    Ok(())
}

async fn spawn_and_wait_for_provisioner(
    provisioner: &str,
    arg2: Option<String>,
    bound_addr: SocketAddr,
    token: CancellationToken,
) -> Result<()> {
    let mut child = Command::new(provisioner)
        .arg(format!("http://{}", bound_addr))
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .spawn()?;
    let mut child_stdin = child.stdin.take().unwrap();

    let arg2_string = arg2.unwrap_or("".to_owned());
    let arg2_bytes = arg2_string.as_bytes();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(arg2_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(arg2_bytes);
    child_stdin.write_all(&buf).await?;
    child_stdin.flush().await?;


    tokio::select! {
        // if this process got a ctrl-c, then this token is cancelled
        _ = token.cancelled() => {

            drop(child_stdin); // drop stdin so that it knows to exit
            let duration = std::time::Duration::from_millis(1000);
            if tokio::time::timeout(duration, child.wait()).await.is_err() {
                log::debug!("sending SIGKILL to provisioner");
                child.kill().await.expect("kill failed");
            }
        },

        result = child.wait() => {
            let status = result?;
            log::error!("provisioner exited with status={}", status);
        }
    }

    Ok(())
}
