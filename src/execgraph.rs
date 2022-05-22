use crate::{
    graphtheory::transitive_closure_dag,
    localrunner::{run_local_process_loop, LocalQueueType},
    logfile2::{LogEntry, LogFile},
    server::{router, State as ServerState},
    sync::new_ready_tracker,
    utils::{CancellationState, CancellationToken},
};
use anyhow::{anyhow, Result};
use bitvec::array::BitArray;
use derivative::Derivative;
use futures::future::join_all;
use hyper::Server;
use petgraph::prelude::*;
use pyo3::AsPyPointer;
use routerify::RouterService;
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    ffi::OsString,
    net::SocketAddr,
    process::Stdio,
    sync::Arc,
};
use tokio::{io::AsyncWriteExt, process::Command, sync::oneshot};
use tracing::{debug, error, trace};

// TODO: remove clone?
#[derive(Derivative)]
#[derivative(Debug, Clone, Default, PartialEq, Hash)]
pub struct Cmd {
    pub cmdline: Vec<OsString>,
    pub key: String,
    pub display: Option<String>,
    pub affinity: BitArray<u64>,

    pub runcount: u32,
    pub priority: u32,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub env: Vec<(OsString, OsString)>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub preamble: Option<Capsule>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub postamble: Option<Capsule>,
}

#[derive(Debug)]
pub struct RemoteProvisionerSpec {
    pub cmd: String,
    pub arg2: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Capsule {
    capsule: pyo3::PyObject,
}

impl Capsule {
    pub fn new(capsule: pyo3::PyObject) -> Self {
        Capsule { capsule }
    }

    #[tracing::instrument]
    fn call(&self) -> Result<i32> {
        const CAPSULE_NAME: &[u8] = b"Execgraph::Capsule\0";
        let capsule_name_ptr = CAPSULE_NAME.as_ptr() as *const i8;

        unsafe {
            let pyobj = self.capsule.as_ptr();
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
                .map(|x| x.clone().into_string().expect("cmdline must be utf-8"))
                .collect::<Vec<String>>()
                .join(" ")
                .replace("\\\n", " ")
                .replace("\t", "\\t"),
        }
    }

    #[tracing::instrument]
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

    #[tracing::instrument]
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
    pub logfile: LogFile,
    completed: HashSet<String>,
}

impl ExecGraph {
    #[tracing::instrument(skip_all)]
    pub fn new(logfile: LogFile) -> ExecGraph {
        ExecGraph {
            deps: Graph::new(),
            key_to_nodeid: HashMap::new(),
            logfile,
            completed: HashSet::new(),
        }
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

    #[tracing::instrument(skip(self))]
    pub fn add_task(&mut self, cmd: Cmd, dependencies: Vec<u32>) -> Result<u32> {
        if !cmd.key.is_empty() {
            if let Some(index) = self.key_to_nodeid.get(&cmd.key) {
                return Ok(index.index().try_into().unwrap());
            }
        }
        if cmd.cmdline.is_empty() {
            return Err(anyhow!("cmd={:?}: cmdline size == 0", cmd));
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
        trace!("Added command to task graph");
        Ok(new_node.index().try_into().unwrap())
    }

    #[tracing::instrument(skip(self))]
    pub async fn execute(
        &mut self,
        target: Option<u32>,
        num_parallel: u32,
        failures_allowed: u32,
        rerun_failures: bool,
        provisioner: Option<RemoteProvisionerSpec>,
    ) -> Result<(u32, Vec<String>)> {
        fn extend_graph_lifetime<'a>(
            g: Arc<DiGraph<&'a Cmd, ()>>,
        ) -> Arc<DiGraph<&'static Cmd, ()>> {
            unsafe { std::mem::transmute::<_, Arc<DiGraph<&'static Cmd, ()>>>(g) }
        }
        fn transmute_lifetime<'a, T>(x: &'a T) -> &'static T {
            unsafe { std::mem::transmute::<_, _>(x) }
        }

        let count_offset = self.completed.len().try_into().unwrap();
        let subgraph = Arc::new(get_subgraph(
            &mut self.deps,
            &mut self.completed,
            &mut self.logfile,
            target,
            rerun_failures,
        )?);
        if subgraph.raw_nodes().is_empty() {
            trace!("Short-circuiting execution because subgraph N=0.");
            return Ok((0, vec![]));
        }
        trace!(
            "Executing dependency graph of N={} tasks",
            subgraph.node_count()
        );

        let token = CancellationToken::new();
        let (mut servicer, tracker) = new_ready_tracker(
            subgraph.clone(),
            &mut self.logfile,
            count_offset,
            failures_allowed,
        );

        // Run local processes
        trace!("Spawning {} local process loops", num_parallel);
        let handles = {
            let local = (0..num_parallel).map(|_| {
                let subgraph = extend_graph_lifetime(subgraph.clone());
                let token = token.clone();
                tokio::spawn(run_local_process_loop(
                    subgraph,
                    transmute_lifetime(&tracker),
                    token,
                    LocalQueueType::NormalLocalQueue, // 0 for local queue
                ))
            });
            if num_parallel > 0 {
                local
                    .chain(std::iter::once({
                        let subgraph = extend_graph_lifetime(subgraph.clone());
                        let token = token.clone();
                        tokio::spawn(run_local_process_loop(
                            subgraph,
                            transmute_lifetime(&tracker),
                            token,
                            LocalQueueType::ConsoleQueue, // 1 for console queue
                        ))
                    }))
                    .collect::<Vec<tokio::task::JoinHandle<_>>>()
            } else {
                local.collect::<Vec<tokio::task::JoinHandle<_>>>()
            }
        };

        //
        // Create the server that can manage farming off tasks to remote machines over http
        //
        let (provisioner_exited_tx, provisioner_exited_rx) = oneshot::channel();

        match provisioner {
            Some(provisioner) => {
                let subgraph = extend_graph_lifetime(subgraph.clone());
                // let p2 = provisioner_arg2.clone();
                let token1 = token.clone();
                let token2 = token.clone();
                let token3 = token.clone();
                let token4 = token.clone();

                tokio::spawn(async move {
                    let state = Arc::new(ServerState::new(
                        subgraph,
                        transmute_lifetime(&tracker),
                        token4,
                    ));
                    let state2 = state.clone();
                    let state3 = state.clone();
                    let router = router(state2);
                    let service = RouterService::new(router).expect("Failed to constuct Router");
                    let addr = SocketAddr::from(([0, 0, 0, 0], 0));
                    let server = Server::bind(&addr).serve(service);
                    let bound_addr = server.local_addr();
                    trace!("Bound server to {}", bound_addr);
                    let graceful = server.with_graceful_shutdown(token1.hard_cancelled());
                    let (server_start_tx, server_start_rx) = oneshot::channel();

                    tokio::spawn(async move {
                        server_start_rx.await.expect("failed to recv");
                        debug!("Spawning remote provisioner {}", provisioner.cmd);
                        if let Err(e) =
                            spawn_and_wait_for_provisioner(&provisioner, bound_addr, token2).await
                        {
                            error!("Provisioner failed: {}", e);
                        }
                        state3.join().await;
                        token3.cancel(CancellationState::HardCancelled);
                        debug!("Remote provisioner exited");
                        provisioner_exited_tx
                            .send(())
                            .expect("could not send to channel");
                    });

                    server_start_tx.send(()).expect("failed to send");
                    if let Err(err) = graceful.await {
                        log::error!("Server error: {}", err);
                    }
                    state.join().await;
                });
            }
            None => {
                trace!("Not spawning remote provisioner or server");
                provisioner_exited_tx.send(()).expect("Could not send");
            }
        };

        // run the background service that will send commands to the ready channel to be picked up by the
        // tasks spawned above. background_serve should wait for sigint and exit when it hits a sigintt too.
        servicer
            .background_serve(token.clone())
            .await
            .expect("background_serve failed");
        token.cancel(CancellationState::HardCancelled);
        join_all(handles).await;
        servicer.drain().expect("failed to drain queue");

        provisioner_exited_rx
            .await
            .expect("failed to close provisioner");

        let n_failed = servicer.n_failed;

        // // get indices of the tasks we executed, mapped back from the subgraph node ids
        // // to the node ids in the deps graph
        let completed: Vec<String> = servicer
            .finished_order
            .iter()
            .map(|&n| subgraph[n].key.clone())
            .collect();

        for item in completed.iter() {
            self.completed.insert(item.clone());
        }
        Ok((n_failed, completed))
    }
}

#[tracing::instrument(skip_all)]
async fn spawn_and_wait_for_provisioner(
    provisioner: &RemoteProvisionerSpec,
    bound_addr: SocketAddr,
    token: CancellationToken,
) -> Result<()> {
    let mut child = Command::new(&provisioner.cmd)
        .arg(format!("http://{}", bound_addr))
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .spawn()?;
    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to take stdin from child process"))?;

    let arg2_string = provisioner
        .arg2
        .as_ref()
        .map(|x| x as &str)
        .unwrap_or_else(|| "");
    let arg2_bytes = arg2_string.as_bytes();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(arg2_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(arg2_bytes);
    child_stdin.write_all(&buf).await?;
    child_stdin.flush().await?;

    tokio::select! {
        // if this process got a ctrl-c, then this token is cancelled
        _ = token.hard_cancelled() => {
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

#[tracing::instrument(skip(deps, completed, logfile))]
fn get_subgraph<'a, 'b: 'a>(
    deps: &'b mut DiGraph<Cmd, ()>,
    completed: &mut HashSet<String>,
    logfile: &mut LogFile,
    target: Option<u32>,
    rerun_failures: bool,
) -> Result<DiGraph<&'a Cmd, ()>> {
    // Compute the priority and just mutate the graph. Ugly, but it
    // keeps everything in the Cmd struct
    let priority = crate::graphtheory::blevel_dag(deps)?;
    for (i, p) in priority.iter().enumerate() {
        let i_u32: u32 = i.try_into().unwrap();
        deps[NodeIndex::from(i_u32)].priority = *p;
    }

    // Get the subgraph containing all cmds in the transitive closure of target.
    // these are all the commands we need to resolve first. if target is none, then
    // just get the whole subraph (with an unnecessary copy unfortunately)
    let subgraph = match target {
        Some(target_index) => {
            // TODO it sucks that we have to clone/reverse this.
            let mut reversed = deps.clone();
            reversed.reverse();
            let tc = transitive_closure_dag(&reversed)?;

            let mut relevant = tc
                .edges_directed(NodeIndex::from(target_index), Direction::Outgoing)
                .map(|e| e.target())
                .collect::<HashSet<NodeIndex>>();
            relevant.insert(NodeIndex::from(target_index));

            let r = deps.filter_map(
                |n, w| match relevant.contains(&n) {
                    true => Some(w),
                    false => None,
                },
                |_e, _w| Some(()),
            );
            trace!(
                "Got N={} nodes in transitive closure of taget = {}",
                r.node_count(),
                target_index
            );
            r
        }
        None => deps.filter_map(|_n, w| Some(w), |_e, _w| Some(())),
    };

    let mut reused_old_keys = HashSet::new();

    // Now let's remove all edges from the dependency graph if the source has already finished
    // or if we've already exeuted the task in a previous call to execute
    let mut filtered_subgraph = subgraph.filter_map(
        |_n, &w| {
            if completed.contains(&w.key) {
                // if we already ran this command within this process, don't record it
                // in reused_old_keys. that's only for stuff that was run in a previous
                // session
                trace!(
                    "Skipping {} because it was already run successfully within this session",
                    w.key
                );
                return None; // returning none excludes it from filtered_subgraph
            }
            let has_success = logfile.has_success(&w.key);
            if has_success {
                trace!(
                    "Skipping {} because it already ran successfully accord to the log file",
                    w.key
                );
                reused_old_keys.insert(w.key.clone());
                return None;
            }

            trace!("Retaining {} because it has not been run before", w.key);
            Some(w)
        },
        |_e, &w| Some(w),
    );

    // If we are skipping failures, we need to further filter the graph down
    // to not include the failures or any downstream tasks, and add the failures
    // that were important to `reused_old_keys`.
    if !rerun_failures {
        trace!("Recomputing transitive closure of filtered subgraph");
        let tc = transitive_closure_dag(&filtered_subgraph)?;
        let failures = tc
            .node_indices()
            .filter(|&n| {
                let w = tc[n];
                logfile.has_failure(&w.key)
            })
            .collect::<HashSet<NodeIndex>>();

        trace!("Found N={} commands that previously failed", failures.len());
        filtered_subgraph = filtered_subgraph.filter_map(
            |n, &w| {
                if failures.contains(&n) {
                    trace!("Skipping {} because it previously failed", w.key);
                    reused_old_keys.insert(w.key.clone());
                    return None;
                }
                for e in tc.edges_directed(n, Direction::Incoming) {
                    if failures.contains(&e.source()) {
                        trace!(
                            "Skipping {} because it's downstream of a task that previously failed.",
                            w.key
                        );
                        return None;
                    }
                }

                Some(w)
            },
            |_e, _w| Some(()),
        );
    }

    // See test_copy_reused_keys_logfile.
    for key in reused_old_keys {
        trace!("Writing backref for key={}", key);
        logfile.write(LogEntry::new_backref(&key))?;
        completed.insert(key);
        // TODO: do we need to do anything more?
    }

    Ok(filtered_subgraph)
}
