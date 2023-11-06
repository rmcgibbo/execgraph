use crate::{
    admin_server::run_admin_service_forever,
    fancy_cancellation_token::{CancellationState, CancellationToken},
    graphtheory::transitive_closure_dag,
    localrunner::{run_local_process_loop, LocalQueueType},
    logfile2::{LogEntry, LogFile, LogFileRO, LogFileRW},
    server::{router, State as ServerState},
    sync::{new_ready_tracker, RetryMode},
};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as b64, Engine};
use bitvec::array::BitArray;
use derivative::Derivative;
use futures::future::join_all;
use hyper::Server;
use petgraph::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    ffi::OsString,
    net::SocketAddr,
    sync::Arc,
};
use tokio::{process::Command, sync::oneshot};
use tracing::{debug, error, trace, warn};

// TODO: remove clone?
#[derive(Derivative)]
#[derivative(Debug, Clone, Default, PartialEq, Hash)]
pub struct Cmd {
    pub cmdline: Vec<OsString>,
    pub key: String,
    pub display: Option<String>,
    pub affinity: BitArray<u64>,

    pub runcount_base: u32,
    pub priority: u32,
    pub storage_root: u32,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub fd_input: Option<(i32, Vec<u8>)>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub preamble: Option<Capsule>,

    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub postamble: Option<Capsule>,

    pub max_retries: u32,
}

lazy_static::lazy_static! {
    static ref ESCAPED_NEWLINE_OR_TAB: aho_corasick::AhoCorasick = aho_corasick::AhoCorasick::new(
        ["\\\n", "\t"]
    ).unwrap();
}

#[derive(Debug, Clone)]
pub struct RemoteProvisionerSpec {
    pub cmd: String,
    pub info: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Capsule {
    #[cfg(feature = "pyo3")]
    capsule: pyo3::PyObject,
}

#[cfg(feature = "pyo3")]
impl Capsule {
    pub fn new(capsule: pyo3::PyObject) -> Self {
        Capsule { capsule }
    }

    fn call(&self, success: bool) -> Result<i32> {
        use pyo3::AsPyPointer;
        const CAPSULE_NAME: &[u8] = b"Execgraph::Capsule-v2\0";
        let capsule_name_ptr = CAPSULE_NAME.as_ptr() as *const i8;
        let success_i32: i32 = if success { 1 } else { 0};

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
                    fn(*const std::ffi::c_void, i32) -> i32,
                >(ptr);
                Ok(f(ctx, success_i32))
            } else {
                Err(anyhow!("Not a capsule!"))
            }
        }
    }
}

#[cfg(not(feature = "pyo3"))]
impl Capsule {
    fn call(&self) -> Result<i32> {
        Ok(0)
    }
}

impl Cmd {
    pub fn display(&self) -> String {
        match &self.display {
            Some(s) => s.to_string(),
            None => {
                let mut buffer = OsString::new();
                let sep = OsString::from(" ");
                for (i, item) in self.cmdline.iter().enumerate() {
                    if i > 0 {
                        buffer.push(&sep);
                    }
                    buffer.push(item);
                }
                let s = buffer.into_string().expect("cmdline must be utf-8");
                let mut dst = String::new();
                ESCAPED_NEWLINE_OR_TAB.replace_all_with(&s, &mut dst, |_m, t, d| {
                    if t == "\\\n" {
                        d.push(' ')
                    } else if t == "\t" {
                        d.push_str("\\t");
                    } else {
                        panic!("");
                    }
                    true
                });
                dst
            }
        }
    }

    pub fn call_preamble(&self) {
        match &self.preamble {
            Some(preamble) => {
                match preamble.call(false) {
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

    pub fn call_postamble(&self, success: bool) {
        match &self.postamble {
            Some(postamble) => {
                match postamble.call(success) {
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
    pub(crate) logfile: LogFile<LogFileRW>,
    pub(crate) readonly_logfiles: Vec<LogFile<LogFileRO>>,
    completed: HashSet<String>,
}

impl ExecGraph {
    #[tracing::instrument(skip_all)]
    pub fn new(
        logfile: LogFile<LogFileRW>,
        readonly_logfiles: Vec<LogFile<LogFileRO>>,
    ) -> ExecGraph {
        ExecGraph {
            deps: Graph::new(),
            key_to_nodeid: HashMap::new(),
            logfile,
            readonly_logfiles,
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

    pub fn add_task(&mut self, cmd: Cmd, dependencies: Vec<u32>) -> Result<u32> {
        if let Some(index) = self.key_to_nodeid.get(&cmd.key) {
            return Ok(index.index().try_into().unwrap());
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
        ratelimit_per_second: u32,
        retry_mode: RetryMode,
    ) -> Result<(u32, Vec<String>)> {
        fn extend_graph_lifetime(
            g: Arc<DiGraph<&Cmd, ()>>,
        ) -> Arc<DiGraph<&'static Cmd, ()>> {
            unsafe { std::mem::transmute::<_, Arc<DiGraph<&'static Cmd, ()>>>(g) }
        }

        let count_offset = self.completed.len().try_into().unwrap();
        let subgraph = Arc::new(get_subgraph(
            &mut self.deps,
            &mut self.completed,
            &mut self.logfile,
            &self.readonly_logfiles,
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
            ratelimit_per_second,
            retry_mode
        );

        // Run local processes
        trace!(
            "Spawning {} local process loops",
            num_parallel + u32::from(num_parallel > 0)
        );
        let mut handles: Vec<tokio::task::JoinHandle<_>> = {
            let local = (0..num_parallel).map(|_| {
                let subgraph = extend_graph_lifetime(subgraph.clone());
                let token = token.clone();
                tokio::spawn(run_local_process_loop(
                    subgraph,
                    tracker.clone(),
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
                            tracker.clone(),
                            token,
                            LocalQueueType::ConsoleQueue, // 1 for console queue
                        ))
                    }))
                    .collect()
            } else {
                local.collect()
            }
        };

        if let Some(provisioner) = provisioner {
            let subgraph = extend_graph_lifetime(subgraph.clone());
            let token1 = token.clone();
            let token2 = token.clone();
            let token3 = token.clone();
            let authorization_token = b64.encode(rand::random::<u128>().to_le_bytes());
            let state = Arc::new(ServerState::new(
                subgraph,
                tracker.clone(),
                token1.clone(),
                provisioner.clone(),
                authorization_token.clone(),
            ));
            let state2 = state.clone();
            let (http_service_up_sender, http_service_up_receiver) =
                tokio::sync::oneshot::channel();

            // Run the http server
            handles.push(tokio::spawn(async move {
                let service = router(state.clone()).into_make_service();
                let addr = SocketAddr::from(([0, 0, 0, 0], 0));
                let server = Server::bind(&addr).serve(service);
                let bound_addr = server.local_addr();
                http_service_up_sender
                    .send(format!("http://{}", bound_addr))
                    .expect("failed to send");
                let graceful = server.with_graceful_shutdown(token1.hard_cancelled());

                let (stop_reaping_pings_tx, stop_reaping_pings_rx) = oneshot::channel();
                let jh = {
                    let state = state.clone();
                    tokio::spawn(
                        async move { state.reap_pings_forever(stop_reaping_pings_rx).await },
                    )
                };
                if let Err(err) = graceful.await {
                    error!("Server error: {}", err);
                }
                debug!("Joining server tasks");
                stop_reaping_pings_tx.send(()).unwrap();
                jh.await.unwrap();
                token1.cancel(CancellationState::HardCancelled);
                debug!("Server task exited");
            }));
            // Run the admin server (unix domain socket)
            handles.push(tokio::spawn(run_admin_service_forever(state2, token3)));
            // Run the provisioner
            handles.push(tokio::spawn(async move {
                let http_service_addr = http_service_up_receiver.await.expect("failed to recv");
                debug!("Spawning remote provisioner {}", provisioner.cmd);
                if let Err(e) = spawn_and_wait_for_provisioner(
                    provisioner.cmd,
                    http_service_addr,
                    token2.clone(),
                    authorization_token,
                )
                .await
                {
                    error!("Provisioner failed: {}", e);
                }
                token2.cancel(CancellationState::HardCancelled);
                debug!("Provisioner task exited");
            }));
        }

        // run the background service that will send commands to the ready channel
        // to be picked up by the tasks spawned above. background_serve should wait for
        // sigint
        servicer
            .background_serve(token.clone())
            .await
            .expect("background_serve failed");
        token.cancel(CancellationState::HardCancelled);

        debug!("Joining {} handles", handles.len());
        join_all(handles).await;
        debug!("Draining servicer");
        servicer.drain().expect("failed to drain queue");

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
        debug!("nfailed={}, ncompleted={}", n_failed, completed.len());
        self.logfile.flush()?;

        self.key_to_nodeid.clear();
        self.deps.clear();

        Ok((n_failed, completed))
    }
}

#[tracing::instrument(skip_all)]
async fn spawn_and_wait_for_provisioner(
    provisioner: String,
    http_service_addr: String,
    cancellation_token: CancellationToken,
    authorization_token: String,
) -> Result<()> {
    let mut child = Command::new(&provisioner)
        .arg(http_service_addr)
        .env("EXECGRAPH_AUTHORIZATION_TOKEN", authorization_token)
        .kill_on_drop(true)
        .spawn()?;
    tokio::select! {
        // if this process got a ctrl-c, then this token is cancelled
        _ = cancellation_token.hard_cancelled() => {
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(child.id().unwrap() as i32), nix::sys::signal::Signal::SIGINT).unwrap();
        },

        result = child.wait() => {
            let status = result?;
            debug!("provisioner exited with status={}", status);
        }
    }

    Ok(())
}

#[tracing::instrument(skip(deps, completed, logfile, readonly_logfiles))]
fn get_subgraph<'a, 'b: 'a>(
    deps: &'b mut DiGraph<Cmd, ()>,
    completed: &mut HashSet<String>,
    logfile: &mut LogFile<LogFileRW>,
    readonly_logfiles: &[LogFile<LogFileRO>],
    target: Option<u32>,
    rerun_failures: bool,
) -> Result<DiGraph<&'a Cmd, ()>> {
    // Compute the priority and just mutate the graph. Ugly, but it
    // keeps everything in the Cmd struct
    let priority = crate::graphtheory::tlevel_dag(deps)?;
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
            let has_success = logfile.has_success(&w.key)
                || readonly_logfiles.iter().any(|l| l.has_success(&w.key));

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
                    || readonly_logfiles.iter().any(|l| l.has_failure(&w.key))
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
    }

    Ok(filtered_subgraph)
}
