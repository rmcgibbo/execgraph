use crate::graphtheory::transitive_closure_dag;
use crate::logfile::load_keys_exit_status_0;
use crate::server::router;
use crate::server::State as ServerState;
use crate::sync::{ReadyTracker, StatusUpdater};
use crate::unsafecode;
use anyhow::{anyhow, Result};
use async_channel::Receiver;
use futures::future::join_all;
use hyper::Server;
use petgraph::prelude::*;
use routerify::RouterService;
use std::collections::HashSet;
use std::fs::File;
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use tokio::process::Command;
use tokio::signal;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cmd {
    pub cmdline: String,
    pub key: String,
    pub display: Option<String>,
    pub queuename: Option<String>,
}

impl Cmd {
    pub fn display(&self) -> &str {
        match &self.display {
            Some(s) => s,
            None => &self.cmdline,
        }
    }
}

#[derive(Debug)]
pub struct ExecGraph {
    deps: Graph<Cmd, (), Directed>,
    keyfile: String,
    completed: Vec<NodeIndex>,
}

impl ExecGraph {
    pub fn new(
        keyfile: String,
    ) -> ExecGraph {
        ExecGraph {
            deps: Graph::new(),
            keyfile,
            completed: vec![],
        }
    }

    pub fn ntasks(&self) -> usize {
        self.deps.raw_nodes().len()
    }

    pub fn scan_keys(&self, s: &str) -> Vec<u32> {
        self.deps
            .raw_nodes()
            .iter()
            .enumerate()
            .filter_map(|(i, n)| {
                if s.contains(&n.weight.key) {
                    Some(i as u32)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_task(&self, id: u32) -> Option<Cmd> {
        self.deps.node_weight(NodeIndex::from(id)).cloned()
    }

    pub fn add_task(&mut self, cmd: Cmd, dependencies: Vec<u32>) -> Result<u32> {
        if !cmd.key.is_empty() {
            if let Some(existing) = self
                .deps
                .raw_nodes()
                .iter()
                .enumerate()
                .filter_map(|(i, n)| {
                    if n.weight.key == cmd.key {
                        Some(i as u32)
                    } else {
                        None
                    }
                })
                .next()
            {
                return Ok(existing);
            }
        }

        let new_node = self.deps.add_node(cmd);
        for dep in dependencies.iter().map(|&i| NodeIndex::from(i)) {
            if dep == new_node || self.deps.node_weight(dep).is_none() {
                self.deps.remove_node(new_node);
                return Err(anyhow!("Invalid dependency index"));
            }
            self.deps.add_edge(dep, new_node, ());
        }

        Ok(new_node.index() as u32)
    }

    fn get_subgraph(&self, target: Option<u32>) -> Result<Graph<(Cmd, NodeIndex), ()>> {
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

        let previously_run_keys = match File::open(&self.keyfile) {
            Ok(file) => load_keys_exit_status_0(file).collect::<HashSet<_>>(),
            _ => HashSet::new(),
        };

        // Now let's remove all edges from the dependency graph if the source has already finished
        // or if we've already exeuted the task in a previous call to execute
        Ok(subgraph.filter_map(
            |_n, w| {
                if self.completed.contains(&w.1)
                    || (!w.0.key.is_empty()) && previously_run_keys.contains(&w.0.key)
                {
                    None
                } else {
                    Some((w.0.clone(), w.1))
                }
            },
            |e, &w| {
                let src = subgraph.edge_endpoints(e).unwrap().0;
                let srckey = &subgraph.node_weight(src).unwrap().0.key;
                if (!srckey.is_empty()) && previously_run_keys.contains(srckey) {
                    None
                } else {
                    Some(w)
                }
            },
        ))
    }

    pub async fn execute(
        &mut self,
        target: Option<u32>,
        num_parallel: u32,
        failures_allowed: u32,
        provisioner: Option<String>,
        provisioner_arg2: Option<String>,
    ) -> Result<(u32, Vec<u32>)> {
        let subgraph = Arc::new(self.get_subgraph(target)?);
        if subgraph.raw_nodes().is_empty() {
            return Ok((0, vec![]));
        }

        let count_offset = self.completed.len() as u32;
        let (mut servicer, tasks_ready, status_updater) =
            ReadyTracker::new(subgraph.clone(), count_offset, failures_allowed);

        // Run local processes
        let handles = (0..num_parallel)
            .map(|_| {
                tokio::spawn(run_local_process_loop(
                    subgraph.clone(),
                    tasks_ready.get(&None).expect("No null queue").clone(),
                    status_updater.clone(),
                ))
            })
            .collect::<Vec<tokio::task::JoinHandle<_>>>();

        //
        // Create the server that can manage farming off tasks to remote machines over http
        //
        let token = CancellationToken::new();
        let (provisioner_exited_tx, provisioner_exited_rx) = oneshot::channel();

        if provisioner.is_some() {
            let subgraph1 = subgraph.clone();
            let tasks_ready1 = tasks_ready.clone();
            let status_updater1 = status_updater.clone();
            let provisioner = format!("{}", provisioner.clone().unwrap());
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
                    drop(
                        spawn_and_wait_for_provisioner(&provisioner, p2, bound_addr, token2).await,
                    );
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
        } else {
            provisioner_exited_tx.send(()).expect("Could not send");
        }

        // run the background service that will send commands to the ready channel to be picked up by the
        // tasks spawned above
        tokio::select! {
            _ = servicer.background_serve(&self.keyfile) => {
                join_all(handles).await;
                token.cancel();
            },
            _ = signal::ctrl_c() => {
                token.cancel();
            },
            _ = token.cancelled() => {},
        };
        provisioner_exited_rx
            .await
            .expect("failed to close provisioner");

        let n_failed = servicer.n_failed;
        // get indices of the tasks we executed, mapped back from the subgraph node ids
        // to the node ids in the deps graph
        let completed: Vec<NodeIndex> = servicer
            .finished_order
            .iter()
            .map(|&n| (subgraph[n].1))
            .collect();

        drop(servicer);
        drop(subgraph);
        self.completed.extend(&completed);
        Ok((
            n_failed,
            completed.iter().map(|n| n.index() as u32).collect(),
        ))
    }
}

async fn run_local_process_loop(
    subgraph: Arc<DiGraph<(Cmd, NodeIndex), ()>>,
    tasks_ready: Receiver<NodeIndex>,
    status_updater: StatusUpdater,
) -> Result<()> {
    while let Ok(subgraph_node_id) = tasks_ready.recv().await {
        let (cmd, _) = subgraph
            .node_weight(subgraph_node_id)
            .unwrap_or_else(|| panic!("failed to get node {:#?}", subgraph_node_id));
        let skip_execution_debugging_race_conditions = cmd.cmdline.is_empty();
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let (hostpid, status) = if skip_execution_debugging_race_conditions {
            let pid = 0;
            let fake_status = 0;
            let hostpid = format!("{}:{}", hostname, pid);
            status_updater
                .send_started(subgraph_node_id, &cmd, &hostpid)
                .await;

            (hostpid, fake_status)
        } else {
            let mut child = Command::new("/bin/sh")
                .arg("-c")
                .arg(&cmd.cmdline)
                .spawn()
                .expect("failed to execute /bin/sh");
            let pid = child
                .id()
                .expect("hasn't been polled yet, so this id should exist");
            let hostpid = format!("{}:{}", hostname, pid);
            status_updater
                .send_started(subgraph_node_id, &cmd, &hostpid)
                .await;
            let status_obj = child.wait().await.expect("sh wasn't running");
            // status_obj.code().or(Some(status_obj.into_raw())).expect("foo")
            let code = match status_obj.code() {
                Some(code) => code,
                None => status_obj.signal().expect("No exit code and no signal?"),
            };

            (hostpid, code)
        };

        status_updater
            .send_finished(subgraph_node_id, &cmd, &hostpid, status)
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
    let mut cmd = Command::new(provisioner);
    let mut rcmd = cmd.arg(format!("http://{}", bound_addr)).kill_on_drop(true);
    if let Some(arg2) = arg2 {
        rcmd = rcmd.arg(arg2);
    }

    let mut child = rcmd.spawn()?;
    tokio::select! {
        // if this process got a ctrl-c, then this token is cancelled
        _ = token.cancelled() => {
            unsafecode::sigint_then_kill(&mut child, std::time::Duration::from_millis(250)).await;
        },

        result = child.wait() => {
            let status = result?;
            log::error!("provisioner exited with status={}", status);
        }
    }

    Ok(())
}
