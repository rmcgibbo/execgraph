use crate::graphtheory::transitive_closure_dag;
use crate::logfile::load_keys_exit_status_0;
use crate::server::router;
use crate::server::State as ServerState;
use crate::sync::{ReadyTracker, StatusUpdater};
use anyhow::{anyhow, Result};
use async_channel::Receiver;
use futures::future::join_all;
use hyper::Server;
use petgraph::prelude::*;
use routerify::RouterService;
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::File;
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use tokio::process::Command;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use crate::sync::DropGuard;

#[derive(Debug, Clone, Default)]
pub struct Cmd {
    pub cmdline: String,
    pub key: String,
    pub display: Option<String>,
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
    provisioner: Option<String>,
}

impl ExecGraph {
    pub fn new(keyfile: String, provisioner: Option<String>) -> ExecGraph {
        ExecGraph {
            deps: Graph::new(),
            keyfile,
            completed: vec![],
            provisioner,
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
                    tasks_ready.clone(),
                    status_updater.clone(),
                ))
            })
            .collect::<Vec<tokio::task::JoinHandle<_>>>();

        //
        // Create the server that can manage farming off tasks to remote machines over http
        //
        let token = CancellationToken::new();
        let token1 = token.clone();

        if self.provisioner.is_some() {
            let subgraph1 = subgraph.clone();
            let tasks_ready1 = tasks_ready.clone();
            let status_updater1 = status_updater.clone();
            let provisioner = self.provisioner.clone().unwrap();
            tokio::spawn(async move {
                let state = ServerState::new(subgraph1, tasks_ready1, status_updater1);
                let router = router(state);
                let service = RouterService::new(router).unwrap();
                let addr = SocketAddr::from(([0, 0, 0, 0], 0));
                let server = Server::bind(&addr).serve(service);
                let bound_addr = server.local_addr();

                let p: Arc<OsString> = Arc::new(provisioner.into());
                tokio::spawn(async move {
                    // if this task exits, it'll dropping the _drop_guard will
                    // trigger the cancellation token
                    let _drop_guard = DropGuard::new(token1);
                    match Command::new(&*p)
                        .arg(format!("http://{}", bound_addr))
                        .kill_on_drop(true)
                        .status()
                        .await
                    {
                        Ok(status) => {
                            log::error!("provisioner exited with status={}", status);
                        }
                        Err(e) => {
                            log::error!("command failed to start: {:?} {}", p, e);
                        }
                    }
                });

                if let Err(err) = server.await {
                    log::error!("Server error: {}", err);
                }
            });
        }

        // run the background service that will send commands to the ready channel to be picked up by the
        // tasks spawned above
        tokio::select! {
            _ = servicer.background_serve(&self.keyfile) => {
                join_all(handles).await;
            },
            _ = signal::ctrl_c() => {},
            _ = token.cancelled() => {},
        };

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
        let (pid, status) = if skip_execution_debugging_race_conditions {
            let pid = 0;
            let fake_status = 0;
            status_updater
                .send_started(subgraph_node_id, &cmd, pid)
                .await;

            (pid, fake_status)
        } else {
            let mut child = Command::new("/bin/sh")
                .arg("-c")
                .arg(&cmd.cmdline)
                .spawn()
                .expect("failed to execute /bin/sh");
            let pid = child
                .id()
                .expect("hasn't been polled yet, so this id should exist");
            status_updater
                .send_started(subgraph_node_id, &cmd, pid)
                .await;
            let status_obj = child.wait().await.expect("sh wasn't running");
            // status_obj.code().or(Some(status_obj.into_raw())).expect("foo")
            let code = match status_obj.code() {
                Some(code) => code,
                None => status_obj.signal().expect("No exit code and no signal?"),
            };

            (pid, code)
        };

        status_updater
            .send_finished(subgraph_node_id, &cmd, pid, status)
            .await;
    }

    Ok(())
}
