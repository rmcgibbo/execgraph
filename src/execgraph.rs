use crate::graphtheory::transitive_closure_dag;
use crate::logfile::load_keys_exit_status_0;
use crate::sync::ReadyTracker;
use anyhow::{anyhow, Result};
use petgraph::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::os::unix::process::ExitStatusExt;
use std::process::Command;

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
}

impl ExecGraph {
    pub fn new(keyfile: String) -> ExecGraph {
        ExecGraph {
            deps: Graph::new(),
            keyfile: keyfile,
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
        self.deps
            .node_weight(NodeIndex::from(id))
            .map(|n| n.clone())
    }

    pub fn add_task(&mut self, cmd: Cmd, dependencies: Vec<u32>) -> Result<u32> {
        if cmd.key != "" {
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

    fn get_subgraph(&self, target: Option<u32>) -> Result<Graph<(&Cmd, NodeIndex), ()>> {
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
            |_n, &w| {
                if self.completed.contains(&w.1) ||  w.0.key != "" && previously_run_keys.contains(&w.0.key) {
                    None
                } else {
                    Some(w)
                }
            },
            |e, &w| {
                let src = subgraph.edge_endpoints(e).unwrap().0;
                let srckey = &subgraph.node_weight(src).unwrap().0.key;
                if srckey != "" && previously_run_keys.contains(srckey) {
                    None
                } else {
                    Some(w)
                }
            },
        ))
    }

    pub fn execute(
        &mut self,
        target: Option<u32>,
        tpool: &rayon::ThreadPool,
    ) -> Result<(u32, Vec<u32>)> {
        let subgraph = self.get_subgraph(target)?;
        // println!("Number of tasks to execute: {}", subgraph.raw_nodes().len());
        if subgraph.raw_nodes().len() == 0 {
            return Ok((0, vec![]));
        }

        let count_offset = self.completed.len() as u32;
        let (mut servicer, receiver, sender) = ReadyTracker::new(&subgraph, count_offset);
        tpool.scope(|s| {
            // this threads consumes events from a channel that record when processes were started and stopped
            // and writes them to a log file and to the console
            s.spawn(|_s| {
                servicer
                    .background_serve(&self.keyfile)
                    .expect("Failed to write log file")
            });
            // these threads try to consume "ready events" from the ready queue, run the command,
            // and then update the ready queue. they also records events via writing to a channel
            // in record_started and record_finished so that we can write the log file
            for _ in 0..tpool.current_num_threads() {
                s.spawn(|_s| {
                    while let Ok(subgraph_node_id) = receiver.recv() {
                        let (cmd, _) = *subgraph
                            .node_weight(subgraph_node_id)
                            .expect(&format!("failed to get node {:#?}", subgraph_node_id));
                        let skip_execution_debugging_race_conditions = cmd.cmdline == "";

                        let (pid, status) = if skip_execution_debugging_race_conditions {
                            let pid = 0;
                            let fake_status = 0;
                            sender.send_started(subgraph_node_id, &cmd, pid);

                            (pid, fake_status)
                        } else {
                            let mut child = Command::new("sh")
                                .arg("-c")
                                .arg(&cmd.cmdline)
                                .spawn()
                                .expect("failed to execute sh");
                            sender.send_started(subgraph_node_id, &cmd, child.id());
                            let status_obj = child.wait().expect("sh wasn't running");
                            // status_obj.code().or(Some(status_obj.into_raw())).expect("foo")
                            let code = match status_obj.code() {
                                Some(code) => code,
                                None => status_obj.signal().expect("No exit code and no signal?"),
                            };

                            (child.id(), code)
                        };

                        sender.send_finished(subgraph_node_id, cmd, pid, status);
                    }
                });
            }
        });

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
            completed.iter().map(|n| n.index() as u32).collect()
        ))
    }
}
