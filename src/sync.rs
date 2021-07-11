use crate::execgraph::Cmd;
use crate::logfile::LogWriter;
use anyhow::Result;
use crossbeam_channel::{unbounded, Receiver, Sender};
use petgraph::prelude::*;
use std::collections::HashMap;

pub struct ReadyTracker<'a, N, E> {
    g: &'a Graph<N, E>,
    ready: Option<Sender<NodeIndex>>,
    completed: Receiver<CompletedEvent>,
    pub finished_order: Vec<NodeIndex>,
    pub n_failed: u32,
    pub n_success: u32,
    count_offset: u32,
}

pub struct StatusUpdater {
    s: Sender<CompletedEvent>,
}

impl<'a, N, E> ReadyTracker<'a, N, E> {
    pub fn new(
        g: &'a Graph<N, E, Directed>,
        count_offset: u32,
    ) -> (ReadyTracker<'a, N, E>, Receiver<NodeIndex>, StatusUpdater) {
        let (ready_s, ready_r) = unbounded();
        let (finished_s, finished_r) = unbounded();

        return (
            ReadyTracker {
                g: g,
                ready: Some(ready_s),
                completed: finished_r,
                finished_order: vec![],
                n_failed: 0,
                n_success: 0,
                count_offset: count_offset,
            },
            ready_r,
            StatusUpdater { s: finished_s },
        );
    }

    pub fn background_serve(&mut self, keyfile: &str) -> Result<()> {
        // for each task, how many unmet first-order dependencies does it have?
        let mut n_unmet_deps: HashMap<NodeIndex, usize> = self
            .g
            .node_indices()
            .map(|i| (i, self.g.edges_directed(i, Direction::Incoming).count()))
            .collect();
        // trigger all of the tasks that have zero unmet dependencies
        for (k, v) in n_unmet_deps.iter() {
            if *v == 0 {
                self.ready.as_ref().expect("foo").send(*k)?;
            }
        }

        let mut writer = LogWriter::new(keyfile)?;
        let total = n_unmet_deps.len() as u32;

        while let Ok(event) = self.completed.recv() {
            match event {
                CompletedEvent::Started(e) => {
                    writer.begin_command(&e.cmd.display(), &e.cmd.key, e.pid)?;
                }
                CompletedEvent::Finished(e) => {
                    self.finished_order.push(e.id);
                    writer.end_command(&e.cmd.display(), &e.cmd.key, e.exit_status, e.pid)?;
                    if e.exit_status == 0 {
                        self.n_success += 1;
                        for edge in self.g.edges_directed(e.id, Direction::Outgoing) {
                            let downstream_id = edge.target();
                            let value = n_unmet_deps
                                .get_mut(&downstream_id)
                                .expect("key doesn't exist");
                            *value -= 1;
                            if *value == 0 {
                                self.ready.as_ref().expect("sfds").send(downstream_id)?;
                            }
                        }
                        println!("[{}/{}] {}", self.n_success + self.count_offset, total + self.count_offset, e.cmd.display());
                    } else {
                        println!("\x1b[1;31mFAILED:\x1b[0m {}", e.cmd.display());
                        self.n_failed += 1;
                    }

                    // In the future, we might want to allow execution to continue even if there
                    // are failures, but right now we're just going to stop immediately. If we do
                    // allow failures, we're going to need to change this logic below a little bit.
                    // In addition to tracking n_failed and n_success, we need to also track the
                    // number of tasks that have been 'poisoned' by failures upstream of them and
                    // never executed.

                    if self.n_failed > 0 || self.n_success >= total {
                        // drop the send side of the channel. this will cause the receive side
                        // to start returning errors, which is exactly what we want and will
                        // break the other threads out of the loop.
                        self.ready = None;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl StatusUpdater {
    pub fn send_started(&self, _v: NodeIndex, cmd: &Cmd, pid: u32) {
        self.s
            .send(CompletedEvent::Started(StartedEvent {
                // id: v,
                cmd: cmd.clone(),
                pid: pid,
            }))
            .expect("cannot send to channel")
    }

    pub fn send_finished(&self, v: NodeIndex, cmd: &Cmd, pid: u32, status: i32) {
        self.s
            .send(CompletedEvent::Finished(FinishedEvent {
                id: v,
                cmd: cmd.clone(),
                pid: pid,
                exit_status: status,
            }))
            .expect("cannot send to channel");
    }
}

struct StartedEvent {
    // id: NodeIndex,
    cmd: Cmd,
    pid: u32,
}
#[derive(Debug)]
struct FinishedEvent {
    id: NodeIndex,
    cmd: Cmd,
    pid: u32,
    exit_status: i32,
}
enum CompletedEvent {
    Started(StartedEvent),
    Finished(FinishedEvent),
}
