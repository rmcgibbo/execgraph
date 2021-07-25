use crate::execgraph::Cmd;
use crate::logfile::LogWriter;
use anyhow::Result;
use async_channel::{unbounded, Receiver, Sender};
use petgraph::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// A wrapper for cancellation token which automatically cancels
/// it on drop. It is created using `drop_guard` method on the `CancellationToken`.
/// Will be in the next release of tokio-util after 0.6.7, but I don't want to depend
/// on the git snapshot for now so I'm just vendoring it
#[derive(Debug)]
pub struct DropGuard {
    pub(super) inner: Option<CancellationToken>,
}
impl DropGuard {
    pub fn new(token: CancellationToken) -> DropGuard {
        DropGuard { inner: Some(token) }
    }
}
impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.cancel();
        }
    }
}
pub type Queuename = Option<String>;

#[derive(Clone, Debug)]
pub struct QueueSnapshot {
    pub n_pending: u32,
    pub n_success: u32,
    pub n_failed: u32,
}

pub struct ReadyTracker {
    pub finished_order: Vec<NodeIndex>,
    pub n_failed: u32,

    g: Arc<Graph<(Cmd, NodeIndex), (), Directed>>,
    ready: Option<HashMap<Queuename, Sender<NodeIndex>>>,
    completed: Receiver<CompletedEvent>,
    n_success: u32,
    n_pending: u32,
    count_offset: u32,
    failures_allowed: u32,
    queuestate: Arc<Mutex<HashMap<Queuename, QueueSnapshot>>>,
}

#[derive(Clone)]
pub struct StatusUpdater {
    s: Sender<CompletedEvent>,
    queuestate: Arc<Mutex<HashMap<Queuename, QueueSnapshot>>>,
}

struct TaskStatus {
    n_unmet_deps: usize,
    poisoned: bool,
    queuename: Queuename,
}

impl ReadyTracker {
    pub fn new(
        g: Arc<Graph<(Cmd, NodeIndex), (), Directed>>,
        count_offset: u32,
        failures_allowed: u32,
    ) -> (
        ReadyTracker,
        HashMap<Queuename, Receiver<NodeIndex>>,
        StatusUpdater,
    ) {
        let mut ready_s = HashMap::<Queuename, Sender<NodeIndex>>::new();
        let mut ready_r = HashMap::<Queuename, Receiver<NodeIndex>>::new();
        for queuename in g
            .node_indices()
            .map(|i| g.node_weight(i).unwrap().0.queuename.clone())
            .chain(std::iter::once(None))
            .collect::<HashSet<_>>()
            .iter()
        {
            let (sender, receiver) = unbounded::<NodeIndex>();
            ready_s.insert(queuename.clone(), sender);
            ready_r.insert(queuename.clone(), receiver);
        }

        let queuestate = Arc::new(Mutex::new(
            ready_s
                .keys()
                .map(|queuename| {
                    (
                        queuename.clone(),
                        QueueSnapshot {
                            n_pending: 0,
                            n_success: 0,
                            n_failed: 0,
                        },
                    )
                })
                .collect::<HashMap<Queuename, QueueSnapshot>>(),
        ));

        let (finished_s, finished_r) = unbounded();

        (
            ReadyTracker {
                finished_order: vec![],
                g,
                ready: Some(ready_s),
                completed: finished_r,
                n_failed: 0,
                n_success: 0,
                n_pending: 0,
                count_offset,
                failures_allowed,
                queuestate: queuestate.clone(),
            },
            ready_r,
            StatusUpdater {
                s: finished_s,
                queuestate: queuestate.clone(),
            },
        )
    }

    pub async fn background_serve(&mut self, keyfile: &str) -> Result<()> {
        // for each task, how many unmet first-order dependencies does it have?
        let mut statuses: HashMap<NodeIndex, TaskStatus> = self
            .g
            .node_indices()
            .map(|i| {
                (
                    i,
                    TaskStatus {
                        n_unmet_deps: self.g.edges_directed(i, Direction::Incoming).count(),
                        poisoned: false,
                        queuename: self.g.node_weight(i).unwrap().0.queuename.clone(),
                    },
                )
            })
            .collect();

        // trigger all of the tasks that have zero unmet dependencies
        for (k, v) in statuses.iter() {
            if v.n_unmet_deps == 0 {
                self.n_pending += 1;
                self.queuestate
                    .lock()
                    .unwrap()
                    .get_mut(&v.queuename)
                    .expect("No such Queuename")
                    .n_pending += 1;
                self.ready
                    .as_mut()
                    .expect("send channel was already dropped?")
                    .get_mut(&v.queuename)
                    .expect("No such Queuename")
                    .send(*k)
                    .await?;
            }
        }

        // every time we put a task into the ready queue, we increment n_pending.
        // every time a task completes, we decrement n_pending.
        // when n_pending goes to zero, then the whole process is finished.
        // note that we don't actuall track the number currently inside the ready queue
        // or inside other stages of processing, because there are going to be
        // accounting bugs that way.

        let mut writer = LogWriter::new(keyfile)?;
        loop {
            match self.completed.recv().await.unwrap() {
                CompletedEvent::Started(e) => {
                    writer.begin_command(&e.cmd.display(), &e.cmd.key, &e.hostpid)?;
                }
                CompletedEvent::Finished(e) => {
                    self.finished_order.push(e.id);
                    writer.end_command(&e.cmd.display(), &e.cmd.key, e.exit_status, &e.hostpid)?;
                    self._finished_bookkeeping(&mut statuses, &e).await?;

                    if self.n_failed >= self.failures_allowed || self.n_pending == 0 {
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

    async fn _finished_bookkeeping<'a>(
        &mut self,
        statuses: &mut HashMap<NodeIndex, TaskStatus>,
        e: &FinishedEvent,
    ) -> Result<()> {
        let is_success = e.exit_status == 0;
        let total = statuses.len() as u32;

        self.queuestate
            .lock()
            .unwrap()
            .get_mut(&statuses.get(&e.id).unwrap().queuename)
            .unwrap()
            .n_pending -= 1;
        self.n_pending -= 1;

        if is_success {
            self.n_success += 1;
            println!(
                "[{}/{}] {}",
                self.n_success,
                total + self.count_offset,
                e.cmd.display()
            );
        } else {
            self.n_failed += 1;
            println!("\x1b[1;31mFAILED:\x1b[0m {}", e.cmd.display());
        }

        for edge in self.g.edges_directed(e.id, Direction::Outgoing) {
            let downstream_id = edge.target();
            let mut status = statuses.get_mut(&downstream_id).expect("key doesn't exist");
            if is_success {
                if !status.poisoned {
                    status.n_unmet_deps -= 1;
                    if status.n_unmet_deps == 0 {
                        self.n_pending += 1;
                        self.queuestate
                            .lock()
                            .unwrap()
                            .get_mut(&status.queuename)
                            .expect("No such Queuename")
                            .n_pending += 1;
                        self.ready
                            .as_mut()
                            .expect("send channel was already dropped?")
                            .get_mut(&status.queuename)
                            .expect("No such Queuename")
                            .send(downstream_id)
                            .await?;
                    }
                }
            } else {
                // failed. mark any (first-order) downstream tasks as poisoned.
                status.poisoned = true;
            }
        }

        Ok(())
    }
}

impl StatusUpdater {
    /// When a task is started, notify the tracker by calling this.
    pub async fn send_started(&self, _v: NodeIndex, cmd: &Cmd, hostpid: &str) {
        let r= self.s
            .send(CompletedEvent::Started(StartedEvent {
                // id: v,
                cmd: cmd.clone(),
                hostpid: hostpid.to_string(),
            }))
            .await;
        if r.is_err() {
            log::debug!("send_started: cannot send to channel: {:#?}", r);
        };
    }

    /// When a task is finished, notify the tracker by calling this.
    pub async fn send_finished(&self, v: NodeIndex, cmd: &Cmd, hostpid: &str, status: i32) {
        let r = self.s
            .send(CompletedEvent::Finished(FinishedEvent {
                id: v,
                cmd: cmd.clone(),
                hostpid: hostpid.to_string(),
                exit_status: status,
            }))
            .await;
        if r.is_err() {
            log::debug!("send_finished: cannot send to channel: {:#?}", r);
        };
    }

    /// Retreive a snapshot of the state of the queue
    pub fn get_queuestate(&self) -> HashMap<Queuename, QueueSnapshot> {
        self.queuestate.lock().unwrap().clone()
    }
}

struct StartedEvent {
    // id: NodeIndex,
    cmd: Cmd,
    hostpid: String,
}
#[derive(Debug)]
struct FinishedEvent {
    id: NodeIndex,
    cmd: Cmd,
    hostpid: String,
    exit_status: i32,
}
enum CompletedEvent {
    Started(StartedEvent),
    Finished(FinishedEvent),
}
