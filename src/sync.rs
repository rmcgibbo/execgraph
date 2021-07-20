use crate::execgraph::Cmd;
use crate::logfile::LogWriter;
use anyhow::Result;
use async_channel::{unbounded, Receiver, Sender};
use petgraph::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
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

#[derive(Clone, Debug)]
pub struct QueueSnapshot {
    pub n_pending: u32,
    pub n_success: u32,
    pub n_failed: u32,
}

pub struct ReadyTracker<N, E> {
    pub finished_order: Vec<NodeIndex>,
    pub n_failed: u32,

    g: Arc<Graph<N, E>>,
    ready: Option<Sender<NodeIndex>>,
    completed: Receiver<CompletedEvent>,
    queuestate_s: watch::Sender<QueueSnapshot>,
    n_success: u32,
    n_pending: u32,
    count_offset: u32,
    failures_allowed: u32,
}

#[derive(Clone)]
pub struct StatusUpdater {
    s: Sender<CompletedEvent>,
    queuestate_r: watch::Receiver<QueueSnapshot>,
}

struct TaskStatus {
    n_unmet_deps: usize,
    poisoned: bool,
}

impl<'a, N, E> ReadyTracker<N, E> {
    pub fn new(
        g: Arc<Graph<N, E, Directed>>,
        count_offset: u32,
        failures_allowed: u32,
    ) -> (ReadyTracker<N, E>, Receiver<NodeIndex>, StatusUpdater) {
        let (ready_s, ready_r) = unbounded();
        let (finished_s, finished_r) = unbounded();
        let (queuestate_s, queuestate_r) = watch::channel(QueueSnapshot {
            n_pending: 0,
            n_success: 0,
            n_failed: 0,
        });

        (
            ReadyTracker {
                finished_order: vec![],
                g,
                ready: Some(ready_s),
                completed: finished_r,
                queuestate_s,
                n_failed: 0,
                n_success: 0,
                n_pending: 0,
                count_offset,
                failures_allowed,
            },
            ready_r,
            StatusUpdater {
                s: finished_s,
                queuestate_r,
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
                    },
                )
            })
            .collect();

        // trigger all of the tasks that have zero unmet dependencies
        for (k, v) in statuses.iter() {
            if v.n_unmet_deps == 0 {
                self.n_pending += 1;
                self.ready
                    .as_ref()
                    .expect("send channel was already dropped?")
                    .send(*k)
                    .await?;
            }
        }

        self.queuestate_s.send(QueueSnapshot {
            n_pending: self.n_pending,
            n_failed: self.n_failed,
            n_success: self.n_success,
        })?;

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
                    writer.begin_command(&e.cmd.display(), &e.cmd.key, e.pid)?;
                }
                CompletedEvent::Finished(e) => {
                    self.finished_order.push(e.id);
                    writer.end_command(&e.cmd.display(), &e.cmd.key, e.exit_status, e.pid)?;
                    self._finished_bookkeeping(&mut statuses, e).await?;

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

    async fn _finished_bookkeeping(
        &mut self,
        statuses: &mut HashMap<NodeIndex, TaskStatus>,
        e: FinishedEvent,
    ) -> Result<()> {
        let is_success = e.exit_status == 0;
        let total = statuses.len() as u32;
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
                        self.ready
                            .as_ref()
                            .expect("send channel was already dropped?")
                            .send(downstream_id)
                            .await?;
                    }
                }
            } else {
                // failed. mark any (first-order) downstream tasks as poisoned.
                status.poisoned = true;
            }
        }

        self.queuestate_s.send(QueueSnapshot {
            n_pending: self.n_pending,
            n_failed: self.n_failed,
            n_success: self.n_success,
        })?;
        Ok(())
    }
}

impl StatusUpdater {
    /// When a task is started, notify the tracker by calling this.
    pub async fn send_started(&self, _v: NodeIndex, cmd: &Cmd, pid: u32) {
        self.s
            .send(CompletedEvent::Started(StartedEvent {
                // id: v,
                cmd: cmd.clone(),
                pid,
            }))
            .await
            .expect("cannot send to channel")
    }

    /// When a task is finished, notify the tracker by calling this.
    pub async fn send_finished(&self, v: NodeIndex, cmd: &Cmd, pid: u32, status: i32) {
        self.s
            .send(CompletedEvent::Finished(FinishedEvent {
                id: v,
                cmd: cmd.clone(),
                pid,
                exit_status: status,
            }))
            .await
            .expect("cannot send to channel");
    }

    /// Retreive a snapshot of the state of the queue
    pub fn get_queuestate(&self) -> QueueSnapshot {
        (*self.queuestate_r.borrow()).clone()
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
