use crate::{execgraph::Cmd, logfile::LogWriter};
use anyhow::Result;
use petgraph::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

const FAIL_COMMAND_PREFIX: &str = "wrk/";

pub type Queuename = Option<String>;

macro_rules! u32checked_add {
    // macth like arm for macro
    ($a:expr,$b:expr) => {
        // macro expand to this code
        {
            $a.checked_add(u32::try_from($b).expect("arithmetic overflow"))
                .expect("arithmetic overflow")
        }
    };
}

#[derive(Clone, Debug)]
pub struct QueueSnapshot {
    pub n_pending: u32,
    pub n_success: u32,
    pub n_failed: u32,
}

pub struct ReadyTracker<'a> {
    pub finished_order: Vec<NodeIndex>,
    pub n_failed: u32,

    g: Arc<Graph<&'a Cmd, (), Directed>>,
    ready: Option<HashMap<Queuename, async_priority_channel::Sender<NodeIndex, u32>>>,
    completed: async_channel::Receiver<CompletedEvent>,
    n_success: u32,
    n_pending: u32,
    count_offset: u32,
    failures_allowed: u32,
    queuestate: Arc<Mutex<HashMap<Queuename, QueueSnapshot>>>,
    inflight: HashSet<(NodeIndex, String)>, // NodeIndex for cmd and HostPid
}

#[derive(Clone)]
pub struct StatusUpdater {
    s: async_channel::Sender<CompletedEvent>,
    queuestate: Arc<Mutex<HashMap<Queuename, QueueSnapshot>>>,
}

struct TaskStatus {
    n_unmet_deps: usize,
    poisoned: bool,
    queuename: Queuename,
}

impl<'a> ReadyTracker<'a> {
    pub fn new(
        g: Arc<DiGraph<&'a Cmd, ()>>,
        count_offset: u32,
        failures_allowed: u32,
    ) -> (
        ReadyTracker,
        HashMap<Queuename, async_priority_channel::Receiver<NodeIndex, u32>>,
        StatusUpdater,
    ) {
        let mut ready_s =
            HashMap::<Queuename, async_priority_channel::Sender<NodeIndex, u32>>::new();
        let mut ready_r =
            HashMap::<Queuename, async_priority_channel::Receiver<NodeIndex, u32>>::new();
        for queuename in g
            .node_indices()
            .map(|i| g[i].queuename.clone())
            .chain(std::iter::once(None))
            .collect::<HashSet<_>>()
            .iter()
        {
            let (sender, receiver) = async_priority_channel::unbounded();
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

        let (finished_s, finished_r) = async_channel::unbounded();

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
                inflight: HashSet::new(),
            },
            ready_r,
            StatusUpdater {
                s: finished_s,
                queuestate,
            },
        )
    }
    pub fn drain(&mut self, logfile: &str) -> Result<()> {
        let mut writer = LogWriter::new(logfile)?;
        loop {
            match self.completed.try_recv() {
                Ok(CompletedEvent::Started(e)) => {
                    let cmd = self.g[e.id];
                    writer.begin_command(&cmd.display(), &cmd.key, cmd.runcount, &e.hostpid)?;
                    assert!(self.inflight.insert((e.id, e.hostpid)));
                }
                Ok(CompletedEvent::Finished(e)) => {
                    let cmd = self.g[e.id];
                    writer
                        .end_command(
                            &cmd.display(),
                            &cmd.key,
                            cmd.runcount,
                            e.exit_status,
                            &e.hostpid,
                        )
                        .unwrap();
                    assert!(self.inflight.remove(&(e.id, e.hostpid)));
                }
                Err(_) => {
                    break;
                }
            }
        }

        for (k, hostpid) in self.inflight.iter() {
            let timeout_status = 130;
            let cmd = self.g[*k];
            writer
                .end_command(
                    &cmd.display(),
                    &cmd.key,
                    cmd.runcount,
                    timeout_status,
                    hostpid,
                )
                .unwrap();
        }
        self.inflight.clear();

        Ok(())
    }

    pub async fn background_serve(
        &mut self,
        logfile: &str,
        token: CancellationToken,
    ) -> Result<()> {
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
                        // TODO: can we avoid this clone?
                        queuename: self.g[i].queuename.clone(),
                    },
                )
            })
            .collect();

        // trigger all of the tasks that have zero unmet dependencies
        for (queuename, ready) in group(statuses.iter().filter_map(|(k, v)| {
            if v.n_unmet_deps == 0 {
                Some((&v.queuename, (*k, self.g[*k].priority)))
            } else {
                None
            }
        })) {
            self.add_to_ready_queue(queuename, ready).await?;
        }

        // every time we put a task into the ready queue, we increment n_pending.
        // every time a task completes, we decrement n_pending.
        // when n_pending goes to zero, then the whole process is finished.
        // note that we don't actuall track the number currently inside the ready queue
        // or inside other stages of processing, because there are going to be
        // accounting bugs that way.

        let mut writer = LogWriter::new(logfile)?;
        loop {
            tokio::select! {
                event = self.completed.recv() => {
                    match event.unwrap() {
                        CompletedEvent::Started(e) => {
                            let cmd = self.g[e.id];
                            writer.begin_command(&cmd.display(), &cmd.key, cmd.runcount, &e.hostpid)?;
                            assert!(self.inflight.insert((e.id, e.hostpid)));
                        }
                        CompletedEvent::Finished(e) => {
                            let cmd = self.g[e.id];
                            self.finished_order.push(e.id);
                            writer.end_command(&cmd.display(), &cmd.key, cmd.runcount, e.exit_status, &e.hostpid)?;
                            self._finished_bookkeeping(&mut statuses, &e).await?;
                            assert!(self.inflight.remove(&(e.id, e.hostpid)));

                            if self.n_failed >= self.failures_allowed || self.n_pending == 0 {
                                // drop the send side of the channel. this will cause the receive side
                                // to start returning errors, which is exactly what we want and will
                                // break the other threads out of the loop.
                                self.ready = None;
                                break;
                            }
                        }
                    }
                },
                _ = token.cancelled() => {
                    self.ready = None;
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    self.ready = None;
                    break;
                }
            };
        }

        Ok(())
    }

    async fn _finished_bookkeeping(
        &mut self,
        statuses: &mut HashMap<NodeIndex, TaskStatus>,
        e: &FinishedEvent,
    ) -> Result<()> {
        let is_success = e.exit_status == 0;
        let total = statuses.len() as u32;
        let cmd = self.g[e.id];

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
                self.n_success + self.count_offset,
                total + self.count_offset,
                cmd.display()
            );
        } else {
            self.n_failed += 1;
            if cmd.key.is_empty() {
                println!(
                    "\x1b[1;31mFAILED:\x1b[0m {}{}",
                    FAIL_COMMAND_PREFIX,
                    cmd.display()
                );
            } else {
                println!(
                    "\x1b[1;31mFAILED:\x1b[0m {}{}.{:x}: {}",
                    FAIL_COMMAND_PREFIX,
                    cmd.key,
                    cmd.runcount,
                    cmd.display()
                );
            }
        }

        if !e.stdout.is_empty() {
            print!("{}", e.stdout);
        }
        if !e.stderr.is_empty() {
            eprint!("{}", e.stderr);
        }

        for (queuename, ready) in group(
            self.g
                .edges_directed(e.id, Direction::Outgoing)
                .filter_map(|edge| {
                    let downstream_id = edge.target();
                    let mut status = statuses.get_mut(&downstream_id).expect("key doesn't exist");
                    if is_success {
                        if !status.poisoned {
                            status.n_unmet_deps -= 1;
                            if status.n_unmet_deps == 0 {
                                return Some((
                                    // TODO: can we avoid this clone?
                                    status.queuename.clone(),
                                    (downstream_id, self.g[downstream_id].priority),
                                ));
                            }
                        }
                    } else {
                        // failed. mark any (first-order) downstream tasks as poisoned.
                        status.poisoned = true;
                    }

                    None
                }),
        ) {
            self.add_to_ready_queue(&queuename, ready).await?;
        }

        Ok(())
    }

    async fn add_to_ready_queue(
        &mut self,
        queuename: &Queuename,
        ready: Vec<(NodeIndex, u32)>,
    ) -> Result<()> {
        let mut queuestate_lock = self.queuestate.lock().unwrap();
        let queuestate = queuestate_lock
            .get_mut(queuename)
            .expect("No such Queuename");
        queuestate.n_pending = u32checked_add!(queuestate.n_pending, ready.len());
        self.n_pending = u32checked_add!(self.n_pending, ready.len());

        self.ready
            .as_mut()
            .expect("send channel was already dropped?")
            .get_mut(queuename)
            .expect("No such Queuename")
            .sendv(ready.into_iter().peekable())
            .await?;

        Ok(())
    }
}

impl StatusUpdater {
    /// When a task is started, notify the tracker by calling this.
    pub async fn send_started(&self, v: NodeIndex, cmd: &Cmd, hostpid: &str) {
        cmd.call_preamble();
        let r = self
            .s
            .send(CompletedEvent::Started(StartedEvent {
                id: v,
                hostpid: hostpid.to_string(),
            }))
            .await;
        if r.is_err() {
            log::debug!("send_started: cannot send to channel: {:#?}", r);
        };
    }

    /// When a task is finished, notify the tracker by calling this.
    pub async fn send_finished(
        &self,
        v: NodeIndex,
        cmd: &Cmd,
        hostpid: &str,
        status: i32,
        stdout: String,
        stderr: String,
    ) {
        cmd.call_postamble();
        let r = self
            .s
            .send(CompletedEvent::Finished(FinishedEvent {
                id: v,
                hostpid: hostpid.to_string(),
                exit_status: status,
                stdout,
                stderr,
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

#[derive(Debug)]
struct StartedEvent {
    id: NodeIndex,
    hostpid: String,
}
#[derive(Debug)]
struct FinishedEvent {
    id: NodeIndex,
    hostpid: String,
    exit_status: i32,
    stdout: String,
    stderr: String,
}
enum CompletedEvent {
    Started(StartedEvent),
    Finished(FinishedEvent),
}

// https://users.rust-lang.org/t/group-iterator-into-hashmap-k-vec-v/31727/2
fn group<K, V, I>(iter: I) -> HashMap<K, Vec<V>>
where
    K: Eq + std::hash::Hash,
    I: Iterator<Item = (K, V)>,
{
    let mut hash_map = match iter.size_hint() {
        (_, Some(len)) => HashMap::with_capacity(len),
        (len, None) => HashMap::with_capacity(len),
    };

    for (key, value) in iter {
        hash_map
            .entry(key)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(value)
    }

    hash_map
}
