use crate::{execgraph::Cmd, logfile2};
use anyhow::{Context, Result};
use bitvec::array::BitArray;
use logfile2::{LogEntry, LogFile, ValueMaps};
use petgraph::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc, Mutex,
    },
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const FAIL_COMMAND_PREFIX: &str = "wrk/";

macro_rules! u32checked_add {
    ($a:expr,$b:expr) => {{
        $a.checked_add(u32::try_from($b).expect("arithmetic overflow"))
            .expect("arithmetic overflow")
    }};
}

// Each task has a vec of features that it requires,
// which are abstract to us but are like ["gpu", "remote", "..."]
// they also might be like ["!remote"].
//
// Each runner will have a list of capabilties, and we'll
// match the runner against the capabilities to determine
// whether the task is eligible to run on the runner.
//
// the local runner will have the capability ["local"]
//

const NUM_RUNNER_TYPES: usize = 64;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub num_ready: u32,
    pub num_inflight: u32,
}

#[derive(Debug, Clone)]
struct TaskItem {
    id: NodeIndex,
    affinity: BitArray<u64>,
    taken: Arc<AtomicBool>,
}

pub struct ReadyTrackerServer<'a> {
    pub finished_order: Vec<NodeIndex>,
    pub n_failed: u32,

    g: Arc<Graph<&'a Cmd, (), Directed>>,
    logfile: &'a mut LogFile,
    ready: Option<[async_priority_channel::Sender<TaskItem, u32>; NUM_RUNNER_TYPES]>,
    completed: async_channel::Receiver<CompletedEvent>,
    n_success: u32,
    n_pending: u32,
    count_offset: u32,
    failures_allowed: u32,
    queuestate: Arc<Mutex<HashMap<BitArray<u64>, Snapshot>>>,
    inflight: HashSet<NodeIndex>,
    statuses: HashMap<NodeIndex, TaskStatus>,
}

#[derive(Debug)]
pub struct ReadyTrackerClient {
    queuestate: Arc<Mutex<HashMap<BitArray<u64>, Snapshot>>>,
    s: async_channel::Sender<CompletedEvent>,
    r: [async_priority_channel::Receiver<TaskItem, u32>; NUM_RUNNER_TYPES],
}

struct TaskStatus {
    n_unmet_deps: usize,
    poisoned: bool,
}

pub fn new_ready_tracker<'a>(
    g: Arc<DiGraph<&'a Cmd, ()>>,
    logfile: &'a mut LogFile,
    count_offset: u32,
    failures_allowed: u32,
) -> (ReadyTrackerServer<'a>, ReadyTrackerClient) {
    let mut ready_s = vec![];
    let mut ready_r = vec![];

    // Create empty channels for each runnertype
    for _ in 0..NUM_RUNNER_TYPES {
        let (sender, receiver) = async_priority_channel::unbounded();
        ready_s.push(sender);
        ready_r.push(receiver);
    }

    let queuestate = Arc::new(Mutex::new(HashMap::new()));
    let (finished_s, finished_r) = async_channel::unbounded();

    // for each task, how many unmet first-order dependencies does it have?
    let statuses: HashMap<NodeIndex, TaskStatus> = g
        .node_indices()
        .map(|i| {
            (
                i,
                TaskStatus {
                    n_unmet_deps: g.edges_directed(i, Direction::Incoming).count(),
                    poisoned: false,
                },
            )
        })
        .collect();

    (
        ReadyTrackerServer {
            finished_order: vec![],
            g,
            ready: Some(ready_s.try_into().unwrap()),
            completed: finished_r,
            n_failed: 0,
            n_success: 0,
            n_pending: 0,
            count_offset,
            failures_allowed,
            queuestate: queuestate.clone(),
            inflight: HashSet::new(),
            logfile,
            statuses,
        },
        ReadyTrackerClient {
            r: ready_r.try_into().unwrap(),
            s: finished_s,
            queuestate,
        },
    )
}

impl<'a> ReadyTrackerServer<'a> {
    #[tracing::instrument(skip_all)]
    pub fn drain(&mut self) -> Result<()> {
        let mut inflight = self.inflight.clone();
        loop {
            match self.completed.try_recv() {
                Ok(CompletedEvent::Started(e)) => {
                    let cmd = self.g[e.id];
                    self.logfile
                        .write(LogEntry::new_started(&cmd.key, "host", 0))?;
                    assert!(inflight.insert(e.id));
                }
                Ok(CompletedEvent::Finished(e)) => {
                    let cmd = self.g[e.id];
                    self._finished_bookkeeping_1(&e)?;
                    self.logfile
                        .write(LogEntry::new_finished(&cmd.key, e.status, e.values))?;
                    assert!(inflight.remove(&e.id));
                }
                Err(_) => {
                    break;
                }
            }
        }

        self.inflight.clear();
        for k in inflight.iter() {
            let timeout_status = 130;
            let cmd = self.g[*k];
            let e = FinishedEvent {
                id: k.to_owned(),
                status: timeout_status,
                stdout: "".to_string(),
                stderr: "".to_string(),
                values: ValueMaps::new(),
            };
            self._finished_bookkeeping_1(&e)?;
            self.logfile
                .write(LogEntry::new_finished(&cmd.key, e.status, e.values))?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn background_serve(&mut self, token: CancellationToken) -> Result<()> {
        // trigger all of the tasks that have zero unmet dependencies
        self.add_to_ready_queue(
            self.statuses
                .iter()
                .filter_map(|(k, v)| if v.n_unmet_deps == 0 { Some(*k) } else { None })
                .collect(),
        )
        .await
        .context("couldn't add to ready queue")?;

        // every time we put a task into the ready queue, we increment n_pending.
        // every time a task completes, we decrement n_pending.
        // when n_pending goes to zero, then the whole process is finished.
        // note that we don't actuall track the number currently inside the ready queue
        // or inside other stages of processing, because there are going to be
        // accounting bugs that way.

        loop {
            tokio::select! {
                event = self.completed.recv() => {
                    if event.is_err() {
                        tracing::error!("{:#?}", event);
                        self.ready = None;
                        break;
                    }
                    match event.unwrap() {
                        CompletedEvent::Started(e) => {
                            let cmd = self.g[e.id];
                            self.logfile.write(LogEntry::new_started(
                                &cmd.key,
                                &e.host,
                                e.pid,
                            ))?;
                            assert!(self.inflight.insert(e.id));
                        }
                        CompletedEvent::Finished(e) => {
                            let cmd = self.g[e.id];
                            self.finished_order.push(e.id);
                            self._finished_bookkeeping(&e).await?;
                            self.logfile.write(LogEntry::new_finished(
                                &cmd.key,
                                e.status,
                                e.values,
                            ))?;
                            assert!(self.inflight.remove(&e.id));

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
                    tracing::debug!("background_serve breaking on token cancellation");
                    self.ready = None;
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::debug!("background_serve breaking on ctrl-c");
                    self.ready = None;
                    break;
                }
            };
        }

        Ok(())
    }

    async fn _finished_bookkeeping(&mut self, e: &FinishedEvent) -> Result<()> {
        self._finished_bookkeeping_1(e)?;
        self._finished_bookkeeping_2(e).await
    }

    fn _finished_bookkeeping_1(&mut self, e: &FinishedEvent) -> Result<()> {
        let is_success = e.status == 0;
        let total = self.statuses.len() as u32;
        let cmd = self.g[e.id];

        {
            self.n_pending -= 1;
            let mut locked = self
                .queuestate
                .lock()
                .expect("Something must have panicked while holding this lock?");
            let num_inflight = &mut locked
                .get_mut(&cmd.affinity)
                .expect("No such queue")
                .num_inflight;
            *num_inflight = num_inflight.checked_sub(1).expect("Underflow");
        }

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

        Ok(())
    }

    async fn _finished_bookkeeping_2(&mut self, e: &FinishedEvent) -> Result<()> {
        let is_success = e.status == 0;
        let statuses = &mut self.statuses;
        let ready = self
            .g
            .edges_directed(e.id, Direction::Outgoing)
            .filter_map(|edge| {
                let downstream_id = edge.target();
                let mut status = statuses.get_mut(&downstream_id).expect("key doesn't exist");
                if is_success {
                    if !status.poisoned {
                        status.n_unmet_deps -= 1;
                        if status.n_unmet_deps == 0 {
                            return Some(downstream_id);
                        }
                    }
                } else {
                    // failed. mark any (first-order) downstream tasks as poisoned.
                    status.poisoned = true;
                }

                None
            })
            .collect();
        self.add_to_ready_queue(ready).await?;
        Ok(())
    }

    async fn add_to_ready_queue(
        &mut self,
        ready: Vec<NodeIndex>, // cmd index and priority
    ) -> Result<()> {
        let mut queuestate_lock = self.queuestate.lock().expect("Panic whole holding lock?");
        self.n_pending = u32checked_add!(self.n_pending, ready.len());
        let mut inserts = vec![vec![]; NUM_RUNNER_TYPES];

        for index in ready.into_iter() {
            let cmd = self.g[index];
            let priority = cmd.priority;
            self.logfile
                .write(LogEntry::new_ready(&cmd.key, cmd.runcount, &cmd.display()))?;
            queuestate_lock
                .entry(cmd.affinity)
                .or_insert_with(|| Snapshot {
                    num_ready: 0,
                    num_inflight: 0,
                })
                .num_ready += 1;

            let taken = Arc::new(AtomicBool::new(false));
            for i in cmd
                .affinity
                .iter()
                .enumerate()
                .filter_map(|(i, b)| if *b { Some(i) } else { None })
            {
                inserts[i].push((
                    TaskItem {
                        id: index,
                        taken: taken.clone(),
                        affinity: cmd.affinity,
                    },
                    priority,
                ));
            }
        }

        drop(queuestate_lock);

        let channels = self
            .ready
            .as_mut()
            .expect("send channel was already dropped?");
        for (i, ready) in inserts.into_iter().enumerate() {
            if !ready.is_empty() {
                if let Err(e) = channels[i].sendv(ready.into_iter().peekable()).await {
                    tracing::error!("Perhaps TOCTOU? {}", e);
                }
            }
        }

        Ok(())
    }
}

impl ReadyTrackerClient {
    pub fn try_recv(&self, runnertypeid: u32) -> Result<NodeIndex, ReadyTrackerClientError> {
        let reciever = self
            .r
            .get(runnertypeid as usize)
            .ok_or(ReadyTrackerClientError::NoSuchRunnerType)?;

        // Each task might be in multiple ready queues, one for each runner type that it's
        // eligible for. So within the ready queue we store an AtomicBool that represents
        // whether this item has already been claimed. A different way to implement this would
        // be to pop off receiver channel and then mutate all of the other channels to remove
        // the item, but instead we're using this tombstone idea.
        loop {
            let (task, _priority) = reciever.try_recv()?;
            let was_taken = task.taken.fetch_or(true, SeqCst);
            if !was_taken {
                let mut s = self.queuestate.lock().expect("foo");
                let x = s.get_mut(&task.affinity).expect("bar");
                x.num_ready -= 1;
                x.num_inflight += 1;

                return Ok(task.id);
            }
        }
    }

    pub async fn recv(&self, runnertypeid: u32) -> Result<NodeIndex, ReadyTrackerClientError> {
        let reciever = self
            .r
            .get(runnertypeid as usize)
            .ok_or(ReadyTrackerClientError::NoSuchRunnerType)?;

        // Each task might be in multiple ready queues, one for each runner type that it's
        // eligible for. So within the ready queue we store an AtomicBool that represents
        // whether this item has already been claimed. A different way to implement this would
        // be to pop off receiver channel and then mutate all of the other channels to remove
        // the item, but instead we're using this tombstone idea.
        loop {
            let (task, _priority) = reciever.recv().await?;
            if !task.taken.fetch_or(true, SeqCst) {
                let mut s = self.queuestate.lock().expect("foo");
                let x = s.get_mut(&task.affinity).expect("bar");
                x.num_ready -= 1;
                x.num_inflight += 1;

                return Ok(task.id);
            }
        }
    }

    /// When a task is started, notify the tracker by calling this.
    pub async fn send_started(&self, v: NodeIndex, cmd: &Cmd, host: &str, pid: u32) {
        cmd.call_preamble();
        let r = self
            .s
            .send(CompletedEvent::Started(StartedEvent {
                id: v,
                host: host.to_string(),
                pid,
            }))
            .await;
        if r.is_err() {
            tracing::debug!("send_started: cannot send to channel: {:#?}", r);
        };
    }

    /// When a task is finished, notify the tracker by calling this.
    pub async fn send_finished(
        &self,
        v: NodeIndex,
        cmd: &Cmd,
        status: i32,
        stdout: String,
        stderr: String,
        values: ValueMaps,
    ) {
        cmd.call_postamble();
        let r = self
            .s
            .send(CompletedEvent::Finished(FinishedEvent {
                id: v,
                status,
                stdout,
                stderr,
                values,
            }))
            .await;
        if r.is_err() {
            tracing::debug!("send_finished: cannot send to channel: {:#?}", r);
        };
    }

    /// Retreive a snapshot of the state of the queue
    pub fn get_queuestate(&self) -> HashMap<BitArray<u64>, Snapshot> {
        self.queuestate
            .lock()
            .expect("Something must have panicked while holding this lock")
            .clone()
    }
}

#[derive(Debug)]
struct StartedEvent {
    id: NodeIndex,
    host: String,
    pid: u32,
}
#[derive(Debug)]
struct FinishedEvent {
    id: NodeIndex,
    status: i32,
    stdout: String,
    stderr: String,
    values: ValueMaps,
}
#[derive(Debug)]
enum CompletedEvent {
    Started(StartedEvent),
    Finished(FinishedEvent),
}

#[derive(Debug, Error)]
pub enum ReadyTrackerClientError {
    #[error("{0}")]
    ChannelRecvError(
        #[source]
        #[from]
        async_priority_channel::RecvError,
    ),

    #[error("{0}")]
    ChannelTryRecvError(
        #[source]
        #[from]
        async_priority_channel::TryRecvError,
    ),

    #[error("No such runner type")]
    NoSuchRunnerType,
}
