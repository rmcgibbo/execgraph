use crate::{
    async_flag::Flag,
    constants::{FAIL_COMMAND_PREFIX, FIZZLED_TIME_CUTOFF},
    execgraph::Cmd,
    logfile2::{self, LogFileRW},
    utils::{AwaitableCounter, CancellationState, CancellationToken},
};
use anyhow::{Context, Result};
use bitvec::array::BitArray;
use dashmap::DashMap;
use logfile2::{LogEntry, LogFile, ValueMaps};

use petgraph::prelude::*;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};
use thiserror::Error;

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
    n_fizzled: u32,

    stdout: grep_cli::StandardStream,
    g: Arc<Graph<&'a Cmd, (), Directed>>,
    logfile: &'a mut LogFile<LogFileRW>,
    ready: Option<[async_priority_channel::Sender<TaskItem, u32>; NUM_RUNNER_TYPES]>,
    completed: async_channel::Receiver<Event>,
    n_success: u32,
    n_pending: u32,
    pending_increased_event: Arc<AwaitableCounter>,
    count_offset: u32,
    failures_allowed: u32,
    queuestate: Arc<DashMap<u64, Snapshot>>,
    inflight: HashMap<NodeIndex, std::time::Instant>,
    statuses: HashMap<NodeIndex, TaskStatus>,
    shutdown_state: ShutdownState,
}

#[derive(Debug)]
pub struct ReadyTrackerClient {
    queuestate: Arc<DashMap<u64, Snapshot>>,
    pending_increased_event: Arc<AwaitableCounter>,
    s: async_channel::Sender<Event>,
    r: [async_priority_channel::Receiver<TaskItem, u32>; NUM_RUNNER_TYPES],
}

struct TaskStatus {
    n_unmet_deps: usize,
    poisoned: bool,
}

pub fn new_ready_tracker<'a>(
    g: Arc<DiGraph<&'a Cmd, ()>>,
    logfile: &'a mut LogFile<LogFileRW>,
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

    let queuestate = Arc::new(DashMap::new());
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

    let pending_increased_event = Arc::new(AwaitableCounter::new());

    (
        ReadyTrackerServer {
            finished_order: vec![],
            g,
            stdout: grep_cli::stdout(termcolor::ColorChoice::AlwaysAnsi),
            ready: Some(ready_s.try_into().unwrap()),
            completed: finished_r,
            n_failed: 0,
            n_fizzled: 0,
            n_success: 0,
            n_pending: 0,
            pending_increased_event: pending_increased_event.clone(),
            count_offset,
            failures_allowed,
            queuestate: queuestate.clone(),
            inflight: HashMap::new(),
            logfile,
            statuses,
            shutdown_state: ShutdownState::Normal,
        },
        ReadyTrackerClient {
            r: ready_r.try_into().unwrap(),
            s: finished_s,
            queuestate,
            pending_increased_event,
        },
    )
}

impl<'a> ReadyTrackerServer<'a> {
    #[tracing::instrument(skip_all)]
    pub fn drain(&mut self) -> Result<()> {
        let mut inflight = self.inflight.clone();
        log::debug!("Draining {} inflight tasks", inflight.len());
        loop {
            match self.completed.try_recv() {
                Ok(Event::Started(e)) => {
                    let cmd = self.g[e.id];
                    self.logfile
                        .write(LogEntry::new_started(&cmd.key, "host", 0))?;
                    assert!(inflight.insert(e.id, Instant::now()).is_none());
                }
                Ok(Event::Finished(e)) => {
                    let cmd = self.g[e.id];
                    self._finished_bookkeeping_1(&e)?;
                    if inflight.remove(&e.id).is_none() {
                        // With execgraph-remote workers, it's  possible to get a FinishedEvent
                        // without having previously received a StartedEvent because of the heartbeat
                        // (ping timeout)-caused disconnect happening before /begun request was
                        // transmitted. Maybe the /begun request was just lost into the ether because
                        // the task was started on a node that was so slow it never was able to send either
                        // a ping or a /begun.
                        //
                        // But let's try to preserve the invariant that every Finished entry in the log
                        // is preceeded by a Started entry, which means that we need to fabricate a fake
                        // Started entry.
                        self.logfile.write(LogEntry::new_started(&cmd.key, "", 0))?;
                    }
                    self.logfile.write(LogEntry::new_finished(
                        &cmd.key,
                        e.status.as_i32(),
                        e.values,
                    ))?;
                }
                Err(_) => {
                    break;
                }
            }
        }

        self.inflight.clear();
        log::debug!("Writing {} FinishedEvents", inflight.len());
        for (k, _) in inflight.iter() {
            let cmd = self.g[*k];
            let e = FinishedEvent::new_disconnected(k.to_owned(), "".to_owned());
            self._finished_bookkeeping_1(&e)?;
            self.logfile.write(LogEntry::new_finished(
                &cmd.key,
                e.status.as_i32(),
                e.values,
            ))?;
        }
        log::debug!("Finished drain");
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
        let mut ctrl_c = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut flush_ticker = tokio::time::interval(std::time::Duration::from_millis(100));

        // every time we put a task into the ready queue, we increment n_pending.
        // every time a task completes, we decrement n_pending.
        // when n_pending goes to zero, then the whole process is finished.
        // note that we don't actuall track the number currently inside the ready queue
        // or inside other stages of processing, because there are going to be
        // accounting bugs that way.

        loop {
            tokio::select! {
                _ = flush_ticker.tick() => {
                    // if you have few long-running tasks, it's pretty annoying if the log file doesn't
                    // update somewhat frequently. flushing every 100ms is a balance between not wanting
                    // too many syscalls for workflows that are generating thousands of events per second
                    // and wanting to see the log for ones that are generating one event per hour.
                    self.logfile.flush()?;
                    self.stdout.flush()?
                },
                event = self.completed.recv() => {
                    if event.is_err() {
                        self.ready = None;
                        break;
                    }
                    match event.unwrap() {
                        Event::Started(e) => {
                            let cmd = self.g[e.id];
                            self.logfile.write(LogEntry::new_started(
                                &cmd.key,
                                &e.host,
                                e.pid,
                            ))?;
                            assert!(self.inflight.insert(e.id, Instant::now()).is_none());
                        }
                        Event::Finished(mut e) => {
                            #[cfg(feature = "coz")]
                            {
                                coz::progress!();
                            }
                            let cmd = self.g[e.id];
                            self.finished_order.push(e.id);
                            self._finished_bookkeeping(&mut e).await?;

                            if self.inflight.remove(&e.id).is_none() {
                                // With execgraph-remote workers, it's  possible to get a FinishedEvent
                                // without having previously received a StartedEvent because of the heartbeat
                                // (ping timeout)-caused disconnect happening before /begun request was
                                // transmitted. Maybe the /begun request was just lost into the ether because
                                // the task was started on a node that was so slow it never was able to send either
                                // a ping or a /begun.
                                //
                                // But let's try to preserve the invariant that every Finished entry in the log
                                // is preceeded by a Started entry, which means that we need to fabricate a fake
                                // Started entry.
                                self.logfile
                                    .write(LogEntry::new_started(&cmd.key, "", 0))?;
                            }
                            self.logfile.write(LogEntry::new_finished(
                                &cmd.key,
                                e.status.as_i32(),
                                e.values,
                            ))?;

                            if self.n_fizzled >= self.failures_allowed {
                                tracing::debug!("background serve triggering soft shutdown because n_bootfailed={} >= failures_allowed={}. note n_pending={}",
                                self.n_fizzled, self.failures_allowed, self.n_pending);
                                token.cancel(CancellationState::CancelledAfterTime(Instant::now() - FIZZLED_TIME_CUTOFF));
                                self.ready = None;
                                self.shutdown_state = ShutdownState::SoftShutdown;
                            }

                            if self.n_pending == 0 || (self.shutdown_state == ShutdownState::SoftShutdown && self.inflight.is_empty()) {
                                // drop the send side of the channel. this will cause the receive side
                                // to start returning errors, which is exactly what we want and will
                                // break the run_local_process_loop runners at the point where they're
                                // waiting to get a task from the ready task receiver
                                self.ready = None;
                                tracing::debug!("background_serve breaking on n_failed={} failures_allowed={} n_pending={}",
                                    self.n_failed, self.failures_allowed, self.n_pending);
                                break;
                            }
                        }
                    }
                },
                _ = token.hard_cancelled() => {
                    tracing::debug!("background_serve breaking on token cancellation");
                    self.ready = None;
                    break;
                }
                _ = ctrl_c.recv() => {
                    tracing::debug!("background_serve breaking on ctrl-c");
                    self.ready = None;
                    break;
                }
            };
        }

        Ok(())
    }

    async fn _finished_bookkeeping(&mut self, e: &mut FinishedEvent) -> Result<()> {
        self._finished_bookkeeping_1(e)?;
        let out = self._finished_bookkeeping_2(e).await;
        if let Some(ref mut flag) = &mut e.flag {
            flag.set();
        }
        out
    }

    fn _finished_bookkeeping_1(&mut self, e: &FinishedEvent) -> Result<()> {
        let is_success = e.status.is_success();
        let total: u32 = self.statuses.len().try_into().unwrap();
        let cmd = self.g[e.id];

        // elapsed is none if the task never started, which happens if we're being
        // called during the drain() shutdown phase on tasks that never began.
        let elapsed = self
            .inflight
            .get(&e.id)
            .map(|&started| Instant::now() - started);

        {
            self.n_pending = self.n_pending.saturating_sub(1);
            let mut v = self
                .queuestate
                .get_mut(&cmd.affinity.data)
                .expect("No such queue");
            v.value_mut().num_inflight = v.value().num_inflight.checked_sub(1).expect("Underflow");
        }

        if is_success {
            self.n_success += 1;
            writeln!(
                self.stdout,
                "[{}/{}] {}",
                self.n_success + self.n_failed + self.count_offset,
                total + self.count_offset,
                cmd.display()
            )?;
        } else {
            self.n_failed += 1;
            let fizzled = matches!(elapsed, Some(elapsed) if elapsed < FIZZLED_TIME_CUTOFF);
            if fizzled {
                self.n_fizzled += 1;
            }

            if self.shutdown_state != ShutdownState::SoftShutdown {
                eprintln!(
                    "\x1b[1;31m{}:\x1b[0m {}{}.{:x}: {}",
                    e.status.fail_description(fizzled),
                    FAIL_COMMAND_PREFIX,
                    cmd.key,
                    cmd.runcount,
                    cmd.display()
                );
            }
        }

        if self.shutdown_state != ShutdownState::SoftShutdown {
            if !e.stdout.is_empty() {
                write!(self.stdout, "{}", e.stdout)?;
            }
            if !e.stderr.is_empty() {
                eprint!("{}", e.stderr);
            }
        }

        Ok(())
    }

    async fn _finished_bookkeeping_2(&mut self, e: &FinishedEvent) -> Result<()> {
        if self.shutdown_state == ShutdownState::SoftShutdown {
            // if we're in a soft shutdown state, that's because we've seen a failure and
            // we're not submitting any more tasks to the ready queue and we've triggered
            // a cancellation token to prevent the ready side from even taking any more tasks
            // out of the ready queue and dispatching them, so there's no point in doing this
            // bookkeeping and adding new tasks to the ready queue. and frankly we've already
            // dropped the ready queue channel, so if we keep going in this task there's nothing
            // we can do with self.ready
            assert!(self.ready.is_none());
            return Ok(());
        }

        let is_success = e.status.is_success();
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
        if ready.is_empty() {
            return Ok(());
        }
        self.n_pending = u32checked_add!(self.n_pending, ready.len());
        let mut inserts = vec![vec![]; NUM_RUNNER_TYPES];

        for index in ready.into_iter() {
            let cmd = self.g[index];
            let priority = cmd.priority;
            self.logfile.write(LogEntry::new_ready(
                &cmd.key,
                cmd.runcount,
                &cmd.display(),
                cmd.storage_root,
            ))?;
            self.queuestate
                .entry(cmd.affinity.data)
                .or_insert_with(|| Snapshot {
                    num_ready: 0,
                    num_inflight: 0,
                })
                .num_ready += 1;

            let taken = Arc::new(AtomicBool::new(false));

            for i in cmd.affinity.iter_ones() {
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

        self.pending_increased_event.incr(); // fire an event

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
                let mut x = self.queuestate.get_mut(&task.affinity.data).expect("bar");
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
                let mut x = self.queuestate.get_mut(&task.affinity.data).expect("bar");
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
            .send(Event::Started(StartedEvent {
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
    pub async fn send_finished(&self, cmd: &Cmd, event: FinishedEvent) {
        cmd.call_postamble();
        let r = self.s.send(Event::Finished(event)).await;
        if r.is_err() {
            tracing::debug!("send_finished: cannot send to channel: {:#?}", r);
        };
    }

    /// Retreive a snapshot of the state of the queue
    /// etag: only return once the pending_increased_event
    ///       has fired at least this number of times. the idea with this is that
    ///       it lets a provisioner do long polling and get a response right after
    ///       the number of pending tasks might have increased, which is probably
    ///       a good time for it to get more compute resources.
    /// timemin: sort of in the same vein as etag, but from the other side. don't return
    ///       for at least this amount of time. Let's sat that pending_increased_event
    ///       is firing very frequently, the provisioner might have some kind of rate
    ///       limit so it's not going to take any action that frequently anyways, so it
    ///       might want to communicate that rate limit here so that it can get the queue
    ///       state after a minimum of a couple seconds.
    pub async fn get_queuestate(
        &self,
        etag: u64,
        timemin: Duration,
        timeout: Duration,
    ) -> (u64, HashMap<u64, Snapshot>) {
        let clock_start = tokio::time::Instant::now();

        let mut etag_new =
            match tokio::time::timeout(timeout, self.pending_increased_event.changed(etag)).await {
                Ok(etag_new) => etag_new,
                Err(_elapsed) => self.pending_increased_event.load(),
            };

        let elapsed = tokio::time::Instant::now() - clock_start;
        if elapsed < timemin {
            tokio::time::sleep_until(clock_start + timemin).await;
            etag_new = self.pending_increased_event.load();
        }

        (
            etag_new,
            self.queuestate
                .iter()
                .map(|r| (*r.key(), r.value().clone()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

struct StartedEvent {
    id: NodeIndex,
    host: String,
    pid: u32,
}
pub struct FinishedEvent {
    /// id of the command that finished in the graph
    pub id: NodeIndex,
    /// exit code of the command
    pub status: ExitStatus,
    /// stdout from the command
    pub stdout: String,
    /// stderr from the command
    pub stderr: String,
    /// key-value pairs from the command produced on fd3
    pub values: ValueMaps,
    /// an optional event that will be triggered once the servicer thread has finished
    /// processing downstream dependencies of this task.
    pub flag: Option<Flag>,
}
impl FinishedEvent {
    pub fn new_cancelled(id: NodeIndex) -> Self {
        FinishedEvent {
            id,
            status: ExitStatus::Cancelled,
            stdout: "".to_string(),
            stderr: "".to_string(),
            values: vec![],
            flag: None,
        }
    }
    pub fn new_disconnected(id: NodeIndex, stderr: String) -> Self {
        FinishedEvent {
            id,
            status: ExitStatus::Disconnected,
            stdout: "".to_string(),
            stderr: stderr,
            values: vec![],
            flag: None,
        }
    }

    pub fn new_error(id: NodeIndex, code: i32, stderr: String) -> Self {
        FinishedEvent {
            id,
            status: ExitStatus::Code(code),
            stdout: "".to_owned(),
            stderr: stderr,
            values: ValueMaps::new(),
            flag: None,
        }
    }
}

enum Event {
    Started(StartedEvent),
    Finished(FinishedEvent),
}

#[derive(Debug)]
pub enum ExitStatus {
    Code(i32),
    Cancelled,
    Disconnected,
}

impl ExitStatus {
    pub fn as_i32(&self) -> i32 {
        match self {
            ExitStatus::Code(c) => *c,
            ExitStatus::Cancelled => 130,
            ExitStatus::Disconnected => 127,
        }
    }
    fn is_success(&self) -> bool {
        matches!(self, ExitStatus::Code(0))
    }

    fn fail_description(&self, fizzled: bool) -> &str {
        assert!(!self.is_success());
        match self {
            ExitStatus::Code(_) if fizzled => "FIZZLED",
            ExitStatus::Code(_) => "FAILED",
            ExitStatus::Cancelled => "CANCELLED",
            ExitStatus::Disconnected => "DISCONNECTED",
        }
    }
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

#[derive(Eq, PartialEq)]
enum ShutdownState {
    Normal,
    SoftShutdown,
}
