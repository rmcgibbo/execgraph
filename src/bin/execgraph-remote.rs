use anyhow::Context;
use async_channel::bounded;
use clap::Parser;
use execgraph::{
    http_extensions::reqwest::{RequestBuilderExt, ResponseExt},
    httpinterface::*,
    localrunner::{
        wait_with_output, ChildOutput, ChildProcessError
    },
};
use execgraph::logfile2::ValueMaps;
use gethostname::gethostname;
use hyper::StatusCode;

use execgraph::constants::HOLD_RATETIMITED_TIME;
use nix::unistd::Pid;
use notify::Watcher;
use reqwest::header::{HeaderMap, HeaderValue};
use std::{convert::TryInto, io::Read, os::unix::prelude::AsRawFd, sync::atomic::AtomicBool, time::Duration};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_command_fds::{CommandFdExt, FdMapping};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use whoami::username;

// https://github.com/SchedMD/slurm/blob/791f9c39e0db919e02ef8857be0faff09a3656b2/src/slurmd/slurmstepd/req.c#L724
const FINAL_SLURM_ERROR_MESSAGE_STRINGS: &str = "CANCELLED|FAILED|ERROR";

// send SIGTERM to child this long before the `duration` expires.
const SEND_SIGTERM_BEFORE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
const ONE_YEAR: std::time::Duration = std::time::Duration::from_secs(31536000);
static SIGUSR1_RECEIVED: AtomicBool = AtomicBool::new(false);

lazy_static::lazy_static! {
    static ref START_TIME: std::time::Instant = std::time::Instant::now();
    static ref SLURM_JOBID: String = (|| {
        let slurm_array_job_id = std::env::var("SLURM_ARRAY_JOB_ID").unwrap_or_else(|_| "".to_string());
        let slurm_array_task_id = std::env::var("SLURM_ARRAY_TASK_ID").unwrap_or_else(|_| "".to_string());
        let slurm_jobid = format!("{}_{}", slurm_array_job_id, slurm_array_task_id);
        slurm_jobid
    })();
}

#[derive(Debug, Parser)]
#[clap(name = "execgraph-remote")]
struct CommandLineArguments {
    /// Url of controller
    url: String,

    /// "Runner type", a number between 0 and 63 signifying the features of this node
    runnertypeid: u32,

    /// Stop accepting new tasks after this amount of time, in seconds.
    #[clap(long = "max-time-accepting-tasks", value_parser = parse_seconds)]
    max_time_accepting_tasks: Option<Duration>,

    /// Total duration.
    #[clap(long = "duration", value_parser = parse_seconds)]
    duration: Option<Duration>,

    /// Path to the log file where slurmstepd will write errors. This should be a file
    /// on a local disk, so that we can watch it with inotify and report back any errors
    /// to the controller.
    #[clap(long = "slurm-error-logfile", value_parser = parse_slurm_error_logfile)]
    slurm_error_logfile: Option<std::path::PathBuf>,

    /// Error message to be echoed on the controller side in leu of our tasks's standard error
    /// if the controller looses contact with this runner due to a network partition or node
    /// failure.
    #[clap(long = "disconnect-err-msg", value_parser = parse_disconnect_error_message, default_value="")]
    disconnect_error_message: String,
}

#[cfg(target_os = "linux")]
fn set_current_process_as_child_subreaper() {
    unsafe {
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0 {
            tracing::error!("Unable to set myself as the subreaper");
            std::process::exit(1);
        }
    }
}

#[cfg(target_os = "macos")]
fn set_current_process_as_child_subreaper() {}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), RemoteError> {
    lazy_static::initialize(&START_TIME);
    unsafe {
        time::util::local_offset::set_soundness(time::util::local_offset::Soundness::Unsound);
    } // YOLO
    tracing_subscriber::fmt()
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    set_current_process_as_child_subreaper();

    tokio::spawn(async move {
        if let Ok(mut stream) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1()) {
            loop {
                stream.recv().await;
                SIGUSR1_RECEIVED.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        } else {
            tracing::error!("Unable to install SIGUSR1 signal handler");
            std::process::exit(1);
        }
    });

    let authorization_token = std::env::var("EXECGRAPH_AUTHORIZATION_TOKEN")
        .context("Reading EXECGRAPH_AUTHORIZATION_TOKEN")
        .unwrap();
    std::env::remove_var("EXECGRAPH_AUTHORIZATION_TOKEN");
    tracing::info!("execgraph-remote SLURM_JOB_ID={}", *SLURM_JOBID);
    tracing::info!("execgraph-remote pid={}", std::process::id());
    unsafe {
        libc::setpgid(libc::getpid(), libc::getpid());
    }

    let opt = CommandLineArguments::parse();
    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", authorization_token))?,
    );
    headers.insert(
        "X-EXECGRAPH-USERNAME",
        HeaderValue::from_bytes(username().as_bytes())?,
    );
    headers.insert(
        "X-EXECGRAPH-SLURM-JOB-ID",
        HeaderValue::from_bytes((*SLURM_JOBID).as_bytes())?,
    );
    headers.insert(
        "X-EXECGRAPH-HOSTNAME",
        HeaderValue::from_bytes(
            gethostname()
                .to_str()
                .ok_or_else(|| {
                    RemoteError::InvalidHostname(format!(
                        "hostname is not unicode-representable: {}",
                        gethostname().to_string_lossy()
                    ))
                })?
                .as_bytes(),
        )?,
    );
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .http2_prior_knowledge()
        .timeout(2 * HOLD_RATETIMITED_TIME)
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let base = reqwest::Url::parse(&opt.url)?;

    // Slurm error logfile events Notify
    let (watcher, mut rx) = async_watcher(opt.slurm_error_logfile.clone())?;

    let mut timing_info = TimingInfo::default();
    let mut start_response = None;
    loop {
        match run_command(
            &opt,
            &base,
            &client,
            &mut rx,
            start_response,
            SigtermChildBeforeSchedulerDeadline::new(&opt),
        )
        .await
        {
            Err(e) => {
                tracing::info!("Finishing loop due to {:#?}", e);
                break;
            }
            Ok((this_timing_info, this_start_response)) => {
                timing_info += this_timing_info;
                start_response = this_start_response;
                if start_response.is_none() {
                    break;
                }
            }
        };
    }
    let total_duration = std::time::Instant::now() - *START_TIME;
    timing_info.debug_average_times();
    if timing_info.n_commands > 0 {
        tracing::debug!(
            "Avg total duration: {:#?}",
            total_duration / timing_info.n_commands
        );
    }

    cleanup_child_processes_carefully();
    drop(watcher);
    Ok(())
}

async fn run_command(
    opt: &CommandLineArguments,
    base: &reqwest::Url,
    client: &reqwest::Client,
    notify_rx: &mut tokio::sync::mpsc::Receiver<String>,
    start: Option<StartResponse>,
    time_to_sigterm_child: SigtermChildBeforeSchedulerDeadline,
) -> Result<(TimingInfo, Option<StartResponse>), RemoteError> {
    let start_route = base.join("start")?;
    let ping_route = base.join("ping")?;
    let begun_route = base.join("begun")?;
    let end_route = base.join("end")?;
    let t_before_start_request = std::time::Instant::now();

    let make_start_request = || StartRequest {
        runnertypeid: opt.runnertypeid,
        disconnect_error_message: opt.disconnect_error_message.clone(),
    };

    let start = match start {
        Some(s) => s,
        None => client
            .get(start_route)
            .postcard(&make_start_request())?
            .send()
            .await
            .context("Unable to send /start")?
            .error_for_status()?
            .postcard::<StartResponse>()
            .await
            .context("Unable to receive /start")?,
    };
    let start_time_request_elapsed = std::time::Instant::now() - t_before_start_request;

    let ping_interval = Duration::from_millis(start.ping_interval_msecs);
    let transaction_id = start.transaction_id;
    let ping_timeout = 2 * ping_interval;
    let token = CancellationToken::new();
    let token1 = token.clone();
    let token2 = token.clone();
    let token3 = token.clone();
    let _drop_guard = token.drop_guard();
    let (pongs_tx, pongs_rx) = bounded::<()>(1);
    // send a pong right at the beginning, because we just received a server message
    // from start(), so that's pretty good
    pongs_tx
        .send(())
        .await
        .expect("This send cannot fail because the channel was just created");

    // Start a background task tha pings the server and puts the pongs into
    // a channel
    let client1 = client.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + ping_interval, ping_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let pongs_tx = pongs_tx.clone();
            let token2 = token1.clone();
            tokio::select! {
                _ = interval.tick() => {
                    let request = client1.post(ping_route.clone())
                        .postcard(&Ping{transaction_id})
                        .expect("Unable to encode ping as postcard?")
                        .send();
                    tokio::spawn(async move {
                        match request.await {
                            Ok(r) if r.status() == StatusCode::OK => {
                                // add to the pongs channel, but if the channel
                                // has been dropped that's okay, just break
                                if pongs_tx.send(()).await.is_err() {
                                    token2.cancel();
                                }
                            }
                            e => {
                                warn!("Ping endpoint response error: {:#?}", e);
                                token2.cancel();
                            }
                        }
                    });
                }
                _ = token1.cancelled() => {
                    break
                }
            }
        }
    });

    // Start a background task that consumes pongs from the channel and signals
    // the cancellation token if it doesn't receive them fast enough
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to = tokio::time::timeout(ping_timeout, pongs_rx.recv()) => {
                    if to.is_err() {
                        warn!("No pong response received without timeout");
                        token2.cancel();
                        break;
                    }
                }
                _ = token2.cancelled() => {
                    break;
                }
            }
        }
    });

    let (read_fd3, write_fd3) = tokio_pipe::pipe().expect("Unable to create pipe");
    let (fd4_read_pipe, fd4_write_pipe) = if start.fd_input.is_some() {
        let (read, write) = tokio_pipe::pipe().expect("Cannot create pipe");
        (Some(read), Some(write))
    } else {
        (None, None)
    };
    let mut fd_mapping = vec![
        // Map the pipe as FD 3 in the child process.
        FdMapping {
            parent_fd: write_fd3.as_raw_fd(),
            child_fd: 3,
        },
    ];
    if fd4_read_pipe.is_some() {
        fd_mapping.push(FdMapping {
            parent_fd: fd4_read_pipe.as_ref().unwrap().as_raw_fd(),
            child_fd: start.fd_input.as_ref().unwrap().0,
        })
    };

    // Run the command and record the pid
    let mut command = tokio::process::Command::new(&start.cmdline[0]);
    let t_before_spawn = std::time::Instant::now();
    let maybe_child = command
        .args(&start.cmdline[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .fd_mappings(fd_mapping)
        .unwrap()
        .spawn();

    // Deadlock potential: After spawning the command and passing it the
    // write end of the pipe we need to close it ourselves
    drop(write_fd3);
    drop(fd4_read_pipe);

    let child = match maybe_child {
        Ok(child) => child,
        Err(_) => {
            // Tell the server that we've started the command
            tokio::select! {
                value = client.post(begun_route)
                .postcard(&BegunRequest{
                    transaction_id,
                    host: gethostname().to_string_lossy().to_string(),
                    pid: 0,
                })?
                .send() => {
                    value.context("Unable to pose to /begun")?
                    .error_for_status()
                    .context("Receiving from /begun")?;
                }
                _ = token3.cancelled() => {
                    return Err(RemoteError::PingTimeout("Failed to receive server pong (290)".to_owned()))
                }
            };

            // Tell the server that we've finished the command
            tokio::select! {
                value = client.post(end_route)
                .postcard(&EndRequest{
                    transaction_id,
                    status: 13,
                    stdout: "".to_owned(),
                    stderr: format!("No such command: {:#?}", &start.cmdline[0]),
                    values: ValueMaps::new(),
                    start_request: still_accepting_tasks(opt).then(make_start_request),
                })?
                .send() => {
                    let r = value?
                        .error_for_status()?
                        .postcard::<EndResponse>()
                        .await?;
                    return Ok((TimingInfo::default(), r.start_response))
                }
                _ = token3.cancelled() => {
                    return Err(RemoteError::PingTimeout("Failed to receive server pong (313)".to_owned()))
                }
            };
        }
    };

    // dump input into fd4 immediately
    if let Some(p) = fd4_write_pipe {
        let (_fd, buf) = start.fd_input.as_ref().unwrap();
        let mut writer = tokio::io::BufWriter::new(p);
        writer.write_all(buf).await.unwrap();
        writer.flush().await.unwrap();
    }

    let pid = child
       .id()
       .expect("child hasn't been waited for yet, so its pid should exist");

    // Tell the server that we've started the command
    tokio::select! {
        value = client.post(begun_route)
        .postcard(&BegunRequest{
            transaction_id,
            host: gethostname().to_string_lossy().to_string(),
            pid,
        })?
        .send() => {
            value?.error_for_status()?;
        }
        _ = token3.cancelled() => {
            return Err(RemoteError::PingTimeout("Failed to receive server pong (335)".to_owned()))
        }
    };

    let mut child_futures = std::pin::pin!(wait_with_output(child, read_fd3));

    let mut stderr_prefix: Option<String> = None;

    // Wait for the command to finish
    let output: ChildOutput = loop {
        let output = tokio::select! {
            maybe_output = &mut child_futures => {
                maybe_output?
            },
            _ = token3.cancelled() => {
                return Err(RemoteError::PingTimeout("Failed to receive server pong (343)".to_owned()));
            },
            maybe_slurm_error_message = notify_rx.recv() => {
                let slurm_error_message = maybe_slurm_error_message.unwrap_or_else(||"Logic error. This channel should not have been dropped.".to_string());
                // This channel is giving us events from inotify when someone (likely slurmstepd)
                // writes to the `slurm_error_logfile`. The assumption is that this happens when
                // SLURM decides to cancel our allocation due to overuse of some resource (time,
                // memory), or because of some user- or administrator- triggered event like
                // scancel or taking the node offline for maintainance. In this case slurmstepd
                // will send us a SIGKILL (and maybe a sigterm, if we're lucky), but it will also
                // write the reason (which is more useful) to its error log, which hopefully is
                // being sent to a file on the local filesystem, so we can watch it with inotify.

                // propagate sigterm to child process
                unsafe { libc::kill(pid.try_into().unwrap(), libc::SIGTERM); }

                // send a notification back to the controller if possible
                if let Err(e) = client.post(end_route)
                    .postcard(&EndRequest {
                        transaction_id,
                        status: -15,
                        stdout: "".to_string(),
                        stderr: format!("{}{}", stderr_prefix.unwrap_or("".to_string()), slurm_error_message),
                        values: vec![],
                        start_request: None,
                    })?.send().await {
                        tracing::error!("Unable to send lasp-gasp message {}", e);
                    }
                return Err(RemoteError::Sigterm)
            },
            _ = tokio::time::sleep_until(time_to_sigterm_child.deadline) => {
                // send sigterm to child, but then go back into this loop and continue, waiting for the child to
                // hopefully actually exit itself.
                unsafe { libc::kill(pid.try_into().unwrap(), libc::SIGTERM); }
                stderr_prefix = Some(time_to_sigterm_child.message());
                continue;
            },
        };
        break output;
    }; // loop


    let t_finished = std::time::Instant::now();

    let time_executing_command = t_finished - t_before_spawn;
    let t_before_end_request = std::time::Instant::now();

    // Tell the server that we've finished the command
    let end_request = EndRequest {
        transaction_id,
        status: output.code().as_i32(),
        stdout: output.stdout_str(),
        stderr: format!(
            "{}{}",
            stderr_prefix.unwrap_or("".to_string()),
            output.stderr_str()
        ),
        values: output.fd3_values(),
        start_request: still_accepting_tasks(opt).then(make_start_request),
    };
    tokio::select! {
        value = client
            .post(end_route)
            .postcard(&end_request)?
            .send() => {
                let r = value
                    .context("Unable to send /end")?
                    .error_for_status().context("Receiving from /end")?
                    .postcard::<EndResponse>()
                    .await
                    .context("Decoding response from /end")?;
                token3.cancel();
                let end_request_elapsed = std::time::Instant::now() - t_before_end_request;
                Ok((TimingInfo {
                    subprocess: time_executing_command,
                    start_request: start_time_request_elapsed,
                    end_request: end_request_elapsed,
                    n_commands: 1,
                }, r.start_response))
        }
        _ = token3.cancelled() => {
            Err(RemoteError::PingTimeout("Failed to receive server pong (420)".to_owned()))
        }
    }
}

fn still_accepting_tasks(opt: &CommandLineArguments) -> bool {
    // If we received a SIGUSR1, then we're not going to accept any more tasks
    if SIGUSR1_RECEIVED.load(std::sync::atomic::Ordering::SeqCst) {
        return false
    }

    match opt.max_time_accepting_tasks {
        None => true,
        Some(t) => (std::time::Instant::now() - *START_TIME) < t,
    }
}

#[derive(Default)]
struct TimingInfo {
    subprocess: Duration,
    start_request: Duration,
    end_request: Duration,
    n_commands: u32,
}

impl std::ops::AddAssign for TimingInfo {
    fn add_assign(&mut self, other: TimingInfo) {
        self.subprocess += other.subprocess;
        self.start_request += other.start_request;
        self.end_request += other.end_request;
        self.n_commands += other.n_commands;
    }
}

impl TimingInfo {
    fn debug_average_times(&self) {
        if self.n_commands > 0 {
            tracing::debug!("Number of commands: {}", self.n_commands);
            tracing::debug!(
                "Avg subprocess duration: {:#?}",
                self.subprocess / self.n_commands
            );
            tracing::debug!(
                "Avg start_request duration: {:#?}",
                self.start_request / self.n_commands
            );
            tracing::debug!(
                "Avg end_request duration: {:#?}",
                self.end_request / self.n_commands
            );
        }
    }
}

#[derive(Debug, Error)]
enum RemoteError {
    #[error("Ping timeout: {0}")]
    PingTimeout(String),

    #[error("SIGTERM")]
    Sigterm,

    #[error("Connection error: {0}")]
    Connection(
        #[source]
        #[from]
        reqwest::Error,
    ),
    #[error("Url Parse Error: {0}")]
    Parse(
        #[source]
        #[from]
        url::ParseError,
    ),
    #[error("InvalidHeader: {0}")]
    InvalidHeaderValue(
        #[source]
        #[from]
        reqwest::header::InvalidHeaderValue,
    ),
    #[error("InvalidHostname: {0}")]
    InvalidHostname(String),

    #[error("Utf8Error: {0}")]
    Utf8Error(
        #[from]
        #[source]
        std::str::Utf8Error,
    ),

    #[error("JsonError: {0}")]
    JsonError(
        #[from]
        #[source]
        serde_json::Error,
    ),

    #[error("IoError: {0}")]
    IoError(
        #[from]
        #[source]
        std::io::Error,
    ),

    #[error("NotifyError: {0}")]
    NotifyError(
        #[from]
        #[source]
        notify::Error,
    ),

    #[error("AnyhowError: {0}")]
    AnyhowError(
        #[from]
        #[source]
        anyhow::Error,
    ),
}

impl From<ChildProcessError> for RemoteError {
    fn from(e: ChildProcessError) -> Self {
        e.into()
    }
}

fn parse_seconds(s: &str) -> anyhow::Result<Duration> {
    let x = s.parse::<u64>()?;
    Ok(Duration::new(x, 0))
}

fn parse_slurm_error_logfile(s: &str) -> anyhow::Result<std::path::PathBuf> {
    let out = s.replace("%u", &username()).replace(
        "%j",
        &std::env::var("SLURM_JOB_ID").expect("Unable to get SLURM_JOB_ID"),
    );
    Ok(std::path::PathBuf::from(out))
}

fn parse_disconnect_error_message(s: &str) -> anyhow::Result<String> {
    let slurm_array_job_id = std::env::var("SLURM_ARRAY_JOB_ID").unwrap_or_else(|_| "".to_string());
    let slurm_array_task_id =
        std::env::var("SLURM_ARRAY_TASK_ID").unwrap_or_else(|_| "".to_string());
    let slurm_jobid = format!("{}_{}", slurm_array_job_id, slurm_array_task_id);
    let out = s
        .replace("%u", &username())
        .replace("%j", &slurm_jobid)
        .replace(
            "%x",
            &std::env::var("SLURM_JOB_NAME").unwrap_or_else(|_| "%x".to_string()),
        )
        .replace(
            "%c",
            &std::env::var("SLURM_CLUSTER_NAME").unwrap_or_else(|_| "%c".to_string()),
        )
        .replace("%h", &gethostname().to_string_lossy());
    Ok(out)
}

// https://github.com/notify-rs/notify/blob/d7e22791faffb7bd9bd10f031c260ae019d7f474/examples/async_monitor.rs
#[allow(dropping_references)]
fn async_watcher(
    filename: Option<std::path::PathBuf>,
) -> notify::Result<(
    notify::RecommendedWatcher,
    tokio::sync::mpsc::Receiver<String>,
)> {
    use notify::EventKind;
    use notify::{Event, Result};
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let re = regex::Regex::new(FINAL_SLURM_ERROR_MESSAGE_STRINGS).unwrap();

    let watcher = match filename {
        Some(p) => {
            // Open the file we're going to watch, and create a buffer to hold its contents
            let mut file = std::fs::OpenOptions::new().read(true).open(&p)?;
            let mut buffer = Vec::new();

            let mut watcher = notify::recommended_watcher(move |res: Result<Event>| {
                let event = res.unwrap();

                // When the file is modified, read it into the buffer
                let is_modify = matches!(event.kind, EventKind::Any | EventKind::Modify(_));
                if is_modify {
                    // This appends to the current buffer
                    file.read_to_end(&mut buffer).unwrap();

                    // Okay, now look in the buffer for the string "CANCELLED". It seems like in
                    // general, slurmstepd will write multiple lines to the file for certain kinds
                    // of errors, and all of them are interesting. So if we fire off an event after
                    // the first modification, then we'll miss the later ones. It appears that
                    // "CANCELLED AT" is the last line in the file, so let's wait for that and then
                    // fire it off.
                    let s = std::str::from_utf8(&buffer)
                        .unwrap_or("Unable to read slurm error file. Utf8 issue?");
                    if re.is_match(s) || s.contains("Unable to read") {
                        futures::executor::block_on(async {
                            // send notification to channel, but if we can't don't panic
                            if let Err(e) = tx.send(s.to_string()).await {
                                tracing::warn!("Unable to send notifcation to channel {}", e);
                            }
                        })
                    }
                }
            })?;
            watcher.watch(&p, notify::RecursiveMode::NonRecursive)?;
            watcher
        }
        // just create a dummy object so things typecheck
        None => notify::recommended_watcher(move |_event: Result<Event>| {
            // take a reference to `tx` to prevent it from getting dropped, so that the recv
            // side of the channel remains in a valid state.
            let _ = format!("{:#?}", &tx);
            unreachable!("This should never get called");
        })?,
    };
    Ok((watcher, rx))
}

fn cleanup_child_processes_carefully() {
    // Before the SIGTERM, maybe the process tree looked like
    //
    //  execgraph-remote (us) ->
    //      shell script ->
    //          actual worker program

    // When we received a SIGTERM, we immediately forwarded the signal to our child, "shell script",
    // but if it exited without killing its child, then because we set ourselves as the subreaper,
    // we will now have a tree like
    //
    //  execgraph-remote (us) ->
    //      actual worker program

    //
    // Find all direct child processes of ourself. SIGTERM them. Wait for them.
    // Keep looping until there are no children. We want to ensure that everyone else
    // dies before we do.
    // If we die before any of them them do, then they'll get re-parented under init(1)
    // which can cause something like slurmstepd to lose them when walking the process tree
    //
    let mut sigtermed_at = std::collections::HashMap::<i32, std::time::Instant>::new();
    loop {
        let children = current_child_processes();
        if children.is_empty() {
            return;
        }

        // Send a signal to our children.
        // If we've already SIGTERMED them and that was more than 5 seconds ago, SIGKILL them.
        // If we haven't signaled them yet, SIGTERM them.
        // If we sigtermed them within the last 5 seconds, don't signal them again.
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(5);
        for pid in children.iter() {
            let signal = match sigtermed_at.get(pid) {
                Some(t) if *t < cutoff => Some(libc::SIGKILL),
                Some(_) => None,
                None => Some(libc::SIGTERM),
            };

            unsafe {
                if let Some(libc::SIGTERM) = signal {
                    tracing::info!("Sending SIGTERM to {}", pid);
                    libc::kill(*pid, libc::SIGTERM);
                    sigtermed_at.insert(*pid, std::time::Instant::now());
                }
                if let Some(libc::SIGKILL) = signal {
                    tracing::info!("Sending SIGKILL to {}", pid);
                    libc::kill(*pid, libc::SIGKILL);
                }
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(50));

        loop {
            match nix::sys::wait::waitpid(
                Pid::from_raw(-1),
                Some(nix::sys::wait::WaitPidFlag::WNOHANG),
            ) {
                Ok(s) if s.pid().is_some() => {
                    // We reaped a child. Keep looping on waitpid()
                    tracing::info!("Reaped child: {:?}", s.pid().unwrap());
                }
                Ok(_s) => {
                    // No unawaited children, we exited ummediately from waitpid()
                    // break out of this waitpid loop.
                    break;
                }
                Err(nix::errno::Errno::ECHILD) => {
                    // The calling process does not have any unwaited-for children.
                    break;
                }
                Err(e) => {
                    tracing::error!(
                        "Unexpected error in cleanup_child_processes_carefully: {:#?}",
                        e
                    );
                    return;
                }
            }
        }
    }
}

fn current_child_processes() -> Vec<i32> {
    use sysinfo::{get_current_pid, PidExt, ProcessExt, RefreshKind, System, SystemExt};

    let mypid = match get_current_pid() {
        Ok(mypid) => mypid,
        Err(_) => {
            return vec![];
        }
    };

    let mut children = vec![];
    let s = System::new_with_specifics(
        RefreshKind::new().with_processes(sysinfo::ProcessRefreshKind::new()),
    );
    for (pid, process) in s.processes() {
        if let Some(parent) = process.parent() {
            if parent == mypid {
                children.push(pid.as_u32() as i32);
            }
        }
    }

    children
}

struct SigtermChildBeforeSchedulerDeadline {
    deadline: tokio::time::Instant,
    description: String,
}

impl SigtermChildBeforeSchedulerDeadline {
    fn new(opt: &CommandLineArguments) -> Self {
        let duration = opt.duration.unwrap_or(ONE_YEAR).min(ONE_YEAR);
        let time_allocated_for_upload_and_stuff = if duration > SEND_SIGTERM_BEFORE_DEADLINE * 2 {
            SEND_SIGTERM_BEFORE_DEADLINE
        } else {
            std::time::Duration::from_secs(0)
        };
        Self {
            deadline: ((*START_TIME)
                + duration.saturating_sub(time_allocated_for_upload_and_stuff))
            .into(),
            description: format!(
                "{} ({} minus {} for cleanup)",
                humantime::format_duration(duration - time_allocated_for_upload_and_stuff),
                humantime::format_duration(duration),
                humantime::format_duration(SEND_SIGTERM_BEFORE_DEADLINE)
            ),
        }
    }

    fn message(&self) -> String {
        let hostname = gethostname();
        let hostname_cow = hostname.to_string_lossy();
        let hostname0 = hostname_cow.split(".").next().unwrap_or(&hostname_cow);

        let current_time = time::OffsetDateTime::now_local()
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        format!(
            "{} TASK IN SLURM JOB {} on {} CANCELLED DUE TO TIME LIMIT AFTER {}\n",
            current_time,
            (*SLURM_JOBID),
            hostname0,
            self.description,
        )
    }
}
