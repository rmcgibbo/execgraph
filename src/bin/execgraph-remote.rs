#![cfg(feature = "extension-module")]

use async_channel::bounded;
use clap::Parser;
use execgraph::{
    httpinterface::*,
    localrunner::{wait_with_output, ChildOutput, ChildProcessError},
    logfile2::ValueMaps,
};
use gethostname::gethostname;
use hyper::StatusCode;
use log::{debug, warn};
use notify::Watcher;
use serde::Deserialize;
use std::{convert::TryInto, io::Read, os::unix::prelude::AsRawFd, time::Duration};
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};
use tokio_command_fds::{CommandFdExt, FdMapping};
use tokio_util::sync::CancellationToken;
use whoami::username;

// https://github.com/SchedMD/slurm/blob/791f9c39e0db919e02ef8857be0faff09a3656b2/src/slurmd/slurmstepd/req.c#L724
const FINAL_SLURM_ERROR_MESSAGE_STRINGS: &str = "CANCELLED|FAILED|ERROR";

#[derive(Debug, Parser)]
#[clap(name = "execgraph-remote")]
struct CommandLineArguments {
    /// Url of controller
    url: String,

    /// "Runner type", a number between 0 and 63 signifying the features of this node
    runnertypeid: u32,

    /// Stop accepting new tasks after this amount of time, in seconds.
    #[clap(long = "max-time-accepting-tasks", parse(try_from_str = parse_seconds))]
    max_time_accepting_tasks: Option<std::time::Duration>,

    /// Path to the log file where slurmstepd will write errors. This should be a file
    /// on a local disk, so that we can watch it with inotify and report back any errors
    /// to the controller.
    #[clap(long = "slurm-error-logfile", parse(try_from_str = parse_slurm_error_logfile))]
    slurm_error_logfile: Option<std::path::PathBuf>,

    /// Error message to be echoed on the controller side in leu of our tasks's standard error
    /// if the controller looses contact with this runner due to a network partition or node
    /// failure.
    #[clap(long = "disconnect-err-msg", parse(try_from_str = parse_disconnect_error_message), default_value="")]
    disconnect_error_message: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), RemoteError> {
    tracing_subscriber::fmt::init();
    let slurm_jobid = std::env::var("SLURM_JOB_ID").unwrap_or_else(|_| "".to_string());

    let opt = CommandLineArguments::from_args();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "X-EXECGRAPH-USERNAME",
        reqwest::header::HeaderValue::from_bytes(username().as_bytes())?,
    );
    headers.insert(
        "X-EXECGRAPH-SLURM-JOB-ID",
        reqwest::header::HeaderValue::from_bytes(slurm_jobid.as_bytes())?,
    );
    headers.insert(
        "X-EXECGRAPH-HOSTNAME",
        reqwest::header::HeaderValue::from_bytes(
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
    let start = std::time::Instant::now();

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .http2_prior_knowledge()
        .build()?;
    let base = reqwest::Url::parse(&opt.url)?;

    let still_accepting_tasks = || match opt.max_time_accepting_tasks {
        None => true,
        Some(t) => (std::time::Instant::now() - start) < t,
    };
    // An infinite stream of SIGERM signals.
    // TODO: there are short sections of the run_command function below where we're not
    // listening on the SIGTERM channel. That's not ideal. The best thing would be to
    // always be listening on the channel and trigger the shutdown sequence immediately
    // in all cases. It's not a big deal if we miss some because we'll probably get
    // SIGKILLed afterwards anyways, but it would be preferable.
    let mut sigterms = signal(SignalKind::terminate())?;

    // Slurm error logfile events Notify
    let (watcher, mut rx) = async_watcher(opt.slurm_error_logfile.clone())?;

    while still_accepting_tasks() {
        match run_command(&opt, &base, &client, &mut sigterms, &mut rx).await {
            Err(RemoteError::Connection(e)) => {
                // break with no error message
                debug!("{:#?}", e);
                break;
            }
            result => result?,
        };
    }

    drop(watcher);
    Ok(())
}

async fn run_command(
    opt: &CommandLineArguments,
    base: &reqwest::Url,
    client: &reqwest::Client,
    sigterms: &mut tokio::signal::unix::Signal,
    notify_rx: &mut tokio::sync::mpsc::Receiver<String>,
) -> Result<(), RemoteError> {
    let start_route = base.join("start")?;
    let ping_route = base.join("ping")?;
    let begun_route = base.join("begun")?;
    let end_route = base.join("end")?;

    let start = client
        .get(start_route)
        .json(&StartRequest {
            runnertypeid: opt.runnertypeid,
            disconnect_error_message: opt.disconnect_error_message.clone(),
        })
        .send()
        .await?
        .error_for_status()?
        .json::<StartResponseFull>()
        .await?;

    let ping_interval = Duration::from_millis(start.data.ping_interval_msecs);
    let transaction_id = start.data.transaction_id;
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
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match client1.post(ping_route.clone())
                        .json(&Ping{transaction_id})
                        .send()
                        .await {
                            Ok(r) if r.status() == StatusCode::OK => {
                                // add to the pongs channel, but if the channel
                                // has been dropped that's okay, just break
                                if pongs_tx.send(()).await.is_err() {
                                    token1.cancel();
                                    break;
                                }
                            }
                            e => {
                                warn!("Ping endpoint response error: {:#?}", e);
                                token1.cancel();
                            }
                        }
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

    // Run the command and record the pid
    let mut command = tokio::process::Command::new(&start.data.cmdline[0]);
    let maybe_child = command
        .args(&start.data.cmdline[1..])
        .envs(start.data.env.iter().cloned())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .fd_mappings(vec![
            // Map the pipe as FD 3 in the child process.
            FdMapping {
                parent_fd: write_fd3.as_raw_fd(),
                child_fd: 3,
            },
        ])
        .unwrap()
        .spawn();

    // Deadlock potential: After spawning the command and passing it the
    // write end of the pipe we need to close it ourselves
    drop(write_fd3);

    let child = match maybe_child {
        Ok(child) => child,
        Err(_) => {
            // Tell the server that we've started the command
            tokio::select! {
                value = client.post(begun_route)
                .json(&BegunRequest{
                    transaction_id,
                    host: gethostname().to_string_lossy().to_string(),
                    pid: 0,
                })
                .send() => {
                    value?.error_for_status()?;
                }
                _ = token3.cancelled() => {
                    return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
                }
            };

            // Tell the server that we've finished the command
            tokio::select! {
                value = client.post(end_route)
                .json(&EndRequest{
                    transaction_id,
                    status: 127,
                    stdout: "".to_owned(),
                    stderr: format!("No such command: {:#?}", &start.data.cmdline[0]),
                    values: ValueMaps::new(),
                })
                .send() => {
                    value?.error_for_status()?;
                }
                _ = token3.cancelled() => {
                    return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
                }
            };

            return Ok(());
        }
    };

    let pid = child
        .id()
        .expect("hasn't been polled yet, so this id should exist");

    // Tell the server that we've started the command
    tokio::select! {
        value = client.post(begun_route)
        .json(&BegunRequest{
            transaction_id,
            host: gethostname().to_string_lossy().to_string(),
            pid,
        })
        .send() => {
            value?.error_for_status()?;
        }
        _ = token3.cancelled() => {
            return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
        }
    };

    // Wait for the command to finish
    let output: ChildOutput = tokio::select! {
        output = wait_with_output(child, read_fd3) => output.unwrap(),
        _ = token3.cancelled() => {
            return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
        },
        maybe_slurm_error_message = notify_rx.recv() => {
            let slurm_error_message = maybe_slurm_error_message.unwrap_or("Logic error. This channel should not have been dropped.".to_string());
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
            client.post(end_route)
                .json(&EndRequest {
                    transaction_id,
                    status: 128+16,
                    stdout: "".to_string(),
                    stderr: slurm_error_message,
                    values: vec![]
                }).send().await.unwrap();
            return Err(RemoteError::SIGTERM)
        },
        _ = sigterms.recv() => {
            // propagate sigterm to child process
            unsafe { libc::kill(pid.try_into().unwrap(), libc::SIGTERM); }
            // read the slurm logfile, which might contant some informationg
            let slurm_error_logfile_contents = match &opt.slurm_error_logfile {
                Some(f) => std::fs::read_to_string(f).unwrap_or_else(|_| "".to_string()),
                None => "".to_string()
            };
            // send a notification back to the controller if possible
            client.post(end_route)
                .json(&EndRequest {
                    transaction_id,
                    status: 128+15,
                    stdout: "".to_string(),
                    stderr: slurm_error_logfile_contents,
                    values: vec![]
                }).send().await.unwrap();

            return Err(RemoteError::SIGTERM)
        }
    };

    // Tell the server that we've finished the command
    tokio::select! {
        value = client.post(end_route)
        .json(&EndRequest{
            transaction_id,
            status: output.code().as_i32(),
            stdout: output.stdout_str(),
            stderr: output.stderr_str(),
            values: output.fd3_values(),
        })
        .send() => {
            value?.error_for_status()?;
        }
        _ = token3.cancelled() => {
            return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
        }
    };

    Ok(())
}

#[derive(Debug, Error)]
enum RemoteError {
    #[error("Ping timeout: {0}")]
    PingTimeout(String),

    #[error("SIGTERM")]
    SIGTERM,

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
}

impl From<ChildProcessError> for RemoteError {
    fn from(e: ChildProcessError) -> Self {
        e.into()
    }
}

#[derive(Deserialize)]
struct StartResponseFull {
    #[serde(rename = "status")]
    _status: String,
    #[serde(rename = "code")]
    _code: u16,
    data: StartResponse,
}

fn parse_seconds(s: &str) -> anyhow::Result<std::time::Duration> {
    let x = s.parse::<u64>()?;
    Ok(std::time::Duration::new(x, 0))
}

fn parse_slurm_error_logfile(s: &str) -> anyhow::Result<std::path::PathBuf> {
    let out = s.replace("%u", &username()).replace(
        "%j",
        &std::env::var("SLURM_JOB_ID").expect("Unable to get SLURM_JOB_ID"),
    );
    Ok(std::path::PathBuf::from(out))
}

fn parse_disconnect_error_message(s: &str) -> anyhow::Result<String> {
    let out = s
        .replace("%u", &username())
        .replace(
            "%j",
            &std::env::var("SLURM_JOB_ID").unwrap_or("%j".to_string()),
        )
        .replace(
            "%x",
            &std::env::var("SLURM_JOB_NAME").unwrap_or("%x".to_string()),
        )
        .replace(
            "%c",
            &std::env::var("SLURM_CLUSTER_NAME").unwrap_or("%c".to_string()),
        )
        .replace(
            "%h",
            &gethostname().to_string_lossy()
        );
    Ok(out)
}

// https://github.com/notify-rs/notify/blob/d7e22791faffb7bd9bd10f031c260ae019d7f474/examples/async_monitor.rs
fn async_watcher(
    filename: Option<std::path::PathBuf>,
) -> notify::Result<(notify::INotifyWatcher, tokio::sync::mpsc::Receiver<String>)> {
    use notify::EventKind;
    use notify::{Event, Result};
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let re = regex::Regex::new(FINAL_SLURM_ERROR_MESSAGE_STRINGS).unwrap();

    let watcher = match filename {
        Some(p) => {
            // Open the file we're going to watch, and create a buffer to hold its contents
            let mut file = std::fs::OpenOptions::new().read(true).open(&p)?;
            let mut buffer = Vec::new();

            let mut watcher = notify::INotifyWatcher::new(move |res: Result<Event>| {
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
                            tx.send(s.to_string()).await.unwrap();
                        })
                    }
                }
            })?;
            watcher.watch(&p, notify::RecursiveMode::NonRecursive)?;
            watcher
        }
        // just create a dummy object so things typecheck
        None => notify::INotifyWatcher::new(move |_event: Result<Event>| {
            // take a reference to `tx` to prevent it from getting dropped, so that the recv
            // side of the channel remains in a valid state.
            drop(&tx);
        })?,
    };
    Ok((watcher, rx))
}
