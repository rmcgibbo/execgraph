#![cfg(feature = "extension-module")]

extern crate reqwest;
use anyhow::Result;
use async_channel::bounded;
use execgraph::httpinterface::*;
use gethostname::gethostname;
use hyper::StatusCode;
use log::{debug, warn};
use serde::Deserialize;
use std::{os::unix::process::ExitStatusExt, time::Duration};
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use whoami::username;

#[tokio::main]
async fn main() -> Result<(), RemoteError> {
    env_logger::init();

    let opt = Opt::from_args();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "X-EXECGRAPH-USERNAME",
        reqwest::header::HeaderValue::from_bytes(username().as_bytes())?,
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

    while still_accepting_tasks() {
        match run_command(&base, &client, &opt.queue).await {
            Err(RemoteError::Connection(e)) => {
                // break with no error message
                debug!("{:#?}", e);
                break;
            }
            result => result?,
        };
    }

    Ok(())
}

async fn run_command(
    base: &reqwest::Url,
    client: &reqwest::Client,
    queue: &Option<String>,
) -> Result<(), RemoteError> {
    let start_route = base.join("start")?;
    let ping_route = base.join("ping")?;
    let begun_route = base.join("begun")?;
    let end_route = base.join("end")?;

    let start = client
        .get(start_route)
        .json(&StartRequest {
            queuename: queue.clone(),
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

    // Run the command and record the pid
    let maybe_child = tokio::process::Command::new(&start.data.cmdline[0])
        .args(&start.data.cmdline[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();

    let mut child = match maybe_child {
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

    let mut stdin = child
        .stdin
        .take()
        .expect("Failed to extract stdin from subprocess");
    stdin
        .write_all(&start.data.stdin)
        .await
        .expect("Unable to write to stdin of subprocess");
    drop(stdin);

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
    let (status, stdout, stderr) = tokio::select! {
        status = child.wait_with_output() => {
            let output = status.expect("sh wasn't running");
            let status_obj = output.status;
            let status = match status_obj.code() {
                Some(code) => code,
                None => status_obj.signal().expect("No exit code and no signal?"),
            };
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (status, stdout, stderr)
        }
        _ = token3.cancelled() => {
            return Err(RemoteError::PingTimeout("Failed to receive server pong".to_owned()))
        }
    };

    // let foo = client.post(base.join("status")?).send().await?.text().await?;
    // println!("{}", foo);

    // Tell the server that we've finished the command
    tokio::select! {
        value = client.post(end_route)
        .json(&EndRequest{
            transaction_id,
            status,
            stdout,
            stderr
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

#[derive(Debug, StructOpt)]
#[structopt(name = "execgraph-remote")]
struct Opt {
    /// Url of controller
    url: String,

    /// Work on tasks for a particular slurm queue. If not supplied,
    /// work on 'general' tasks.
    queue: Option<String>,

    /// Stop accepting new tasks after this amount of time, in seconds.
    #[structopt(long = "max-time-accepting-tasks", parse(try_from_str = parse_seconds))]
    max_time_accepting_tasks: Option<std::time::Duration>,
}

#[derive(Debug)]
enum RemoteError {
    PingTimeout(String),
    Connection(reqwest::Error),
    Parse(url::ParseError),
    InvalidHeaderValue(reqwest::header::InvalidHeaderValue),
    InvalidHostname(String),
}

impl From<url::ParseError> for RemoteError {
    fn from(err: url::ParseError) -> RemoteError {
        RemoteError::Parse(err)
    }
}

impl From<reqwest::Error> for RemoteError {
    fn from(err: reqwest::Error) -> RemoteError {
        RemoteError::Connection(err)
    }
}

impl From<reqwest::header::InvalidHeaderValue> for RemoteError {
    fn from(err: reqwest::header::InvalidHeaderValue) -> RemoteError {
        RemoteError::InvalidHeaderValue(err)
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

fn parse_seconds(s: &str) -> Result<std::time::Duration> {
    let x = s.parse::<u64>()?;
    Ok(std::time::Duration::new(x, 0))
}
