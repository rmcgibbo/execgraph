extern crate reqwest;
use log::{debug, warn};
use std::result::Result;
use hyper::StatusCode;
use execgraph::httpinterface::*;
use std::time::Duration;
use async_channel::bounded;
use tokio_util::sync::CancellationToken;
use serde::Deserialize;
use std::os::unix::process::ExitStatusExt;
use whoami::username;
use gethostname::gethostname;
use structopt::StructOpt;
use execgraph::sync::DropGuard;

#[tokio::main]
async fn main() -> Result<(), RemoteError> {
    env_logger::init();

    let opt = Opt::from_args();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("X-EXECGRAPH-USERNAME", reqwest::header::HeaderValue::from_str(&username()).unwrap());
    headers.insert("X-EXECGRAPH-HOSTNAME", reqwest::header::HeaderValue::from_str(&gethostname().to_str().unwrap()).unwrap());

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .http2_prior_knowledge()
        .build()?;
    let base = reqwest::Url::parse(&opt.url)?;

    loop {
        match run_command(&base, &client).await {
            Err(RemoteError::Connection(e))  => {
                // break with no error message
                debug!("{:#?}", e);
                break;
            }
            result => {result?},
        };
    }

    Ok(())
}

async fn run_command(base: &reqwest::Url, client: &reqwest::Client) -> Result<(), RemoteError> {
    let start_route = base.join("start")?;
    let ping_route = base.join("ping")?;
    let begun_route = base.join("begun")?;
    let end_route = base.join("end")?;

    let start = client
        .get( start_route)
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
    let _drop_guard = DropGuard::new(token);
    let (pongs_tx, pongs_rx) = bounded::<()>(1);
    // send a pong right at the beginning, because we just received a server message
    // from start(), so that's pretty good
    pongs_tx.send(()).await.unwrap();

    // Start a background task tha pings the server and puts the pongs into
    // a channel
    let client1 = client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + ping_interval, ping_interval);
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
    let mut child = tokio::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(&start.data.cmdline)
        .spawn()
        .expect("failed to execute /bin/sh");
    let pid = child
        .id()
        .expect("hasn't been polled yet, so this id should exist");


    // Tell the server that we've started the command
    tokio::select! {
        value = client.post(begun_route)
        .json(&BegunRequest{
            transaction_id,
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
    let status: i32 = tokio::select! {
        status = child.wait() => {
            let status_obj = status.expect("sh wasn't running");
            match status_obj.code() {
                Some(code) => code,
                None => status_obj.signal().expect("No exit code and no signal?"),
            }
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
    url: String,
}

#[derive(Debug)]
enum RemoteError {
    PingTimeout(String),
    Connection(reqwest::Error),
    Parse(url::ParseError)
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

#[derive(Deserialize)]
struct StartResponseFull {
    #[serde(rename = "status")]
    _status: String,
    #[serde(rename = "code")]
    _code: u16,
    data: StartResponse,
}


// #[derive(Deserialize)]
// struct StatusReplyFull {
//     #[serde(rename = "status")]
//     _status: String,
//     #[serde(rename = "code")]
//     _code: u16,
//     data: StatusReply,
// }
