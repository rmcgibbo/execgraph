use crate::{
    execgraph::Cmd,
    httpinterface::*,
    sync::{ExitStatus, ReadyTrackerClient},
};
use anyhow::Result;
use hyper::{Body, Request, Response, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use routerify::{ext::RequestExt, Middleware, RequestInfo, Router};
use routerify_json_response::json_success_resp;
use serde::de::DeserializeOwned;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::SystemTime};
use tokio::{sync::Mutex, time::timeout};
use tokio_util::sync::CancellationToken;
type RouteError = Box<dyn std::error::Error + Send + Sync + 'static>;
use crate::{constants::PING_INTERVAL_MSECS, logfile2::ValueMaps};

#[derive(Debug)]
struct ConnectionState {
    pings: async_channel::Sender<()>,
    cancel: CancellationToken,
    cmd: Cmd,
    node_id: NodeIndex,
    joinhandle: tokio::task::JoinHandle<()>,
    disconnect_error_message: String,
}
#[derive(Debug)]
pub struct State<'a> {
    connections: Mutex<HashMap<u32, ConnectionState>>,
    subgraph: Arc<DiGraph<&'a Cmd, ()>>,
    tracker: &'a ReadyTrackerClient,
    token: crate::utils::CancellationToken,
}

impl<'a> State<'a> {
    pub fn new(
        subgraph: Arc<DiGraph<&'a Cmd, ()>>,
        tracker: &'a ReadyTrackerClient,
        token: crate::utils::CancellationToken,
    ) -> State<'a> {
        State {
            connections: Mutex::new(HashMap::new()),
            subgraph,
            tracker,
            token,
        }
    }

    /// Ensure that all tasks spawned by the server are done. Rust doesn't allow
    /// async drop, so this can't be in the drop. But you really should call this before
    /// dropping the server.
    pub async fn join(&self) {
        let cstates: Vec<_> = {
            let mut connections = self.connections.lock().await;
            let keys: Vec<_> = connections
                .iter()
                .map(|(k, v)| {
                    v.cancel.cancel();
                    *k
                })
                .collect();
            keys.iter().filter_map(|k| connections.remove(k)).collect()
        };

        let n_joined = cstates.len();
        for cs in cstates {
            cs.joinhandle.await.expect("Unable to join");
        }
        log::debug!("Joined all {} ping threads", n_joined);
    }
}

#[derive(Debug)]
struct JsonResponseErr {
    code: StatusCode,
    message: String,
}
impl std::fmt::Display for JsonResponseErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl From<hyper::Error> for JsonResponseErr {
    fn from(err: hyper::Error) -> JsonResponseErr {
        JsonResponseErr {
            code: StatusCode::UNPROCESSABLE_ENTITY,
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for JsonResponseErr {
    fn from(err: serde_json::Error) -> JsonResponseErr {
        JsonResponseErr {
            code: StatusCode::BAD_REQUEST,
            message: err.to_string(),
        }
    }
}

impl std::error::Error for JsonResponseErr {}

// impl std::fmt::Debug for State {
//     // fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("State")
//             .field("connections", &self.connections)
//             .field("subgraph", subgraph)
//         .finish()
//     }
// }

fn json_response_err<T: std::string::ToString>(code: StatusCode, message: T) -> RouteError {
    Box::new(JsonResponseErr {
        code,
        message: message.to_string(),
    })
}

// Get the "state" from inside one of the request handlers.
fn get_state(req: &Request<Body>) -> Arc<State<'static>> {
    req.data::<Arc<State>>()
        .expect("Unable to access router state")
        .clone()
}

// Define an error handler function which will accept the `routerify::Error`
// and the request information and generates an appropriate response.
#[tracing::instrument]
async fn error_handler(err: RouteError, _: RequestInfo) -> Response<Body> {
    //log::warn!("{}", err);

    let e2 = err.downcast_ref::<JsonResponseErr>();
    match e2 {
        Some(e) => routerify_json_response::json_failed_resp_with_message(e.code, &e.message)
            .expect("failed to construct error"),
        _ => routerify_json_response::json_failed_resp_with_message(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", err),
        )
        .expect("Failed to build error"),
    }
}

#[tracing::instrument]
async fn get_json_body<T: DeserializeOwned>(req: Request<Body>) -> Result<T, JsonResponseErr> {
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|e| -> JsonResponseErr { e.into() })?;
    serde_json::from_slice(bytes.to_vec().as_slice()).map_err(|e| e.into())
}

#[tracing::instrument]
async fn middleware_after(
    res: Response<Body>,
    req_info: RequestInfo,
) -> Result<Response<Body>, RouteError> {
    let (started, remote_addr) = req_info
        .context::<(tokio::time::Instant, SocketAddr)>()
        .expect("Unable to access request context");
    let duration = started.elapsed();
    log::debug!(
        "{} {} {} {}us {}",
        remote_addr,
        req_info.method(),
        req_info.uri().path(),
        duration.as_micros(),
        res.status().as_u16()
    );
    Ok(res)
}

#[tracing::instrument]
async fn middleware_before(req: Request<Body>) -> Result<Request<Body>, RouteError> {
    req.set_context((tokio::time::Instant::now(), req.remote_addr()));
    Ok(req)
}

async fn ping_timeout_handler(transaction_id: u32, state: Arc<State<'_>>) {
    // Uhh, this is actually unsafe and segfault-prone and should be refactored.
    // Here's the problem: in execgraph, we transmute the State that's used in this
    // server.rs file to have static lifetime, even though it actually does _not_
    // have static lifetime. This is not good. In particular it means that every
    // tokio task spawned inside this file needs to get cleaned up before the state
    // struct gets destroyed, and since tokio tasks outlive their parents we need to
    // manually make extra sure they all get joined, or else we can segfault. The way
    // we do that is by having a join() method on state. The join() method iterates
    // over all of the items in the connections HashMap, pulls out the joinhandles, and
    // joins them.
    // This works, as long as everything we need to join is actually _in_ the connections
    // hashmap. But here in this function, we're removing stuff from the connections hashmap,
    // and we're removing things before the thread actually finishes, because after the
    // remove() is done, we call dereference the command and call send_finished(), which
    // could take a little while. So it might be possible, during a shutdown sequence, for
    // this code to remove this transaction from the connections hashmap, for join() to never
    // thus join this very thread, for state to then get deallocated out from underneath us,
    // and then for send_finished to segfault.
    //
    // In order to prevent that, we hold the connections lock for the entire duration of this
    // function. Since join() also acquires this lock, that means that we should be good.
    // But this is fragile and as you can probably tell from this long comment, I didn't
    // implement it light this at the beginning, because the connections lock really ought
    // not to be guarding anyhing more than the hashmap. So this should be refactored to be
    // more clearly safe.
    // The invariant that we absolutely must hold in order for the unsafe
    // transmute of the state into 'static is: every task spawned from inside this server
    // must be guarenteed to be fully completed once State::join() finishes, which means that
    // every time a background task is spawned with tokio::spawn, we must carefully guard the
    // JoinHandle and make sure it's joined.

    let mut connections = state.connections.lock().await;
    let cstate = match connections.remove(&transaction_id) {
        Some(cs) => cs,
        None => return,
    };

    state
        .tracker
        .send_finished(
            cstate.node_id,
            &cstate.cmd,
            ExitStatus::Disconnected,
            "".to_owned(),
            cstate.disconnect_error_message,
            ValueMaps::new(),
        )
        .await;
}

fn spawn_ping_collector_thread(
    state: Arc<State<'static>>,
    token: CancellationToken,
    transaction_id: u32,
    ping_rx: async_channel::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    let task_start_time_approx = SystemTime::now();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to = timeout(std::time::Duration::from_millis(2*PING_INTERVAL_MSECS), ping_rx.recv()) => {
                    match to {
                        Err(_) => {
                            log::debug!("Ping timeout. Task dead?");
                            ping_timeout_handler(transaction_id, state).await;
                            break;
                        }, Ok(Err(e)) => {
                            // send side of the ping channel has been dropped. server probably shutting down?
                            log::debug!("Unable to receive from ping timeout channel. {}", e);
                            break;
                        },
                        _ => {
                            log::debug!("Received ping. All good.");
                        }
                    }
                }
                _ = token.cancelled() => {
                    // called from /end handler and ping timeout
                    break;
                }
                cancel_state = state.token.soft_cancelled(task_start_time_approx) => {
                    if let crate::utils::CancellationState::CancelledAfterTime(_) = cancel_state {
                        ping_timeout_handler(transaction_id, state).await;
                    }
                    break;
                }
            }
        }
        log::debug!("Ping loop task finished {}", transaction_id);
    })
}

// ------------------------------------------------------------------ //

// GET /status
#[tracing::instrument]
async fn status_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<StatusRequest>(req)
        .await
        .unwrap_or(StatusRequest {
            timeout_ms: 0,
            timemin_ms: 0,
            etag: 0,
        });

    let (etag, snapshot) = state
        .tracker
        .get_queuestate(
            request.etag,
            std::time::Duration::from_millis(request.timemin_ms),
            std::time::Duration::from_millis(request.timeout_ms),
        )
        .await;

    let resp = snapshot
        .into_iter()
        .map(|(name, queue)| {
            (
                name,
                StatusQueueReply {
                    num_ready: queue.num_ready,
                    num_inflight: queue.num_inflight,
                },
            )
        })
        .collect::<HashMap<u64, StatusQueueReply>>();

    json_success_resp(&StatusReply { queues: resp, etag })
}

// POST /ping
#[tracing::instrument]
async fn ping_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<Ping>(req).await?;

    // For debugging: delay responding to pings here to trigger timeouts
    // tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let guard = state.connections.lock().await;
    let cstate = guard
        .get(&request.transaction_id)
        .ok_or_else(|| json_response_err(StatusCode::NOT_FOUND, "No active transaction"))?;
    cstate.pings.send(()).await?;
    Ok(Response::builder().status(200).body("".into()).unwrap())
}

// GET /start
#[tracing::instrument]
async fn start_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<StartRequest>(req).await?;
    let transaction_id = rand::random::<u32>();

    let node_id = state
        .tracker
        .try_recv(request.runnertypeid)
        .map_err(|e| match e {
            crate::sync::ReadyTrackerClientError::ChannelTryRecvError(_) => json_response_err(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("Channel closed: {:?}", e),
            ),
            crate::sync::ReadyTrackerClientError::NoSuchRunnerType => json_response_err(
                StatusCode::NOT_FOUND,
                format!("Invalid runner type: {:?}", request.runnertypeid),
            ),
            crate::sync::ReadyTrackerClientError::ChannelRecvError(_) => unreachable!(),
        })?;

    // Create a new channel to handle keepalive pings. The client will send us pings every
    // 15 seconds or so, and each ping will be associated with a transaction ID. We'll make
    // 1 channel per transaction ID, and the HTTP endpoint will dump the message into the
    // channel by transaction ID. Then, we'll create a task below which will consume pings
    // from that channel. If they don't come fast enough we'll call ping_timeout_handler
    // which will drop the ConnectionState (causing future pings to return errors) and notify
    // the task tracker that the task failed.

    let (ping_tx, ping_rx) = async_channel::bounded::<()>(1);
    let cmd = state.subgraph[node_id];
    cmd.call_preamble();
    {
        let mut lock = state.connections.lock().await;
        let token = CancellationToken::new();
        lock.insert(
            transaction_id,
            ConnectionState {
                pings: ping_tx,
                cancel: token.clone(),
                cmd: cmd.clone(),
                node_id,
                disconnect_error_message: request.disconnect_error_message,
                joinhandle: spawn_ping_collector_thread(
                    state.clone(),
                    token,
                    transaction_id,
                    ping_rx,
                ),
            },
        );
    }
    json_success_resp(&StartResponse {
        transaction_id,
        cmdline: cmd.cmdline.clone(),
        env: cmd.env.clone(),
        ping_interval_msecs: PING_INTERVAL_MSECS,
    })
}

// POST /begun
#[tracing::instrument]
async fn begun_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<BegunRequest>(req).await?;

    {
        let mut lock = state.connections.lock().await;
        let cstate = lock
            .get_mut(&request.transaction_id)
            .ok_or_else(|| json_response_err(StatusCode::NOT_FOUND, "No active transaction"))?;
        state
            .tracker
            .send_started(cstate.node_id, &cstate.cmd, &request.host, request.pid)
            .await;
    }

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

// POST /end
#[tracing::instrument]
async fn end_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<EndRequest>(req).await?;

    let cstate = {
        let mut lock = state.connections.lock().await;
        let cstate = lock
            .remove(&request.transaction_id)
            .ok_or_else(|| json_response_err(StatusCode::NOT_FOUND, "No active transaction"))?;
        cstate.cancel.cancel();
        cstate
    };

    state
        .tracker
        .send_finished(
            cstate.node_id,
            &cstate.cmd,
            ExitStatus::Code(request.status),
            request.stdout,
            request.stderr,
            request.values,
        )
        .await;

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

pub fn router(state: Arc<State<'static>>) -> Router<Body, RouteError> {
    Router::builder()
        // Attach the handlers.
        .data(state)
        .middleware(Middleware::pre(middleware_before))
        .middleware(Middleware::post_with_info(middleware_after))
        .get("/status", status_handler)
        .post("/ping", ping_handler)
        .get("/start", start_handler)
        .post("/begun", begun_handler)
        .post("/end", end_handler)
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}
