use crate::{execgraph::Cmd, httpinterface::*, sync::ReadyTrackerClient};
use anyhow::Result;
use hyper::{Body, Request, Response, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use routerify::{ext::RequestExt, Middleware, RequestInfo, Router};
use routerify_json_response::json_success_resp;
use serde::de::DeserializeOwned;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{sync::Mutex, time::timeout};
use tokio_util::sync::CancellationToken;
type RouteError = Box<dyn std::error::Error + Send + Sync + 'static>;
use crate::logfile2::ValueMaps;

static PING_INTERVAL_MSECS: u64 = 15_000;

#[derive(Debug)]
struct ConnectionState {
    pings: async_channel::Sender<()>,
    cancel: CancellationToken,
    cmd: Cmd,
    node_id: NodeIndex,
}
#[derive(Debug)]
pub struct State<'a> {
    connections: Mutex<HashMap<u32, ConnectionState>>,
    subgraph: Arc<DiGraph<&'a Cmd, ()>>,
    tracker: &'a ReadyTrackerClient,
}

impl<'a> State<'a> {
    pub fn new(subgraph: Arc<DiGraph<&'a Cmd, ()>>, tracker: &'a ReadyTrackerClient) -> State<'a> {
        State {
            connections: Mutex::new(HashMap::new()),
            subgraph,
            tracker,
        }
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

#[tracing::instrument]
async fn ping_timeout_handler(transaction_id: u32, state: Arc<State<'_>>) {
    let cstate = {
        let mut lock = state.connections.lock().await;
        let cstate = match lock.remove(&transaction_id) {
            Some(cs) => cs,
            None => return,
        };

        cstate.cancel.cancel();
        cstate
    };

    let timeout_status = 130;
    state
        .tracker
        .send_finished(
            cstate.node_id,
            &cstate.cmd,
            timeout_status,
            "".to_owned(),
            "".to_owned(),
            ValueMaps::new(),
        )
        .await;
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

    let (etag, snapshot) = match request.timeout_ms {
        0 => {
            state
                .tracker
                .get_queuestate(
                    request.etag,
                    std::time::Duration::from_millis(request.timemin_ms),
                )
                .await
        }
        _ => {
            let dur = std::time::Duration::from_millis(request.timeout_ms);
            tokio::time::timeout(
                dur,
                state.tracker.get_queuestate(
                    request.etag,
                    std::time::Duration::from_millis(request.timemin_ms),
                ),
            )
            .await?
        }
    };

    let resp = snapshot
        .iter()
        .map(|(name, queue)| {
            (
                name.data,
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

    let (ping_tx, ping_rx) = async_channel::bounded::<()>(1);
    let token = CancellationToken::new();
    let token1 = token.clone();
    let state1 = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to = timeout(std::time::Duration::from_millis(2*PING_INTERVAL_MSECS), ping_rx.recv()) => {
                    match to {
                        Err(_) => {
                            ping_timeout_handler(transaction_id, state1).await;
                            break;
                        }, Ok(Err(e)) => {
                            // send side of the ping channel has been dropped. server probably shutting down?
                            log::debug!("{}", e);
                            break;
                        },
                        _ => {
                            log::debug!("Got ping within timeout");
                        }
                    }

                }
                _ = token1.cancelled() => {
                    break;
                }
            }
        }
    });

    let cmd = state.subgraph[node_id];
    cmd.call_preamble();
    {
        let mut lock = state.connections.lock().await;
        lock.insert(
            transaction_id,
            ConnectionState {
                pings: ping_tx,
                cancel: token,
                cmd: cmd.clone(),
                node_id,
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
            request.status,
            request.stdout,
            request.stderr,
            request.values,
        )
        .await;

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

pub fn router(state: State<'static>) -> Router<Body, RouteError> {
    Router::builder()
        // Attach the handlers.
        .data(Arc::new(state))
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
