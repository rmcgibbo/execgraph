use crate::{
    constants::PING_TIMEOUT_MSECS,
    execgraph::Cmd,
    httpinterface::*,
    sync::{ExitStatus, ReadyTrackerClient},
    timewheel::{TimeWheel, TimerID},
    utils::CancellationState,
};
use anyhow::Result;
use dashmap::DashMap;
use hyper::{Body, Request, Response, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use routerify::{ext::RequestExt, Middleware, RequestInfo, Router};
use routerify_json_response::json_success_resp;
use serde::de::DeserializeOwned;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};
use tokio::sync::oneshot;

type RouteError = Box<dyn std::error::Error + Send + Sync + 'static>;
use crate::{constants::PING_INTERVAL_MSECS, logfile2::ValueMaps};

const TIMEWHEEL_DURATION_MSECS: u64 = PING_TIMEOUT_MSECS + 1;

#[derive(Debug)]
struct ConnectionState {
    node_id: NodeIndex,
    cmd: Cmd,
    start_time_approx: Instant,
    timer_id: TimerID,
    disconnect_error_message: String,
}
#[derive(Debug)]
pub struct State<'a> {
    connections: DashMap<u32, ConnectionState>,
    subgraph: Arc<DiGraph<&'a Cmd, ()>>,
    tracker: &'a ReadyTrackerClient,
    token: crate::utils::CancellationToken,
    timeouts: TimeWheel<u32>,
}

impl<'a> State<'a> {
    pub fn new(
        subgraph: Arc<DiGraph<&'a Cmd, ()>>,
        tracker: &'a ReadyTrackerClient,
        token: crate::utils::CancellationToken,
    ) -> State<'a> {
        State {
            connections: DashMap::new(),
            subgraph,
            tracker,
            token,
            timeouts: TimeWheel::new(std::time::Duration::from_millis(TIMEWHEEL_DURATION_MSECS)),
        }
    }

    pub async fn reap_pings_forever(&self, mut stop: oneshot::Receiver<()>) {
        let start = tokio::time::Instant::now();
        // the ping timeout is 30 seconds, so we've got a time wheel with 256 slots which gives
        // about 117ms per tick, which seems totally fine. that means that rather than timing out after
        // precisely 30 seconds it might be timing out after 30.117 seconds.
        let tick_time = self.timeouts.tick_duration();
        let mut count = 0;
        loop {
            let ticked = self.timeouts.tick();
            let mut expired = vec![];
            if let Some(mut ticked) = ticked {
                for (_, transaction_id) in ticked.drain() {
                    if let Some((_, cstate)) = self.connections.remove(&transaction_id) {
                        expired.push(cstate);
                    }
                }
            }

            if let Some(CancellationState::CancelledAfterTime(t)) = self.token.is_soft_cancelled() {
                let mut keys_to_remove = vec![];
                for entry in self.connections.iter() {
                    let cstate = entry.value();
                    if cstate.start_time_approx > t {
                        keys_to_remove.push(*entry.key());
                    }
                }
                for key in keys_to_remove {
                    expired.push(self.connections.remove(&key).unwrap().1);
                }
            }

            for cstate in expired {
                self.tracker
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

            tokio::select! {
                _ = tokio::time::sleep_until(start + count * tick_time) => {},
                _ = &mut stop => {return;}
            };

            count += 1;
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

async fn get_json_body<T: DeserializeOwned>(req: Request<Body>) -> Result<T, JsonResponseErr> {
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|e| -> JsonResponseErr { e.into() })?;
    serde_json::from_slice(bytes.to_vec().as_slice()).map_err(|e| e.into())
}

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

async fn middleware_before(req: Request<Body>) -> Result<Request<Body>, RouteError> {
    req.set_context((tokio::time::Instant::now(), req.remote_addr()));
    Ok(req)
}

// ------------------------------------------------------------------ //

// GET /status
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
async fn ping_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<Ping>(req).await?;

    // For debugging: delay responding to pings here to trigger timeouts
    // tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let mut cstate = state
        .connections
        .get_mut(&request.transaction_id)
        .ok_or_else(|| json_response_err(StatusCode::NOT_FOUND, "No active transaction"))?;

    cstate.value_mut().timer_id = {
        state.timeouts.cancel(cstate.value().timer_id);
        state.timeouts.insert(
            std::time::Duration::from_millis(PING_TIMEOUT_MSECS),
            request.transaction_id,
        )
    };

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

// GET /start
async fn start_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<StartRequest>(req).await?;
    start_request_impl(state, request)
}

fn start_request_impl(
    state: Arc<State>,
    request: StartRequest,
) -> Result<Response<Body>, RouteError> {
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

    let cmd = state.subgraph[node_id];
    cmd.call_preamble();

    let timer_id = state.timeouts.insert(
        std::time::Duration::from_millis(PING_TIMEOUT_MSECS),
        transaction_id,
    );

    state.connections.insert(
        transaction_id,
        ConnectionState {
            start_time_approx: std::time::Instant::now(),
            timer_id,
            cmd: cmd.clone(),
            node_id,
            disconnect_error_message: request.disconnect_error_message,
        },
    );

    json_success_resp(&StartResponse {
        transaction_id,
        cmdline: cmd.cmdline.clone(),
        env: cmd.env.clone(),
        ping_interval_msecs: PING_INTERVAL_MSECS,
    })
}

// POST /begun
async fn begun_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<BegunRequest>(req).await?;

    {
        let cstate = state
            .connections
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
async fn end_handler(req: Request<Body>) -> Result<Response<Body>, RouteError> {
    let state = get_state(&req);
    let request = get_json_body::<EndRequest>(req).await?;

    let cstate = {
        let (_transaction_id, cstate) = state
            .connections
            .remove(&request.transaction_id)
            .ok_or_else(|| json_response_err(StatusCode::NOT_FOUND, "No active transaction"))?;
        cstate
    };
    state.timeouts.cancel(cstate.timer_id);

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

    match request.start_request {
        Some(start_request) => start_request_impl(state, start_request),
        None => Ok(Response::builder().status(200).body("".into()).unwrap()),
    }
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
