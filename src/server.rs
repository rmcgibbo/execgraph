use crate::{
    async_flag::AsyncFlag,
    constants::PING_TIMEOUT_MSECS,
    execgraph::Cmd,
    fancy_cancellation_token::{self, CancellationState},
    http_extensions::axum::Postcard,
    httpinterface::*,
    sync::{ExitStatus, FinishedEvent, ReadyTrackerClient},
    timewheel::{TimeWheel, TimerID},
};
use anyhow::Result;
use axum::{
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json, Router,
};
use dashmap::DashMap;
use hyper::{Body, Request, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Instant};
use std::{sync::Mutex, time::Duration};
use tokio::sync::oneshot;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tracing::{error, Span};

use crate::constants::PING_INTERVAL_MSECS;

const TIMEWHEEL_DURATION_MSECS: u64 = PING_TIMEOUT_MSECS + 1;

type Histogram = quantiles::ckms::CKMS<u32>;

lazy_static::lazy_static! {
    static ref END_LATENCY:   Mutex<Histogram> = Mutex::new(Histogram::new(0.01));
    static ref START_LATENCY: Mutex<Histogram> = Mutex::new(Histogram::new(0.01));
    static ref BEGUN_LATENCY: Mutex<Histogram> = Mutex::new(Histogram::new(0.01));
    static ref PING_LATENCY: Mutex<Histogram> = Mutex::new(Histogram::new(0.01));
}

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
    token: fancy_cancellation_token::CancellationToken,
    timeouts: TimeWheel<u32>,
}

impl<'a> State<'a> {
    pub fn new(
        subgraph: Arc<DiGraph<&'a Cmd, ()>>,
        tracker: &'a ReadyTrackerClient,
        token: fancy_cancellation_token::CancellationToken,
    ) -> State<'a> {
        State {
            connections: DashMap::new(),
            subgraph,
            tracker,
            token,
            timeouts: TimeWheel::new(Duration::from_millis(TIMEWHEEL_DURATION_MSECS)),
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
                        &cstate.cmd,
                        FinishedEvent::new_disconnected(
                            cstate.node_id,
                            cstate.disconnect_error_message,
                        ),
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
enum AppError {
    Shutdown,
    NoSuchTransaction,
    NoSuchRunnerType,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::Shutdown => StatusCode::GONE,
            AppError::NoSuchTransaction => StatusCode::NOT_FOUND,
            AppError::NoSuchRunnerType => StatusCode::NOT_FOUND,
        };
        let body = Json(json!({ "message": format!("{:#?}", self) }));

        (status, body).into_response()
    }
}

// ------------------------------------------------------------------ //

// GET /status
async fn status_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    payload: Option<Json<StatusRequest>>,
) -> Result<Json<StatusReply>, AppError> {
    let request = match payload {
        None => StatusRequest {
            timeout_ms: 0,
            timemin_ms: 0,
            etag: 0,
        },
        Some(Json(r)) => r,
    };

    let (etag, snapshot) = state
        .tracker
        .get_queuestate(
            request.etag,
            Duration::from_millis(request.timemin_ms),
            Duration::from_millis(request.timeout_ms),
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

    Ok(Json(StatusReply {
        queues: resp,
        etag,
        server_metrics: collect_server_metrics(),
    }))
}

// POST /ping
async fn ping_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<Ping>,
) -> Result<Response, AppError> {
    // For debugging: delay responding to pings here to trigger timeouts
    // tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let mut cstate = match state.connections.get_mut(&request.transaction_id) {
        Some(cstate) => cstate,
        None => return Ok((StatusCode::OK, "").into_response()),
    };

    cstate.value_mut().timer_id = {
        state.timeouts.cancel(cstate.value().timer_id);
        state.timeouts.insert(
            std::time::Duration::from_millis(PING_TIMEOUT_MSECS),
            request.transaction_id,
        )
    };

    Ok((StatusCode::OK, "").into_response())
}

// GET /start
async fn start_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<StartRequest>,
) -> Result<Postcard<StartResponse>, AppError> {
    Ok(Postcard(start_request_impl(state, request)?))
}

fn start_request_impl(state: Arc<State>, request: StartRequest) -> Result<StartResponse, AppError> {
    let transaction_id = rand::random::<u32>();

    let node_id = state
        .tracker
        .try_recv(request.runnertypeid)
        .map_err(|e| match e {
            crate::sync::ReadyTrackerClientError::ChannelTryRecvError(_) => AppError::Shutdown,
            crate::sync::ReadyTrackerClientError::NoSuchRunnerType => AppError::NoSuchRunnerType,
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

    Ok(StartResponse {
        transaction_id,
        cmdline: cmd.cmdline.clone(),
        fd_input: cmd.fd_input.clone(),
        ping_interval_msecs: PING_INTERVAL_MSECS,
    })
}

// POST /begun
async fn begun_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<BegunRequest>,
) -> Result<Response, AppError> {
    let cstate = state
        .connections
        .get_mut(&request.transaction_id)
        .ok_or(AppError::NoSuchTransaction)?;

    state
        .tracker
        .send_started(cstate.node_id, &cstate.cmd, &request.host, request.pid)
        .await;
    Ok((StatusCode::OK, "").into_response())
}

// POST /end
async fn end_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<EndRequest>,
) -> Result<Postcard<EndResponse>, AppError> {
    let cstate = {
        let (_transaction_id, cstate) = state
            .connections
            .remove(&request.transaction_id)
            .ok_or(AppError::NoSuchTransaction)?;
        cstate
    };
    state.timeouts.cancel(cstate.timer_id);
    let flag = AsyncFlag::new();

    state
        .tracker
        .send_finished(
            &cstate.cmd,
            FinishedEvent {
                id: cstate.node_id,
                status: ExitStatus::Code(request.status),
                stdout: request.stdout,
                stderr: request.stderr,
                values: request.values,
                flag: Some(flag.clone()),
            },
        )
        .await;

    match request.start_request {
        Some(start_request) => {
            tokio::select! {
            _ = state.token.hard_cancelled() => {}
            _ = flag.wait() => {}
            };
            let resp = start_request_impl(state, start_request)?;
            Ok(Postcard(EndResponse {
                start_response: Some(resp),
            }))
        }
        None => Ok(Postcard(EndResponse {
            start_response: None,
        })),
    }
}

fn collect_server_metrics() -> ServerMetrics {
    let mut p50_latency = HashMap::new();
    let mut p99_latency = HashMap::new();
    for (key, hist) in [
        ("ping", &*PING_LATENCY),
        ("start", &*START_LATENCY),
        ("end", &*END_LATENCY),
        ("begun", &*BEGUN_LATENCY),
    ] {
        p50_latency.insert(
            key.to_owned(),
            hist.lock().unwrap().query(0.50).map(|x| x.1).unwrap_or(0),
        );
        p99_latency.insert(
            key.to_owned(),
            hist.lock().unwrap().query(0.99).map(|x| x.1).unwrap_or(0),
        );
    }
    ServerMetrics {
        p50_latency,
        p99_latency,
    }
}

async fn fallback(
    method: hyper::Method,
    uri: hyper::Uri,
    body: axum::body::Bytes,
) -> (StatusCode, String) {
    error!("/404 method={} url={} body={:?}", method, uri, body);
    (
        StatusCode::NOT_FOUND,
        format!("`{}` not allowed for {}", method, uri),
    )
}

async fn custom_middleware<B>(req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    let start = std::time::Instant::now();
    let hist = match req.uri().path() {
        "/end" => Some(&*END_LATENCY),
        "/begun" => Some(&*BEGUN_LATENCY),
        "/start" => Some(&*START_LATENCY),
        "/ping" => Some(&*PING_LATENCY),
        _ => None,
    };
    let out = next.run(req).await;
    if let Some(hist) = hist {
        let mut guard = hist.lock().unwrap();
        guard.insert(start.elapsed().as_micros() as u32);
    }
    Ok(out)
}

pub fn router(state: Arc<State<'static>>) -> Router {
    use axum::handler::Handler;
    use axum::routing::{get, post};
    use tower_http::trace::TraceLayer;
    use tower_http::trace::{DefaultOnEos, DefaultOnFailure};
    use tracing::Level;

    let middleware = ServiceBuilder::new()
        .layer(axum::middleware::from_fn(custom_middleware))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    let ray = rand::random::<u32>();
                    tracing::info_span!("req", "uri" = request.uri().path(), "id" = ray)
                })
                .on_request(|request: &Request<Body>, _span: &Span| {
                    tracing::info!(
                        "started {:?} {} {}",
                        request.headers(),
                        request.method(),
                        request.uri().path()
                    )
                })
                .on_response(|response: &Response<_>, latency: Duration, _span: &Span| {
                    tracing::info!(
                        "status={} latency={:?}",
                        response.status().as_u16(),
                        latency
                    )
                })
                .on_failure(DefaultOnFailure::new().level(Level::INFO))
                .on_eos(DefaultOnEos::new().level(Level::INFO)),
        )
        .add_extension(state);

    Router::new()
        .route("/start", get(start_handler))
        .route("/begun", post(begun_handler))
        .route("/end", post(end_handler))
        .route("/ping", post(ping_handler))
        .route("/status", get(status_handler))
        .fallback(fallback.into_service())
        .layer(middleware.into_inner())
}
