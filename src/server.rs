use crate::{
    async_flag::AsyncFlag,
    constants::PING_TIMEOUT_MSECS,
    execgraph::{Cmd, RemoteProvisionerSpec},
    fancy_cancellation_token::{self, CancellationState},
    http_extensions::axum::Postcard,
    httpinterface::*,
    sync::{ExitStatus, FinishedEvent, ReadyTrackerClient},
    time::timewheel::{TimeWheel, TimerID},
};
use anyhow::Result;
use axum::{
    headers::authorization::Bearer,
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json, Router, TypedHeader,
};
use dashmap::{DashMap, DashSet};
use hyper::{header::AUTHORIZATION, Body, Request, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Instant,
};
use std::{sync::Mutex, time::Duration};
use tokio::sync::oneshot;
use tower::ServiceBuilder;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use tower_http::ServiceBuilderExt;
use tracing::{error, Span};

use crate::constants::{HOLD_RATETIMITED_TIME, PING_INTERVAL_MSECS};

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
    slurm_jobid: String,
}

pub struct State<'a> {
    /// For each open transaction (i.e. a execgraph-remote working on a task(), we track
    /// index it via a random transaction id (u32) which is given out during the /start
    /// and then required for each subsequent call. Each connection maintains some state
    /// about what command is being worked on, when it started, and stuff like that.
    connections: DashMap<u32, ConnectionState>,
    subgraph: Arc<DiGraph<&'a Cmd, ()>>,
    /// the ReadyTrackerClient is the datastructure in sync.rs that maintains the queues
    /// of which taks are ready. this is where the server gets tasks from to respond to clients
    /// with, and what it informs when tasks finish.
    pub(crate) tracker: &'a ReadyTrackerClient,
    /// this some metadata that can be maniupated and is used by the provisioner to launch
    /// more runners.
    pub(crate) provisioner: RwLock<RemoteProvisionerSpec>,
    /// this is the cancellation token, when we trigger it things are supposed to shut
    /// down
    pub(crate) token: fancy_cancellation_token::CancellationToken,
    /// this datastructure is used to figure out when tasks haven't pinged recently enough
    /// and should be considered dead
    timeouts: TimeWheel<u32>,
    /// the /start, /begun, /end, and /ping endpoints are designed to only be called from
    /// execgraph-remote, so they required a bearer token to be set in the http headers,
    /// and we check whether it's equal to this. the /status endpoint can be called by anyone,
    /// and the functions in the auth_server.rs server which is listening on a unix socket
    /// can be called by anyone with OS-level permission to access the unix socket.
    /// The point of this bearer token is really to make sure that it's only the execgraph-remote
    /// runners from _this_ workflow that are contacting this server. It's not real security.
    authorization_token: String,

    /// Avoid a race requires us to know slurm jobids that have been
    /// scanceled so we don't hand out tasks to those runners.
    cancelled_slurm_jobids: RwLock<HashSet<String>>,


}

impl<'a> State<'a> {
    pub fn new(
        subgraph: Arc<DiGraph<&'a Cmd, ()>>,
        tracker: &'a ReadyTrackerClient,
        token: fancy_cancellation_token::CancellationToken,
        provisioner: RemoteProvisionerSpec,
        authorization_token: String,
    ) -> State<'a> {
        State {
            connections: DashMap::new(),
            subgraph,
            tracker,
            token,
            timeouts: TimeWheel::new(Duration::from_millis(TIMEWHEEL_DURATION_MSECS)),
            provisioner: RwLock::new(provisioner),
            authorization_token,
            cancelled_slurm_jobids: RwLock::new(HashSet::new()),
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
pub enum AppError {
    Shutdown,
    NoSuchTransaction,
    NoSuchRunnerType,
    RateLimited,
    Unauthorized,
    BadRequest,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::Shutdown => StatusCode::GONE,
            AppError::NoSuchTransaction => StatusCode::NOT_FOUND,
            AppError::NoSuchRunnerType => StatusCode::NOT_FOUND,
            AppError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::BadRequest => StatusCode::BAD_REQUEST,
        };
        let body = Json(json!({ "message": format!("{:#?}", self) }));

        (status, body).into_response()
    }
}

// ------------------------------------------------------------------ //

// GET /status
pub async fn status_handler(
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
        rate: state.tracker.get_rate(),
        ratelimit: state.tracker.get_ratelimit(),
        provisioner_info: state.provisioner.read().unwrap().info.clone(),
    }))
}

// POST /ping
async fn ping_handler(
    TypedHeader(authorization): TypedHeader<axum::headers::Authorization<Bearer>>,
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<Ping>,
) -> Result<Response, AppError> {
    if authorization.token() != state.authorization_token {
        return Err(AppError::Unauthorized);
    };
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

// POST /mark-slurm-job-cancelation
async fn mark_slurm_job_cancelation(
    TypedHeader(authorization): TypedHeader<axum::headers::Authorization<Bearer>>,
    Extension(state): Extension<Arc<State<'static>>>,
    Json(request): Json<MarkSlurmJobCancelationRequest>,
) -> Result<Postcard<MarkSlurmJobCancelationReply>, AppError> {
    if authorization.token() != state.authorization_token {
        return Err(AppError::Unauthorized);
    };

    let mut reply_jobids = Vec::new();

    // Acquire a write lock on cancelled_slurm_jobids
    let mut cancelled_slurm_jobids = state.cancelled_slurm_jobids.write().unwrap();

    // Find all running jobids
    let running_jobids = state.connections.iter().map(|c| c.slurm_jobid.clone()).collect::<HashSet<String>>();
    // Add the requested slurm jobids to the set that are canceled if
    // they're not already running

    for jobid in request.jobids.into_iter() {
        if !running_jobids.contains(&jobid) {
            reply_jobids.push(jobid.clone());
            cancelled_slurm_jobids.insert(jobid);

        }
    };

    // Drop the write lock
    drop(cancelled_slurm_jobids);

    Ok(Postcard(MarkSlurmJobCancelationReply {jobids: reply_jobids}))
}

// GET /start
async fn start_handler(
    TypedHeader(authorization): TypedHeader<axum::headers::Authorization<Bearer>>,
    headers: axum::http::header::HeaderMap,
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<StartRequest>,
) -> Result<Postcard<StartResponse>, AppError> {

    // check auth token
    if authorization.token() != state.authorization_token {
        return Err(AppError::Unauthorized);
    };

    // extract required slurm jobid
    let runner_slurm_jobid = match headers.get("X-EXECGRAPH-SLURM-JOB-ID") {
        Some(headervalue) => match headervalue.to_str() {
            Ok(jobid) => jobid.to_owned(),
            Err(_) => {return Err(AppError::BadRequest);}
        }
        None => {return Err(AppError::BadRequest)}
    };

    // If this jobid has been scanceled, don't give out a job and just tell the runner
    // to shut down. This happens with a read lock on cancelled_slurm_jobids so if someone
    // is modying the set of cancelled_slurm_jobids, we have to wait till they finish to
    // know if we should tell this runner to shut down.
    {
        let cancelled_slurm_jobids = state.cancelled_slurm_jobids.read().unwrap();
        if cancelled_slurm_jobids.contains(&runner_slurm_jobid) {
            return Err(AppError::Shutdown);
        }
    }

    Ok(Postcard(start_request_impl(state, request, runner_slurm_jobid).await?))
}

async fn start_request_impl(
    state: Arc<State<'static>>,
    request: StartRequest,
    runner_slurm_jobid: String,
) -> Result<StartResponse, AppError> {
    // Grab the next task off the ready queue, which might pause if we're rate limited.
    // Hold the connection for up to ``HOLD_RATELIMITED_TIME`` (30 seconds), and then if
    // we're still rate limited, respond to the execgraph-remote with a "RateLimited"
    // (a 429 http code) which will cause it to exit.
    let node_id = async {
        tokio::select! {
            _ = state.token.hard_cancelled() => {
                Err(AppError::Shutdown)
            },
            _ = tokio::time::sleep(HOLD_RATETIMITED_TIME) => {
                Err(AppError::RateLimited)
            }
            r = state.tracker.recv(request.runnertypeid) => {
                r.map_err(|e| match e {
                    crate::sync::ReadyTrackerClientError::ChannelRecvError(_) => AppError::Shutdown,
                    crate::sync::ReadyTrackerClientError::SoftShutdown => AppError::Shutdown,
                    crate::sync::ReadyTrackerClientError::NoSuchRunnerType => AppError::NoSuchRunnerType,
                    crate::sync::ReadyTrackerClientError::ChannelTryRecvError(_) => unreachable!(),
                })
            }
        }
    }.await?;
    let cmd = state.subgraph[node_id];
    cmd.call_preamble();
    let transaction_id = rand::random::<u32>();

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
            slurm_jobid: runner_slurm_jobid,
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
    TypedHeader(authorization): TypedHeader<axum::headers::Authorization<Bearer>>,
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<BegunRequest>,
) -> Result<Response, AppError> {
    if authorization.token() != state.authorization_token {
        return Err(AppError::Unauthorized);
    };
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
    TypedHeader(authorization): TypedHeader<axum::headers::Authorization<Bearer>>,
    headers: axum::http::header::HeaderMap,
    Extension(state): Extension<Arc<State<'static>>>,
    Postcard(request): Postcard<EndRequest>,
) -> Result<Postcard<EndResponse>, AppError> {

    // check auth token
    if authorization.token() != state.authorization_token {
        return Err(AppError::Unauthorized);
    };

    // extract required slurm jobid
    let runner_slurm_jobid = match headers.get("X-EXECGRAPH-SLURM-JOB-ID") {
        Some(headervalue) => match headervalue.to_str() {
            Ok(jobid) => jobid.to_owned(),
            Err(_) => {return Err(AppError::BadRequest);}
        }
        None => {return Err(AppError::BadRequest)}
    };

    let cstate = {
        let (_transaction_id, cstate) = state
            .connections
            .remove(&request.transaction_id)
            .ok_or(AppError::NoSuchTransaction)?;
        cstate
    };
    // If the RPC trying to end a job is coming from a different slurm jobid
    // than the RPC which started the job, something fishy is going on.
    if cstate.slurm_jobid != runner_slurm_jobid {
        return Err(AppError::NoSuchTransaction);
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
            let resp = start_request_impl(state, start_request, runner_slurm_jobid).await?;
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

//
// Router for the main server. This server is serving on a random port, and
// its job is primarily to give work out to execgraph-remote and accept finished
// work.
pub fn router(state: Arc<State<'static>>) -> Router {
    use axum::routing::{get, post};
    use tower_http::trace::TraceLayer;
    use tower_http::trace::{DefaultOnEos, DefaultOnFailure};
    use tracing::Level;

    let middleware = ServiceBuilder::new()
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            AUTHORIZATION,
        )))
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
        .route("/mark-slurm-job-cancelation", post(mark_slurm_job_cancelation))
        .fallback(fallback)
        .layer(middleware.into_inner())
}
