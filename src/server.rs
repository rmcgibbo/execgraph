use crate::execgraph::Cmd;
use crate::httpinterface::*;
use crate::sync::StatusUpdater;
use anyhow::Result;
use async_channel::{bounded, Receiver, Sender};
use hyper::{Body, Request, Response, StatusCode};
use petgraph::graph::{DiGraph, NodeIndex};
use routerify::ext::RequestExt;
use routerify::{Middleware, RequestInfo, Router};
use routerify_json_response::{json_failed_resp_with_message, json_success_resp};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

static PING_INTERVAL_MSECS: u64 = 15_000;

pub fn get_random_u32() -> u32 {
    let mut buf = [0u8; 4];
    getrandom::getrandom(&mut buf).unwrap();
    u32::from_be_bytes(buf)
}

struct ConnectionState {
    pings: Sender<()>,
    cancel: CancellationToken,
    cmd: Cmd,
    node_id: NodeIndex,
    pid: Option<u32>,
}

pub struct State {
    connections: Mutex<HashMap<u32, ConnectionState>>,
    subgraph: Arc<DiGraph<(Cmd, NodeIndex), ()>>,
    tasks_ready: Receiver<NodeIndex>,
    status_updater: StatusUpdater,
}

impl State {
    pub fn new(
        subgraph: Arc<DiGraph<(Cmd, NodeIndex), ()>>,
        tasks_ready: Receiver<NodeIndex>,
        status_updater: StatusUpdater,
    ) -> State {
        State {
            connections: Mutex::new(HashMap::new()),
            subgraph,
            tasks_ready,
            status_updater,
        }
    }
}

// Define an error handler function which will accept the `routerify::Error`
// and the request information and generates an appropriate response.
async fn error_handler(err: routerify::RouteError, _: RequestInfo) -> Response<Body> {
    log::warn!("{}", err);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(format!("Something went wrong: {}", err)))
        .unwrap()
}

async fn get_json_body<T: DeserializeOwned>(req: &mut Request<Body>) -> Result<T> {
    let body = hyper::body::to_bytes(req.body_mut()).await?;
    serde_json::from_slice(&body.to_vec().as_slice()).map_err(|err| err.into())
}

async fn middleware_after(
    res: Response<Body>,
    req_info: RequestInfo,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let (started, remote_addr) = req_info
        .context::<(tokio::time::Instant, SocketAddr)>()
        .unwrap();
    let duration = started.elapsed();
    log::trace!(
        "{} {} {} {}us {}",
        remote_addr,
        req_info.method(),
        req_info.uri().path(),
        duration.as_micros(),
        res.status().as_u16()
    );
    Ok(res)
}

async fn middleware_before(
    req: Request<Body>,
) -> Result<Request<Body>, routerify_json_response::Error> {
    req.set_context((tokio::time::Instant::now(), req.remote_addr()));
    Ok(req)
}

async fn ping_timeout_handler(transaction_id: u32, state: Arc<State>) {
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

    match cstate.pid {
        Some(pid) => {
            state
                .status_updater
                .send_finished(cstate.node_id, &cstate.cmd, pid, timeout_status)
                .await;
        }
        None => {
            log::warn!("Protocol out of order");
        }
    };
}

// ------------------------------------------------------------------ //

// GET /ready
async fn status_handler(
    req: Request<Body>,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let state = req.data::<Arc<State>>().unwrap();
    let snapshot = state.status_updater.get_queuestate();
    let num_ready = state.tasks_ready.len() as u32;

    // the 'pending' count includes all tasks between the stages of having
    // been added to the ready queue and having been marked as completed.
    // so we'll say that the that can be broken into the number in the ready
    // queue and the number that are currently inflight.
    if snapshot.n_pending < num_ready {
        panic!("this shouldn't happen, and indiciates some kind of internal accounting bug");
    }

    json_success_resp(&StatusReply {
        num_ready,
        num_failed: snapshot.n_failed,
        num_success: snapshot.n_success,
        num_inflight: snapshot.n_pending - num_ready,
    })
}

// POST /ping
async fn ping_handler(
    mut req: Request<Body>,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let request = match get_json_body::<Ping>(&mut req).await {
        Ok(request) => request,
        Err(e) => {
            return json_failed_resp_with_message(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("{}", e),
            );
        }
    };

    // For debugging: delay responding to pings here to trigger timeouts
    // tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let state = req.data::<Arc<State>>().unwrap();
    let resp = match state.connections.lock().await.get(&request.transaction_id) {
        Some(cstate) => {
            cstate.pings.send(()).await?;
            Ok(Response::builder().status(200).body("".into()).unwrap())
        }
        None => json_failed_resp_with_message(StatusCode::NOT_FOUND, "No active transaction"),
    };

    resp
}

// GET /start
async fn start_handler(
    req: Request<Body>,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let transaction_id = get_random_u32();
    let state = req.data::<Arc<State>>().unwrap();

    let (ping_tx, ping_rx) = bounded::<()>(1);
    let token = CancellationToken::new();
    let token1 = token.clone();
    let state1 = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to = timeout(std::time::Duration::from_millis(2*PING_INTERVAL_MSECS), ping_rx.recv()) => {
                    if to.is_err() {
                        ping_timeout_handler(transaction_id, state1).await;
                        break;
                    }
                }
                _ = token1.cancelled() => {
                    break;
                }
            }
        }
    });

    let node_id = match state.tasks_ready.recv().await {
        Ok(node_id) => {node_id},
        Err(e) => {
            return json_failed_resp_with_message(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("{}", e),
            );
        }
    };
    let (cmd, _) = state
        .subgraph
        .node_weight(node_id)
        .unwrap_or_else(|| panic!("failed to get node {:#?}", node_id));
    let cmdline = cmd.cmdline.clone();

    {
        let mut lock = state.connections.lock().await;
        lock.insert(
            transaction_id,
            ConnectionState {
                pings: ping_tx,
                cancel: token,
                cmd: cmd.clone(),
                node_id,
                pid: None,
            },
        );
    }

    json_success_resp(&StartResponse {
        transaction_id,
        cmdline,
        ping_interval_msecs: PING_INTERVAL_MSECS,
    })
}

// POST /begun
async fn begun_handler(
    mut req: Request<Body>,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let request = match get_json_body::<BegunRequest>(&mut req).await {
        Ok(request) => request,
        Err(e) => {
            return json_failed_resp_with_message(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("{}", e),
            );
        }
    };
    let state = req.data::<Arc<State>>().unwrap();

    {
        let mut lock = state.connections.lock().await;
        match lock.get_mut(&request.transaction_id) {
            Some(cstate) => {
                state
                    .status_updater
                    .send_started(cstate.node_id, &cstate.cmd, request.pid)
                    .await;
                cstate.pid = Some(request.pid);
            }
            None => {
                return json_failed_resp_with_message(
                    StatusCode::NOT_FOUND,
                    "No active transaction",
                );
            }
        }
    }

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

// POST /end
async fn end_handler(
    mut req: Request<Body>,
) -> Result<Response<Body>, routerify_json_response::Error> {
    let request = match get_json_body::<EndRequest>(&mut req).await {
        Ok(request) => request,
        Err(e) => {
            return json_failed_resp_with_message(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("{}", e),
            );
        }
    };
    let state = req.data::<Arc<State>>().unwrap();

    let cstate = {
        let mut lock = state.connections.lock().await;
        let cstate = match lock.remove(&request.transaction_id) {
            Some(cs) => cs,
            None => {
                return json_failed_resp_with_message(
                    StatusCode::NOT_FOUND,
                    "No active transaction",
                );
            }
        };

        cstate.cancel.cancel();
        cstate
    };

    match cstate.pid {
        Some(pid) => {
            state
                .status_updater
                .send_finished(cstate.node_id, &cstate.cmd, pid, request.status)
                .await;
        }
        None => {
            return json_failed_resp_with_message(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Protocol out of order",
            );
        }
    };

    Ok(Response::builder().status(200).body("".into()).unwrap())
}

pub fn router(state: State) -> Router<Body, routerify_json_response::Error> {
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
