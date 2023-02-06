use crate::constants::ADMIN_SOCKET_PREFIX;
use crate::fancy_cancellation_token::CancellationState;
use crate::fancy_cancellation_token::CancellationToken;
use crate::httpinterface::ShutdownRequest;
use crate::httpinterface::StatusReply;
use crate::httpinterface::UpdateRatelimitRequest;
use crate::httpinterface::UpdateRemoteProvisionerInfoRequest;
use crate::server::status_handler;
use crate::server::{AppError, State};
use axum::routing::{get, post};
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::Server;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

async fn post_ratelimit_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    payload: Json<UpdateRatelimitRequest>,
) -> Result<Json<StatusReply>, AppError> {
    state.tracker.set_ratelimit(payload.per_second);
    status_handler(Extension(state), None).await
}

async fn post_provisioner_info_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    payload: Json<UpdateRemoteProvisionerInfoRequest>,
) -> Result<Json<StatusReply>, AppError> {
    {
        let mut guard = state.provisioner.lock().unwrap();
        guard.info = payload.provisioner_info.clone();
    }
    status_handler(Extension(state), None).await
}

async fn post_shutdown_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    payload: Json<ShutdownRequest>,
) -> Result<Json<StatusReply>, AppError> {
    if payload.soft {
        state
            .token
            .cancel(CancellationState::CancelledAfterTime(Instant::now()));
    } else {
        state.token.cancel(CancellationState::HardCancelled);
    }
    status_handler(Extension(state), None).await
}

fn admin_router(state: Arc<State<'static>>) -> Router {
    Router::new()
        .route("/ratelimit", post(post_ratelimit_handler))
        .route("/provisioner_info", post(post_provisioner_info_handler))
        .route("/status", get(status_handler))
        .route("/shutdown", post(post_shutdown_handler))
        .layer(Extension(state))
}

pub async fn run_admin_service_forever(state: Arc<State<'static>>, token: CancellationToken) {
    let uid = unsafe { libc::getuid() };
    let pid = std::process::id();

    let path = PathBuf::from(format!(
        "/run/user/{}/{}-{}.sock",
        uid, ADMIN_SOCKET_PREFIX, pid
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let service = admin_router(state).into_make_service();
    use tokio_stream::wrappers::UnixListenerStream;

    let uds = tokio::net::UnixListener::bind(path.clone()).unwrap();
    let stream = UnixListenerStream::new(uds);
    let acceptor = hyper::server::accept::from_stream(stream);
    let server = Server::builder(acceptor).serve(service);
    let graceful = server.with_graceful_shutdown(token.hard_cancelled());
    if let Err(err) = graceful.await {
        error!("Server error: {}", err);
    }
    std::fs::remove_file(path).unwrap();
}
