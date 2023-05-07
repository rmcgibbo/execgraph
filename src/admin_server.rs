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
        let mut guard = state.provisioner.write().unwrap();
        guard.info = payload.provisioner_info.clone();
    }
    status_handler(Extension(state), None).await
}

async fn post_shutdown_handler(
    Extension(state): Extension<Arc<State<'static>>>,
    payload: Json<ShutdownRequest>,
) -> Result<Json<StatusReply>, AppError> {
    use time::format_description::well_known::Rfc2822;
    use time::OffsetDateTime;
    let local = OffsetDateTime::now_local().unwrap();

    eprintln!(
        "{}: \x1b[1;33mWarning\x1b[0m: {} shutdown triggered by admin '{}'",
        local.format(&Rfc2822).unwrap(),
        if payload.soft { "Soft" } else { "Hard" },
        payload.username,
    );
    if payload.soft {
        state.tracker.trigger_soft_shutdown();
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

    let mut path = PathBuf::from(format!(
        "/run/user/{}/{}-{}.sock",
        uid, ADMIN_SOCKET_PREFIX, pid
    ));

    if let Err(error) = std::fs::create_dir_all(path.parent().unwrap()) {
        if (error.kind() == std::io::ErrorKind::PermissionDenied)
            && std::env::var("SLURM_JOBID").is_ok()
        {
            path = PathBuf::from(format!(
                "/scratch/slurm/{}/{}-{}.sock",
                std::env::var("SLURM_JOBID").unwrap(),
                ADMIN_SOCKET_PREFIX,
                pid
            ));
        } else {
            tracing::error!(
                "Skipping admin socket. Not sure where to put it. Error: {}",
                error
            );
            return;
        }
    }
    let service = admin_router(state).into_make_service();
    use tokio_stream::wrappers::UnixListenerStream;

    // If path alreay exists, then a previous program may have used this path (because it has the same PID)
    // but shutdown uncleanly and never removed the file (kill -9 or something). Since the file is in /run, it's
    // specific to this box. And since we have our current PID, the other instance that made this file must
    // be over (this wouldn't be true if we were making this file on a shared filesystem).
    let _ = std::fs::remove_file(&path);
    let uds = tokio::net::UnixListener::bind(path.clone()).unwrap();
    let stream = UnixListenerStream::new(uds);
    let acceptor = hyper::server::accept::from_stream(stream);
    let server = Server::builder(acceptor).serve(service);
    tokio::spawn(clean_up_socket_dir(path.parent().unwrap().to_path_buf()));
    let graceful = server.with_graceful_shutdown(token.hard_cancelled());
    if let Err(err) = graceful.await {
        error!("Server error: {}", err);
    }
    std::fs::remove_file(path).unwrap();
}

async fn clean_up_socket_dir(socket_dir: PathBuf) -> std::io::Result<()> {
    // No rush on this, but go through any socket files in the directory and if the process doesn't
    // exist anmore, just remove then. Since we can't clean up our own socket file ourselves with
    // 100% accuracy, let later instances of the program do some removal.
    use regex::Regex;
    use std::os::unix::fs::FileTypeExt;
    use sysinfo::{Pid, SystemExt};
    let socket_re = Regex::new(&format!(r"{}-(\d+).sock", ADMIN_SOCKET_PREFIX)).unwrap();
    let mut system = sysinfo::System::new();

    for entry in std::fs::read_dir(socket_dir)? {
        if let Ok(entry) = entry {
            let path = entry.path();
            let pathstr = path.file_name().unwrap().to_string_lossy();
            let captures = socket_re.captures(&pathstr);
            if entry.file_type()?.is_socket() && captures.is_some() {
                let captures = captures.unwrap();
                let pid = captures
                    .get(1)
                    .map_or("", |m| m.as_str())
                    .parse::<usize>()
                    .unwrap();
                if !system.refresh_process(Pid::from(pid)) {
                    tracing::debug!(
                        "No such process: {pid} Removing unused socket file {:?}",
                        path
                    );
                    std::fs::remove_file(path)?;
                }
            }
        }
    }

    Ok(())
}
