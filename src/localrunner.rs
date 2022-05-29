use crate::utils::{CancellationState, CancellationToken};
use anyhow::Result;
use petgraph::graph::DiGraph;
use std::{
    collections::HashMap,
    os::unix::prelude::{ExitStatusExt, FromRawFd, RawFd},
    process::Stdio,
    sync::Arc,
};
use thiserror::Error;
use tokio::{io::AsyncReadExt, process::Command};
use tokio_command_fds::{CommandFdExt, FdMapping};
use tracing::debug;

use crate::{execgraph::Cmd, logfile2::ValueMaps, sync::ReadyTrackerClient};

#[derive(Debug)]
pub enum LocalQueueType {
    NormalLocalQueue,
    ConsoleQueue,
}

pub async fn run_local_process_loop(
    subgraph: Arc<DiGraph<&Cmd, ()>>,
    tracker: &ReadyTrackerClient,
    token: CancellationToken,
    local_queue_type: LocalQueueType,
) {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let runnertypeid = match &local_queue_type {
        LocalQueueType::NormalLocalQueue => 0,
        LocalQueueType::ConsoleQueue => 1,
    };

    while let Ok(subgraph_node_id) = tracker.recv(runnertypeid).await {
        // SAFETY: we're making a pipe to pass to the child process as fd3.
        // Need to be careful.
        let (read_fd3, write_fd3) = anon_pipe();

        let cmd = subgraph[subgraph_node_id];
        cmd.call_preamble();

        let mut command = Command::new(&cmd.cmdline[0]);
        let maybe_child = match &local_queue_type {
            LocalQueueType::NormalLocalQueue => command
                .args(&cmd.cmdline[1..])
                .kill_on_drop(true)
                .envs(cmd.env.iter().cloned())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
            LocalQueueType::ConsoleQueue => command
                .args(&cmd.cmdline[1..])
                .kill_on_drop(true)
                .envs(cmd.env.iter().cloned())
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit()),
        }
        .fd_mappings(vec![
            // Map the pipe as FD 3 in the child process.
            FdMapping {
                parent_fd: write_fd3.as_raw_fd(),
                child_fd: 3,
            },
        ])
        .unwrap()
        .spawn();

        // Deadlock potential: After spawning the command and passing it the
        // write end of the pipe we need to close it ourselves
        if unsafe { libc::close(write_fd3.as_raw_fd()) } != 0 {
            panic!("Cannot close: {}", std::io::Error::last_os_error());
        }
        // SAFETY: this takes ownership over the file descriptor and will close it.
        let fd3_file = unsafe { tokio::fs::File::from_raw_fd(read_fd3.into_raw_fd()) };
        let start_time = std::time::SystemTime::now();

        let child = match maybe_child {
            Ok(child) => child,
            Err(_) => {
                tracker
                    .send_started(subgraph_node_id, cmd, &hostname, 0)
                    .await;
                tracker
                    .send_finished(
                        subgraph_node_id,
                        cmd,
                        127,
                        "".to_owned(),
                        format!("No such command: {:#?}", &cmd.cmdline[0]),
                        ValueMaps::new(),
                    )
                    .await;
                continue;
            }
        };

        let pid = child
            .id()
            .expect("child hasn't been waited for yet, so its pid should exist");

        tracker
            .send_started(subgraph_node_id, cmd, &hostname, pid)
            .await;

        let (output, fd3_values) = tokio::select! {
            cancel = token.soft_cancelled(start_time) => {
                if let CancellationState::CancelledAfterTime(_) = cancel {
                    // if we were soft canceled, send a finished notification.
                    tracker
                        .send_finished(subgraph_node_id, cmd, 130, "".to_string(), "".to_string(), vec![])
                        .await;
                }
                debug!("Received cancellation");
                return;
            },
            wait_with_output = wait_for_child_output_and_another_file_descriptor(child, fd3_file) => {
                wait_with_output.expect("sh wasn't running")
            }
        };

        let status = match output.status.code() {
            Some(code) => code,
            None => output.status.signal().expect("No exit code and no signal?"),
        };
        let (stdout, stderr) = match &local_queue_type {
            LocalQueueType::ConsoleQueue => ("".to_string(), "".to_string()),
            LocalQueueType::NormalLocalQueue => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                (stdout, stderr)
            }
        };
        tracing::debug!("Finished cmd");
        tracker
            .send_finished(subgraph_node_id, cmd, status, stdout, stderr, fd3_values)
            .await;
    }

    debug!("Exiting loop at 141 (LocalQueueType={:#?}", local_queue_type);
}

pub async fn wait_for_child_output_and_another_file_descriptor(
    child: tokio::process::Child,
    fd3_file: tokio::fs::File,
) -> Result<(std::process::Output, ValueMaps), ChildProcessError> {
    async fn read_to_end(mut fd3_file: tokio::fs::File) -> std::io::Result<Vec<u8>> {
        let mut fd3_read_buffer = Vec::new();
        fd3_file.read_to_end(&mut fd3_read_buffer).await?;
        Ok(fd3_read_buffer)
    }

    fn parse_line(line: &str) -> Result<HashMap<String, String>, shell_words::ParseError> {
        shell_words::split(line).map(|fields| {
            fields
                .iter()
                .flat_map(|s| {
                    s.find('=')
                        .map(|pos| (s[..pos].to_string(), s[pos + 1..].to_string()))
                })
                .collect::<HashMap<String, String>>()
        })
    }

    // we need to read from the fd3 pipe and wait for the normal stdout/stderr
    // concurrently, otherwise it might deadlock when the pipe gets full
    let (a, fd3_bytes) = futures::join!(child.wait_with_output(), read_to_end(fd3_file));

    // Parse key-value pairs on fd3
    let values = std::str::from_utf8(&fd3_bytes?)
        .map(|s| {
            s.lines()
                .filter_map(|line| parse_line(line).ok())
                .collect::<ValueMaps>()
        })
        .unwrap_or_default();

    Ok((a?, values))
}

#[derive(Debug, Error)]
pub enum ChildProcessError {
    #[error("{0}")]
    Utf8Error(
        #[from]
        #[source]
        std::str::Utf8Error,
    ),

    #[error("{0}")]
    IoError(
        #[from]
        #[source]
        std::io::Error,
    ),
}

#[derive(Copy, Clone)]
pub struct FileDesc {
    fd: RawFd,
}

pub fn anon_pipe() -> (FileDesc, FileDesc) {
    let mut fds = [0; 2];

    // The only known way right now to create atomically set the CLOEXEC flag is
    // to use the `pipe2` syscall. This was added to Linux in 2.6.27, glibc 2.9
    // and musl 0.9.3, and some other targets also have it.
    unsafe {
        if libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) != 0 {
            panic!("Cannot create pipe: {}", std::io::Error::last_os_error());
        }
        (FileDesc::from_raw_fd(fds[0]), FileDesc::from_raw_fd(fds[1]))
    }
}

impl FileDesc {
    unsafe fn from_raw_fd(fd: i32) -> Self {
        Self { fd }
    }
    pub fn as_raw_fd(&self) -> i32 {
        self.fd
    }
    pub fn into_raw_fd(self) -> i32 {
        self.fd
    }
}
