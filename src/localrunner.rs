use crate::{
    fancy_cancellation_token::{CancellationState, CancellationToken},
    sync::ExitStatus,
};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use petgraph::prelude::*;
use std::{
    os::unix::prelude::{AsRawFd, ExitStatusExt},
    process::Stdio,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tokio_command_fds::{CommandFdExt, FdMapping};
use tokio_pipe::PipeRead;
use tracing::debug;

use crate::{
    execgraph::Cmd,
    sync::{FinishedEvent, ReadyTrackerClient},
};

// This averages about 5ms, so 25 gives us some buffer without too much
pub const TIME_TO_GET_SUBPID: std::time::Duration = std::time::Duration::from_millis(25);

#[derive(Debug)]
pub enum LocalQueueType {
    NormalLocalQueue,
    ConsoleQueue,
}

pub async fn run_local_process_loop(
    subgraph: Arc<DiGraph<&Cmd, ()>>,
    tracker: Arc<ReadyTrackerClient>,
    token: CancellationToken,
    local_queue_type: LocalQueueType,
) {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let runnertypeid = match &local_queue_type {
        LocalQueueType::NormalLocalQueue => 0,
        LocalQueueType::ConsoleQueue => 1,
    };

    while let Ok((subgraph_node_id, runcount)) = tracker.recv(runnertypeid).await {
        let cmd = subgraph[subgraph_node_id];
        cmd.call_preamble();

        // we're making a pipe to pass to the child process as fd3.
        let (mut fd3_read_pipe, fd3_write_pipe) = tokio_pipe::pipe().expect("Cannot create pipe");
        let (fd4_read_pipe, fd4_write_pipe) = if cmd.fd_input.is_some() {
            let (read, write) = tokio_pipe::pipe().expect("Cannot create pipe");
            (Some(read), Some(write))
        } else {
            (None, None)
        };
        let mut fd_mapping = vec![
            // Map the pipe as FD 3 in the child process.
            FdMapping {
                parent_fd: fd3_write_pipe.as_raw_fd(),
                child_fd: 3,
            },
        ];
        if fd4_read_pipe.is_some() {
            fd_mapping.push(FdMapping {
                parent_fd: fd4_read_pipe.as_ref().unwrap().as_raw_fd(),
                child_fd: cmd.fd_input.as_ref().unwrap().0,
            })
        }

        let mut command = Command::new(&cmd.cmdline[0]);
        let maybe_child = match &local_queue_type {
            LocalQueueType::NormalLocalQueue => command
                .args(&cmd.cmdline[1..])
                .env("EXECGRAPH_RUNCOUNT", format!("{}", runcount))
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
            LocalQueueType::ConsoleQueue => command
                .args(&cmd.cmdline[1..])
                .kill_on_drop(true)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit()),
        }
        .fd_mappings(fd_mapping)
        .unwrap()
        .spawn();

        // Deadlock potential: After spawning the command and passing it the
        // write end of the pipe we need to close it ourselves
        drop(fd3_write_pipe);
        drop(fd4_read_pipe);

        let start_time = std::time::Instant::now();
        let child = match maybe_child {
            Ok(child) => child,
            Err(e) => {
                tracker
                    .send_started(subgraph_node_id, cmd, &hostname, 0, "".to_string())
                    .await;
                tracker
                    .send_finished(
                        FinishedEvent::new_error(
                            subgraph_node_id,
                            -1,
                            format!("Unable to start {:#?}: {:#?}", &cmd.cmdline[0], e),
                        ),
                    )
                    .await;
                continue;
            }
        };

        // Dump input into fd4 immediately
        if let Some(p) = fd4_write_pipe {
            let (_fd, buf) = cmd.fd_input.as_ref().unwrap();
            let mut writer = tokio::io::BufWriter::new(p);
            writer.write_all(buf).await.unwrap();
            writer.flush().await.unwrap();
        }

        // Get PID from fd3, or if it doesn't send one, just the child PID
        let pid = if let Ok(Ok(pid)) =
            tokio::time::timeout(TIME_TO_GET_SUBPID, fd3_read_pipe.read_u32()).await
        {
            pid
        } else {
            child
                .id()
                .expect("child hasn't been waited for yet, so its pid should exist")
        };

        tracker
            .send_started(subgraph_node_id, cmd, &hostname, pid, "".to_string())
            .await;

        let (fd3_channel_write, fd3_channel_read) = async_channel::unbounded();
        let forward_messages_thread = tokio::spawn(forward_messages(
            subgraph_node_id,
            fd3_channel_read,
            tracker.clone(),
        ));
        let output: ChildOutput = tokio::select! {
            cancel = token.soft_cancelled(start_time) => {
                if let CancellationState::CancelledAfterTime(_) = cancel {
                    // if we were soft canceled, send a finished notification.
                    tracker
                        .send_finished(FinishedEvent::new_cancelled(subgraph_node_id))
                        .await;
                }
                debug!("Received cancellation {:#?}", cmd.display);
                return;
            },
            output = wait_with_output(child, fd3_read_pipe, fd3_channel_write) => {
                output.unwrap()
            }
        };
        let execgraph_internal_nonretryable_error = forward_messages_thread
            .await
            .expect("Cannot join thread")
            .expect("forward_messages crashed?");

        tracing::debug!("Finished cmd");
        tracker
            .send_finished(output.to_event(subgraph_node_id, execgraph_internal_nonretryable_error))
            .await;
    }

    debug!(
        "Exiting loop at 141 (LocalQueueType={:#?})",
        local_queue_type
    );
}

pub async fn wait_with_output(
    mut child: tokio::process::Child,
    fd: PipeRead,
    on_fd3: async_channel::Sender<Vec<u8>>,
) -> Result<ChildOutput, ChildProcessError> {
    async fn read_fd3(
        pipe: &mut Option<impl AsyncRead + std::marker::Unpin>,
        on_fd3: async_channel::Sender<Vec<u8>>,
    ) -> std::io::Result<()> {
        if let Some(pipe) = pipe {
            loop {
                let mut buf = Vec::new();
                let nread = pipe.read_buf(&mut buf).await?;
                if nread == 0 {
                    break;
                } else if let Err(e) = on_fd3.send(buf).await {
                    tracing::error!("Unable to send to channel: {}", e);
                }
            }
        }
        Ok(())
    }
    async fn read_to_end(
        pipe: &mut Option<impl AsyncRead + std::marker::Unpin>,
    ) -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        if let Some(pipe) = pipe {
            pipe.read_to_end(&mut buf).await?;
        }
        Ok(buf)
    }
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let mut fd = Some(fd);

    let f1 = read_to_end(&mut stdout_pipe);
    let f2 = read_to_end(&mut stderr_pipe);
    let f3 = read_fd3(&mut fd, on_fd3);

    let (status, stdout, stderr, _) =
        futures::future::try_join4(child.wait(), f1, f2, f3).await?;
    drop(stdout_pipe);
    drop(stderr_pipe);
    drop(fd);

    Ok(ChildOutput {
        status,
        stdout,
        stderr,
    })
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ExitDisposition {
    Exited,
    Signaled,
    Lost,
}

impl ExitDisposition {
    pub fn is_signaled_or_lost(&self) -> bool {
        match self {
            ExitDisposition::Exited => false,
            ExitDisposition::Signaled => true,
            ExitDisposition::Lost => true,
        }
    }
}

pub struct ChildOutput {
    pub status: std::process::ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl ChildOutput {
    pub fn to_event(self, id: NodeIndex, nonretryable: bool) -> FinishedEvent {
        FinishedEvent {
            id,
            status: self.code(),
            stdout: self.stdout_str(),
            stderr: self.stderr_str(),
            disposition: self.disposition(),
            nonretryable,
            flag: None,
        }
    }

    pub fn disposition(&self) -> ExitDisposition {
        if self.status.signal().is_some() {
            ExitDisposition::Signaled
        } else {
            ExitDisposition::Exited
        }
    }

    pub fn code(&self) -> ExitStatus {
        let code = match self.status.code() {
            Some(code) => code,
            None => -self.status.signal().expect("No exit code and no signal?"),
        };
        ExitStatus::Code(code)
    }

    pub fn stdout_str(&self) -> String {
        String::from_utf8_lossy(&self.stdout).to_string()
    }

    pub fn stderr_str(&self) -> String {
        String::from_utf8_lossy(&self.stderr).to_string()
    }
}

async fn forward_messages(
    node_index: NodeIndex,
    fd3_channel_read: async_channel::Receiver<Vec<u8>>,
    tracker: Arc<ReadyTrackerClient>,
) -> anyhow::Result<bool> {
    let mut execgraph_internal_nonretryable_error = false;
    loop {
        match fd3_channel_read.recv().await {
            Ok(value) => {
                if std::str::from_utf8(&value).is_ok_and(|x| x == "__execgraph_internal_nonretryable_error=1\n") {
                    execgraph_internal_nonretryable_error = true;
                } else {
                    tracker.send_setvalue(node_index, value).await
                }
            }
            Err(_) => {
                return Ok(execgraph_internal_nonretryable_error);
            }
        }
    }
}
