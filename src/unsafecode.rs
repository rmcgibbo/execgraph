use std::convert::TryFrom;

pub async fn sigint_then_kill(child: &mut tokio::process::Child, duration: std::time::Duration) {
    if let Some(pid) = child.id() {
        // try to send sigint
        log::debug!("sending SIGINT to provisioner");
        let pid_as_i32 = i32::try_from(pid).expect("pid doesn't fit in i32");
        unsafe {
            if libc::kill(pid_as_i32, libc::SIGINT) != 0 {
                panic!("kill failed");
            }
        }
        // wait up to 'duration' and the send sigkill
        if tokio::time::timeout(duration, child.wait()).await.is_err() {
            log::debug!("sending SIGKILL to provisioner");
            child.kill().await.expect("kill failed");
        }
    }
}
