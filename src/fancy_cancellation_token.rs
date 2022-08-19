use std::{sync::Arc, time::Instant};

#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<CancellationTokenState>,
}

#[derive(Clone, Debug)]
pub enum CancellationState {
    CancelledAfterTime(Instant),
    HardCancelled,
}

struct CancellationTokenState {
    sender: tokio::sync::watch::Sender<Option<CancellationState>>,
    receiver: tokio::sync::watch::Receiver<Option<CancellationState>>,
}

impl CancellationToken {
    pub fn new() -> CancellationToken {
        let (s, r) = tokio::sync::watch::channel(None);
        CancellationToken {
            inner: std::sync::Arc::new(CancellationTokenState {
                sender: s,
                receiver: r,
            }),
        }
    }

    pub fn is_soft_cancelled(&self) -> Option<CancellationState> {
        let r = self.inner.receiver.clone();
        let x = r.borrow();
        x.clone()
    }

    /// Returns a `Future` that gets fulfilled when "soft cancellation"
    /// is requested for work items after ``time``.
    ///
    /// The idea behind soft cancellation is that we want to cancel
    /// work items that were begun after a cutoff time.
    ///
    pub async fn soft_cancelled(&self, time: Instant) -> CancellationState {
        let mut r = self.inner.receiver.clone();

        loop {
            {
                let s = r.borrow_and_update();
                match *s {
                    Some(CancellationState::CancelledAfterTime(cancel_time))
                        if time > cancel_time =>
                    {
                        return s.as_ref().unwrap().clone();
                    }
                    Some(CancellationState::HardCancelled) => {
                        return s.as_ref().unwrap().clone();
                    }
                    _ => {}
                }
            }
            r.changed().await.expect("Sender cannot have been dropped");
        }
    }

    /// Returns a `Future` that gets fulfilled when hard cancellation is requested.
    pub async fn hard_cancelled(&self) {
        let mut r = self.inner.receiver.clone();

        loop {
            {
                let s = r.borrow_and_update();
                if let Some(CancellationState::HardCancelled) = *s {
                    return;
                }
            }
            r.changed().await.expect("Sender cannot have been dropped");
        }
    }

    pub fn cancel(&self, state: CancellationState) {
        if let CancellationState::CancelledAfterTime(new_time) = state {
            // if we're trying to do a soft cancel, check the current state
            let existing = self.inner.receiver.borrow();
            match *existing {
                Some(CancellationState::CancelledAfterTime(existing_time))
                    if new_time > existing_time =>
                {
                    // if the existing time is older than the new time, no need to do anything
                    return;
                }
                Some(CancellationState::HardCancelled) => {
                    // if we're already hard canceled, don't override it with a new soft cancel
                    return;
                }
                _ => {}
            }
        }
        self.inner.sender.send_replace(Some(state));
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken").finish()
    }
}
