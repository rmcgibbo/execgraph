use event_listener::Event;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::usize;

#[derive(Clone)]
pub struct Flag {
    inner: Arc<FlagInner>,
}

impl Default for Flag {
    fn default() -> Self {
        Self::new()
    }
}

impl Flag {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FlagInner {
                flag: AtomicBool::new(false),
                event: Event::new(),
            }),
        }
    }

    pub fn set(&mut self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.event.notify(usize::MAX);
    }

    pub async fn wait(&self) {
        loop {
            // Check the flag.
            if self.inner.flag.load(Ordering::SeqCst) {
                break;
            }

            // Start listening for events.
            let listener = self.inner.event.listen();

            // Check the flag again after creating the listener.
            if self.inner.flag.load(Ordering::SeqCst) {
                break;
            }

            // Wait for a notification and continue the loop.
            listener.await;
        }
    }
}

struct FlagInner {
    flag: AtomicBool,
    event: Event,
}
