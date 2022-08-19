use event_listener::Event;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::usize;

#[derive(Clone)]
pub struct AsyncFlag {
    inner: Arc<Inner<AtomicBool>>,
}
#[derive(Clone)]
pub struct AsyncCounter {
    inner: Arc<Inner<AtomicU64>>,
}

struct Inner<T> {
    value: T,
    event: Event,
}

impl Default for AsyncFlag {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for AsyncCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncFlag {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                value: AtomicBool::new(false),
                event: Event::new(),
            }),
        }
    }

    pub fn set(&mut self) {
        self.inner.value.store(true, Ordering::SeqCst);
        self.inner.event.notify(usize::MAX);
    }

    pub async fn wait(&self) {
        loop {
            // Check the flag.
            if self.inner.value.load(Ordering::SeqCst) {
                break;
            }

            // Start listening for events.
            let listener = self.inner.event.listen();

            // Check the flag again after creating the listener.
            if self.inner.value.load(Ordering::SeqCst) {
                break;
            }

            // Wait for a notification and continue the loop.
            listener.await;
        }
    }
}

impl AsyncCounter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                value: AtomicU64::new(0),
                event: Event::new(),
            }),
        }
    }

    /// Increment the counter
    pub fn incr(&self) {
        self.inner.value.fetch_add(1, Ordering::SeqCst);
        self.inner.event.notify(usize::MAX);
    }

    /// Load the current value
    pub fn load(&self) -> u64 {
        self.inner.value.load(Ordering::SeqCst)
    }

    /// Wait for the counter to go above the value `from`. Returns
    /// the new value
    pub async fn wait(&self, from: u64) -> u64 {
        loop {
            // Check the flag.
            let value = self.inner.value.load(Ordering::SeqCst);
            if value > from {
                return value;
            }

            // Start listening for events.
            let listener = self.inner.event.listen();

            // Check the flag again after creating the listener.
            let value = self.inner.value.load(Ordering::SeqCst);
            if value > from {
                return value;
            }

            listener.await;
        }
    }
}

impl core::fmt::Debug for AsyncCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwaitableCounter").finish()
    }
}

impl core::fmt::Debug for AsyncFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFlag").finish()
    }
}
