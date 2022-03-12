use event_listener::Event;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct AwaitableCounter {
    value: AtomicU64,
    event: Event,
}

impl AwaitableCounter {
    pub fn new() -> Self {
        AwaitableCounter {
            value: AtomicU64::new(0),
            event: Event::new(),
        }
    }

    // Increment the counter
    pub fn incr(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
        self.event.notify(usize::MAX);
    }

    // Wait for the counter to go above the value `from`. Returns
    // the new value
    pub async fn changed(&self, from: u64) -> u64 {
        loop {
            // Check the flag.
            let value = self.value.load(Ordering::SeqCst);
            if value > from {
                return value;
            }

            // Start listening for events.
            let listener = self.event.listen();

            // Check the flag again after creating the listener.
            let value = self.value.load(Ordering::SeqCst);
            if value > from {
                return value;
            }

            listener.await;
        }
    }
}
