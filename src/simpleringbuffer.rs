// Simple circular buffer for holding the average runtime of tasks
use std::sync::{Arc, RwLock};
use ringbuffer::{AllocRingBuffer, RingBuffer};

pub struct RingbufferForDurationAverages {
    buffer: Arc<RwLock<AllocRingBuffer<std::time::Duration>>>,
}

impl RingbufferForDurationAverages {
   pub fn new(capacity: usize) -> Self {
        assert!(capacity <= 1000); // for good measure
        Self {
            buffer: Arc::new(
                RwLock::new(
                    AllocRingBuffer::new(capacity)
                )
            )
        }
    }

    pub fn push(&mut self, value: std::time::Duration) {
        let mut guard = self.buffer.write().unwrap();
        guard.push(value);
    }

    pub fn view(&self) -> RingbufferForDurationAveragesReadonlyView {
        return RingbufferForDurationAveragesReadonlyView {
            buffer: self.buffer.clone()
        }
    }
}

pub struct RingbufferForDurationAveragesReadonlyView {
    buffer: Arc<RwLock<AllocRingBuffer<std::time::Duration>>>
}

impl RingbufferForDurationAveragesReadonlyView {
    pub fn mean(&self) -> std::time::Duration {
        let guard = self.buffer.read().unwrap();
        let n_items = guard.len() as u64;
        assert!(n_items <= 1000); // don't want to overflow, just checking
        // 2**64 miliseconds / 1000 == 584 554.531 years, so this sum should not overflow

        let sum = guard.iter().map(|x| x.as_millis() as u64).sum::<u64>();
        std::time::Duration::from_millis(sum.checked_div(n_items).unwrap_or(0))
    }
}