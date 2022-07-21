use arr_macro::arr;
use slotmap::{DefaultKey, SlotMap};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub struct TimerID {
    key: DefaultKey,
    slot: u8,
}

#[derive(Debug)]
pub struct TimeWheel<Entry> {
    current: AtomicU8,
    slots: [Mutex<Option<SlotMap<DefaultKey, Entry>>>; 256],
    duration: Duration,
}

impl<Entry> TimeWheel<Entry> {
    /// Create a new empty TimeWheel
    pub fn new(duration: Duration) -> Self {
        TimeWheel {
            slots: arr![Mutex::new(None); 256],
            current: AtomicU8::new(0u8),
            duration,
        }
    }

    pub fn tick_duration(&self) -> Duration {
        self.duration / 256
    }

    /// Move the wheel by one tick and return all entries in the current slot
    pub fn tick(&self) -> Option<SlotMap<DefaultKey, Entry>> {
        let old = self.current.fetch_add(1u8, Ordering::SeqCst);
        let index = old.wrapping_add(1u8) as usize;
        let mut guard = self.slots[index].lock().unwrap();
        guard.take()
    }

    /// Insert an entry at into the wheel at `dur` from now in the future
    pub fn insert(&self, dur: Duration, e: Entry) -> TimerID {
        if dur > self.duration {
            panic!("dur {:#?} cannot exceed duration {:#?}", dur, self.duration);
        }
        let step = ((256 * dur.as_nanos()) / self.duration.as_nanos()) as u8;
        let pos = self.current.load(Ordering::SeqCst).wrapping_add(step);
        let index = pos as usize;

        let mut guard = self.slots[index].lock().unwrap();
        if guard.is_none() {
            let bucket = Some(SlotMap::with_key());
            *guard = bucket;
        }
        if let Some(ref mut bucket) = *guard {
            TimerID {
                key: bucket.insert(e),
                slot: pos,
            }
        } else {
            unreachable!();
        }
    }

    /// Cancel a timer in the time wheel
    pub fn cancel(&self, k: TimerID) {
        let TimerID { key, slot } = k;
        let index = slot as usize;
        let mut guard = self.slots[index].lock().unwrap();
        if let Some(ref mut bucket) = *guard {
            bucket.remove(key);
        }
    }
}
