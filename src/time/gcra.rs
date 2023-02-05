///
/// Generic cell rate algorithm (GCRA) for rate limiting
///
/// This is modelled off some Go code I found, but heavily modified
///   https://github.com/256dpi/gcra/blob/v1.0.2/gcra.go (MIT)
///
/// I've made the code simple -- this rate limiter only supports refilling
/// the bucket over a period of one second, and it only supports a "burst capacity"
/// that's equal to the rate per second. So the idea is that you supply as input
/// one parameter, which is the maximum event rate per second. If you want to limit
/// to a rate of 10 events per second, you can run all 10 events right at the beginning
/// of the second (burst) or evently like 1 per 10th of a second. The capacity doesn't
/// accumulate beyond  10.
///
/// This also uses interior mutability with atomics, so the interface doesn't rely on
/// having a mutable reference. You're also allowed to modify the rate of the bucket
/// without having a mutable reference.
///
use std::{sync::atomic::AtomicU64, time::Duration};

const NANOS_PER_SEC: u64 = 1_000_000_000;
const PERIOD: u64 = 1 * NANOS_PER_SEC;

pub struct RateLimiter {
    tau: AtomicU64,
    tat: AtomicU64,
}

impl RateLimiter {
    pub fn new(rate_per_second: u64) -> RateLimiter {
        let tau = if rate_per_second == 0 {
            0
        } else {
            PERIOD / rate_per_second
        };
        let tat = now() + tau * rate_per_second;
        RateLimiter {
            tau: AtomicU64::from(tau),
            tat: AtomicU64::from(tat),
        }
    }

    pub fn reset(&self, rate_per_second: u64) {
        let tau = if rate_per_second == 0 {
            0
        } else {
            PERIOD / rate_per_second
        };
        let tat = now() + tau * rate_per_second;
        self.tau.store(tau, std::sync::atomic::Ordering::SeqCst);
        self.tat.store(tat, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn rate_per_second(&self) -> u64 {
        let tau = self.tau.load(std::sync::atomic::Ordering::SeqCst);
        if tau == 0 {
            0
        } else {
            PERIOD / tau
        }
    }

    pub async fn until_ready(&self) {
        loop {
            let retry_in = self.compute(1);
            if retry_in.is_zero() {
                return;
            }
            tokio::time::sleep(retry_in).await;
        }
    }

    fn compute(&self, cost: u64) -> Duration {
        let now = now();
        loop {
            let tau = self.tau.load(std::sync::atomic::Ordering::SeqCst);
            let tat = self.tat.load(std::sync::atomic::Ordering::SeqCst);
            let (new_tat, retry_in) = Self::compute_raw(now, tat, tau, cost);
            if self
                .tat
                .compare_exchange(
                    tat,
                    new_tat,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                return retry_in;
            }
        }
    }

    fn compute_raw(now: u64, tat: u64, tau: u64, cost: u64) -> (u64, Duration) {
        if tau == 0 {
            return (tat, Duration::from_nanos(0));
        }

        let increment = tau * cost;
        // In this version, the burst capacity is hardcoded to be equal to the rate, so this calculation is always PERIOD
        // let burst_fffset = self.tau * self.burst;
        let burst_offset = PERIOD;
        let tat = std::cmp::max(tat, now);

        let new_tat = tat + increment;
        let allow_at = new_tat - burst_offset;

        if allow_at > now {
            (tat, Duration::from_nanos(allow_at - now))
        } else {
            (new_tat, Duration::from_nanos(0))
        }
    }
}

fn now() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts);
    }

    (ts.tv_sec as u64) * NANOS_PER_SEC + (ts.tv_nsec as u64)
}

#[tokio::test]
async fn test_1() {
    use std::sync::Arc;
    let limiter = Arc::new(RateLimiter::new(50));
    let mut handles = vec![];
    let start = std::time::Instant::now();
    for _i in 0..200 {
        let rl = limiter.clone();
        handles.push(tokio::spawn(async move {
            rl.until_ready().await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let x = (std::time::Instant::now() - start).as_secs_f64();
    assert!((x - 4.0).abs() < 0.01);
}
