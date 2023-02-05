use std::sync::Mutex;

pub struct RateCounter {
    t0: std::time::Instant,
    inner: Mutex<Inner>,
    k: f64,
}
struct Inner {
    t_last: std::time::Instant,
    lambda_last: f64,
}

impl RateCounter {
    ///
    /// This object is for tracking the rate that an event occurs. You initialize it with a "decay time", \tau,
    /// and then every time an event happens, you call update(). Then, any time you call ``get_rate()`` it will
    /// give you an approximation of the rate at which events have been occuring, in units of events/second,
    /// roughly averaged over the last ``decay_time`` seconds.
    ///
    /// The algorithm is from https://stackoverflow.com/a/23617678
    ///
    pub fn new(tau: f64) -> RateCounter {
        RateCounter {
            t0: std::time::Instant::now(),
            inner: Mutex::new(Inner {
                t_last: std::time::Instant::now(),
                lambda_last: 0.,
            }),
            k: 1.0 / tau,
        }
    }
    pub fn update(&self, now: std::time::Instant) {
        let mut inner = self.inner.lock().unwrap();
        let dur = (now - inner.t_last).as_secs_f64();
        inner.lambda_last = self.k + (-self.k * dur).exp() * inner.lambda_last;
        inner.t_last = now;
    }
    pub fn get_rate(&self) -> f64 {
        let inner = self.inner.lock().unwrap();
        let now = std::time::Instant::now();
        let numerator = (-self.k * (now - inner.t_last).as_secs_f64()).exp() * inner.lambda_last;
        let denomenator = 1.0 - (-self.k * (now - self.t0).as_secs_f64()).exp();

        if denomenator > 0.0 {
            numerator / denomenator
        } else {
            0.
        }
    }
}
