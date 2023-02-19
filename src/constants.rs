pub const FAIL_COMMAND_PREFIX: &str = "wrk/";
pub const ADMIN_SOCKET_PREFIX: &str = "wrk";
pub const FIZZLED_TIME_CUTOFF: std::time::Duration = std::time::Duration::from_secs(5);
pub const PING_INTERVAL_MSECS: u64 = 15_000;
pub const PING_TIMEOUT_MSECS: u64 = 2 * PING_INTERVAL_MSECS;
pub const HOLD_RATETIMITED_TIME: std::time::Duration = std::time::Duration::from_secs(30);
pub const DEFAULT_RATE_COUNTER_TIMESCALE: f64 = 30.; // seconds
