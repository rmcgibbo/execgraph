pub const FAIL_COMMAND_PREFIX: &str = "wrk/";
pub const FIZZLED_TIME_CUTOFF: std::time::Duration = std::time::Duration::from_secs(5);
pub const PING_INTERVAL_MSECS: u64 = 15_000;
pub const PING_TIMEOUT_MSECS: u64 = 2 * PING_INTERVAL_MSECS;
