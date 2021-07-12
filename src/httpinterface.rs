use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Ping {
    pub transaction_id: u32,
}

#[derive(Serialize, Deserialize)]
pub struct StatusReply {
    pub num_ready: u32,
    pub num_failed: u32,
    pub num_success: u32,
    pub num_inflight: u32,
}
#[derive(Serialize, Deserialize)]
pub struct StartResponse {
    pub transaction_id: u32,
    pub cmdline: String,
    pub ping_interval_msecs: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BegunRequest {
    pub transaction_id: u32,
    pub pid: u32,
}

#[derive(Serialize, Deserialize)]
pub struct EndRequest {
    pub transaction_id: u32,
    pub status: i32,
}
