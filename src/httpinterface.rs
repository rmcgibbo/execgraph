use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ffi::OsString};

use crate::logfile2::ValueMaps;

#[derive(Serialize, Deserialize)]
pub struct Ping {
    pub transaction_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusRequest {
    pub etag: u64,
    pub timemin_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateRatelimitRequest {
    pub per_second: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateRemoteProvisionerInfo {
    pub provisioner_info: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerMetrics {
    pub p50_latency: HashMap<String, u32>,
    pub p99_latency: HashMap<String, u32>,
}

#[derive(Serialize, Deserialize)]
pub struct StatusReply {
    #[serde(with = "vectorize")]
    pub queues: HashMap<u64, StatusQueueReply>,
    pub etag: u64,
    pub server_metrics: ServerMetrics,
    pub rate: f64,
    pub ratelimit: u32,
    pub provisioner_info: Option<String>,
}
#[derive(Serialize, Deserialize)]
pub struct StatusQueueReply {
    pub num_ready: u32,
    pub num_inflight: u32,
}

#[derive(Serialize, Deserialize)]
pub struct StartRequest {
    pub runnertypeid: u32,
    pub disconnect_error_message: String,
}

#[derive(Serialize, Deserialize)]
pub struct StartResponse {
    pub transaction_id: u32,
    pub cmdline: Vec<OsString>,
    pub fd_input: Option<(i32, Vec<u8>)>,
    pub ping_interval_msecs: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BegunRequest {
    pub transaction_id: u32,
    pub host: String,
    pub pid: u32,
}

#[derive(Serialize, Deserialize)]
pub struct EndRequest {
    pub transaction_id: u32,
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
    pub values: ValueMaps,
    pub start_request: Option<StartRequest>,
}

#[derive(Serialize, Deserialize)]
pub struct EndResponse {
    pub start_response: Option<StartResponse>,
}

pub mod vectorize {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::iter::FromIterator;

    pub fn serialize<'a, T, K, V, S>(target: T, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: IntoIterator<Item = (&'a K, &'a V)>,
        K: Serialize + 'a,
        V: Serialize + 'a,
    {
        let container: Vec<_> = target.into_iter().collect();
        serde::Serialize::serialize(&container, ser)
    }

    pub fn deserialize<'de, T, K, V, D>(des: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: FromIterator<(K, V)>,
        K: Deserialize<'de>,
        V: Deserialize<'de>,
    {
        let container: Vec<_> = serde::Deserialize::deserialize(des)?;
        Ok(container.into_iter().collect::<T>())
    }
}
