use crate::sync::Queuename;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Ping {
    pub transaction_id: u32,
}

#[derive(Serialize, Deserialize)]
pub struct StatusReply {
    #[serde(with = "vectorize")]
    pub queues: HashMap<Queuename, StatusQueueReply>,
}
#[derive(Serialize, Deserialize)]
pub struct StatusQueueReply {
    pub num_ready: u32,
    pub num_failed: u32,
    pub num_success: u32,
    pub num_inflight: u32,
}

#[derive(Serialize, Deserialize)]
pub struct StartRequest {
    pub queuename: Option<String>,
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
    pub hostpid: String,
}

#[derive(Serialize, Deserialize)]
pub struct EndRequest {
    pub transaction_id: u32,
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
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
