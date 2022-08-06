use axum::http::HeaderValue;
use hyper::{
    header::{self},
    HeaderMap,
};
pub const SMALL_REQUEST_SIZE_NO_COMPRESSION: usize = 128;

lazy_static::lazy_static! {
pub static ref APPLICATION_POSTCARD: header::HeaderValue =
    header::HeaderValue::from_static("application/postcard");
pub static ref LZ4: header::HeaderValue = header::HeaderValue::from_static("lz4");
pub static ref APPLICATION_JSON: header::HeaderValue =
    header::HeaderValue::from_static("application/json");
}

pub enum SerdeType {
    Postcard,
    Json,
    Unknown(HeaderValue),
    Unspecified,
}
pub enum CompressionType {
    LZ4,
    Unknown(HeaderValue),
    Uncompresed,
}

pub fn get_compression(headers: &HeaderMap) -> CompressionType {
    headers
        .get(header::CONTENT_ENCODING)
        .map(|v| {
            if v == *LZ4 {
                CompressionType::LZ4
            } else {
                CompressionType::Unknown(v.clone())
            }
        })
        .unwrap_or(CompressionType::Uncompresed)
}

pub fn get_content_type(headers: &HeaderMap) -> SerdeType {
    headers
        .get(header::CONTENT_TYPE)
        .map(|v| {
            if v == *APPLICATION_POSTCARD {
                SerdeType::Postcard
            } else if v == *APPLICATION_JSON {
                SerdeType::Json
            } else {
                SerdeType::Unknown(v.clone())
            }
        })
        .unwrap_or(SerdeType::Unspecified)
}
