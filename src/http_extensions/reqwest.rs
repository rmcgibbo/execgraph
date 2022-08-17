use anyhow::{anyhow, Result};
use axum::async_trait;
use lzzzz::lz4f;
use reqwest::header::{HeaderMap, CONTENT_ENCODING, CONTENT_TYPE};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::common::{
    get_compression, get_content_type, CompressionType, SerdeType, APPLICATION_POSTCARD, LZ4,
    LZ4_ENCODING_PREFS, SMALL_REQUEST_SIZE_NO_COMPRESSION,
};

pub trait RequestBuilderExt {
    fn postcard<T: Serialize + ?Sized>(self, data: &T) -> Result<Self>
    where
        Self: Sized;
}

impl RequestBuilderExt for reqwest::RequestBuilder {
    fn postcard<T: Serialize + ?Sized>(self, data: &T) -> Result<Self>
    where
        Self: Sized,
    {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, APPLICATION_POSTCARD.clone());
        let bytes = postcard::to_allocvec(data)?;
        let compressed = if bytes.len() > SMALL_REQUEST_SIZE_NO_COMPRESSION {
            let mut compressed = Vec::new();
            lz4f::compress_to_vec(&bytes, &mut compressed, &LZ4_ENCODING_PREFS)?;
            headers.insert(CONTENT_ENCODING, LZ4.clone());
            compressed
        } else {
            bytes
        };

        Ok(self.headers(headers).body(compressed))
    }
}

#[async_trait]
pub trait ResponseExt {
    async fn postcard<T: DeserializeOwned>(self) -> Result<T>;
}

#[async_trait]
impl ResponseExt for reqwest::Response {
    async fn postcard<T: DeserializeOwned>(self) -> Result<T> {
        let compression = get_compression(self.headers());
        let content_type = get_content_type(self.headers());
        let bytes = self.bytes().await?;

        let decomp = match compression {
            CompressionType::LZ4 => {
                let mut decomp = vec![];
                lz4f::decompress_to_vec(&bytes, &mut decomp)?;
                decomp
            }
            CompressionType::Uncompresed => bytes.to_vec(),
            CompressionType::Unknown(s) => return Err(anyhow!("Unknown format {:?}", s)),
        };

        match content_type {
            SerdeType::Postcard => Ok(postcard::from_bytes(&decomp)?),
            SerdeType::Json => Ok(serde_json::from_slice(&decomp)?),
            SerdeType::Unknown(s) => Err(anyhow!("Unknown format {:?}", s)),
            SerdeType::Unspecified => Err(anyhow!("Unspecified content type")),
        }
    }
}
