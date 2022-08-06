pub struct Postcard<T>(pub T);

use axum::async_trait;
use axum::body::Bytes;
use axum::body::HttpBody;
use axum::extract::FromRequest;
use axum::extract::RequestParts;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::BoxError;
use hyper::header;
use lzzzz::lz4;
use serde::de::DeserializeOwned;
use serde::Serialize;

use thiserror::Error;

use super::common::APPLICATION_POSTCARD;
use super::common::LZ4;
use super::common::{
    get_compression, get_content_type, CompressionType, SerdeType,
    SMALL_REQUEST_SIZE_NO_COMPRESSION,
};

#[derive(Debug, Error)]
pub enum PostcardRejection {
    #[error("Decompression error: {0}")]
    DecompressionError(
        #[source]
        #[from]
        lzzzz::Error,
    ),

    #[error("Bytes rejection : {0}")]
    BytesRejection(
        #[source]
        #[from]
        axum::extract::rejection::BytesRejection,
    ),

    #[error("Postcard decoding error : {0}")]
    PostcardError(
        #[source]
        #[from]
        postcard::Error,
    ),

    #[error("JSON decoding error : {0}")]
    JsonError(
        #[source]
        #[from]
        serde_json::Error,
    ),

    #[error("Unknown content type : {0:?}")]
    UnknownContentType(HeaderValue),

    #[error("Unknown compression type : {0:?}")]
    UnknownCompressionType(HeaderValue),

    #[error("Unspecified content type")]
    UnspecifiedContentType,
}

impl IntoResponse for PostcardRejection {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
    }
}

#[async_trait]
impl<T, B> FromRequest<B> for Postcard<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = PostcardRejection;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req).await?;
        let compression = get_compression(req.headers());
        let content_type = get_content_type(req.headers());
        let decomp = match compression {
            CompressionType::LZ4 => {
                let mut decomp = vec![0; bytes.len()];
                let n = lz4::decompress(&bytes, &mut decomp)?;
                decomp.truncate(n);
                decomp
            }
            CompressionType::Uncompresed => bytes.to_vec(),
            CompressionType::Unknown(s) => {
                return Err(PostcardRejection::UnknownCompressionType(s))
            }
        };

        match content_type {
            SerdeType::Postcard => Ok(Postcard(postcard::from_bytes(&decomp)?)),
            SerdeType::Json => Ok(Postcard(serde_json::from_slice(&decomp)?)),
            SerdeType::Unknown(s) => Err(PostcardRejection::UnknownContentType(s)),
            SerdeType::Unspecified => Err(PostcardRejection::UnspecifiedContentType),
        }
    }
}

impl<T: Serialize + Sized> IntoResponse for Postcard<T> {
    fn into_response(self) -> axum::response::Response {
        match postcard::to_allocvec(&self.0) {
            Ok(bytes) if bytes.len() < SMALL_REQUEST_SIZE_NO_COMPRESSION => {
                let mut res = (StatusCode::OK, bytes).into_response();
                res.headers_mut()
                    .insert(header::CONTENT_TYPE, APPLICATION_POSTCARD.clone());
                res
            }
            Ok(bytes) => {
                let mut compressed = Vec::new();
                match lzzzz::lz4::compress_to_vec(
                    &bytes,
                    &mut compressed,
                    lzzzz::lz4::ACC_LEVEL_DEFAULT,
                ) {
                    Ok(_) => {
                        let mut res = (StatusCode::OK, compressed).into_response();
                        res.headers_mut()
                            .insert(header::CONTENT_TYPE, APPLICATION_POSTCARD.clone());
                        res.headers_mut()
                            .insert(header::CONTENT_ENCODING, LZ4.clone());
                        res
                    }
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)).into_response(),
                }
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)).into_response(),
        }
    }
}
