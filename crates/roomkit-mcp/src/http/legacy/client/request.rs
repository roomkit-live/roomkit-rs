//! HTTP encoding is shared with the existing client, without sending through it.
use super::{body::Body, io::Operations};
use crate::http::client::{Failure, HttpFailure};
use hyper::body::Bytes;
use reqwest::{
    Method, Url,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;
use std::sync::Arc;
pub(super) fn build(
    encoder: &reqwest::Client,
    method: Method,
    url: &Url,
    headers: HeaderMap,
    message: Option<&Value>,
    operations: Arc<Operations>,
    failure: &Failure,
) -> Result<hyper::Request<Body>, HttpFailure> {
    let fail = || failure.record(None, "HTTP connection failed");
    let mut prepared = encoder
        .request(method.clone(), url.clone())
        .headers(headers);
    if let Some(message) = message {
        prepared = prepared.json(message);
    }
    let prepared = prepared.build().map_err(|_| fail())?;
    let mut headers = prepared.headers().clone();
    let bytes = prepared
        .body()
        .and_then(reqwest::Body::as_bytes)
        .map(Bytes::copy_from_slice);
    let host = &url[url::Position::BeforeHost..url::Position::AfterPort];
    if !headers.contains_key("host") {
        headers.insert("host", HeaderValue::from_str(host).map_err(|_| fail())?);
    }
    if !headers.contains_key("accept") {
        headers.insert("accept", HeaderValue::from_static("*/*"));
    }
    if let Some(bytes) = &bytes {
        if !headers.contains_key("content-type") {
            headers.insert("content-type", HeaderValue::from_static("application/json"));
        }
        headers.insert(
            "content-length",
            HeaderValue::from_str(&bytes.len().to_string()).map_err(|_| fail())?,
        );
    }
    let uri = &url[url::Position::BeforePath..url::Position::AfterQuery];
    operations.begin(bytes.is_some());
    let body = Body::new(bytes, operations);
    let mut request = hyper::Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .map_err(|_| fail())?;
    *request.headers_mut() = headers;
    Ok(request)
}
