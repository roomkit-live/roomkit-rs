//! A response keeps its connection until EOF, then returns it to the idle pool.

use http_body_util::BodyExt;
use hyper::body::{Bytes, Incoming};
use reqwest::{StatusCode, header::HeaderMap};

use super::{Client, connection::Connection};
use crate::http::client::HttpFailure;

pub(in crate::http) struct Response {
    pub(super) incoming: hyper::Response<Incoming>,
    pub(super) connection: Option<Connection>,
    pub(super) client: Client,
}

impl Response {
    pub(super) fn status(&self) -> StatusCode {
        self.incoming.status()
    }
    pub(super) fn headers(&self) -> &HeaderMap {
        self.incoming.headers()
    }
    pub(super) fn content_length(&self) -> Option<u64> {
        self.headers()
            .get("content-length")?
            .to_str()
            .ok()?
            .parse()
            .ok()
    }

    pub(in crate::http) async fn chunk(&mut self) -> Result<Option<Bytes>, HttpFailure> {
        while let Some(frame) = self.incoming.body_mut().frame().await {
            let frame = frame.map_err(|_| {
                self.client
                    .failure
                    .record(None, "HTTP response interrupted")
            })?;
            if let Ok(bytes) = frame.into_data() {
                return Ok(Some(bytes));
            }
        }
        if let Some(mut connection) = self.connection.take() {
            connection.operations.idle();
            connection.idle_since = tokio::time::Instant::now();
            let mut idle = self.client.lease.idle.lock().await;
            if idle.len() < 20 && !connection.sender.is_closed() {
                idle.push(connection);
            }
        }
        Ok(None)
    }
}
