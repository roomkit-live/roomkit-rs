//! The negotiation's requests over Streamable HTTP (RFC §21.2,
//! RK-21.2-056): each its own POST, answered as Python's MCP SDK reads an
//! answer, a status of 400 or more as a JSON-RPC error (the server's own
//! in a JSON body, else `-32601` for a 404 and `-32603` for any other), so
//! that a probe a server or a gateway refuses that way falls back to the
//! handshake; the session the handshake opens is kept for rmcp's worker,
//! from its response's headers on. Only the handshake's refusal is a
//! diagnostic of the connection: a probe's status never outlives the probe.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{
    Method, Response,
    header::{HeaderName, HeaderValue},
};
use rmcp::model::{ClientJsonRpcMessage, ClientRequest, GetMeta};
use serde_json::{Value, json};

use super::client::{Client, content_type};
use crate::negotiation::Link;
use crate::server::MESSAGE_LIMIT;

/// `-32600`: a request answered with no answer to read.
const INVALID_REQUEST: i64 = -32600;

/// The negotiation over one Streamable HTTP endpoint.
pub(super) struct Negotiating {
    client: Client,
    uri: Arc<str>,
}

impl Client {
    /// The negotiation over `uri`, through this client.
    pub(super) fn negotiating(&self, uri: Arc<str>) -> Negotiating {
        Negotiating {
            client: self.clone(),
            uri,
        }
    }
}

impl Negotiating {
    /// `request` posted, and its answer as Python's SDK reads it.
    async fn post(&mut self, request: ClientJsonRpcMessage) -> Result<Value, String> {
        let ClientJsonRpcMessage::Request(sent) = &request else {
            return Err("the negotiation sends requests".to_owned());
        };
        let id = serde_json::to_value(&sent.id).map_err(|error| error.to_string())?;
        let handshake = matches!(sent.request, ClientRequest::InitializeRequest(_));
        let mut headers = std::collections::HashMap::new();
        if matches!(sent.request, ClientRequest::DiscoverRequest(_)) {
            // What a 2026-07-28 request says of itself over HTTP.
            headers.insert(
                HeaderName::from_static("mcp-method"),
                HeaderValue::from_static("server/discover"),
            );
            if let Some(version) = sent.request.get_meta().protocol_version()
                && let Ok(version) = HeaderValue::from_str(version.as_str())
            {
                headers.insert(HeaderName::from_static("mcp-protocol-version"), version);
            }
        }
        let wire = crate::wire::message(&request).map_err(|error| error.to_string())?;
        let posted = self
            .client
            .request(Method::POST, &self.uri, None, None, headers)
            .json(&wire);
        let response = self
            .client
            .send(posted)
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        if status.as_u16() == 202 {
            return Ok(refused(
                &id,
                INVALID_REQUEST,
                "server answered a request with 202 Accepted",
            ));
        }
        if status.is_redirection() {
            return Ok(refused(
                &id,
                INVALID_REQUEST,
                "the MCP server redirected the request",
            ));
        }
        if status.as_u16() >= 400 {
            return Ok(self.refusal(response, &id, handshake).await);
        }
        if handshake
            && let Some(session) = response
                .headers()
                .get("mcp-session-id")
                .and_then(|value| value.to_str().ok())
                .filter(|session| !session.is_empty() && session.len() <= 1024)
        {
            self.client.capture.open_session(session.to_owned());
        }
        self.answer(response, &id).await
    }

    /// The JSON-RPC error a status of 400 or more reads as: the server's own,
    /// in a JSON body, else Python's SDK's stand-in. The handshake's status
    /// is what a connection it fails reports.
    async fn refusal(&self, response: Response, id: &Value, handshake: bool) -> Value {
        let status = response.status().as_u16();
        if handshake {
            self.client
                .failure
                .record(Some(status), "HTTP request rejected");
        }
        if content_type(&response, "application/json")
            && let Ok(body) = self.client.body(response).await
            && let Ok(answer) = serde_json::from_slice::<Value>(&body)
            && let Some(error) = answer.get("error").filter(|error| error.is_object())
        {
            return json!({"jsonrpc": "2.0", "id": id, "error": error});
        }
        if status == 404 {
            return refused(id, -32601, "Not Found");
        }
        refused(id, -32603, "Server returned an error response")
    }

    /// The answer in a successful response's JSON body or SSE stream.
    async fn answer(&self, response: Response, id: &Value) -> Result<Value, String> {
        if content_type(&response, "application/json") {
            let body = self
                .client
                .body(response)
                .await
                .map_err(|error| error.to_string())?;
            return serde_json::from_slice(&body).map_err(|error| error.to_string());
        }
        if !content_type(&response, "text/event-stream") {
            return Ok(refused(id, INVALID_REQUEST, "Unexpected content type"));
        }
        let mut events = super::sse::stream(
            response,
            self.client.clone(),
            self.client.failure.clone(),
            MESSAGE_LIMIT,
        );
        while let Some(event) = events.next().await {
            let event = event.map_err(|error| error.to_string())?;
            let Some(data) = event.data.filter(|data| !data.trim().is_empty()) else {
                continue;
            };
            let message: Value = serde_json::from_str(&data).map_err(|error| error.to_string())?;
            if message.get("method").is_none() && message.get("id").is_some() {
                return Ok(message);
            }
        }
        Err("connection closed: negotiation response".to_owned())
    }
}

/// A JSON-RPC error answering `id`.
fn refused(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

impl Link for Negotiating {
    async fn exchange(
        &mut self,
        mut request: ClientJsonRpcMessage,
        bound: Option<Duration>,
    ) -> Result<Option<Value>, String> {
        self.client.capture.stamp(&mut request);
        let answer = self.post(request);
        match bound {
            Some(bound) => match tokio::time::timeout(bound, answer).await {
                Ok(answer) => answer.map(Some),
                Err(_elapsed) => Ok(None),
            },
            None => answer.await.map(Some),
        }
    }
}
