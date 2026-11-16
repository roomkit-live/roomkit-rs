//! Host access beside the model's discovered tools (RFC §21.2).

use std::collections::BTreeMap;
use std::time::Duration;

use rmcp::model::{ClientRequest, CustomRequest};
use rmcp::service::ServiceError;
use roomkit::model::Metadata;
use serde_json::Value;

use crate::{McpError, McpToolProvider, texts::bounded_message};

/// Python's `request(*, timeout=30.0)` keyword arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestOptions {
    /// How long the server may take to answer.
    pub timeout: Duration,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

/// Python's `read_resource(*, timeout=30.0)` keyword arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadResourceOptions {
    /// How long the server may take to answer.
    pub timeout: Duration,
}

impl Default for ReadResourceOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

impl McpToolProvider {
    /// The admitted tools' metadata from the connection's one listing,
    /// as an owned copy: Python's `tool_meta()`.
    ///
    /// # Errors
    /// Returns `NotConnected` outside a live connection.
    pub fn tool_meta(&self) -> Result<BTreeMap<String, Metadata>, McpError> {
        Ok(self.connected_server()?.tool_meta())
    }

    /// The handshake's server information, including extension fields.
    ///
    /// # Errors
    /// Returns `NotConnected` outside a live connection.
    pub fn server_info(&self) -> Result<Metadata, McpError> {
        Ok(self.connected_server()?.host_capture().server_info())
    }

    /// Extensions declared by the server with object-valued settings.
    ///
    /// # Errors
    /// Returns `NotConnected` outside a live connection.
    pub fn server_extensions(&self) -> Result<BTreeMap<String, Metadata>, McpError> {
        Ok(self.connected_server()?.host_capture().server_extensions())
    }

    /// Send a host request, preserving result fields even when they resemble a
    /// typed SDK result. `None` sends an explicit empty parameters object.
    ///
    /// # Errors
    /// Returns `NotConnected`, a server/transport `Call` error, or
    /// `CallTimeout` when the configured deadline expires. A malformed result
    /// metadata field returns `Validation`, as Python's generic result model.
    pub async fn request(
        &self,
        method: &str,
        params: Option<&Metadata>,
        options: RequestOptions,
    ) -> Result<Metadata, McpError> {
        let mut result = self
            .host_request(method, params.cloned().unwrap_or_default(), options.timeout)
            .await?;
        validate_metadata(&result)?;
        // Python's generic Result.model_dump(exclude_none=True) removes null
        // model fields, but leaves nulls inside nested JSON values intact.
        result.retain(|_, value| !value.is_null());
        Ok(result)
    }

    /// Read a resource as JSON, including ordered contents, explicit nulls and
    /// server extension fields that the SDK's concrete result cannot represent.
    ///
    /// # Errors
    /// As `request`; an invalid URI or result is a `Validation` error.
    pub async fn read_resource(
        &self,
        uri: &str,
        options: ReadResourceOptions,
    ) -> Result<Metadata, McpError> {
        self.connected_server()?;
        let params = [("uri".into(), Value::String(crate::resources::uri(uri)?))]
            .into_iter()
            .collect();
        let result = self
            .host_request("resources/read", params, options.timeout)
            .await?;
        crate::resources::result(result)
    }

    async fn host_request(
        &self,
        method: &str,
        params: Metadata,
        timeout: Duration,
    ) -> Result<Metadata, McpError> {
        let server = self.connected_server()?;
        let ticket = server.host_capture().ticket();
        let mut request = CustomRequest::new(method, Some(Value::Object(params.clone())));
        request.extensions.insert(ticket.marker(params));
        let response = server
            .host_peer()
            .send_request(ClientRequest::CustomRequest(request));
        tokio::time::timeout(timeout, response)
            .await
            .map_err(|_| McpError::CallTimeout {
                tool: method.into(),
                after: timeout,
            })?
            .map_err(|error| McpError::Call {
                message: bounded_message(&if let ServiceError::McpError(error) = &error {
                    error.message.to_string()
                } else {
                    error.to_string()
                }),
            })?;
        ticket
            .take()
            .and_then(|value| value.as_object().cloned())
            .ok_or_else(|| McpError::Call {
                message: "expected an MCP result object".into(),
            })
    }
}

fn validate_metadata(result: &Metadata) -> Result<(), McpError> {
    if result
        .get("_meta")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(McpError::Validation {
            reason: "expected result metadata to be an object or null",
        });
    }
    Ok(())
}
