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
    /// typed SDK result. `params` go out as written, but for a `_meta` that
    /// is `null` or empty, which goes as none; no parameters, or an empty
    /// set, go out as no `params` member (RFC §21.2, RK-21.2-046).
    ///
    /// # Errors
    /// Returns `NotConnected`; `Validation`, with nothing sent, when the
    /// `_meta` of `params` is neither an object nor `null`; a
    /// server/transport `Call` error, or `CallTimeout` when the configured
    /// deadline expires. A malformed result metadata field returns
    /// `Validation`, as Python's generic result model.
    pub async fn request(
        &self,
        method: &str,
        params: Option<&Metadata>,
        options: RequestOptions,
    ) -> Result<Metadata, McpError> {
        self.connected_server()?;
        let mut result = self
            .host_request(method, host_params(params)?, options.timeout)
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
            .host_request("resources/read", Some(params), options.timeout)
            .await?;
        crate::resources::result(result)
    }

    /// Send `method` with `params` written as given: `None` writes no
    /// `params` member.
    async fn host_request(
        &self,
        method: &str,
        params: Option<Metadata>,
        timeout: Duration,
    ) -> Result<Metadata, McpError> {
        let server = self.connected_server()?;
        let ticket = server.host_capture().ticket();
        let mut request = CustomRequest::new(method, params.clone().map(Value::Object));
        request.extensions.insert(ticket.marker(params));
        let response = server.host_request(ClientRequest::CustomRequest(request));
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

/// A host request's parameters as they go out (RFC §21.2, RK-21.2-046): as
/// the caller wrote them, but for a `_meta` that is `null` or empty, which
/// goes as none; `None` when nothing is left, so that the request carries no
/// `params` member, as Python's SDK sends it.
///
/// # Errors
/// Returns `Validation` when `_meta` is neither an object nor `null`, as
/// Python's request model refuses it.
fn host_params(params: Option<&Metadata>) -> Result<Option<Metadata>, McpError> {
    let Some(params) = params else {
        return Ok(None);
    };
    let mut sent = params.clone();
    let empty = match sent.get("_meta") {
        None => false,
        Some(Value::Null) => true,
        Some(Value::Object(meta)) => meta.is_empty(),
        Some(_) => {
            return Err(McpError::Validation {
                reason: "expected request metadata to be an object or null",
            });
        }
    };
    if empty {
        sent.shift_remove("_meta");
    }
    Ok((!sent.is_empty()).then_some(sent))
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
