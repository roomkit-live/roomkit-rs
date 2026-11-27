//! A connected server's protocol, independent of its transport (RFC §24.7).

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use roomkit::model::Metadata;
use serde_json::Value;

/// A server's concrete error, retained for downcasting instead of stringified.
pub type McpSkillServerError = Box<dyn Error + Send + Sync>;

/// One borrowed asynchronous server operation, usable through a trait object.
pub type McpSkillServerFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, McpSkillServerError>> + Send + 'a>>;

/// The three operations a skill registry asks of a connected server (RFC §24.7).
///
/// Python's `MCPSkillServer`. The host owns connection and closure; this protocol
/// also admits implementations that have no MCP session or transport dependency.
pub trait McpSkillServer: Send + Sync {
    /// An owned snapshot of the extension settings declared at connection.
    ///
    /// # Errors
    /// Propagates the server's error, including an MCP provider outside a connection.
    fn server_extensions(&self) -> Result<BTreeMap<String, Metadata>, McpSkillServerError>;

    /// Send an unmodelled request and return its JSON object. `None` is Python's
    /// default for `params`; an MCP provider sends it, and an empty set, as no
    /// `params` member (RFC §21.2, RK-21.2-046).
    ///
    /// # Errors
    /// Propagates the server's original error without changing its concrete type.
    fn request<'a>(
        &'a self,
        method: &'a str,
        params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata>;

    /// Read a resource as JSON. A custom server may return any JSON shape;
    /// consumers decide whether it holds content for the requested URI.
    ///
    /// # Errors
    /// Propagates the server's original error without changing its concrete type.
    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value>;
}
