//! Host-selected origin and approval callback identity (RFC §24.7).

use super::{McpSkillServer, McpSkillServerError};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// One attempt to execute a host tool while a served skill is active.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillExecutionRequest {
    /// The registry's origin-qualified skill name.
    pub name: String,
    /// The host's server label.
    pub label: String,
    /// The held SKILL.md URI.
    pub uri: String,
    /// The held entry's fingerprint, absent for dynamic content.
    pub fingerprint: Option<String>,
    /// The conversation scope, when present.
    pub room_id: Option<String>,
    /// The tool being requested.
    pub tool: String,
}

type ApprovalFuture = Pin<Box<dyn Future<Output = Result<bool, McpSkillServerError>> + Send>>;
type ApprovalFn = dyn Fn(SkillExecutionRequest) -> ApprovalFuture + Send + Sync;

/// Python's asynchronous `ApproveExecution`, with identity shared across clones.
#[derive(Clone)]
pub struct ApproveExecution(Arc<ApprovalFn>);
impl ApproveExecution {
    /// Wrap the host's asynchronous approval function.
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(SkillExecutionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<bool, McpSkillServerError>> + Send + 'static,
    {
        Self(Arc::new(move |request| Box::pin(callback(request))))
    }
    /// Ask the host; the execution gate refuses any returned error.
    ///
    /// # Errors
    /// Preserves the callback's concrete error and source.
    pub async fn call(&self, request: SkillExecutionRequest) -> Result<bool, McpSkillServerError> {
        (self.0)(request).await
    }
}
impl fmt::Debug for ApproveExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApproveExecution").finish_non_exhaustive()
    }
}
impl PartialEq for ApproveExecution {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ApproveExecution {}

/// Python's keyword options to `SkillRegistry.add_mcp_server` (RFC §24.7).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddMcpServerOptions {
    /// Admit content without stable resource digests; false by default.
    pub allow_dynamic: bool,
    /// Optional host execution approval callback.
    pub approve_execution: Option<ApproveExecution>,
    /// Host tool patterns that execute code, empty by default.
    pub execution_tools: Vec<String>,
}

/// An immutable origin shared by the registry and its copies (RFC §24.7).
pub struct ServedOrigin {
    /// The host's label for this server.
    pub label: String,
    /// The connected server; the host owns its lifetime.
    pub server: Arc<dyn McpSkillServer>,
    /// Whether dynamic skills may be admitted.
    pub allow_dynamic: bool,
    /// Optional asynchronous execution approval.
    pub approve_execution: Option<ApproveExecution>,
    /// Patterns of host tools that execute code.
    pub execution_tools: Vec<String>,
}
impl fmt::Debug for ServedOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServedOrigin")
            .field("label", &self.label)
            .field("allow_dynamic", &self.allow_dynamic)
            .field("approve_execution", &self.approve_execution)
            .field("execution_tools", &self.execution_tools)
            .finish_non_exhaustive()
    }
}
