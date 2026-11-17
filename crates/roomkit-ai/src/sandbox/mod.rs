//! A host-supplied command executor. RoomKit supplies no execution environment.

use roomkit::model::Metadata;
use serde::{Deserialize, Serialize, Serializer};

use crate::tools::{BoxFuture, ToolError};

mod catalogue;
pub(crate) mod tools;

pub use catalogue::{SANDBOX_PREAMBLE, SANDBOX_TOOL_PREFIX, SANDBOX_TOOL_SCHEMAS};

/// Python's `SandboxResult`: success is derived from the required exit code.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct SandboxResult {
    /// The command's exit code.
    pub exit_code: i64,
    /// Its output, empty by default.
    #[serde(default)]
    pub output: String,
    /// Its error output, empty by default.
    #[serde(default)]
    pub error: String,
    /// Additional host data, empty by default.
    #[serde(default)]
    pub metadata: Metadata,
}

impl SandboxResult {
    /// Constructs an outcome with empty output, error and metadata.
    pub fn new(exit_code: i64) -> Self {
        Self {
            exit_code,
            output: String::new(),
            error: String::new(),
            metadata: Metadata::new(),
        }
    }

    /// Whether the current exit code is zero; also included in serialized JSON.
    pub const fn success(&self) -> bool {
        self.exit_code == 0
    }
}

impl Serialize for SandboxResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            exit_code: i64,
            output: &'a str,
            error: &'a str,
            metadata: &'a Metadata,
            success: bool,
        }
        Wire {
            exit_code: self.exit_code,
            output: &self.output,
            error: &self.error,
            metadata: &self.metadata,
            success: self.success(),
        }
        .serialize(serializer)
    }
}

/// Python's `SandboxExecutor`: the integrator chooses the environment and policy.
pub trait SandboxExecutor: Send + Sync {
    /// Executes a command with the first `sandbox_` prefix removed.
    ///
    /// # Errors
    /// Host failures follow the ordinary private tool-error reporting path.
    fn execute<'a>(
        &'a self,
        command: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> BoxFuture<'a, Result<SandboxResult, ToolError>>;

    /// Returns available schemas; called again when a turn builds its context.
    ///
    /// # Errors
    /// A catalogue failure propagates, including during channel construction.
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError>;

    /// Releases host resources; the default does nothing. As for `ScriptExecutor`,
    /// a panic propagates from direct channel close and is aggregated by kit close.
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}
