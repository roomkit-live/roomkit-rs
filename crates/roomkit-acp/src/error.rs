//! What talking to an ACP agent fails with.

use std::sync::Arc;

use serde_json::Value;

/// What an ACP connection, its transport or their construction failed
/// with: the exceptions the Python implementation's ACP SDK raises, and the
/// `ValueError` and `RuntimeError` of its channel (HITL 3592a76c).
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AcpError {
    /// The agent answered a request with a JSON-RPC error: the SDK's
    /// `RequestError`, its code, message and data as the agent sent them,
    /// its text the message alone, as `str()` reads a `RequestError`.
    #[error("{message}")]
    Rpc {
        /// The JSON-RPC error code.
        code: i32,
        /// The error's message.
        message: String,
        /// The error's data, if the agent sent any.
        data: Option<Value>,
    },
    /// The connection is closed, or its peer went away: the SDK's
    /// `ConnectionError("Connection closed")`. A request still waiting for
    /// its answer fails with it.
    #[error("Connection closed")]
    ConnectionClosed,
    /// An argument was refused: Python's `ValueError`, in its words.
    #[error("{0}")]
    Value(String),
    /// The agent's program could not be started: Python's `OSError` from
    /// the spawn. The program is named, never its arguments, which may hold
    /// secrets.
    #[error("cannot start the ACP agent {program:?}: {source}")]
    Spawn {
        /// The program.
        program: String,
        /// Why it could not start.
        #[source]
        source: Arc<std::io::Error>,
    },
    /// The agent's answer is not the response the protocol defines for its
    /// request: the SDK's validation error.
    #[error("the ACP agent's answer to {method} is not valid: {reason}")]
    InvalidResponse {
        /// The request's method.
        method: String,
        /// What the answer's reading refused.
        reason: String,
    },
    /// The connection cannot go on: Python's `RuntimeError`, in its words
    /// (a protocol version the agent selected that is not 1, a closed
    /// channel).
    #[error("{0}")]
    Runtime(String),
}

impl AcpError {
    /// A JSON-RPC error with `code` and `message`, and `data` when given.
    pub(crate) fn rpc(code: i32, message: &str, data: Option<Value>) -> Self {
        Self::Rpc {
            code,
            message: message.to_owned(),
            data,
        }
    }

    /// The error the SDK's `RequestError.method_not_found(method)` is:
    /// `-32601`, `Method not found`, the method in its data.
    pub(crate) fn method_not_found(method: impl Into<Value>) -> Self {
        Self::rpc(
            METHOD_NOT_FOUND,
            "Method not found",
            Some(serde_json::json!({ "method": method.into() })),
        )
    }

    /// The error object a JSON-RPC response carries for this error: its
    /// own code, message and data for [`AcpError::Rpc`], an internal error
    /// naming it for any other, as the SDK answers a handler that raised.
    pub(crate) fn to_error_object(&self) -> Value {
        match self {
            Self::Rpc {
                code,
                message,
                data,
            } => serde_json::json!({ "code": code, "message": message, "data": data }),
            Self::ConnectionClosed
            | Self::Value(_)
            | Self::Spawn { .. }
            | Self::InvalidResponse { .. }
            | Self::Runtime(_) => serde_json::json!({
                "code": INTERNAL_ERROR,
                "message": "Internal error",
                "data": { "details": self.to_string() },
            }),
        }
    }
}

/// JSON-RPC's code for a method the peer does not serve.
pub(crate) const METHOD_NOT_FOUND: i32 = -32601;

/// JSON-RPC's code for parameters the method refuses.
pub(crate) const INVALID_PARAMS: i32 = -32602;

/// JSON-RPC's code for a failure while serving a request.
pub(crate) const INTERNAL_ERROR: i32 = -32603;
