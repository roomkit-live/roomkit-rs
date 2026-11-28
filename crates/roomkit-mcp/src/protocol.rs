//! The MCP revisions a connection speaks, and how rmcp opens it once its
//! revision is agreed on (RFC §21.2, RK-21.2-056): by MCP 2026-07-28's
//! discovery or by the `initialize` handshake of the earlier revisions.
//! With `protocol` `"auto"` roomkit-mcp negotiates first, as Python's MCP
//! SDK does (`crate::negotiation`); `"legacy"`, and the legacy HTTP+SSE
//! transport, go straight to the handshake.
use rmcp::model::{ClientConfig, ProtocolVersion};
use rmcp::service::RunningService;
use rmcp::transport::IntoTransport;
use rmcp::{ClientLifecycleMode, ClientServiceExt, RoleClient};
use serde_json::{Value, json};

use crate::McpError;

/// The revision a discovery agrees on.
pub(crate) const MODERN: &str = "2026-07-28";

/// The revisions of the `initialize` handshake, as Python's pinned SDK
/// accepts them.
pub(crate) const HANDSHAKE_VERSIONS: [&str; 4] =
    ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];

/// The `_meta` entries every request of a 2026-07-28 connection carries,
/// which the protocol owns.
pub(crate) const PROTOCOL_META: [&str; 3] = [
    "io.modelcontextprotocol/protocolVersion",
    "io.modelcontextprotocol/clientInfo",
    "io.modelcontextprotocol/clientCapabilities",
];

/// Where a discovery result says what the server is: its `_meta`.
pub(crate) const SERVER_INFO_META: &str = "io.modelcontextprotocol/serverInfo";

/// How a connection agrees on the MCP revision, and what its requests
/// carry: Python's `protocol` and `propagate_trace_context` (RFC §21.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Protocol {
    /// Probe the server's discovery first (`"auto"`), or go straight to the
    /// handshake (`"legacy"`).
    pub(crate) probe: bool,
    /// Carry the host's trace context on each request.
    pub(crate) propagate_trace_context: bool,
}

impl Protocol {
    /// Python's `protocol` and `propagate_trace_context`, checked.
    ///
    /// # Errors
    /// Returns [`McpError::Configuration`] for a `protocol` that is neither
    /// `"auto"` nor `"legacy"`, as Python's `ValueError`.
    pub(crate) fn new(protocol: &str, propagate_trace_context: bool) -> Result<Self, McpError> {
        let probe = match protocol {
            "auto" => true,
            "legacy" => false,
            _ => {
                return Err(McpError::Configuration {
                    reason: "protocol must be 'auto' or 'legacy'",
                });
            }
        };
        Ok(Self {
            probe,
            propagate_trace_context,
        })
    }
}

/// What the client says of itself in the handshake: MCP 2025-11-25, as
/// Python's SDK opens it.
pub(crate) fn client() -> ClientConfig {
    ClientConfig::default().with_protocol_version(ProtocolVersion::V_2025_11_25)
}

/// rmcp's connection over `transport`, opened by a discovery when `modern`
/// (the one the negotiation agreed on, which the transport answers), by the
/// handshake otherwise (RFC §21.2).
///
/// # Errors
/// Returns what failed the opening, as rmcp says it.
pub(crate) async fn serve<T, E, A>(
    transport: T,
    modern: bool,
) -> Result<RunningService<RoleClient, ClientConfig>, String>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let lifecycle = if modern {
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        }
    } else {
        ClientLifecycleMode::Initialize
    };
    client()
        .serve_with_lifecycle(transport, lifecycle)
        .await
        .map_err(|error| error.to_string())
}

/// Whether the connection speaks `version`: 2026-07-28 once a discovery
/// agreed on it, the handshake's revisions otherwise (a handshake that
/// answers 2026-07-28 is refused, as Python's SDK refuses it).
pub(crate) fn supported(version: &ProtocolVersion, modern: bool) -> bool {
    if modern {
        return version.as_str() == MODERN;
    }
    HANDSHAKE_VERSIONS.contains(&version.as_str())
}

/// The raw snapshot is already retained. On the SDK's parsing copy of a
/// handshake or a discovery, avoid imposing newer model fields on values
/// Python treats as arbitrary extras: Python's `compatible_handshake`.
pub(crate) fn compatible_handshake(message: &mut Value) {
    if let Some(capabilities) = message
        .pointer_mut("/result/capabilities")
        .and_then(Value::as_object_mut)
    {
        if let Some(extensions) = capabilities
            .get_mut("extensions")
            .and_then(Value::as_object_mut)
        {
            extensions.retain(|_, settings| settings.is_object());
        } else {
            capabilities.shift_remove("extensions");
        }
    }
    if let Some(info) = message
        .pointer_mut("/result/serverInfo")
        .and_then(Value::as_object_mut)
    {
        info.shift_remove("description");
        if let Some(icons) = info.get_mut("icons").and_then(Value::as_array_mut) {
            for icon in icons.iter_mut().filter_map(Value::as_object_mut) {
                icon.shift_remove("theme");
            }
        }
    }
}

/// A discovery `result` with the cache members Python's model defaults
/// where the server left them out, which rmcp's model requires: a
/// handshake's result is left as it is.
pub(crate) fn with_discovery_defaults(result: &mut Value) {
    let Some(result) = result.as_object_mut() else {
        return;
    };
    for (name, default) in [
        ("resultType", json!("complete")),
        ("ttlMs", json!(0)),
        ("cacheScope", json!("private")),
    ] {
        result.entry(name).or_insert(default);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_mcp_discovery.py::test_a_listing_s_cache_hints_do_not_fail_the_connection,
    // for the discovery: the cache members Python's model defaults, which
    // rmcp's requires.
    #[test]
    fn a_discovery_without_its_cache_members_is_read_with_python_s_defaults() {
        let mut result = json!({"supportedVersions":["2026-07-28"],"capabilities":{}});
        with_discovery_defaults(&mut result);
        assert_eq!(
            (
                &result["resultType"],
                &result["ttlMs"],
                &result["cacheScope"]
            ),
            (&json!("complete"), &json!(0), &json!("private"))
        );
        assert!(serde_json::from_value::<rmcp::model::DiscoverResult>(result).is_ok());
    }
}
