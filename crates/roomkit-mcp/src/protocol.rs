//! The MCP revisions a connection speaks, and how it agrees on one with its
//! server (RFC §21.2, RK-21.2-056): with `protocol` `"auto"` it probes MCP
//! 2026-07-28's discovery (`server/discover`) and falls back to the
//! `initialize` handshake of the earlier revisions as Python's MCP SDK
//! does (`negotiate_auto`); `"legacy"`, and the legacy HTTP+SSE transport,
//! go straight to the handshake.
//!
//! rmcp's `Auto` lifecycle runs the probe, its 10-second bound and the
//! fallback, but fails a connection Python's SDK reaches by the handshake;
//! [`as_the_sdk_reads_a_discovery`] shows it, in those cases, a refusal it
//! falls back on.
use rmcp::model::{ClientConfig, DiscoverResult, ProtocolVersion};
use rmcp::service::{ClientInitializeError, RunningService};
use rmcp::transport::IntoTransport;
use rmcp::{ClientLifecycleMode, ClientServiceExt, RoleClient};
use serde_json::{Value, json};

use crate::McpError;

/// The revision a discovery agrees on.
pub(crate) const MODERN: &str = "2026-07-28";

/// The revisions of the `initialize` handshake, as Python's pinned SDK
/// accepts them.
const HANDSHAKE_VERSIONS: [&str; 4] = ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];

/// The `_meta` entries every request of a 2026-07-28 connection carries,
/// which the protocol owns.
pub(crate) const PROTOCOL_META: [&str; 3] = [
    "io.modelcontextprotocol/protocolVersion",
    "io.modelcontextprotocol/clientInfo",
    "io.modelcontextprotocol/clientCapabilities",
];

/// Where a discovery result says what the server is: its `_meta`.
pub(crate) const SERVER_INFO_META: &str = "io.modelcontextprotocol/serverInfo";

/// Python's JSON-RPC code for a method a server does not know: what rmcp is
/// shown in place of a discovery it should fall back from.
const METHOD_NOT_FOUND: i64 = -32601;

/// `-32022`: the server speaks none of the revisions the probe offered.
const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

/// The codes rmcp reads as a modern server's refusal and fails on, where
/// Python's SDK falls back to the handshake: `-32021` (a client capability
/// missing) and `-32020` (a header mismatch).
const MODERN_REFUSALS: [i64; 2] = [-32021, -32020];

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

    /// The same, never probing: the legacy HTTP+SSE transport's.
    #[cfg(feature = "http")]
    pub(crate) fn handshake_only(self) -> Self {
        Self {
            probe: false,
            ..self
        }
    }
}

/// What the client says of itself in the handshake: MCP 2025-11-25, as
/// Python's SDK opens it.
pub(crate) fn client() -> ClientConfig {
    ClientConfig::default().with_protocol_version(ProtocolVersion::V_2025_11_25)
}

/// Agrees on the revision with the server over `transport` (RFC §21.2):
/// probing discovery first when `probe`, the handshake otherwise.
///
/// # Errors
/// Returns what failed the negotiation, as rmcp says it: for a handshake
/// that failed after the probe, what failed the handshake, as Python's SDK
/// raises it.
pub(crate) async fn negotiate<T, E, A>(
    transport: T,
    probe: bool,
) -> Result<RunningService<RoleClient, ClientConfig>, String>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let lifecycle = if probe {
        ClientLifecycleMode::Auto {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            legacy_version: Some(ProtocolVersion::V_2025_11_25),
        }
    } else {
        ClientLifecycleMode::Initialize
    };
    client()
        .serve_with_lifecycle(transport, lifecycle)
        .await
        .map_err(|error| {
            if let ClientInitializeError::LegacyFallbackFailed { fallback, .. } = &error {
                fallback.to_string()
            } else {
                error.to_string()
            }
        })
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
/// Python treats as arbitrary extras, and give a discovery the cache
/// members Python's model defaults when the server left them out.
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
    let info = if message.pointer("/result/supportedVersions").is_some() {
        message
            .pointer_mut("/result/_meta")
            .and_then(|meta| meta.get_mut(SERVER_INFO_META))
    } else {
        message.pointer_mut("/result/serverInfo")
    };
    if let Some(info) = info.and_then(Value::as_object_mut) {
        info.shift_remove("description");
        if let Some(icons) = info.get_mut("icons").and_then(Value::as_array_mut) {
            for icon in icons.iter_mut().filter_map(Value::as_object_mut) {
                icon.shift_remove("theme");
            }
        }
    }
    if let Some(result) = message.get_mut("result").and_then(Value::as_object_mut)
        && result.contains_key("supportedVersions")
    {
        for (name, default) in [
            ("resultType", json!("complete")),
            ("ttlMs", json!(0)),
            ("cacheScope", json!("private")),
        ] {
            result.entry(name).or_insert(default);
        }
    }
}

/// The SDK's copy of the answer to the `probes`-th discovery probe, as
/// Python's SDK reads it: whether it agrees on 2026-07-28. Where Python's
/// SDK falls back to the handshake and rmcp would fail the connection, rmcp
/// is shown a refusal it falls back on:
/// - a result it cannot read, or that names no revision both speak;
/// - a `-32022` that names no revision both speak, or a second one, unless
///   it names only revisions neither handshake nor discovery knows, which
///   fails the connection in both;
/// - a refusal of a modern server's (`-32021`, `-32020`).
pub(crate) fn as_the_sdk_reads_a_discovery(message: &mut Value, probes: usize) -> bool {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    if let Some(error) = message.get_mut("error").and_then(Value::as_object_mut) {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        if code == UNSUPPORTED_PROTOCOL_VERSION {
            let supported = supported_versions(error.get("data"));
            let retried = probes == 1
                && supported
                    .as_ref()
                    .is_some_and(|versions| versions.iter().any(|version| version == MODERN));
            let unknown = supported.as_ref().is_some_and(|versions| {
                !versions
                    .iter()
                    .any(|version| HANDSHAKE_VERSIONS.contains(&version.as_str()))
            });
            if retried || unknown {
                return false;
            }
        } else if !MODERN_REFUSALS.contains(&code) {
            return false;
        }
        *message = refusal(&id);
        return false;
    }
    let agreed = message
        .get("result")
        .cloned()
        .and_then(|result| serde_json::from_value::<DiscoverResult>(result).ok())
        .is_some_and(|result| {
            result
                .supported_versions
                .iter()
                .any(|version| version.as_str() == MODERN)
        });
    if !agreed {
        *message = refusal(&id);
    }
    agreed
}

/// The `supported` revisions a `-32022` names, as Python's SDK reads its
/// data: `None` unless it names them and the revision requested.
fn supported_versions(data: Option<&Value>) -> Option<Vec<String>> {
    let data = data?.as_object()?;
    data.get("requested")?.as_str()?;
    data.get("supported")?
        .as_array()?
        .iter()
        .map(|version| version.as_str().map(str::to_owned))
        .collect()
}

/// What rmcp is shown in place of a discovery it should fall back from.
fn refusal(id: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": METHOD_NOT_FOUND,
            "message": "server/discover agreed on no revision: fall back to initialize",
        },
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn discovery(versions: &Value) -> Value {
        json!({"jsonrpc":"2.0","id":0,"result":{
            "supportedVersions":versions,"capabilities":{},
            "_meta":{SERVER_INFO_META:{"name":"modern","version":"2"}},
        }})
    }

    fn refused(code: i64, data: &Value) -> Value {
        json!({"jsonrpc":"2.0","id":0,"error":{"code":code,"message":"no","data":data}})
    }

    // Python: mcp.client._probe.negotiate_auto, read on SDK 2.3: what
    // agrees on 2026-07-28, what falls back to the handshake, and what
    // fails the connection.
    #[test]
    fn a_discovery_answer_reads_as_python_s_sdk_reads_it() {
        let both = json!({"supported":["2026-07-28","2025-11-25"],"requested":"2026-07-28"});
        let modern_only = json!({"supported":["2026-07-28"],"requested":"2026-07-28"});
        let unknown = json!({"supported":["2099-01-01"],"requested":"2026-07-28"});
        // (answer, probes so far, agreed, shown to rmcp as sent)
        for (sent, probes, agreed, as_sent) in [
            (discovery(&json!(["2026-07-28"])), 1, true, true),
            (discovery(&json!(["2025-11-25"])), 1, false, false),
            (json!({"jsonrpc":"2.0","id":0,"result":{}}), 1, false, false),
            (refused(-32601, &Value::Null), 1, false, true),
            (refused(-32602, &Value::Null), 1, false, true),
            (refused(-32020, &Value::Null), 1, false, false),
            (refused(-32022, &both), 1, false, true),
            (refused(-32022, &both), 2, false, false),
            (refused(-32022, &modern_only), 2, false, true),
            (refused(-32022, &unknown), 1, false, true),
            (
                refused(-32022, &json!({"supported":["2099-01-01"]})),
                1,
                false,
                false,
            ),
        ] {
            let mut answer = sent.clone();
            compatible_handshake(&mut answer);
            let shown = answer.clone();
            let read = as_the_sdk_reads_a_discovery(&mut answer, probes);
            assert_eq!(read, agreed, "{sent}");
            assert_eq!(answer == shown, as_sent, "{sent} read as {answer}");
            if !as_sent {
                assert_eq!(answer["error"]["code"], json!(METHOD_NOT_FOUND), "{sent}");
            }
        }
    }

    // Python: tests/test_mcp_discovery.py::test_a_listing_s_cache_hints_do_not_fail_the_connection,
    // for the discovery: the cache members Python's model defaults.
    #[test]
    fn a_discovery_without_its_cache_members_is_read_with_python_s_defaults() {
        let mut answer = discovery(&json!(["2026-07-28"]));
        compatible_handshake(&mut answer);
        assert_eq!(
            (
                &answer["result"]["resultType"],
                &answer["result"]["ttlMs"],
                &answer["result"]["cacheScope"]
            ),
            (&json!("complete"), &json!(0), &json!("private"))
        );
        assert!(as_the_sdk_reads_a_discovery(&mut answer, 1));
    }
}
