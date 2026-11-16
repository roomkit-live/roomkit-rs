//! Share Python's initialize-based MCP protocol family. A newer protocol's
//! lifecycle metadata must be introduced in both implementations together.
use rmcp::model::{ClientConfig, ProtocolVersion};
use serde_json::Value;

pub(crate) fn client() -> ClientConfig {
    ClientConfig::default().with_protocol_version(ProtocolVersion::V_2025_11_25)
}

pub(crate) fn supported(version: &ProtocolVersion) -> bool {
    matches!(
        version.as_str(),
        "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25"
    )
}

/// The raw snapshot is already retained. On the SDK's parsing copy, avoid
/// imposing newer model fields on values Python treats as arbitrary extras.
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
