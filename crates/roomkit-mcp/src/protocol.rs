//! Share Python's initialize-based MCP protocol family. A newer protocol's
//! lifecycle metadata must be introduced in both implementations together.
use rmcp::model::{ClientConfig, ProtocolVersion};

pub(crate) fn client() -> ClientConfig {
    ClientConfig::default().with_protocol_version(ProtocolVersion::V_2025_11_25)
}

pub(crate) fn supported(version: &ProtocolVersion) -> bool {
    matches!(
        version.as_str(),
        "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25"
    )
}
