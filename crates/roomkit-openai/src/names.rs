//! The tool names OpenAI's own endpoint accepts (RFC §6.7 "Declaring
//! tools"): checked when a request declares the turn's tools, so that a
//! name the vendor refuses fails before anything is sent, naming the tool
//! and the rule, as Python's provider checks them.

use roomkit_ai::AiTool;

use crate::error::OpenAiError;

/// The vendor the rule is OpenAI's.
const VENDOR: &str = "openai";

/// The names OpenAI's endpoint accepts, as the reference implementation
/// measured them on the wire (`providers/openai/ai.py`, RMK-309), written
/// as its pattern is.
const RULE: &str = "[A-Za-z0-9_-]{1,128}";

/// The longest name the rule takes.
const LONGEST: usize = 128;

/// OpenAI's own endpoint, the one base URL whose rule this crate knows.
use crate::config::OPENAI_BASE_URL;

/// The first of `tools` whose name OpenAI's endpoint refuses, as an error,
/// when `base_url` is that endpoint; a server behind any other base URL
/// decides itself, a vendor-compatible gateway included (RFC §6.7).
///
/// # Errors
///
/// Returns [`AiError::ToolName`] for that tool.
pub(crate) fn check(base_url: &str, tools: &[AiTool]) -> Result<(), OpenAiError> {
    if !is_openai_endpoint(base_url) {
        return Ok(());
    }
    match tools.iter().find(|tool| !accepted(&tool.name)) {
        Some(tool) => Err(OpenAiError::ToolName {
            name: tool.name.clone(),
            vendor: VENDOR.to_owned(),
            rule: RULE.to_owned(),
        }),
        None => Ok(()),
    }
}

/// Whether OpenAI's endpoint accepts `name` for a tool.
fn accepted(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= LONGEST
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
}

/// Whether `base_url` is OpenAI's own endpoint, as the reference
/// implementation compares them: its scheme, host and path, case aside, the
/// scheme's default port written out or not, a trailing slash or not; a URL
/// carrying credentials is another endpoint.
pub(crate) fn is_openai_endpoint(base_url: &str) -> bool {
    same_endpoint(base_url, OPENAI_BASE_URL)
}

/// Whether `base_url` names the endpoint `official` does, as the reference
/// implementation compares them (Python's `is_vendor_endpoint`): scheme,
/// host and path, case aside, the scheme's default port written out or
/// not, a trailing slash or not; a URL carrying credentials is another.
pub(crate) fn same_endpoint(base_url: &str, official: &str) -> bool {
    let (Ok(url), Ok(official)) = (
        reqwest::Url::parse(base_url.trim()),
        reqwest::Url::parse(official),
    ) else {
        return false;
    };
    url.username().is_empty()
        && url.password().is_none()
        && url.scheme() == official.scheme()
        && url.host_str() == official.host_str()
        && url.port_or_known_default() == official.port_or_known_default()
        && url.path().trim_end_matches('/') == official.path().trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// RFC §6.7: on OpenAI's own endpoint, however it is written, a name
    /// outside `[A-Za-z0-9_-]{1,128}` is refused; a server behind another
    /// base URL, or OpenAI's with credentials, decides itself.
    #[test]
    fn a_tool_name_openai_refuses_is_refused_on_openai_s_endpoint_alone() {
        let refused = |base_url: &str, name: &str| {
            let tool = AiTool {
                parameters: json!({ "type": "object" }),
                ..AiTool::new(name, "Reads a file").unwrap()
            };
            check(base_url, &[tool]).is_err()
        };
        let openai = "https://api.openai.com/v1";
        let (long, longest) = ("d".repeat(129), "c".repeat(128));
        assert_eq!(
            [
                refused(openai, "files.read"),
                refused(openai, "files:read"),
                refused(openai, &long),
                refused("HTTPS://API.OpenAI.com:443/v1/", "files.read"),
                refused(openai, &longest),
                refused(openai, "files_read-2"),
                refused("http://localhost:8000/v1", "files.read"),
                refused("https://user:key@api.openai.com/v1", "files.read"),
            ],
            [true, true, true, true, false, false, false, false]
        );
    }
}
