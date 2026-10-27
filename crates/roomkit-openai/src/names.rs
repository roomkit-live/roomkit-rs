//! The tool names OpenAI's own endpoint accepts (RFC §6.7 "Declaring
//! tools"): checked when the agent takes its tools, so that a name the
//! vendor refuses fails the agent's construction, naming the tool and the
//! rule, rather than every turn's request.

use roomkit_ai::{AiError, ToolDefinition};

/// The vendor the rule is OpenAI's.
const VENDOR: &str = "openai";

/// The names OpenAI's endpoint accepts, as the reference implementation
/// measured them on the wire (`providers/openai/ai.py`, RMK-309), written
/// as its pattern is.
const RULE: &str = "[A-Za-z0-9_-]{1,128}";

/// The longest name the rule takes.
const LONGEST: usize = 128;

/// OpenAI's own endpoint, the one base URL whose rule this crate knows.
const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// The first of `tools` whose name OpenAI's endpoint refuses, as an error,
/// when `base_url` is that endpoint; a server behind any other base URL
/// decides itself, a vendor-compatible gateway included (RFC §6.7).
///
/// # Errors
///
/// Returns [`AiError::ToolName`] for that tool.
pub(crate) fn check(base_url: &str, tools: &[ToolDefinition]) -> Result<(), AiError> {
    if !is_openai_endpoint(base_url) {
        return Ok(());
    }
    match tools.iter().find(|tool| !accepted(&tool.name)) {
        Some(tool) => Err(AiError::ToolName {
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
fn is_openai_endpoint(base_url: &str) -> bool {
    let (Ok(url), Ok(openai)) = (
        reqwest::Url::parse(base_url.trim()),
        reqwest::Url::parse(OPENAI_BASE_URL),
    ) else {
        return false;
    };
    url.username().is_empty()
        && url.password().is_none()
        && url.scheme() == openai.scheme()
        && url.host_str() == openai.host_str()
        && url.port_or_known_default() == openai.port_or_known_default()
        && url.path().trim_end_matches('/') == openai.path()
}
