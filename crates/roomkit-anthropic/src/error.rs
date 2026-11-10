//! Provider failures and credential-safe diagnostics (RFC §6.7).
use roomkit_ai::{ProviderError, ProviderErrorOptions};
use serde_json::Value;

pub(crate) fn failed(message: impl Into<String>) -> ProviderError {
    ProviderError::new(
        message,
        ProviderErrorOptions {
            provider: "anthropic".into(),
            ..ProviderErrorOptions::default()
        },
    )
}

pub(crate) fn transport(error: reqwest::Error) -> ProviderError {
    let retryable =
        error.is_connect() || error.is_timeout() || error.is_body() || error.is_request();
    let mut failure = failed(error.without_url().to_string());
    failure.retryable = retryable;
    failure
}

pub(crate) fn response(body: &Value, http_status: Option<u16>, secrets: &[&str]) -> ProviderError {
    let error = body.get("error").unwrap_or(body);
    let status = http_status.or_else(|| described_status(body, 3));
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("the provider returned an error");
    let mut message = message.to_owned();
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        message = message.replace(secret, "[redacted]");
    }
    message = message
        .chars()
        .take(500)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let mut failure = failed(message);
    failure.status_code = status;
    failure.retryable = status.map_or_else(|| transient(&failure.message), retryable_status);
    failure
}

fn described_status(error: &Value, depth: usize) -> Option<u16> {
    if depth == 0 || !error.is_object() {
        return None;
    }
    for nested in ["error", "data"] {
        if let Some(status) = error
            .get(nested)
            .and_then(|value| described_status(value, depth - 1))
        {
            return Some(status);
        }
    }
    ["code", "type", "status"]
        .iter()
        .find_map(|field| error.get(field).and_then(status_in))
}

fn status_in(value: &Value) -> Option<u16> {
    if let Some(status) = value.as_u64().or_else(|| value.as_str()?.parse().ok()) {
        return (400..=599)
            .contains(&status)
            .then(|| u16::try_from(status).ok())
            .flatten();
    }
    match value.as_str()? {
        "invalid_request_error" | "INVALID_ARGUMENT" => Some(400),
        "authentication_error" | "UNAUTHENTICATED" => Some(401),
        "permission_error" | "PERMISSION_DENIED" => Some(403),
        "not_found_error" | "NOT_FOUND" => Some(404),
        "request_too_large" => Some(413),
        "rate_limit_error" | "rate_limit_exceeded" | "RESOURCE_EXHAUSTED" => Some(429),
        "api_error" | "server_error" | "internal_error" | "INTERNAL" => Some(500),
        "service_unavailable" | "UNAVAILABLE" => Some(503),
        "timeout_error" | "DEADLINE_EXCEEDED" => Some(504),
        "overloaded_error" => Some(529),
        _ => None,
    }
}

fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 409 | 429 | 500..=599)
}

fn transient(message: &str) -> bool {
    let lower = message.to_lowercase();
    let words: Vec<_> = lower
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|word| !word.is_empty())
        .collect();
    words.iter().any(|word| {
        matches!(
            *word,
            "overloaded"
                | "ratelimit"
                | "ratelimited"
                | "ratelimits"
                | "rate_limit"
                | "rate_limited"
                | "rate_limits"
        ) || word.parse().is_ok_and(retryable_status)
    }) || words
        .windows(2)
        .any(|pair| matches!(pair, ["rate", "limit" | "limited" | "limits"]))
        || words
            .windows(3)
            .any(|phrase| phrase == ["too", "many", "requests"])
}
