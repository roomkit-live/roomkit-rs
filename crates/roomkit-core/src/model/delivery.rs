//! What a delivery to a channel produced (RFC §5.13).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::positive::python_float;
use super::{ChannelId, ErrorRecord, Metadata, Timestamp};

/// Where a delivery stands (RFC §5.13): the Python implementation's
/// `Literal["sent", "queued", "failed"]` of `DeliveryResult.status`, named
/// after its field. Python's `DeliveryStatus` is another model, a
/// provider's status webhook.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryResultStatus {
    /// Handed to the provider.
    Sent,
    /// Queued, for example behind a rate limit.
    Queued,
    /// Failed.
    Failed,
}

/// The outcome of one delivery to one channel (RFC §5.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryResult {
    /// The target channel.
    pub channel_id: ChannelId,
    /// Where the delivery stands.
    pub status: DeliveryResultStatus,
    /// The provider's message id.
    #[serde(default)]
    pub provider_message_id: Option<String>,
    /// What failed, when it failed.
    #[serde(default)]
    pub error: Option<DeliveryError>,
    /// When to retry, when rate limited.
    #[serde(default)]
    pub retry_after: Option<Timestamp>,
    /// The provider's own answer.
    #[serde(default)]
    pub provider_result: Option<ProviderResult>,
}

impl DeliveryResult {
    /// The delivery to `channel_id`, standing at `status`, with no provider
    /// id, error, retry or provider answer.
    pub fn new(channel_id: ChannelId, status: DeliveryResultStatus) -> Self {
        Self {
            channel_id,
            status,
            provider_message_id: None,
            error: None,
            retry_after: None,
            provider_result: None,
        }
    }

    /// The failed delivery to `channel_id` of a call that raised `error`,
    /// which declares nothing of itself, as the Python implementation's
    /// `_delivery_error` reports it: its type as the code (Python's class
    /// name; a boxed Rust error has none, and reads `ChannelError`), its
    /// message, retryable. Outside the public API.
    #[doc(hidden)]
    pub fn raised(channel_id: ChannelId, error: &ErrorRecord) -> Self {
        Self {
            error: Some(DeliveryError::new(
                error.error_type.clone(),
                error.message.clone(),
            )),
            ..Self::new(channel_id, DeliveryResultStatus::Failed)
        }
    }

    /// The failed delivery to `channel_id` of a call that raised Python's
    /// `ProviderDeliveryError(result)`, a provider's explicit refusal: its
    /// message id, the error [`DeliveryError::of_provider`] reads, and the
    /// provider's answer kept. Outside the public API.
    #[doc(hidden)]
    pub fn refused(channel_id: ChannelId, result: ProviderResult) -> Self {
        Self {
            provider_message_id: result.provider_message_id.clone(),
            error: Some(DeliveryError::of_provider(&result)),
            provider_result: Some(result),
            ..Self::new(channel_id, DeliveryResultStatus::Failed)
        }
    }
}

/// Why a delivery failed (RFC §5.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryError {
    /// A machine-readable code.
    pub code: String,
    /// A human-readable description.
    pub message: String,
    /// Whether a retry may succeed, `true` by default.
    #[serde(default = "retryable_by_default")]
    pub retryable: bool,
}

impl DeliveryError {
    /// A retryable error of `code` saying `message`, as Python's
    /// `retryable=True` default.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: true,
        }
    }

    /// The error of Python's `ProviderDeliveryError(result)`
    /// (`core/exceptions.py`): its message `result.error`, else
    /// `provider_send_failed`; its code `result.metadata["code"]`, else
    /// `result.error`, else `ProviderDeliveryFailed`, each read only when
    /// Python reads it true and written as `str()` writes it; retryable as
    /// `result.metadata["retryable"]` says when it is a bool, else
    /// retryable, as the retry loop's `_is_retryable` reads a declared
    /// flag. Outside the public API.
    #[doc(hidden)]
    pub fn of_provider(result: &ProviderResult) -> Self {
        let error = result.error.clone().filter(|error| !error.is_empty());
        let code = result
            .metadata
            .get("code")
            .and_then(python_str)
            .or_else(|| error.clone())
            .unwrap_or_else(|| "ProviderDeliveryFailed".to_owned());
        let retryable = match result.metadata.get("retryable") {
            Some(Value::Bool(declared)) => *declared,
            _ => true,
        };
        Self {
            code,
            message: error.unwrap_or_else(|| "provider_send_failed".to_owned()),
            retryable,
        }
    }
}

/// `value` as Python's `str()` writes it, when Python reads it true: a
/// string as it is, a bool `True`, a number in Python's form, a list or an
/// object as JSON; `None` for null, `false`, `0`, an empty string, list or
/// object.
fn python_str(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Bool(false) => None,
        Value::Bool(true) => Some("True".to_owned()),
        Value::String(text) => (!text.is_empty()).then(|| text.clone()),
        Value::Number(number) => match (number.as_i64(), number.as_u64(), number.as_f64()) {
            (Some(integer), ..) => (integer != 0).then(|| integer.to_string()),
            (None, Some(integer), _) => Some(integer.to_string()),
            (None, None, float) => float.filter(|float| *float != 0.0).map(python_float),
        },
        Value::Array(items) => (!items.is_empty()).then(|| value.to_string()),
        Value::Object(fields) => (!fields.is_empty()).then(|| value.to_string()),
    }
}

fn retryable_by_default() -> bool {
    true
}

/// What a provider's send returned (RFC §5.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderResult {
    /// Whether the provider accepted the message.
    pub success: bool,
    /// The provider's message id.
    #[serde(default)]
    pub provider_message_id: Option<String>,
    /// The provider's error, when it refused.
    #[serde(default)]
    pub error: Option<String>,
    /// Provider-specific data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl ProviderResult {
    /// A provider's answer, accepted or not, with no message id, error or
    /// data.
    pub fn new(success: bool) -> Self {
        Self {
            success,
            provider_message_id: None,
            error: None,
            metadata: Metadata::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// Python's `retryable=True` default, read and built.
    #[test]
    fn a_delivery_error_is_retryable_by_default() {
        let read: DeliveryError =
            serde_json::from_value(json!({ "code": "timeout", "message": "no answer" })).unwrap();
        assert_eq!(read, DeliveryError::new("timeout", "no answer"));
        assert!(read.retryable);
    }

    /// Python's `ProviderDeliveryError(result)`, its `str()`, `code` and
    /// `retryable` for each result (probe
    /// `tasks/probes/step5-lot2/provider_delivery_error.py`).
    #[test]
    fn a_provider_s_refusal_reads_as_python_s_provider_delivery_error() {
        let result = |error: Option<&str>, metadata: Value| ProviderResult {
            error: error.map(str::to_owned),
            metadata: serde_json::from_value(metadata).unwrap(),
            ..ProviderResult::new(false)
        };
        let read = [
            result(
                Some("rejected"),
                json!({ "code": "E42", "retryable": false }),
            ),
            result(Some("busy"), json!({})),
            result(Some("blocked"), json!({ "code": "30007" })),
            result(None, json!({})),
            result(Some(""), json!({ "code": 30007, "retryable": "no" })),
            result(None, json!({ "code": 0, "retryable": true })),
            result(None, json!({ "code": true })),
            result(None, json!({ "code": 1.5 })),
        ]
        .map(|result| {
            let error = DeliveryError::of_provider(&result);
            (error.message, error.code, error.retryable)
        });
        let python =
            |message: &str, code: &str, retryable| (message.to_owned(), code.to_owned(), retryable);
        assert_eq!(
            read,
            [
                python("rejected", "E42", false),
                python("busy", "busy", true),
                python("blocked", "30007", true),
                python("provider_send_failed", "ProviderDeliveryFailed", true),
                python("provider_send_failed", "30007", true),
                python("provider_send_failed", "ProviderDeliveryFailed", true),
                python("provider_send_failed", "True", true),
                python("provider_send_failed", "1.5", true),
            ]
        );
    }
}
