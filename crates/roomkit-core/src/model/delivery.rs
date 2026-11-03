//! What a delivery to a channel produced (RFC §5.13).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::positive::python_float;
use super::{ChannelId, ErrorRecord, Metadata, Timestamp};
use crate::rules::python_repr as python_repr_str;

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
    /// as the Python implementation's `_delivery_error` reports it: the
    /// `code` its attributes declare, as `str()` writes it, when Python
    /// reads it true, else its type (Python's class name; a boxed Rust
    /// error has none, and reads `ChannelError`); its message; retryable
    /// as Python reads its `retryable` attribute true, retryable when it
    /// has none. Outside the public API.
    #[doc(hidden)]
    pub fn raised(channel_id: ChannelId, error: &ErrorRecord) -> Self {
        let code = error
            .attributes
            .get("code")
            .and_then(python_str)
            .unwrap_or_else(|| error.error_type.clone());
        let retryable = error
            .attributes
            .get("retryable")
            .is_none_or(|retryable| python_str(retryable).is_some());
        Self {
            error: Some(DeliveryError {
                retryable,
                ..DeliveryError::new(code, error.message.clone())
            }),
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
/// string as it is, any other value as [`python_repr`] writes it; `None`
/// for null, `false`, `0`, an empty string, list or object.
fn python_str(value: &Value) -> Option<String> {
    let truthy = match value {
        Value::Null | Value::Bool(false) => false,
        Value::Bool(true) => true,
        Value::String(text) => !text.is_empty(),
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::Array(items) => !items.is_empty(),
        Value::Object(fields) => !fields.is_empty(),
    };
    if !truthy {
        return None;
    }
    Some(match value {
        Value::String(text) => text.clone(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            python_repr(value)
        }
    })
}

/// `value` as Python's `repr()` writes the value JSON reads it as: `None`,
/// `True`, `False`, an integer, a float in Python's form, a string quoted,
/// a list `[1, 2]`, a dict `{'a': 1}`, its keys in the map's order.
fn python_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Number(number) => match (number.as_i64(), number.as_u64(), number.as_f64()) {
            (Some(integer), ..) => integer.to_string(),
            (None, Some(integer), _) => integer.to_string(),
            (None, None, Some(float)) => python_float(float),
            (None, None, None) => number.to_string(),
        },
        Value::String(text) => python_repr_str(text),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(python_repr).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|(key, value)| format!("{}: {}", python_repr_str(key), python_repr(value)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
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
            result(None, json!({ "code": [1, 2] })),
            result(None, json!({ "code": { "a": 1 } })),
            result(
                None,
                json!({ "code": { "b": [1, "it's"], "c": null, "d": true, "e": 1e-5 } }),
            ),
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
                python("provider_send_failed", "[1, 2]", true),
                python("provider_send_failed", "{'a': 1}", true),
                python(
                    "provider_send_failed",
                    "{'b': [1, \"it's\"], 'c': None, 'd': True, 'e': 1e-05}",
                    true
                ),
            ]
        );
    }

    /// Python's `_delivery_error(message, exc)`: `str(exc.code or
    /// type(exc).__name__)` and `bool(getattr(exc, "retryable", True))`,
    /// read off the error's record (RMK-1239).
    #[test]
    fn a_raised_error_s_delivery_reads_its_declared_code_and_retryable() {
        let raised = |attributes: Value| {
            let record = ErrorRecord {
                attributes: Box::new(serde_json::from_value(attributes).unwrap()),
                ..ErrorRecord::new("ProviderError", "refused")
            };
            let error = DeliveryResult::raised(ChannelId::new("ai").unwrap(), &record)
                .error
                .unwrap();
            (error.code, error.retryable)
        };
        let read = [
            raised(json!({})),
            raised(json!({ "retryable": false })),
            raised(json!({ "retryable": null })),
            raised(json!({ "retryable": 0, "code": "E42" })),
            raised(json!({ "retryable": "no", "code": 30007 })),
            raised(json!({ "code": "" })),
            raised(json!({ "code": null })),
        ];
        let read_as = |code: &str, retryable: bool| (code.to_owned(), retryable);
        assert_eq!(
            read,
            [
                read_as("ProviderError", true),
                read_as("ProviderError", false),
                read_as("ProviderError", false),
                read_as("E42", false),
                read_as("30007", true),
                read_as("ProviderError", true),
                read_as("ProviderError", true),
            ]
        );
    }
}
