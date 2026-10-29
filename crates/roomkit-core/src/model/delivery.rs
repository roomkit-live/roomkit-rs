//! What a delivery to a channel produced (RFC §5.13).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, Timestamp};

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
}
