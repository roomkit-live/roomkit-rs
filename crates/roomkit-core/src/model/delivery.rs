//! What a delivery to a channel produced (RFC §5.13).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, Timestamp};

/// Where a delivery stands (RFC §5.13).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryStatus {
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
    pub status: DeliveryStatus,
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

/// Why a delivery failed (RFC §5.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryError {
    /// A machine-readable code.
    pub code: String,
    /// A human-readable description.
    pub message: String,
    /// Whether a retry may succeed.
    pub retryable: bool,
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
