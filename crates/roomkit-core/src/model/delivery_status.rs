//! A provider's report on a message the room sent (RFC §10.4, A delivery
//! status follows its message).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, RoomId, Timestamp};

/// Where a message the room sent stands, as its provider reports it in a
/// webhook: Python's `DeliveryStatus(*, room_id=None, channel_id=None,
/// provider, message_id, status, recipient="", sender="", error_code=None,
/// error_message=None, timestamp=None, raw=<factory>)`.
///
/// `RoomKit::process_delivery_status` hands it to the `ON_DELIVERY_STATUS`
/// hooks of the room it names, or of the room its recipient's binding of
/// `channel_id` names (RFC §10.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeliveryStatus {
    /// The room the message was sent from, when the provider's webhook
    /// carries it; `None` routes the status by its channel and recipient.
    #[serde(default)]
    pub room_id: Option<RoomId>,
    /// The channel the message went out on.
    #[serde(default)]
    pub channel_id: Option<ChannelId>,
    /// The provider's name: `twilio`, `telnyx`...
    pub provider: String,
    /// The provider's id of the message.
    pub message_id: String,
    /// The provider's word for where it stands: `sent`, `delivered`,
    /// `failed`...
    pub status: String,
    /// The address the message went to, as the provider wrote it; `""` when
    /// it names none.
    #[serde(default)]
    pub recipient: String,
    /// The address the message went from, as the provider wrote it.
    #[serde(default)]
    pub sender: String,
    /// The provider's error code, when it failed.
    #[serde(default)]
    pub error_code: Option<String>,
    /// The provider's error message, when it failed.
    #[serde(default)]
    pub error_message: Option<String>,
    /// When the provider reported it.
    #[serde(default)]
    pub timestamp: Option<Timestamp>,
    /// The webhook's payload as the provider sent it.
    #[serde(default)]
    pub raw: Metadata,
}

impl DeliveryStatus {
    /// The status `status` the provider `provider` reports of its message
    /// `message_id`, naming no room, channel, recipient or sender, as
    /// Python's constructor defaults them.
    pub fn new(
        provider: impl Into<String>,
        message_id: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            room_id: None,
            channel_id: None,
            provider: provider.into(),
            message_id: message_id.into(),
            status: status.into(),
            recipient: String::new(),
            sender: String::new(),
            error_code: None,
            error_message: None,
            timestamp: None,
            raw: Metadata::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: the serialized form `DeliveryStatus.model_dump(mode="json")`
    // writes, which a webhook's handler and the conformance runners read.
    #[test]
    fn a_status_reads_and_writes_python_s_serialized_form() {
        let written = serde_json::json!({
            "room_id": null,
            "channel_id": "sms",
            "provider": "mock",
            "message_id": "m",
            "status": "delivered",
            "recipient": "+15550000001",
            "sender": "",
            "error_code": null,
            "error_message": null,
            "timestamp": null,
            "raw": {},
        });
        let status: DeliveryStatus = serde_json::from_value(written.clone()).unwrap();
        assert_eq!(
            status.channel_id.as_ref().map(ChannelId::as_str),
            Some("sms")
        );
        assert_eq!(serde_json::to_value(&status).unwrap(), written);
        let minimal: DeliveryStatus = serde_json::from_value(serde_json::json!({
            "provider": "mock", "message_id": "m", "status": "sent",
        }))
        .unwrap();
        assert_eq!(minimal, DeliveryStatus::new("mock", "m", "sent"));
    }
}
