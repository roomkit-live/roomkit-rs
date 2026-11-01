//! What an SMS provider's webhook says, once read: Python's
//! `roomkit.providers.sms`, its `WebhookMeta`. Reading a provider's payload
//! into it (`extract_sms_meta`) is the provider's, not ported yet.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::RoomKitError;
use crate::model::{
    ChannelId, CompositeContent, DeliveryStatus, EventContent, InboundMessage, MediaContent,
    Metadata, TextContent, Timestamp,
};

/// The MIME type of an attachment its provider names none for.
const UNKNOWN_MIME_TYPE: &str = "application/octet-stream";

/// The event types of an outbound message's delivery status, as Telnyx
/// names them.
const STATUS_EVENTS: [&str; 4] = [
    "message.sent",
    "message.delivered",
    "message.failed",
    "message.finalized",
];

/// What an SMS provider's webhook says, normalized: Python's
/// `WebhookMeta(provider, sender, recipient, body, external_id, timestamp,
/// raw, media_urls=<factory>, direction=None, event_type=None)`. A message
/// received ([`is_inbound`](Self::is_inbound)) becomes an
/// [`InboundMessage`], an outbound message's delivery status
/// ([`is_status`](Self::is_status)) a [`DeliveryStatus`]:
/// [`RoomKit::process_webhook`](crate::RoomKit::process_webhook) tells them
/// apart.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WebhookMeta {
    /// The provider's name: `telnyx`, `twilio`...
    pub provider: String,
    /// The number that sent the message.
    pub sender: String,
    /// The number that received it.
    pub recipient: String,
    /// Its text.
    pub body: String,
    /// The provider's id of the message.
    pub external_id: Option<String>,
    /// When it was received, if the provider says.
    pub timestamp: Option<Timestamp>,
    /// The webhook's payload as the provider sent it.
    pub raw: Metadata,
    /// Its attachments, each a `url` and a `mime_type`.
    #[serde(default)]
    pub media_urls: Vec<BTreeMap<String, Option<String>>>,
    /// `inbound` or `outbound`, when the provider says.
    #[serde(default)]
    pub direction: Option<String>,
    /// The webhook's event type (`message.received`, `message.delivered`),
    /// when the provider has one.
    #[serde(default)]
    pub event_type: Option<String>,
}

impl WebhookMeta {
    /// What a webhook of `provider` says of a message from `sender` to
    /// `recipient`, with no attachment, direction or event type.
    pub fn new(
        provider: impl Into<String>,
        sender: impl Into<String>,
        recipient: impl Into<String>,
        body: impl Into<String>,
        external_id: Option<String>,
        timestamp: Option<Timestamp>,
        raw: Metadata,
    ) -> Self {
        Self {
            provider: provider.into(),
            sender: sender.into(),
            recipient: recipient.into(),
            body: body.into(),
            external_id,
            timestamp,
            raw,
            media_urls: Vec::new(),
            direction: None,
            event_type: None,
        }
    }

    /// Whether the webhook is a message received: its direction says
    /// `inbound`, or its event type `message.received`; not when either
    /// says it went out (`outbound`, `message.sent`, `message.finalized`).
    /// A webhook that says neither is one, as older payloads carry
    /// neither.
    pub fn is_inbound(&self) -> bool {
        match self.direction.as_deref() {
            Some("outbound") => return false,
            Some("inbound") => return true,
            _ => {}
        }
        !matches!(
            self.event_type.as_deref(),
            Some("message.sent" | "message.finalized")
        )
    }

    /// Whether the webhook is an outbound message's delivery status: its
    /// direction `outbound` and its event type one of `message.sent`,
    /// `message.delivered`, `message.failed`, `message.finalized`.
    pub fn is_status(&self) -> bool {
        self.direction.as_deref() == Some("outbound")
            && self
                .event_type
                .as_deref()
                .is_some_and(|event_type| STATUS_EVENTS.contains(&event_type))
    }

    /// The delivery status the webhook reports: its event type without its
    /// `message.` prefix (`unknown` for none), and the first error its
    /// payload lists under `data.payload.errors`, its `code` and its
    /// `detail`.
    pub fn to_status(&self) -> DeliveryStatus {
        let status = self
            .event_type
            .as_deref()
            .unwrap_or("unknown")
            .replace("message.", "");
        let error = first_error(&self.raw);
        let field = |key| error.map(|error| python_str(error.get(key)));
        DeliveryStatus {
            recipient: self.recipient.clone(),
            sender: self.sender.clone(),
            error_code: field("code"),
            error_message: field("detail"),
            timestamp: self.timestamp,
            raw: self.raw.clone(),
            ..DeliveryStatus::new(
                self.provider.clone(),
                self.external_id.clone().unwrap_or_default(),
                status,
            )
        }
    }

    /// The message the webhook received, on the channel `channel_id`: from
    /// its sender, keyed by its provider's id, which is also its external
    /// id, its metadata the provider, the recipient and the timestamp.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`], Python's `ValueError`, for a webhook that is
    /// not a message received ([`is_inbound`](Self::is_inbound)), and for
    /// an attachment its content refuses.
    pub fn to_inbound(&self, channel_id: &ChannelId) -> Result<InboundMessage, RoomKitError> {
        if !self.is_inbound() {
            return Err(RoomKitError::Value(format!(
                "Cannot convert outbound webhook to InboundMessage (provider={}, \
                 direction={}, event_type={}). This is likely a delivery status webhook, not \
                 an inbound message.",
                self.provider,
                python_option(self.direction.as_deref()),
                python_option(self.event_type.as_deref()),
            )));
        }
        let mut metadata = Metadata::new();
        metadata.insert("provider".to_owned(), Value::from(self.provider.clone()));
        metadata.insert("recipient".to_owned(), Value::from(self.recipient.clone()));
        let timestamp = self.timestamp.map(Timestamp::to_iso_offset);
        metadata.insert("timestamp".to_owned(), Value::from(timestamp));
        Ok(InboundMessage {
            external_id: self.external_id.clone(),
            idempotency_key: self.external_id.clone(),
            metadata,
            ..InboundMessage::new(
                channel_id.clone(),
                self.sender.clone(),
                inbound_content(&self.body, &self.media_urls)?,
            )
        })
    }
}

/// The content of a message of text `body` with the attachments `media`,
/// as Python's `build_inbound_content` builds it: the text alone; one
/// attachment, captioned by the text when there is one; or the text and
/// the attachments as parts. An attachment with no `url` is left out.
fn inbound_content(
    body: &str,
    media: &[BTreeMap<String, Option<String>>],
) -> Result<EventContent, RoomKitError> {
    let mut parts = media
        .iter()
        .filter_map(|attachment| {
            let url = attachment.get("url").cloned().flatten()?;
            let mime_type = attachment
                .get("mime_type")
                .cloned()
                .flatten()
                .filter(|mime_type| !mime_type.is_empty())
                .unwrap_or_else(|| UNKNOWN_MIME_TYPE.to_owned());
            (!url.is_empty()).then(|| MediaContent::new(url, mime_type))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(match parts.len() {
        0 => TextContent::new(body).into(),
        1 => {
            let mut only = parts.remove(0);
            if !body.is_empty() {
                only.caption = Some(body.to_owned());
            }
            only.into()
        }
        _ => {
            let text = (!body.is_empty()).then(|| TextContent::new(body).into());
            let parts = text
                .into_iter()
                .chain(parts.into_iter().map(EventContent::from))
                .collect();
            CompositeContent::new(parts)?.into()
        }
    })
}

/// The first error a provider's payload lists under `data.payload.errors`.
fn first_error(raw: &Metadata) -> Option<&serde_json::Map<String, Value>> {
    raw.get("data")?
        .get("payload")?
        .get("errors")?
        .as_array()?
        .first()?
        .as_object()
}

/// `value` as Python's `str()` writes what `.get(key, "")` read: `""` for
/// an absent member, a string as it is, `None`, `True` and `False`, a
/// number in its digits.
fn python_str(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) => "None".to_owned(),
        Some(Value::Bool(true)) => "True".to_owned(),
        Some(Value::Bool(false)) => "False".to_owned(),
        Some(other) => other.to_string(),
    }
}

/// An optional text as Python's f-string writes it: `None` for none.
fn python_option(value: Option<&str>) -> &str {
    value.unwrap_or("None")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn meta(body: &str) -> WebhookMeta {
        WebhookMeta::new(
            "twilio",
            "+15551234567",
            "+15559876543",
            body,
            Some("SM123".to_owned()),
            None,
            Metadata::new(),
        )
    }

    fn attachment(url: &str, mime_type: Option<&str>) -> BTreeMap<String, Option<String>> {
        BTreeMap::from([
            ("url".to_owned(), Some(url.to_owned())),
            ("mime_type".to_owned(), mime_type.map(str::to_owned)),
        ])
    }

    fn sms() -> ChannelId {
        ChannelId::new("sms-channel").unwrap()
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound
    #[test]
    fn to_inbound() {
        let timestamp: Timestamp = "2026-01-28T12:00:00Z".parse().unwrap();
        let meta = WebhookMeta {
            timestamp: Some(timestamp),
            ..meta("Hello")
        };
        let inbound = meta.to_inbound(&sms()).unwrap();
        assert_eq!(inbound.channel_id, sms());
        assert_eq!(inbound.sender_id, "+15551234567");
        assert_eq!(inbound.content, TextContent::new("Hello").into());
        assert_eq!(inbound.external_id.as_deref(), Some("SM123"));
        assert_eq!(inbound.idempotency_key.as_deref(), Some("SM123"));
        assert_eq!(inbound.metadata["provider"], "twilio");
        assert_eq!(inbound.metadata["recipient"], "+15559876543");
        assert_eq!(inbound.metadata["timestamp"], "2026-01-28T12:00:00+00:00");
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound_no_timestamp
    #[test]
    fn to_inbound_no_timestamp() {
        let meta = WebhookMeta {
            external_id: None,
            ..meta("Hi")
        };
        let inbound = meta.to_inbound(&sms()).unwrap();
        assert_eq!(inbound.metadata["timestamp"], Value::Null);
        assert_eq!(inbound.external_id, None);
        assert_eq!(inbound.idempotency_key, None);
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound_with_media
    #[test]
    fn to_inbound_with_media() {
        let meta = WebhookMeta {
            media_urls: vec![attachment(
                "https://example.com/img.jpg",
                Some("image/jpeg"),
            )],
            ..meta("Check this")
        };
        let EventContent::Media(media) = meta.to_inbound(&sms()).unwrap().content else {
            panic!("a media content");
        };
        assert_eq!(media.url, "https://example.com/img.jpg");
        assert_eq!(media.mime_type, "image/jpeg");
        assert_eq!(media.caption.as_deref(), Some("Check this"));
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound_media_only
    #[test]
    fn to_inbound_media_only() {
        let meta = WebhookMeta {
            media_urls: vec![attachment(
                "https://example.com/img.jpg",
                Some("image/jpeg"),
            )],
            ..meta("")
        };
        let EventContent::Media(media) = meta.to_inbound(&sms()).unwrap().content else {
            panic!("a media content");
        };
        assert_eq!(media.caption, None);
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound_multiple_media
    #[test]
    fn to_inbound_multiple_media() {
        let meta = WebhookMeta {
            media_urls: vec![
                attachment("https://example.com/1.jpg", Some("image/jpeg")),
                attachment("https://example.com/2.png", None),
                BTreeMap::from([("mime_type".to_owned(), Some("image/gif".to_owned()))]),
            ],
            ..meta("Photos")
        };
        let EventContent::Composite(composite) = meta.to_inbound(&sms()).unwrap().content else {
            panic!("a composite content");
        };
        let parts = composite.parts();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], TextContent::new("Photos").into());
        let EventContent::Media(second) = &parts[2] else {
            panic!("a media part");
        };
        assert_eq!(second.mime_type, UNKNOWN_MIME_TYPE);
    }

    // Python: tests/test_providers/test_sms_utils.py::TestWebhookMetaDataclass::test_to_inbound_rejects_outbound
    #[test]
    fn to_inbound_rejects_outbound() {
        let meta = WebhookMeta {
            direction: Some("outbound".to_owned()),
            event_type: Some("message.sent".to_owned()),
            ..meta("Hi")
        };
        let Err(RoomKitError::Value(message)) = meta.to_inbound(&sms()) else {
            panic!("a ValueError");
        };
        assert!(message.contains("Cannot convert outbound webhook"));
    }

    // Python: src/roomkit/providers/sms/meta.py WebhookMeta.is_inbound and
    // is_status, which test_shared_channel_routing's webhook reads.
    #[test]
    fn a_webhook_is_a_message_received_or_an_outbound_status() {
        let with = |direction: Option<&str>, event_type: Option<&str>| WebhookMeta {
            direction: direction.map(str::to_owned),
            event_type: event_type.map(str::to_owned),
            ..meta("")
        };
        assert!(with(None, None).is_inbound());
        assert!(with(Some("inbound"), Some("message.sent")).is_inbound());
        assert!(!with(None, Some("message.finalized")).is_inbound());
        assert!(with(None, Some("message.received")).is_inbound());
        assert!(!with(Some("outbound"), None).is_inbound());
        assert!(with(Some("outbound"), Some("message.delivered")).is_status());
        assert!(!with(Some("outbound"), Some("message.queued")).is_status());
        assert!(!with(None, Some("message.delivered")).is_status());
    }

    // Python: src/roomkit/providers/sms/meta.py WebhookMeta.to_status.
    #[test]
    fn a_status_reads_its_event_type_and_its_first_error() {
        let failed = WebhookMeta {
            direction: Some("outbound".to_owned()),
            event_type: Some("message.failed".to_owned()),
            raw: serde_json::from_value(json!({
                "data": {"payload": {"errors": [{"code": 40001, "detail": "Not reachable"}]}}
            }))
            .unwrap(),
            ..meta("")
        };
        let status = failed.to_status();
        assert_eq!(status.status, "failed");
        assert_eq!(status.message_id, "SM123");
        assert_eq!(status.provider, "twilio");
        assert_eq!(status.recipient, "+15559876543");
        assert_eq!(status.error_code.as_deref(), Some("40001"));
        assert_eq!(status.error_message.as_deref(), Some("Not reachable"));
        let unknown = WebhookMeta {
            event_type: None,
            external_id: None,
            ..meta("")
        }
        .to_status();
        assert_eq!(
            (unknown.status.as_str(), unknown.message_id.as_str()),
            ("unknown", "")
        );
        assert_eq!(unknown.error_code, None);
    }
}
