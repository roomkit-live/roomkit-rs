//! What a call's `delivery_results` reports of each target that failed, as
//! the Python implementation's `_delivery_results` reports every target its
//! broadcast's errors name (RFC §5.13): a transport whose `on_event()` or
//! `deliver()` raised, Python's `ProviderDeliveryError` read for its code
//! and retryability, a provider's refusal, a binding with no recipient, and
//! an agent whose turn raised or answered an error.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DeliveryResult, ErrorRecord, InboundMessage, Metadata, ProviderResult, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use serde_json::{Value, json};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r").unwrap()
}

fn metadata(value: Value) -> Metadata {
    serde_json::from_value(value).unwrap()
}

/// A provider's refusal saying `error`, of message id `id`, declaring
/// `declared`.
fn refusal(id: Option<&str>, error: &str, declared: Value) -> ProviderResult {
    ProviderResult {
        provider_message_id: id.map(str::to_owned),
        error: Some(error.to_owned()),
        metadata: metadata(declared),
        ..ProviderResult::new(false)
    }
}

/// A target of the probe's room, named for how it fails.
struct Failing {
    id: &'static str,
}

impl Channel for Failing {
    fn declaration(&self) -> ChannelDeclaration {
        let (channel_type, category) = match self.id {
            "raising" | "erring" => (ChannelType::AI, ChannelCategory::Intelligence),
            "sms_refused" | "sms_no_recipient" => (ChannelType::SMS, ChannelCategory::Transport),
            _ => (ChannelType::WEBSOCKET, ChannelCategory::Transport),
        };
        let websocket = channel_type == ChannelType::WEBSOCKET;
        let mut declaration = ChannelDeclaration::new(
            channel(self.id),
            channel_type,
            category,
            ChannelDirection::Bidirectional,
        );
        if websocket {
            declaration.recipient_key = None;
        }
        declaration
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(match self.id {
            "on_event_refused" => Err(RoomKitError::ProviderDelivery {
                result: refusal(
                    Some("pm-1"),
                    "rejected",
                    json!({ "code": "E42", "retryable": false }),
                ),
            }
            .into()),
            "on_event_plain" | "raising" => Err(if self.id == "raising" {
                "turn failed".into()
            } else {
                "socket gone".into()
            }),
            "erring" => Ok(ChannelOutput {
                error: Some(ErrorRecord::new("RuntimeError", "output error")),
                ..ChannelOutput::empty()
            }),
            _ => Ok(ChannelOutput::empty()),
        })
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(match self.id {
            "deliver_refused" => Err(RoomKitError::ProviderDelivery {
                result: refusal(None, "busy", json!({})),
            }
            .into()),
            "sms_refused" => Ok(ChannelOutput {
                provider_result: Some(refusal(Some("pm-2"), "blocked", json!({ "code": "30007" }))),
                ..ChannelOutput::empty()
            }),
            _ => Ok(ChannelOutput::empty()),
        })
    }
}

/// The targets of the probe's room, each named for how it fails.
const TARGETS: [&str; 7] = [
    "on_event_refused",
    "on_event_plain",
    "deliver_refused",
    "sms_refused",
    "sms_no_recipient",
    "raising",
    "erring",
];

/// What `delivery_results` holds of each of [`TARGETS`] once a message
/// addressed to the two agents went through the room.
async fn reported() -> (Vec<Option<DeliveryResult>>, Option<Vec<ChannelId>>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    for id in TARGETS {
        kit.register_channel(Failing { id }).unwrap();
    }
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    for id in TARGETS {
        let recipient = if id == "sms_refused" {
            json!({ "phone_number": "+15551230000" })
        } else {
            json!({})
        };
        let options = AttachChannelOptions {
            metadata: metadata(recipient),
            ..Default::default()
        };
        kit.attach_channel(&room_id(), &channel(id), options)
            .await
            .unwrap();
    }
    let mut message = InboundMessage::new(channel("ws"), "u1", TextContent::new("hi"));
    message.addressed_to = Some(vec![channel("raising"), channel("erring")]);
    let routed = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let result = kit.process_inbound(message, routed).await.unwrap();
    kit.close().await.unwrap();
    let reported = TARGETS
        .iter()
        .map(|id| result.delivery_results.get(*id).cloned())
        .collect();
    (reported, result.unavailable_targets)
}

/// The failed targets of the Python implementation's probe
/// (`tasks/probes/step5-lot2/failed_target_results.py` and
/// `unreached_agents.py`), each with what `delivery_results` holds of it:
/// Python's code, message and retryability, but `ChannelError` where Python
/// names the class of a plain exception, a boxed error having none.
#[tokio::test]
async fn each_failed_target_is_reported_as_python_reports_it() {
    let (reported, unavailable) = reported().await;
    let raised = |id: &str, error: &ErrorRecord| DeliveryResult::raised(channel(id), error);
    let refused = |id: &str, result: ProviderResult| DeliveryResult::refused(channel(id), result);
    let no_recipient = json!({
        "code": "no_recipient",
        "retryable": false,
        "channel_id": "sms_no_recipient",
        "recipient_key": "phone_number",
    });
    let expected = [
        refused(
            "on_event_refused",
            refusal(
                Some("pm-1"),
                "rejected",
                json!({ "code": "E42", "retryable": false }),
            ),
        ),
        raised(
            "on_event_plain",
            &ErrorRecord::new("ChannelError", "socket gone"),
        ),
        refused("deliver_refused", refusal(None, "busy", json!({}))),
        refused(
            "sms_refused",
            refusal(Some("pm-2"), "blocked", json!({ "code": "30007" })),
        ),
        refused(
            "sms_no_recipient",
            refusal(None, "no_recipient", no_recipient),
        ),
        raised("raising", &ErrorRecord::new("ChannelError", "turn failed")),
        raised("erring", &ErrorRecord::new("RuntimeError", "output error")),
    ];
    let read = |result: &Option<DeliveryResult>| {
        let error = result.as_ref().and_then(|result| result.error.clone());
        error.map(|error| (error.code, error.message, error.retryable))
    };
    let python = |code: &str, message: &str, retryable| {
        Some((code.to_owned(), message.to_owned(), retryable))
    };
    assert_eq!(
        (
            reported.iter().map(read).collect::<Vec<_>>(),
            reported,
            unavailable
        ),
        (
            vec![
                python("E42", "rejected", false),
                python("ChannelError", "socket gone", true),
                python("busy", "busy", true),
                python("30007", "blocked", true),
                python("no_recipient", "no_recipient", false),
                python("ChannelError", "turn failed", true),
                python("RuntimeError", "output error", true),
            ],
            expected.map(Some).to_vec(),
            Some(Vec::new())
        )
    );
}
