//! What a call's `delivery_results` reports of each target that failed, as
//! the Python implementation's `_delivery_results` reports every target its
//! broadcast's errors name (RFC §5.13): a transport whose `on_event()` or
//! `deliver()` raised, Python's `ProviderDeliveryError` read for its code
//! and retryability, a provider's refusal, a binding with no recipient, and
//! an agent whose turn raised or answered an error.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};

#[path = "store/abstract_only_store.rs"]
mod abstract_only_store;
#[path = "support/warnings.rs"]
mod warnings;

use abstract_only_store::AbstractOnly;
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DeliveryError, DeliveryResult, DeliveryResultStatus, ErrorRecord, InboundMessage, Metadata,
    ProviderResult, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use roomkit_core::ops::ChannelConfig;
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

impl Failing {
    /// The configuration the kit keeps of the channel.
    fn config(&self) -> ChannelConfig {
        let (channel_type, category) = match self.id {
            "raising" | "erring" | "agent_value" | "agent_cancelled" | "agent_kit_closed" => {
                (ChannelType::AI, ChannelCategory::Intelligence)
            }
            "sms_refused" | "sms_no_recipient" => (ChannelType::SMS, ChannelCategory::Transport),
            _ => (ChannelType::WEBSOCKET, ChannelCategory::Transport),
        };
        let websocket = channel_type == ChannelType::WEBSOCKET;
        let mut declaration = ChannelConfig::new(
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
}

impl Channel for Failing {
    fn channel_id(&self) -> ChannelId {
        self.config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.config().category
    }

    fn direction(&self) -> ChannelDirection {
        self.config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.config().supports_streaming_delivery
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
            "agent_value" => Err(RoomKitError::Value("v".to_owned()).into()),
            "agent_kit_closed" => Err(RoomKitError::KitClosed.into()),
            "agent_cancelled" => Err(RoomKitError::Cancelled.into()),
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
            "deliver_kit_closed" => Err(RoomKitError::KitClosed.into()),
            "deliver_room_not_found" => Err(RoomKitError::RoomNotFound(room_id()).into()),
            "deliver_shutdown_cancelled" => Err(RoomKitError::ShutdownCancelled.into()),
            "deliver_internal" => Err(RoomKitError::Internal("a defect".to_owned()).into()),
            "sms_refused" => Ok(ChannelOutput {
                provider_result: Some(refusal(Some("pm-2"), "blocked", json!({ "code": "30007" }))),
                ..ChannelOutput::empty()
            }),
            _ => Ok(ChannelOutput::empty()),
        })
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// The targets of the probe's room, each named for how it fails.
const TARGETS: [&str; 14] = [
    "on_event_refused",
    "on_event_plain",
    "deliver_refused",
    "deliver_kit_closed",
    "deliver_room_not_found",
    "deliver_shutdown_cancelled",
    "deliver_internal",
    "sms_refused",
    "sms_no_recipient",
    "raising",
    "erring",
    "agent_value",
    "agent_cancelled",
    "agent_kit_closed",
];

/// What `delivery_results` holds of each of [`TARGETS`] once a message
/// addressed to the two agents went through the room.
async fn reported() -> (Vec<Option<DeliveryResult>>, Option<Vec<ChannelId>>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
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
    message.addressed_to = Some(vec![
        channel("raising"),
        channel("erring"),
        channel("agent_value"),
        channel("agent_cancelled"),
        channel("agent_kit_closed"),
    ]);
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

/// A failed delivery to `id`, as the Python implementation's probe reads
/// it: its code, message and retryability, the provider's message id and
/// the provider's answer kept.
fn failed(
    id: &str,
    (code, message, retryable): (&str, &str, bool),
    provider_message_id: Option<&str>,
    provider_result: Option<ProviderResult>,
) -> DeliveryResult {
    DeliveryResult {
        channel_id: channel(id),
        status: DeliveryResultStatus::Failed,
        provider_message_id: provider_message_id.map(str::to_owned),
        error: Some(DeliveryError {
            code: code.to_owned(),
            message: message.to_owned(),
            retryable,
        }),
        retry_after: None,
        provider_result,
    }
}

/// The failed targets of the Python implementation's probe
/// (`tasks/probes/step5-lot2/failed_target_results.out` and
/// `unreached_agents.out`), each with what `delivery_results` holds of it,
/// its values Python's: but `ChannelError` where Python names the class of
/// a plain exception, a boxed error having none. A kit error reads its
/// class's code, `kit_closed`, or else its Python class's name, as Python's
/// (`rmk1030-rs-review-B/light/py_delivery_error_code.out`; maintainer's
/// decision Q2, 2026-10-30), a defect Python's assertion,
/// `AssertionError`; on d192534 `RoomNotFoundError` and
/// `ShutdownCancelledError` read `ChannelError`, and on b626fbb a defect
/// did. An agent's kit error reads as a transport's, and a cancelled
/// agent's turn as Python's `TurnLinkError` (RMK-1047,
/// `tasks/probes/rmk1041/py_cancelled_target_38ef0625.out`); on 4a2e822
/// both read `ChannelError`.
#[tokio::test]
async fn each_failed_target_is_reported_as_python_reports_it() {
    let (reported, unavailable) = reported().await;
    let no_recipient = json!({
        "code": "no_recipient",
        "retryable": false,
        "channel_id": "sms_no_recipient",
        "recipient_key": "phone_number",
    });
    let expected = [
        failed(
            "on_event_refused",
            ("E42", "rejected", false),
            Some("pm-1"),
            Some(refusal(
                Some("pm-1"),
                "rejected",
                json!({ "code": "E42", "retryable": false }),
            )),
        ),
        failed(
            "on_event_plain",
            ("ChannelError", "socket gone", true),
            None,
            None,
        ),
        failed(
            "deliver_refused",
            ("busy", "busy", true),
            None,
            Some(refusal(None, "busy", json!({}))),
        ),
        failed(
            "deliver_kit_closed",
            ("kit_closed", &RoomKitError::KitClosed.to_string(), true),
            None,
            None,
        ),
        failed(
            "deliver_room_not_found",
            (
                "RoomNotFoundError",
                &RoomKitError::RoomNotFound(room_id()).to_string(),
                true,
            ),
            None,
            None,
        ),
        failed(
            "deliver_shutdown_cancelled",
            (
                "ShutdownCancelledError",
                &RoomKitError::ShutdownCancelled.to_string(),
                true,
            ),
            None,
            None,
        ),
        failed(
            "deliver_internal",
            (
                "AssertionError",
                &RoomKitError::Internal("a defect".to_owned()).to_string(),
                true,
            ),
            None,
            None,
        ),
        failed(
            "sms_refused",
            ("30007", "blocked", true),
            Some("pm-2"),
            Some(refusal(Some("pm-2"), "blocked", json!({ "code": "30007" }))),
        ),
        failed(
            "sms_no_recipient",
            ("no_recipient", "no_recipient", false),
            None,
            Some(refusal(None, "no_recipient", no_recipient)),
        ),
        failed("raising", ("ChannelError", "turn failed", true), None, None),
        failed("erring", ("RuntimeError", "output error", true), None, None),
        failed("agent_value", ("ValueError", "v", true), None, None),
        failed(
            "agent_cancelled",
            ("TurnLinkError", "The turn ended before its output", true),
            None,
            None,
        ),
        // A kit error crosses under its class, its code kept (RMK-1128).
        failed(
            "agent_kit_closed",
            ("kit_closed", &RoomKitError::KitClosed.to_string(), true),
            None,
            None,
        ),
    ];
    assert_eq!(
        (reported, unavailable),
        (expected.map(Some).to_vec(), Some(Vec::new()))
    );
}

/// A custom store whose `update_event` keeps the stored results, as its
/// contract says, and which inherits the trait's default record cannot record
/// the failure: the kit logs why rather than record nothing, and the call
/// reports its deliveries all the same.
// Python: tests/test_delivery_outcomes.py::TestOnlyAFailingSetIsRemembered::test_a_store_that_cannot_record_says_why
#[tokio::test]
async fn a_store_that_cannot_record_says_why() {
    let (warnings, _capturing) = warnings::Warnings::capture();
    let kit = RoomKit::new(RoomKitOptions::with_store(AbstractOnly::default()));
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(Failing { id: "raising" }).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "raising"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("ws"), "u1", TextContent::new("hi"));
    let routed = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };

    let result = kit.process_inbound(message, routed).await.unwrap();

    let event = result.event.unwrap();
    let stored = kit.store().get_event(&event.id).await.unwrap().unwrap();
    assert_eq!(
        (
            stored.delivery_results,
            result.delivery_results.contains_key("raising")
        ),
        (Metadata::new(), true)
    );
    let said = warnings.containing("Could not record delivery results");
    assert!(
        said.iter()
            .any(|line| line.contains("override record_delivery_results")),
        "{said:?}"
    );
    kit.close().await.unwrap();
}
