//! The JSON form of the model's records (docs/architecture.md, gap G-001).
//!
//! Each test writes a record's documented form in full, every field present,
//! and checks it both ways: the record serializes to it, and it parses back
//! to the record. A change to the form fails here first.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "the fixture helpers fail the test on a malformed fixture"
)]

use std::collections::BTreeMap;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    Access, AgentResponsePolicy, ChannelBinding, ChannelCapabilities, ChannelCategory,
    ChannelDirection, ChannelId, ChannelMediaType, ChannelOutput, ChannelType, Confidence,
    Consumption, ConsumptionState, DeliveryError, DeliveryResult, DeliveryStatus, EventContent,
    EventDirection, EventId, EventSource, EventStatus, EventType, HookResult, IdentificationStatus,
    InboundMessage, InjectedEvent, Metadata, Observation, ObservationId, Participant,
    ParticipantId, ParticipantRole, ParticipantStatus, Room, RoomEvent, RoomId, RoomStatus,
    RoomTimers, Task, TaskId, TaskStatus, Timestamp, VerificationStatus, Visibility,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

fn assert_json_form<T>(record: &T, form: &Value)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        &serde_json::to_value(record).unwrap(),
        form,
        "serialized form"
    );
    assert_eq!(
        &serde_json::from_value::<T>(form.clone()).unwrap(),
        record,
        "parsed form"
    );
}

fn at(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn metadata(value: Value) -> Metadata {
    let Value::Object(map) = value else {
        panic!("metadata must be an object, got {value}");
    };
    map
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn full_event() -> RoomEvent {
    let mut source = EventSource::new(channel("sms"), ChannelType::Sms, EventDirection::Inbound);
    source.participant_id = Some(ParticipantId::new("p_jean").unwrap());
    source.verification = Some(VerificationStatus::Unverified);
    source.external_id = Some("ext_9".to_owned());
    source.provider = Some("TwilioSMS".to_owned());
    source.raw_payload = metadata(json!({ "Body": "Bonjour" }));
    source.provider_message_id = Some("SM123".to_owned());

    let mut event = RoomEvent::new(
        EventId::new("evt_2").unwrap(),
        room_id(),
        EventType::Message,
        source,
        EventContent::text("Bonjour"),
        at("2026-10-22T14:03:00Z"),
    );
    event.status = EventStatus::Delivered;
    event.blocked_by = None;
    event.visibility = Visibility::channels([channel("ws_advisor")]).unwrap();
    event.addressed_to = Some(vec![channel("ai")]);
    event.index = 4;
    event.chain_depth = 1;
    event.parent_event_id = Some(EventId::new("evt_0").unwrap());
    event.responds_to = Some(EventId::new("evt_1").unwrap());
    event.consumption = BTreeMap::from([(
        channel("ws_advisor"),
        Consumption {
            state: ConsumptionState::Consumed,
            at: at("2026-10-22T14:03:05Z"),
            detail: Metadata::new(),
            participants: BTreeMap::new(),
        },
    )]);
    event.correlation_id = Some("crm_77".to_owned());
    event.idempotency_key = Some("msg_SM123".to_owned());
    event.metadata = metadata(json!({ "edited": false }));
    event.channel_data = metadata(json!({ "segments": 1 }));
    event.delivery_results = Metadata::new();
    event
}

fn full_event_form() -> Value {
    json!({
        "id": "evt_2",
        "room_id": "room_1",
        "type": "message",
        "source": {
            "channel_id": "sms",
            "channel_type": "sms",
            "direction": "inbound",
            "participant_id": "p_jean",
            "verification": "unverified",
            "external_id": "ext_9",
            "provider": "TwilioSMS",
            "raw_payload": { "Body": "Bonjour" },
            "provider_message_id": "SM123",
        },
        "content": { "type": "text", "body": "Bonjour", "language": null },
        "status": "delivered",
        "blocked_by": null,
        "visibility": "ws_advisor",
        "addressed_to": ["ai"],
        "index": 4,
        "chain_depth": 1,
        "parent_event_id": "evt_0",
        "responds_to": "evt_1",
        "consumption": {
            "ws_advisor": {
                "state": "consumed",
                "at": "2026-10-22T14:03:05Z",
                "detail": {},
                "participants": {},
            },
        },
        "correlation_id": "crm_77",
        "idempotency_key": "msg_SM123",
        "created_at": "2026-10-22T14:03:00Z",
        "metadata": { "edited": false },
        "channel_data": { "segments": 1 },
        "delivery_results": {},
    })
}

#[test]
fn room_event() {
    assert_json_form(&full_event(), &full_event_form());
}

#[test]
fn room() {
    let mut room = Room::new(room_id(), at("2026-10-22T14:00:00Z"));
    room.organization_id = Some("org_1".to_owned());
    room.status = RoomStatus::Paused;
    room.updated_at = at("2026-10-22T14:30:00Z");
    room.timers = RoomTimers {
        inactive_after_seconds: Some(1800),
        closed_after_seconds: Some(86_400),
        last_activity_at: Some(at("2026-10-22T14:03:00Z")),
    };
    room.agent_response_policy = AgentResponsePolicy::AddressedOnly;
    room.metadata = metadata(json!({ "crm": "c_1" }));
    room.event_count = 5;
    room.latest_index = Some(4);

    assert_json_form(
        &room,
        &json!({
            "id": "room_1",
            "organization_id": "org_1",
            "status": "paused",
            "created_at": "2026-10-22T14:00:00Z",
            "updated_at": "2026-10-22T14:30:00Z",
            "closed_at": null,
            "timers": {
                "inactive_after_seconds": 1800,
                "closed_after_seconds": 86400,
                "last_activity_at": "2026-10-22T14:03:00Z",
            },
            "agent_response_policy": "addressed_only",
            "metadata": { "crm": "c_1" },
            "event_count": 5,
            "latest_index": 4,
        }),
    );
}

#[test]
fn channel_binding() {
    let mut binding = ChannelBinding::new(
        channel("ws_advisor"),
        room_id(),
        ChannelType::Websocket,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
        at("2026-10-22T14:00:00Z"),
    );
    binding.access = Access::ReadOnly;
    binding.muted = true;
    binding.visibility = Visibility::Transport;
    binding.participant_id = Some(ParticipantId::new("p_advisor").unwrap());
    binding.last_read_index = Some(3);
    binding.capabilities = ChannelCapabilities {
        media_types: [ChannelMediaType::Text, ChannelMediaType::Rich].into(),
        max_length: Some(4096),
        supports_edit: true,
        ..ChannelCapabilities::default()
    };
    binding.metadata = metadata(json!({ "persona": "formal" }));

    assert_json_form(
        &binding,
        &json!({
            "channel_id": "ws_advisor",
            "room_id": "room_1",
            "channel_type": "websocket",
            "category": "transport",
            "direction": "bidirectional",
            "access": "read_only",
            "muted": true,
            "output_muted": false,
            "visibility": "transport",
            "participant_id": "p_advisor",
            "group": false,
            "last_read_index": 3,
            "attached_at": "2026-10-22T14:00:00Z",
            "capabilities": {
                "media_types": ["text", "rich"],
                "max_length": 4096,
                "supports_rich_text": false,
                "supports_buttons": false,
                "max_buttons": null,
                "supports_cards": false,
                "supports_quick_replies": false,
                "supports_templates": false,
                "supports_media": false,
                "supported_media_types": [],
                "max_media_size_bytes": null,
                "supports_audio": false,
                "supports_video": false,
                "supports_threading": false,
                "supports_typing": false,
                "supports_read_receipts": false,
                "supports_reactions": false,
                "supports_edit": true,
                "supports_delete": false,
                "rate_limit": null,
                "custom": {},
            },
            "rate_limit": null,
            "retry_policy": null,
            "metadata": { "persona": "formal" },
        }),
    );
}

#[test]
fn participant() {
    let mut participant = Participant::new(
        ParticipantId::new("p_jean").unwrap(),
        room_id(),
        channel("sms"),
        at("2026-10-22T14:00:00Z"),
    );
    participant.display_name = Some("Jean Tremblay".to_owned());
    participant.role = ParticipantRole::Owner;
    participant.status = ParticipantStatus::Active;
    participant.identification = IdentificationStatus::Identified;
    participant.identity_id = Some("id_jean".parse().unwrap());
    participant.resolved_by = Some("auto".to_owned());

    assert_json_form(
        &participant,
        &json!({
            "id": "p_jean",
            "room_id": "room_1",
            "channel_id": "sms",
            "display_name": "Jean Tremblay",
            "role": "owner",
            "status": "active",
            "identification": "identified",
            "identity_id": "id_jean",
            "candidates": null,
            "connected_via": ["sms"],
            "external_id": null,
            "resolved_at": null,
            "resolved_by": "auto",
            "joined_at": "2026-10-22T14:00:00Z",
            "metadata": {},
        }),
    );
}

#[test]
fn inbound_message() {
    let mut message = InboundMessage::new(
        channel("sms"),
        "+15551234567",
        EventContent::text("Bonjour"),
    );
    message.idempotency_key = Some("msg_SM123".to_owned());
    message.addressed_to = Some(vec![channel("ai")]);
    message.response_visibility = Some(Visibility::Transport);

    assert_json_form(
        &message,
        &json!({
            "channel_id": "sms",
            "sender_id": "+15551234567",
            "content": { "type": "text", "body": "Bonjour", "language": null },
            "event_type": "message",
            "external_id": null,
            "thread_id": null,
            "parent_event_id": null,
            "idempotency_key": "msg_SM123",
            "raw_payload": {},
            "provider_message_id": null,
            "metadata": {},
            "visibility": "all",
            "addressed_to": ["ai"],
            "response_visibility": "transport",
            "chain_depth": 0,
            "standalone": false,
            "delivery_key": null,
        }),
    );
}

#[test]
fn task_and_observation() {
    let mut task = Task::new(
        TaskId::new("task_1").unwrap(),
        room_id(),
        "Call the customer back",
        at("2026-10-22T14:03:00Z"),
    );
    task.status = TaskStatus::InProgress;
    task.assigned_to = Some("advisor_2".to_owned());
    let mut observation = Observation::new(
        ObservationId::new("obs_1").unwrap(),
        room_id(),
        channel("pii_scanner"),
        "SIN sent by SMS",
        at("2026-10-22T14:03:00Z"),
    );
    observation.category = Some("compliance_violation".to_owned());
    observation.confidence = Confidence::new(0.9).unwrap();

    assert_json_form(
        &(task, observation),
        &json!([
            {
                "id": "task_1",
                "room_id": "room_1",
                "title": "Call the customer back",
                "description": null,
                "assigned_to": "advisor_2",
                "status": "in_progress",
                "created_at": "2026-10-22T14:03:00Z",
                "metadata": {},
            },
            {
                "id": "obs_1",
                "room_id": "room_1",
                "channel_id": "pii_scanner",
                "content": "SIN sent by SMS",
                "category": "compliance_violation",
                "confidence": 0.9,
                "created_at": "2026-10-22T14:03:00Z",
                "metadata": {},
            },
        ]),
    );
}

#[test]
fn delivery_result() {
    let result = DeliveryResult {
        channel_id: channel("sms"),
        status: DeliveryStatus::Failed,
        provider_message_id: None,
        error: Some(DeliveryError {
            code: "no_recipient".to_owned(),
            message: "the binding names no recipient".to_owned(),
            retryable: false,
        }),
        retry_after: None,
        provider_result: None,
    };

    assert_json_form(
        &result,
        &json!({
            "channel_id": "sms",
            "status": "failed",
            "provider_message_id": null,
            "error": {
                "code": "no_recipient",
                "message": "the binding names no recipient",
                "retryable": false,
            },
            "retry_after": null,
            "provider_result": null,
        }),
    );
}

#[test]
fn channel_output() {
    let output = ChannelOutput::respond(vec![full_event()]);
    let mut form = json!({
        "responded": true,
        "response_events": [],
        "response_metadata": {},
        "provider_result": null,
        "tasks": [],
        "observations": [],
        "metadata_updates": {},
        "error": null,
    });
    form["response_events"] = json!([full_event_form()]);

    assert_json_form(&output, &form);
}

#[test]
fn hook_result_blocking_with_an_injected_event() {
    let result: HookResult = HookResult::block("SIN detected").inject(InjectedEvent::to_channels(
        full_event(),
        vec![channel("sms")],
    ));
    let form = json!({
        "action": "block",
        "event": null,
        "reason": "SIN detected",
        "injected_events": [
            { "event": full_event_form(), "target_channel_ids": ["sms"] },
        ],
        "tasks": [],
        "observations": [],
        "metadata": {},
    });

    assert_json_form(&result, &form);
}
