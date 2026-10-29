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

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    Access, AgentResponsePolicy, AudioContent, ChannelBinding, ChannelCategory, ChannelDirection,
    ChannelId, ChannelMediaType, ChannelOutput, ChannelType, Confidence, DeleteContent, DeleteType,
    DeliveryError, DeliveryResult, DeliveryResultStatus, EditContent, EditSource, EventContent,
    EventId, EventSource, EventStatus, EventType, HookResult, IdentificationStatus, Identity,
    InboundMessage, InjectedEvent, Latitude, LocationContent, Longitude, MediaContent, Metadata,
    Observation, ObservationId, Participant, ParticipantId, ParticipantRole, ParticipantStatus,
    RichContent, Room, RoomEvent, RoomId, RoomStatus, RoomTimers, SystemContent, Task, TaskId,
    TaskStatus, TemplateContent, TextContent, Timestamp, ToolCallContent, VerificationStatus,
    VideoContent, Visibility,
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
    let mut source = EventSource::new(channel("sms"), ChannelType::SMS);
    source.participant_id = Some(ParticipantId::new("p_jean").unwrap());
    source.verification = Some(VerificationStatus::Unverified);
    source.external_id = Some("ext_9".to_owned());
    source.provider = Some("TwilioSMS".to_owned());
    source.raw_payload = metadata(json!({ "Body": "Bonjour" }));
    source.provider_message_id = Some("SM123".to_owned());

    let mut event = RoomEvent::at(
        EventId::new("evt_2").unwrap(),
        room_id(),
        EventType::Message,
        source,
        TextContent::new("Bonjour"),
        at("2026-10-22T14:03:00Z"),
    );
    event.status = EventStatus::Delivered;
    event.blocked_by = None;
    event.visibility = Visibility::channels([channel("ws_advisor")]).unwrap();
    event.addressed_to = Some(vec![channel("ai")]);
    event.response_visibility = Some(Visibility::Transport);
    event.index = 4;
    event.chain_depth = 1;
    event.parent_event_id = Some(EventId::new("evt_0").unwrap());
    event.responds_to = Some(EventId::new("evt_1").unwrap());
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
        "response_visibility": "transport",
        "index": 4,
        "chain_depth": 1,
        "parent_event_id": "evt_0",
        "responds_to": "evt_1",
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

/// RFC §7.3: an empty visibility that reaches an event other than through
/// a door, a store's among others, reads as `none`, so that it restricts
/// rather than opens; the doors refuse it.
#[test]
fn a_room_event_s_empty_visibilities_read_none() {
    let mut form = full_event_form();
    form["visibility"] = json!("");
    form["response_visibility"] = json!(" ");
    let event: RoomEvent = serde_json::from_value(form).unwrap();
    let message = serde_json::from_value::<InboundMessage>(json!({
        "channel_id": "sms",
        "sender_id": "+15551234567",
        "content": { "type": "text", "body": "Hi" },
        "visibility": "",
    }));
    assert_eq!(
        (
            event.visibility,
            event.response_visibility,
            message.is_err()
        ),
        (Visibility::None, Some(Visibility::None), true)
    );
}

#[test]
fn room() {
    let mut room = Room::at(room_id(), at("2026-10-22T14:00:00Z"));
    room.organization_id = Some("org_1".to_owned());
    room.status = RoomStatus::Paused;
    room.updated_at = at("2026-10-22T14:30:00Z");
    room.paused_at = Some(at("2026-10-22T14:30:00Z"));
    room.timers = RoomTimers {
        inactive_after_seconds: Some(1800),
        closed_after_seconds: Some(86_400),
        last_activity_at: Some(at("2026-10-22T14:03:00Z")),
    };
    room.agent_response_policy = AgentResponsePolicy::AddressedOnly;
    room.metadata = metadata(json!({ "crm": "c_1" }));
    room.event_count = 5;
    room.latest_index = Some(4);
    room.delivered_index = 3;

    assert_json_form(
        &room,
        &json!({
            "id": "room_1",
            "organization_id": "org_1",
            "status": "paused",
            "created_at": "2026-10-22T14:00:00Z",
            "updated_at": "2026-10-22T14:30:00Z",
            "closed_at": null,
            "paused_at": "2026-10-22T14:30:00Z",
            "timers": {
                "inactive_after_seconds": 1800,
                "closed_after_seconds": 86400,
                "last_activity_at": "2026-10-22T14:03:00Z",
            },
            "agent_response_policy": "addressed_only",
            "metadata": { "crm": "c_1" },
            "event_count": 5,
            "latest_index": 4,
            "delivered_index": 3,
        }),
    );
}

#[test]
fn channel_binding() {
    let mut binding = ChannelBinding::at(
        channel("ws_advisor"),
        room_id(),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
        at("2026-10-22T14:00:00Z"),
    );
    binding.access = Access::ReadOnly;
    binding.muted = true;
    binding.visibility = Visibility::Transport;
    binding.participant_id = Some(ParticipantId::new("p_advisor").unwrap());
    binding.last_read_index = Some(3);
    binding.capabilities.media_types = [ChannelMediaType::Text, ChannelMediaType::Rich].into();
    binding.capabilities.max_length = Some(4096);
    binding.capabilities.supports_edit = true;
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
                "max_audio_duration_seconds": null,
                "supported_audio_formats": [],
                "supports_video": false,
                "max_video_duration_seconds": null,
                "supported_video_formats": [],
                "supports_threading": false,
                "supports_typing": false,
                "supports_read_receipts": false,
                "supports_reactions": false,
                "supports_edit": true,
                "supports_delete": false,
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
    let mut participant = Participant::at(
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

/// RFC §5.6: the addresses an identity uses are keyed by channel type.
#[test]
fn identity() {
    let mut identity = Identity {
        organization_id: Some("bank".to_owned()),
        ..Identity::new("id_jean".parse().unwrap())
    };
    identity.display_name = Some("Jean Tremblay".to_owned());
    identity.link(ChannelType::SMS, "+15551234567");
    identity.link(ChannelType::EMAIL, "jean@example.com");
    identity.external_id = Some("crm_42".to_owned());
    identity.email = Some("jean@example.com".to_owned());
    identity.external_ids = [("hubspot".to_owned(), "h_7".to_owned())].into();

    assert_json_form(
        &identity,
        &json!({
            "id": "id_jean",
            "organization_id": "bank",
            "display_name": "Jean Tremblay",
            "email": "jean@example.com",
            "phone": null,
            "channel_addresses": {
                "sms": ["+15551234567"],
                "email": ["jean@example.com"],
            },
            "external_id": "crm_42",
            "external_ids": { "hubspot": "h_7" },
            "metadata": {},
        }),
    );
}

#[test]
fn inbound_message() {
    let mut message =
        InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("Bonjour"));
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
    let mut task = Task::at(
        TaskId::new("task_1").unwrap(),
        room_id(),
        "Call the customer back",
        at("2026-10-22T14:03:00Z"),
    );
    task.status = TaskStatus::InProgress;
    task.assigned_to = Some("advisor_2".to_owned());
    let mut observation = Observation::at(
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
        status: DeliveryResultStatus::Failed,
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
    let output = ChannelOutput {
        responded: true,
        response_events: vec![full_event()],
        ..ChannelOutput::empty()
    };
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

/// RFC §5.3: a content built from its required members is the content its
/// minimal form reads as, every other member at its default: an edit's
/// `edit_source` null, a delete's `delete_type` `SENDER`. Since the
/// contents are `#[non_exhaustive]`, a crate outside the core builds them
/// this way only, and sets an optional member with its `with_*`.
#[test]
fn a_content_s_constructor_reads_as_its_minimal_form() {
    let target = EventId::new("evt_1").unwrap();
    let latitude = Latitude::new(45.5).unwrap();
    let longitude = Longitude::new(-73.6).unwrap();
    let built = [
        EventContent::Text(TextContent::new("Bonjour")),
        EventContent::Rich(RichContent::new("**Bonjour**")),
        EventContent::Media(MediaContent::new("https://example.com/a.png", "image/png").unwrap()),
        EventContent::Location(LocationContent::new(latitude, longitude)),
        EventContent::Audio(AudioContent::new("https://example.com/a.ogg").unwrap()),
        EventContent::Video(VideoContent::new("https://example.com/a.mp4").unwrap()),
        EventContent::System(SystemContent::new("Room paused")),
        EventContent::Template(TemplateContent::new("welcome")),
        EventContent::Edit(EditContent::new(
            target.clone(),
            TextContent::new("Bonsoir"),
        )),
        EventContent::Delete(DeleteContent::new(target.clone())),
        EventContent::ToolCall(ToolCallContent::new("lookup", "call_1")),
        EventContent::Edit(EditContent {
            edit_source: Some(EditSource::System),
            ..EditContent::new(target.clone(), TextContent::new("Bonsoir"))
        }),
        EventContent::Delete(DeleteContent {
            delete_type: DeleteType::Admin,
            ..DeleteContent::new(target)
        }),
    ];
    let minimal = [
        json!({ "type": "text", "body": "Bonjour" }),
        json!({ "type": "rich", "body": "**Bonjour**" }),
        json!({ "type": "media", "url": "https://example.com/a.png", "mime_type": "image/png" }),
        json!({ "type": "location", "latitude": 45.5, "longitude": -73.6 }),
        json!({ "type": "audio", "url": "https://example.com/a.ogg" }),
        json!({ "type": "video", "url": "https://example.com/a.mp4", "mime_type": "video/mp4" }),
        json!({ "type": "system", "body": "Room paused" }),
        json!({ "type": "template", "template_id": "welcome" }),
        json!({
            "type": "edit",
            "target_event_id": "evt_1",
            "new_content": { "type": "text", "body": "Bonsoir" },
        }),
        json!({ "type": "delete", "target_event_id": "evt_1" }),
        json!({ "type": "tool_call", "tool_name": "lookup", "tool_id": "call_1" }),
        json!({
            "type": "edit",
            "target_event_id": "evt_1",
            "new_content": { "type": "text", "body": "Bonsoir" },
            "edit_source": "system",
        }),
        json!({ "type": "delete", "target_event_id": "evt_1", "delete_type": "admin" }),
    ]
    .map(|form| serde_json::from_value::<EventContent>(form).unwrap());
    assert_eq!(built, minimal);
}
