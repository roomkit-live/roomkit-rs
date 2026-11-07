//! Python's enum values on the wire (step 5, lot 6): rustdoc shows no serde
//! rename, so each Rust variant a Python member maps to serializes to that
//! member's value in the pinned snapshot (`api/python`, roomkit's
//! `tests/public_api_signatures.json`). `Addressing`, which never crosses
//! the wire, has no value to compare.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use serde::Serialize;
use serde_json::Value;

use roomkit::HookExecution;
use roomkit::HookTrigger;
use roomkit::model::Access;
use roomkit::model::AgentResponsePolicy;
use roomkit::model::ChannelCategory;
use roomkit::model::ChannelDirection;
use roomkit::model::ChannelMediaType;
use roomkit::model::ChannelType;
use roomkit::model::DeleteType;
use roomkit::model::EditSource;
use roomkit::model::EventDirection;
use roomkit::model::EventStatus;
use roomkit::model::EventType;
use roomkit::model::IdentificationStatus;
use roomkit::model::ParticipantRole;
use roomkit::model::ParticipantStatus;
use roomkit::model::RoomStatus;
use roomkit::model::TaskStatus;
use roomkit::model::VerificationRequestStatus;
use roomkit::model::VerificationStatus;
use roomkit::model::Visibility;

/// The snapshot of Python's public API this one mirrors.
const SNAPSHOT: &str = include_str!("../../../api/python/public_api_signatures.json");

/// That each of `cases`, a Python member of `class` and the Rust value it
/// maps to, serializes to the member's value.
fn serializes_to<T: Serialize>(class: &str, cases: &[(&str, T)]) {
    let snapshot: Value = serde_json::from_str(SNAPSHOT).unwrap();
    for (member, rust) in cases {
        let spec = snapshot
            .get(class)
            .and_then(|members| members.get(*member))
            .unwrap();
        let python = spec
            .get(1)
            .and_then(Value::as_str)
            .unwrap()
            .trim_matches('\'');
        assert_eq!(
            (
                spec.get(0).and_then(Value::as_str),
                serde_json::to_value(rust).unwrap()
            ),
            (Some("member"), Value::String(python.to_owned())),
            "{class}.{member}"
        );
    }
}

/// `roomkit.HookExecution`'s members.
#[test]
fn hook_execution_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.HookExecution",
        &[
            ("SYNC", HookExecution::Sync),
            ("ASYNC", HookExecution::Async),
        ],
    );
}

/// `roomkit.HookTrigger`'s members.
#[test]
fn hook_trigger_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.HookTrigger",
        &[
            ("BEFORE_BROADCAST", HookTrigger::BeforeBroadcast),
            ("AFTER_BROADCAST", HookTrigger::AfterBroadcast),
            ("ON_EVENT_UPDATED", HookTrigger::OnEventUpdated),
            ("ON_EVENT_DELETED", HookTrigger::OnEventDeleted),
            ("ON_ROOM_CREATED", HookTrigger::OnRoomCreated),
            ("ON_ROOM_PAUSED", HookTrigger::OnRoomPaused),
            ("ON_ROOM_CLOSED", HookTrigger::OnRoomClosed),
            ("ON_CHANNEL_ATTACHED", HookTrigger::OnChannelAttached),
            ("ON_CHANNEL_DETACHED", HookTrigger::OnChannelDetached),
            ("ON_CHANNEL_MUTED", HookTrigger::OnChannelMuted),
            ("ON_CHANNEL_UNMUTED", HookTrigger::OnChannelUnmuted),
            ("ON_SESSION_STARTED", HookTrigger::OnSessionStarted),
            ("ON_ERROR", HookTrigger::OnError),
            ("ON_TASK_CREATED", HookTrigger::OnTaskCreated),
            ("ON_TRANSCRIPTION", HookTrigger::OnTranscription),
            ("BEFORE_TTS", HookTrigger::BeforeTts),
            ("BEFORE_TOOL_USE", HookTrigger::BeforeToolUse),
            ("ON_TOOL_CALL", HookTrigger::OnToolCall),
            ("ON_DELIVERY_STATUS", HookTrigger::OnDeliveryStatus),
            ("ON_STATUS_POSTED", HookTrigger::OnStatusPosted),
            ("ON_FEEDBACK", HookTrigger::OnFeedback),
            ("BEFORE_DELIVER", HookTrigger::BeforeDeliver),
            ("AFTER_DELIVER", HookTrigger::AfterDeliver),
            ("ON_TASK_DELEGATED", HookTrigger::OnTaskDelegated),
            ("ON_TASK_COMPLETED", HookTrigger::OnTaskCompleted),
            ("ON_HANDOFF", HookTrigger::OnHandoff),
            ("ON_HANDOFF_REJECTED", HookTrigger::OnHandoffRejected),
            ("ON_PHASE_TRANSITION", HookTrigger::OnPhaseTransition),
            ("ON_USER_INPUT_REQUIRED", HookTrigger::OnUserInputRequired),
            (
                "ON_VERIFICATION_COMPLETED",
                HookTrigger::OnVerificationCompleted,
            ),
            ("ON_VERIFICATION_ENDED", HookTrigger::OnVerificationEnded),
            ("AFTER_TOOL_ROUND", HookTrigger::AfterToolRound),
            ("BEFORE_AI_GENERATION", HookTrigger::BeforeAiGeneration),
            ("ON_AI_RESPONSE", HookTrigger::OnAiResponse),
            (
                "ON_VERIFICATION_REQUIRED",
                HookTrigger::OnVerificationRequired,
            ),
        ],
    );
}

/// `roomkit.orchestration.StatusLevel`'s members.
// Python: tests/orchestration/test_status_bus.py::test_status_level_values
#[test]
fn status_level_serializes_to_python_s_values() {
    use roomkit::orchestration::StatusLevel;
    serializes_to(
        "roomkit.orchestration.StatusLevel",
        &[
            ("OK", StatusLevel::Ok),
            ("FAILED", StatusLevel::Failed),
            ("PENDING", StatusLevel::Pending),
            ("INFO", StatusLevel::Info),
            ("COMPLETED", StatusLevel::Completed),
        ],
    );
}

/// `roomkit.PendingInputStatus`'s members.
#[test]
fn pending_input_status_serializes_to_python_s_values() {
    use roomkit::human_input::PendingInputStatus;
    serializes_to(
        "roomkit.PendingInputStatus",
        &[
            ("PENDING", PendingInputStatus::Pending),
            ("RESOLVED", PendingInputStatus::Resolved),
            ("REJECTED", PendingInputStatus::Rejected),
            ("TIMED_OUT", PendingInputStatus::TimedOut),
        ],
    );
}

/// `roomkit.DeliveryItemStatus`'s members.
#[test]
fn delivery_item_status_serializes_to_python_s_values() {
    use roomkit::DeliveryItemStatus;
    serializes_to(
        "roomkit.DeliveryItemStatus",
        &[
            ("PENDING", DeliveryItemStatus::Pending),
            ("IN_PROGRESS", DeliveryItemStatus::InProgress),
            ("DELIVERED", DeliveryItemStatus::Delivered),
            ("FAILED", DeliveryItemStatus::Failed),
            ("DEAD_LETTER", DeliveryItemStatus::DeadLetter),
            ("BLOCKED", DeliveryItemStatus::Blocked),
            ("UNKNOWN", DeliveryItemStatus::Unknown),
        ],
    );
}

/// `roomkit.Access`'s members.
#[test]
fn access_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.Access",
        &[
            ("READ_WRITE", Access::ReadWrite),
            ("READ_ONLY", Access::ReadOnly),
            ("WRITE_ONLY", Access::WriteOnly),
            ("NONE", Access::None),
        ],
    );
}

/// `roomkit.AgentResponsePolicy`'s members.
#[test]
fn agent_response_policy_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.AgentResponsePolicy",
        &[
            ("AGENT_CHAIN", AgentResponsePolicy::AgentChain),
            ("ADDRESSED_ONLY", AgentResponsePolicy::AddressedOnly),
        ],
    );
}

/// `roomkit.ChannelCategory`'s members.
#[test]
fn channel_category_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.ChannelCategory",
        &[
            ("TRANSPORT", ChannelCategory::Transport),
            ("INTELLIGENCE", ChannelCategory::Intelligence),
        ],
    );
}

/// `roomkit.models.ChannelDirection`'s members.
#[test]
fn channel_direction_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.ChannelDirection",
        &[
            ("INBOUND", ChannelDirection::Inbound),
            ("OUTBOUND", ChannelDirection::Outbound),
            ("BIDIRECTIONAL", ChannelDirection::Bidirectional),
        ],
    );
}

/// `roomkit.models.ChannelMediaType`'s members.
#[test]
fn channel_media_type_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.ChannelMediaType",
        &[
            ("TEXT", ChannelMediaType::Text),
            ("RICH", ChannelMediaType::Rich),
            ("MEDIA", ChannelMediaType::Media),
            ("AUDIO", ChannelMediaType::Audio),
            ("VIDEO", ChannelMediaType::Video),
            ("LOCATION", ChannelMediaType::Location),
            ("TEMPLATE", ChannelMediaType::Template),
        ],
    );
}

/// `roomkit.ChannelType`'s members.
#[test]
fn channel_type_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.ChannelType",
        &[
            ("SMS", ChannelType::SMS),
            ("MMS", ChannelType::MMS),
            ("RCS", ChannelType::RCS),
            ("EMAIL", ChannelType::EMAIL),
            ("WHATSAPP", ChannelType::WHATSAPP),
            ("WHATSAPP_PERSONAL", ChannelType::WHATSAPP_PERSONAL),
            ("WEBSOCKET", ChannelType::WEBSOCKET),
            ("MESSENGER", ChannelType::MESSENGER),
            ("TEAMS", ChannelType::TEAMS),
            ("DISCORD", ChannelType::DISCORD),
            ("BUZZ", ChannelType::BUZZ),
            ("WEBHOOK", ChannelType::WEBHOOK),
            ("VOICE", ChannelType::VOICE),
            ("REALTIME_VOICE", ChannelType::REALTIME_VOICE),
            ("AUDIO_VIDEO", ChannelType::AUDIO_VIDEO),
            ("REALTIME_AUDIO_VIDEO", ChannelType::REALTIME_AUDIO_VIDEO),
            ("VIDEO", ChannelType::VIDEO),
            ("CONFERENCE", ChannelType::CONFERENCE),
            ("TELEGRAM", ChannelType::TELEGRAM),
            ("CLI", ChannelType::CLI),
            ("PUSH", ChannelType::PUSH),
            ("HOST", ChannelType::HOST),
            ("AI", ChannelType::AI),
            ("SYSTEM", ChannelType::SYSTEM),
        ],
    );
}

/// `roomkit.models.DeleteType`'s members.
#[test]
fn delete_type_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.DeleteType",
        &[
            ("SENDER", DeleteType::Sender),
            ("SYSTEM", DeleteType::System),
            ("ADMIN", DeleteType::Admin),
        ],
    );
}

/// `roomkit.models.EditSource`'s members.
#[test]
fn edit_source_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.EditSource",
        &[
            ("SENDER", EditSource::Sender),
            ("SYSTEM", EditSource::System),
        ],
    );
}

/// `roomkit.EventDirection`'s members.
#[test]
fn event_direction_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.EventDirection",
        &[
            ("INBOUND", EventDirection::Inbound),
            ("OUTBOUND", EventDirection::Outbound),
        ],
    );
}

/// `roomkit.EventStatus`'s members.
#[test]
fn event_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.EventStatus",
        &[
            ("PENDING", EventStatus::Pending),
            ("DELIVERED", EventStatus::Delivered),
            ("READ", EventStatus::Read),
            ("FAILED", EventStatus::Failed),
            ("BLOCKED", EventStatus::Blocked),
        ],
    );
}

/// `roomkit.EventType`'s members.
#[test]
fn event_type_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.EventType",
        &[
            ("MESSAGE", EventType::Message),
            ("SYSTEM", EventType::System),
            ("INSTRUCTION", EventType::Instruction),
            ("TYPING", EventType::Typing),
            ("READ_RECEIPT", EventType::ReadReceipt),
            ("DELIVERY_RECEIPT", EventType::DeliveryReceipt),
            ("PRESENCE", EventType::Presence),
            ("REACTION", EventType::Reaction),
            ("EDIT", EventType::Edit),
            ("DELETE", EventType::Delete),
            ("PARTICIPANT_JOINED", EventType::ParticipantJoined),
            ("PARTICIPANT_LEFT", EventType::ParticipantLeft),
            ("PARTICIPANT_IDENTIFIED", EventType::ParticipantIdentified),
            ("PARTICIPANT_UPDATED", EventType::ParticipantUpdated),
            ("CHANNEL_ATTACHED", EventType::ChannelAttached),
            ("CHANNEL_DETACHED", EventType::ChannelDetached),
            ("CHANNEL_MUTED", EventType::ChannelMuted),
            ("CHANNEL_UNMUTED", EventType::ChannelUnmuted),
            ("CHANNEL_UPDATED", EventType::ChannelUpdated),
            ("DTMF", EventType::Dtmf),
            ("RECORDING_STARTED", EventType::RecordingStarted),
            ("RECORDING_STOPPED", EventType::RecordingStopped),
            ("TASK_CREATED", EventType::TaskCreated),
            ("TASK_DELEGATED", EventType::TaskDelegated),
            ("TASK_COMPLETED", EventType::TaskCompleted),
            ("OBSERVATION", EventType::Observation),
            ("TOOL_CALL_START", EventType::ToolCallStart),
            ("TOOL_CALL_END", EventType::ToolCallEnd),
            ("INSTRUCTION_READ", EventType::InstructionRead),
        ],
    );
}

/// `roomkit.models.IdentificationStatus`'s members.
#[test]
fn identification_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.IdentificationStatus",
        &[
            ("IDENTIFIED", IdentificationStatus::Identified),
            ("PENDING", IdentificationStatus::Pending),
            ("AMBIGUOUS", IdentificationStatus::Ambiguous),
            ("UNKNOWN", IdentificationStatus::Unknown),
            ("CHALLENGE_SENT", IdentificationStatus::ChallengeSent),
            ("REJECTED", IdentificationStatus::Rejected),
        ],
    );
}

/// `roomkit.models.ParticipantRole`'s members.
#[test]
fn participant_role_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.ParticipantRole",
        &[
            ("OWNER", ParticipantRole::Owner),
            ("AGENT", ParticipantRole::Agent),
            ("MEMBER", ParticipantRole::Member),
            ("OBSERVER", ParticipantRole::Observer),
            ("BOT", ParticipantRole::Bot),
        ],
    );
}

/// `roomkit.models.ParticipantStatus`'s members.
#[test]
fn participant_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.ParticipantStatus",
        &[
            ("ACTIVE", ParticipantStatus::Active),
            ("INACTIVE", ParticipantStatus::Inactive),
            ("LEFT", ParticipantStatus::Left),
            ("BANNED", ParticipantStatus::Banned),
        ],
    );
}

/// `roomkit.RoomStatus`'s members.
#[test]
fn room_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.RoomStatus",
        &[
            ("ACTIVE", RoomStatus::Active),
            ("PAUSED", RoomStatus::Paused),
            ("CLOSED", RoomStatus::Closed),
            ("ARCHIVED", RoomStatus::Archived),
        ],
    );
}

/// `roomkit.models.TaskStatus`'s members.
#[test]
fn task_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.models.TaskStatus",
        &[
            ("PENDING", TaskStatus::Pending),
            ("IN_PROGRESS", TaskStatus::InProgress),
            ("COMPLETED", TaskStatus::Completed),
            ("FAILED", TaskStatus::Failed),
            ("CANCELLED", TaskStatus::Cancelled),
        ],
    );
}

/// `roomkit.VerificationStatus`'s members.
#[test]
fn verification_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.VerificationStatus",
        &[
            ("UNVERIFIED", VerificationStatus::Unverified),
            ("PENDING", VerificationStatus::Pending),
            ("VERIFIED", VerificationStatus::Verified),
            ("EXPIRED", VerificationStatus::Expired),
            ("REVOKED", VerificationStatus::Revoked),
        ],
    );
}

/// `roomkit.VerificationRequestStatus`'s members.
#[test]
fn verification_request_status_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.VerificationRequestStatus",
        &[
            ("OPEN", VerificationRequestStatus::Open),
            ("COMPLETED", VerificationRequestStatus::Completed),
            ("FAILED", VerificationRequestStatus::Failed),
            ("EXPIRED", VerificationRequestStatus::Expired),
            ("CANCELLED", VerificationRequestStatus::Cancelled),
        ],
    );
}

/// `roomkit.Visibility`'s members.
#[test]
fn visibility_serializes_to_python_s_values() {
    serializes_to(
        "roomkit.Visibility",
        &[
            ("ALL", Visibility::All),
            ("NONE", Visibility::None),
            ("INTERNAL", Visibility::Internal),
            ("TRANSPORT", Visibility::Transport),
            ("INTELLIGENCE", Visibility::Intelligence),
        ],
    );
}
