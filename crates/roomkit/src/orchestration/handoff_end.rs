//! How a handoff ends once the room's lock decided it (RFC §19.6): the state
//! it leaves, its record in the timeline, its entry on the status bus and its
//! hooks. Python's `HandoffHandler.handle` past its lock, and `_handoff_line`.

use roomkit_core::model::{
    ChannelId, EventType, Metadata, RoomEvent, RoomId, TextContent, Visibility,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use roomkit_core::rules::transcript::{RUNTIME_RECORD, without_mark_copies};
use serde_json::json;

use crate::error::RoomKitError;
use crate::kit::{RoomKit, SendEventOptions};
use crate::orchestration::handoff::{HandoffHandler, HandoffRequest, HandoffResult};
use crate::orchestration::lifecycle::post_agent_lifecycle;
use crate::orchestration::{ConversationState, StatusLevel, TransitionOptions};
use crate::store::ConversationStore;
use crate::tasks::text::{identifier, quoted};

/// How the record of a handoff opens in the timeline: Python's
/// `HANDOFF_OPENING`.
const HANDOFF_OPENING: &str = "[Handoff";

/// The characters of a handoff's reason the room's timeline records.
const REASON_CHARS: usize = 500;

/// `state` once the handoff took place, its summary kept in its context for
/// the next agent's memory (`HandoffMemoryProvider`).
pub(super) fn moved_state(
    state: &ConversationState,
    phase: &str,
    agent: Option<&str>,
    calling_agent_id: &ChannelId,
    request: &HandoffRequest,
) -> Result<ConversationState, RoomKitError> {
    let to_agent = agent
        .map(ChannelId::new)
        .transpose()
        .map_err(|error| RoomKitError::Value(error.to_string()))?;
    let options = TransitionOptions {
        to_agent,
        reason: request.reason.clone(),
        metadata: Metadata::from_iter([
            ("summary".to_owned(), json!(request.summary)),
            (
                "channel_escalation".to_owned(),
                json!(request.channel_escalation),
            ),
        ]),
    };
    let mut moved = state.transition(phase, options);
    moved
        .context
        .insert("handoff_summary".to_owned(), json!(request.summary));
    moved
        .context
        .insert("handoff_from".to_owned(), json!(calling_agent_id.as_str()));
    moved
        .context
        .insert("handoff_reason".to_owned(), json!(request.reason));
    moved.human_hold = agent.is_none();
    Ok(moved)
}

/// Says a refused handoff: the caller's failed entry, `ON_HANDOFF_REJECTED`.
pub(super) async fn rejected<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    calling_agent_id: &ChannelId,
    request: &HandoffRequest,
    result: &HandoffResult,
) {
    let detail = if result.reason.is_empty() {
        "handoff rejected"
    } else {
        &result.reason
    };
    let metadata = Metadata::from_iter([
        ("room_id".to_owned(), json!(room_id.as_str())),
        ("to_agent".to_owned(), json!(request.target_agent_id)),
        ("reason".to_owned(), json!(request.reason)),
    ]);
    post_agent_lifecycle(
        kit,
        calling_agent_id,
        StatusLevel::Failed,
        "handoff",
        detail,
        metadata,
    );
    let event = hook_event(kit, room_id, calling_agent_id, request, result);
    kit.observe_orchestration(HookTrigger::OnHandoffRejected, event)
        .await;
}

/// Records an accepted handoff, says it on the status bus, fires
/// `ON_HANDOFF` and `ON_PHASE_TRANSITION`: its result.
pub(super) async fn accepted<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    calling_agent_id: &ChannelId,
    request: &HandoffRequest,
    phase: String,
    agent: Option<String>,
    handler: &HandoffHandler<S>,
) -> Result<HandoffResult, RoomKitError> {
    let target = request.target_agent_id.as_str();
    record_handoff(kit, room_id, calling_agent_id, request, handler).await?;
    tracing::info!(target: "roomkit::orchestration", "Handoff: {calling_agent_id} -> {target} (phase={phase}, room={room_id})");
    let detail = if request.reason.is_empty() {
        format!("handoff from {calling_agent_id}")
    } else {
        request.reason.clone()
    };
    let posted = Metadata::from_iter([
        ("room_id".to_owned(), json!(room_id.as_str())),
        ("from_agent".to_owned(), json!(calling_agent_id.as_str())),
        ("phase".to_owned(), json!(phase)),
        ("summary".to_owned(), json!(request.summary)),
    ]);
    let target_id =
        ChannelId::new(target).map_err(|error| RoomKitError::Value(error.to_string()))?;
    post_agent_lifecycle(
        kit,
        &target_id,
        StatusLevel::Info,
        "handoff",
        &detail,
        posted,
    );
    let result = HandoffResult {
        accepted: true,
        new_agent_id: agent,
        new_phase: Some(phase),
        message: format!(
            "Conversation transferred to {target}. Do not respond — the receiving agent will \
             greet the user."
        ),
        reason: String::new(),
    };
    for trigger in [HookTrigger::OnHandoff, HookTrigger::OnPhaseTransition] {
        let event = hook_event(kit, room_id, calling_agent_id, request, &result);
        kit.observe_orchestration(trigger, event).await;
    }
    Ok(result)
}

/// Records a handoff in the room's timeline, a system event no agent answers
/// (RFC §19.6), sent from the handler's event channel or the calling agent's
/// own.
async fn record_handoff<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    calling_agent_id: &ChannelId,
    request: &HandoffRequest,
    handler: &HandoffHandler<S>,
) -> Result<(), RoomKitError> {
    let source = handler.event_channel().unwrap_or(calling_agent_id);
    let metadata = Metadata::from_iter([
        ("handoff".to_owned(), json!(true)),
        ("from_agent".to_owned(), json!(calling_agent_id.as_str())),
        ("to_agent".to_owned(), json!(request.target_agent_id)),
        ("summary".to_owned(), json!(request.summary)),
        ("_orchestration_internal".to_owned(), json!(true)),
        // The runtime's record: its mark is kept, a copy replaced (§6.4).
        (RUNTIME_RECORD.to_owned(), json!("handoff")),
    ]);
    let options = SendEventOptions {
        event_type: EventType::System,
        visibility: Visibility::All,
        metadata,
        ..SendEventOptions::default()
    };
    let line = TextContent::new(handoff_line(calling_agent_id, request));
    kit.send_event(room_id, source, line, options).await?;
    Ok(())
}

/// The event an orchestration hook is handed: Python's `_fire_hook`'s.
fn hook_event<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    calling_agent_id: &ChannelId,
    request: &HandoffRequest,
    result: &HandoffResult,
) -> RoomEvent {
    let metadata = Metadata::from_iter([
        ("handoff".to_owned(), json!(true)),
        ("from_agent".to_owned(), json!(calling_agent_id.as_str())),
        ("to_agent".to_owned(), json!(request.target_agent_id)),
        ("accepted".to_owned(), json!(result.accepted)),
        ("new_phase".to_owned(), json!(result.new_phase)),
    ]);
    kit.internal_event(
        room_id,
        calling_agent_id,
        EventType::System,
        request.reason.clone(),
        metadata,
    )
}

/// The timeline's record of a handoff: the agents by their ids, the reason
/// the calling model gave quoted (RFC §6.4, §19.6), since the target agent
/// reads it back in its history; a copy of a mark in it is replaced, the
/// record keeping its own. Python's `_handoff_line`.
fn handoff_line(calling_agent_id: &ChannelId, request: &HandoffRequest) -> String {
    let source = identifier(calling_agent_id.as_str(), "agent");
    let target = identifier(&request.target_agent_id, "agent");
    // Python's `str.strip`, which also takes the separators U+001C to U+001F.
    let blank = |c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c);
    let reason = if request.reason.trim_matches(blank).is_empty() {
        String::new()
    } else {
        format!(
            " {}",
            quoted(&without_mark_copies(&request.reason), REASON_CHARS)
        )
    };
    format!("{HANDOFF_OPENING}: {source} -> {target}]{reason}")
}
