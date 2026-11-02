//! A room's conversation state (RFC §19.2): the rules of a transition and
//! the stored form's round trip, which the shared scenarios read only
//! through the room they are saved in.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::model::{ChannelId, Room, RoomId, Timestamp};
use roomkit::orchestration::{PhaseTransition, TransitionOptions};
use roomkit::{
    ConversationPhase, ConversationState, get_conversation_state, set_conversation_state,
};
use serde_json::json;

/// An agent's id as the state holds it.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the state's agent fields are options"
)]
fn agent(id: &str) -> Option<ChannelId> {
    Some(ChannelId::new(id).unwrap())
}

fn to(to_agent: &str) -> TransitionOptions {
    TransitionOptions {
        to_agent: agent(to_agent),
        ..Default::default()
    }
}

fn active(id: &str) -> ConversationState {
    ConversationState {
        active_agent_id: agent(id),
        ..ConversationState::new()
    }
}

fn now() -> Timestamp {
    ConversationState::new().phase_started_at
}

fn room() -> Room {
    Room::new(RoomId::new("room-1").unwrap())
}

/// A state written as the room stores it, then read back.
fn round_trip(state: &ConversationState) -> ConversationState {
    serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap()
}

// Python: tests/orchestration/test_state.py::TestConversationPhase::test_built_in_phases_are_strings
#[test]
fn built_in_phases_are_strings() {
    let phases = [
        (ConversationPhase::Intake, "intake"),
        (ConversationPhase::Qualification, "qualification"),
        (ConversationPhase::Handling, "handling"),
        (ConversationPhase::Escalation, "escalation"),
        (ConversationPhase::Resolution, "resolution"),
        (ConversationPhase::Followup, "followup"),
    ];
    for (phase, text) in phases {
        assert_eq!(String::from(phase), text);
        assert_eq!(text, phase);
        assert_eq!(json!(phase), json!(text));
    }
}

// Python: tests/orchestration/test_state.py::TestConversationStateDefaults::test_defaults
#[test]
fn a_new_state_has_python_s_defaults() {
    let state = ConversationState::new();
    assert_eq!(state.phase, ConversationPhase::Intake);
    assert_eq!(state.active_agent_id, None);
    assert_eq!(state.previous_agent_id, None);
    assert_eq!(state.handoff_count, 0);
    assert_eq!(state.phase_history, []);
    assert!(state.context.is_empty());
}

// Python: tests/orchestration/test_state.py::TestConversationStateDefaults::test_phase_started_at_is_recent
#[test]
fn a_new_state_s_phase_started_now() {
    let before = now();
    let state = ConversationState::new();
    assert!(before <= state.phase_started_at && state.phase_started_at <= now());
}

// Python: tests/orchestration/test_state.py::TestTransition::test_returns_new_instance
#[test]
fn a_transition_leaves_the_state_as_it_is() {
    let original = ConversationState::new();
    let _new = original.transition("handling", to("agent-a"));
    assert_eq!(original.phase, ConversationPhase::Intake);
    assert_eq!(original.active_agent_id, None);
    assert_eq!(original.phase_history, []);
}

// Python: tests/orchestration/test_state.py::TestTransition::test_updates_phase_and_agent
#[test]
fn a_transition_sets_the_phase_and_the_agent() {
    let new = ConversationState::new().transition("handling", to("agent-a"));
    assert_eq!(new.phase, "handling");
    assert_eq!(new.active_agent_id, agent("agent-a"));
}

// Python: tests/orchestration/test_state.py::TestTransition::test_records_previous_agent
#[test]
fn a_transition_records_the_previous_agent() {
    let new = active("agent-a").transition("review", to("agent-b"));
    assert_eq!(new.previous_agent_id, agent("agent-a"));
    assert_eq!(new.active_agent_id, agent("agent-b"));
}

// Python: tests/orchestration/test_state.py::TestTransition::test_handoff_count_increments_on_agent_change
// Python: tests/orchestration/test_state.py::TestTransition::test_handoff_count_stable_on_same_agent
// Python: tests/orchestration/test_state.py::TestTransition::test_handoff_count_increments_from_none
// Python: tests/orchestration/test_state.py::TestTransition::test_handoff_count_increments_to_none
#[test]
fn a_change_of_agent_counts_a_handoff() {
    let cases = [
        (active("agent-a"), to("agent-b"), 1),
        (active("agent-a"), to("agent-a"), 0),
        (ConversationState::new(), to("agent-a"), 1),
        (active("agent-a"), TransitionOptions::default(), 1),
    ];
    for (state, options, count) in cases {
        assert_eq!(state.transition("next", options).handoff_count, count);
    }
}

// Python: tests/orchestration/test_state.py::TestTransition::test_phase_history_appended
#[test]
fn each_transition_is_recorded() {
    let qualified = TransitionOptions {
        reason: "qualified".to_owned(),
        ..to("agent-a")
    };
    let complete = TransitionOptions {
        reason: "code complete".to_owned(),
        ..to("agent-b")
    };
    let state = ConversationState::new()
        .transition("handling", qualified)
        .transition("review", complete);
    let recorded: Vec<_> = state
        .phase_history
        .iter()
        .map(|t| {
            let (from, to) = (t.from_agent.clone(), t.to_agent.clone());
            (
                t.from_phase.as_str(),
                t.to_phase.as_str(),
                from,
                to,
                t.reason.as_str(),
            )
        })
        .collect();
    assert_eq!(
        recorded,
        [
            ("intake", "handling", None, agent("agent-a"), "qualified"),
            (
                "handling",
                "review",
                agent("agent-a"),
                agent("agent-b"),
                "code complete"
            ),
        ]
    );
}

// Python: tests/orchestration/test_state.py::TestTransition::test_transition_metadata
#[test]
fn a_transition_records_its_metadata() {
    let options = TransitionOptions {
        metadata: json!({"summary": "user wants help"})
            .as_object()
            .unwrap()
            .clone(),
        ..to("agent-a")
    };
    let new = ConversationState::new().transition("handling", options);
    assert_eq!(
        json!(new.phase_history[0].metadata),
        json!({"summary": "user wants help"})
    );
}

// Python: tests/orchestration/test_state.py::TestTransition::test_phase_started_at_updated
#[test]
fn a_transition_starts_its_phase_now() {
    let state = ConversationState::new();
    let new = state.transition("handling", TransitionOptions::default());
    assert!(new.phase_started_at >= state.phase_started_at);
    assert!(new.phase_history[0].timestamp <= new.phase_started_at);
}

// Python: tests/orchestration/test_state.py::TestTransition::test_custom_phase_strings
#[test]
fn a_phase_is_any_string() {
    let new = ConversationState::new().transition("my-custom-phase", to("agent-x"));
    assert_eq!(new.phase, "my-custom-phase");
    assert_eq!(new.phase_history[0].to_phase, "my-custom-phase");
}

// Python: tests/orchestration/test_state.py::TestTransition::test_context_preserved_through_transition
#[test]
fn a_transition_keeps_the_context() {
    let state = ConversationState {
        context: json!({"key": "value"}).as_object().unwrap().clone(),
        ..ConversationState::new()
    };
    let new = state.transition("handling", TransitionOptions::default());
    assert_eq!(json!(new.context), json!({"key": "value"}));
}

// Python: tests/orchestration/test_state.py::TestSerialization::test_model_dump_and_validate
// Python: tests/orchestration/test_state.py::TestSerialization::test_datetime_survives_json_roundtrip
// Python: tests/orchestration/test_state.py::TestSerialization::test_transition_timestamp_survives_roundtrip
#[test]
fn a_state_survives_its_stored_form() {
    let reason = TransitionOptions {
        reason: "test".to_owned(),
        ..to("agent-b")
    };
    let state = active("agent-a").transition("handling", reason);
    assert_eq!(round_trip(&state), state);
    let fresh = ConversationState::new();
    assert_eq!(round_trip(&fresh).phase_started_at, fresh.phase_started_at);
}

// Python: tests/orchestration/test_state.py::TestSerialization::test_context_dict_survives_roundtrip
#[test]
fn a_nested_context_survives_the_stored_form() {
    let context = json!({
        "handoff_summary": "user needs portfolio help",
        "handoff_from": "agent-triage",
        "nested": {"a": [1, 2, 3]},
    });
    let state = ConversationState {
        context: context.as_object().unwrap().clone(),
        ..ConversationState::new()
    };
    assert_eq!(json!(round_trip(&state).context), context);
}

// Python: tests/orchestration/test_state.py::TestRoomMetadataRoundTrip::test_set_and_get
#[test]
fn a_state_set_on_a_room_is_read_back() {
    let qualified = TransitionOptions {
        reason: "qualified".to_owned(),
        ..to("agent-b")
    };
    let state = active("agent-a").transition("handling", qualified);
    let restored = get_conversation_state(&set_conversation_state(&room(), &state)).unwrap();
    assert_eq!(restored, state);
    assert_eq!(restored.handoff_count, 1);
    assert_eq!(restored.phase_history[0].reason, "qualified");
}

// Python: tests/orchestration/test_state.py::TestRoomMetadataRoundTrip::test_get_from_empty_room
#[test]
fn a_room_without_a_state_reads_the_default() {
    let state = get_conversation_state(&room()).unwrap();
    assert_eq!(state.phase, ConversationPhase::Intake);
    assert_eq!((state.active_agent_id, state.handoff_count), (None, 0));
}

/// A stored state with fields left out reads Python's defaults for them,
/// as pydantic's validation does.
#[test]
fn a_partial_stored_state_reads_the_defaults_for_the_rest() {
    let mut stored = room();
    stored.metadata.insert(
        "_conversation_state".to_owned(),
        json!({"phase": "handling"}),
    );
    let state = get_conversation_state(&stored).unwrap();
    assert_eq!(state.phase, "handling");
    assert_eq!((state.active_agent_id, state.handoff_count), (None, 0));
}

/// Python's `model_validate` raises on a stored state that is not one.
#[test]
fn a_stored_state_that_is_not_one_is_refused() {
    let mut stored = room();
    stored
        .metadata
        .insert("_conversation_state".to_owned(), json!({"phase": 5}));
    assert!(matches!(
        get_conversation_state(&stored),
        Err(roomkit::RoomKitError::Value(_))
    ));
}

// Python: tests/orchestration/test_state.py::TestRoomMetadataRoundTrip::test_preserves_existing_metadata
// Python: tests/orchestration/test_state.py::TestRoomMetadataRoundTrip::test_original_room_unchanged
#[test]
fn setting_a_state_keeps_the_room_s_metadata_and_leaves_the_room_alone() {
    let mut original = room();
    original
        .metadata
        .insert("custom_key".to_owned(), json!("custom_value"));
    let updated = set_conversation_state(&original, &ConversationState::new());
    assert_eq!(updated.metadata["custom_key"], json!("custom_value"));
    assert!(updated.metadata.contains_key("_conversation_state"));
    assert!(!original.metadata.contains_key("_conversation_state"));
}

// Python: tests/orchestration/test_state.py::TestRoomMetadataRoundTrip::test_multiple_transitions_roundtrip
#[test]
fn several_transitions_survive_the_room() {
    let fixes = TransitionOptions {
        reason: "fixes needed".to_owned(),
        ..to("agent-a")
    };
    let state = ConversationState::new()
        .transition("handling", to("agent-a"))
        .transition("review", to("agent-b"))
        .transition("coding", fixes);
    let restored = get_conversation_state(&set_conversation_state(&room(), &state)).unwrap();
    assert_eq!(restored.phase, "coding");
    assert_eq!(restored.active_agent_id, agent("agent-a"));
    assert_eq!(restored.previous_agent_id, agent("agent-b"));
    assert_eq!(
        (restored.handoff_count, restored.phase_history.len()),
        (3, 3)
    );
}

// Python: tests/orchestration/test_state.py::TestPhaseTransition::test_defaults
// Python: tests/orchestration/test_state.py::TestPhaseTransition::test_timestamp_is_utc
#[test]
fn a_phase_transition_has_python_s_defaults_its_instant_in_utc() {
    let before = now();
    let transition = PhaseTransition::new("a", "b");
    assert_eq!((transition.from_agent, transition.to_agent), (None, None));
    assert_eq!(transition.reason, "");
    assert!(transition.metadata.is_empty());
    assert!(before <= transition.timestamp);
    assert!(json!(transition.timestamp).as_str().unwrap().ends_with('Z'));
}
