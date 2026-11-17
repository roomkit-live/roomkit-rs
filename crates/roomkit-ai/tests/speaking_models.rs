//! Public speaking carriers, Python's `test_speaking_public_models.py` and `test_thinker.py`.

use roomkit::ClassedError;
use roomkit::model::{
    ChannelId, ChannelType, EventSource, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit_ai::classifiers::SharedState;
use roomkit_ai::speaking::{
    CutReply, Judgments, SharedRoomEvent, SpeakDecision, SpeakDecisionEvent, SpeakDecisionOptions,
    SpeakMode, SpeakTurn, Speakers, Thought, ThoughtEvent,
};
use roomkit_ai::{AiContext, AiGenerationEvent, SharedAiContext};
use serde_json::json;

// Python: test_thinker.py::test_speaking_empties_what_it_wanted_to_say.
#[test]
fn speaking_empties_what_it_wanted_to_say() {
    let thought = Thought {
        text: "They ask for a price.".into(),
        want_to_say: vec!["It costs 1,200 a year.".into()],
        urgent: true,
    };
    let said = thought.said();
    assert_eq!(said.text, thought.text);
    assert_eq!(said.want_to_say, Vec::<String>::new());
    assert!(!said.urgent);
    assert_eq!(thought.want_to_say, ["It costs 1,200 a year."]);
    assert!(thought.urgent);
    assert_eq!(
        thought.as_state(),
        json!({"thinking": "They ask for a price.", "want_to_say": ["It costs 1,200 a year."], "urgent": true})
            .as_object().unwrap().clone()
    );
}

// Python: test_speaking_public_models.py, the State/event/context aliases.
#[test]
fn retained_input_handles_read_the_same_objects_after_mutation() {
    let state = SharedState::new(json!({"text": "Hi"}));
    let retained_state = state.clone();
    state.update(|state| state["text"] = json!("Changed"));
    assert!(state.same_object(&retained_state));
    assert_eq!(retained_state.snapshot()["text"], "Changed");

    let event = SharedRoomEvent::new(RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        TextContent::new("Hi"),
    ));
    let retained_event = event.clone();
    event.update(|event| {
        event.metadata.insert("seen".into(), json!(true));
    });
    assert!(event.same_object(&retained_event));
    assert_eq!(retained_event.snapshot().metadata["seen"], true);

    let context = SharedAiContext::new(AiContext::default());
    let retained_context = context.clone();
    context.update(|context| context.system_prompt = Some("Changed".into()));
    assert!(context.same_object(&retained_context));
    assert_eq!(
        retained_context.snapshot().system_prompt.as_deref(),
        Some("Changed")
    );
    assert_eq!(context, retained_context); // Comparing the same cell must not lock it twice.
}

// The payload side of test_speaking_inter_api_alias.py's public kit witness.
#[test]
fn a_generation_payload_retains_its_context_and_replacement_selects_another() {
    let original = SharedAiContext::new(AiContext::default());
    let mut event = AiGenerationEvent::new(original.clone(), ChannelId::new("ai").unwrap());
    let retained = event.clone();
    original.update(|context| context.system_prompt = Some("Changed".into()));
    assert!(retained.ai_context.same_object(&original));
    assert_eq!(
        retained.ai_context.snapshot().system_prompt.as_deref(),
        Some("Changed")
    );
    event.ai_context = SharedAiContext::new(AiContext::default());
    event
        .ai_context
        .update(|context| context.system_prompt = Some("Replacement".into()));
    assert!(!event.ai_context.same_object(&retained.ai_context));
    assert_eq!(
        original.snapshot().system_prompt.as_deref(),
        Some("Changed")
    );
}

#[expect(clippy::unwrap_used, reason = "fixed test fixture identifiers")]
fn event(channel: &str) -> SharedRoomEvent {
    RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new(channel).unwrap(), ChannelType::SMS),
        TextContent::new("Hello"),
    )
    .into()
}

// Python: test_speaking_public_models.py::test_decision_defaults_are_isolated_and_serialize_as_plain_data.
// Python: test_speaking_public_models.py::test_frozen_decision_keeps_the_supplied_judgment_dictionary_live.
#[test]
fn decision_defaults_are_isolated_and_serialize_as_plain_data() {
    let first: SpeakDecision = SpeakMode::Silent.into();
    let second: SpeakDecision = SpeakMode::Silent.into();
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        json!({
            "mode": "silent", "reason": "", "judgments": {}, "notes": [], "final": false,
        })
    );
    first.judgments.insert("heard", 0.75);
    assert_eq!(second.judgments.snapshot(), Vec::<(String, f64)>::new());
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        json!({
            "mode": "silent", "reason": "", "judgments": {"heard": 0.75}, "notes": [], "final": false,
        })
    );
    let judgments = Judgments::from_iter([("heard", 0.25)]);
    let decision = SpeakDecision::new(
        SpeakMode::Silent,
        SpeakDecisionOptions {
            judgments: judgments.clone(),
            ..SpeakDecisionOptions::default()
        },
    )
    .unwrap();
    judgments.insert("heard", 0.75);
    assert_eq!(decision.judgments.get("heard"), Some(0.75));
}

// Python: test_speaking_public_models.py::test_speak_turn_retains_its_event_speakers_and_optional_carriers.
#[test]
fn speak_turn_retains_its_event_speakers_and_optional_carriers() {
    let event = event("sms");
    let id = event.snapshot().id.to_string();
    let speakers = Speakers::from_iter([(id.as_str(), "Alice".into())]);
    let mut turn = SpeakTurn {
        speakers: speakers.clone(),
        ..SpeakTurn::new(event.clone())
    };
    assert_eq!(turn.people, Vec::<String>::new());
    assert_eq!(turn.recent, Vec::<SharedRoomEvent>::new());
    assert_eq!(turn.channel_id, "");
    speakers.insert(&id, "Bob".into());
    event.update(|event| {
        event.metadata.insert("seen".into(), json!(true));
    });
    assert!(event.same_object(&turn.event));
    assert_eq!(turn.speakers.get(&id).as_deref(), Some("Bob"));
    assert_eq!(turn.event.snapshot().metadata["seen"], true);
    assert!(!turn.by_agent(&event));
    turn.channel_id = "sms".into();
    assert!(turn.by_agent(&event));
}

// Python: test_speaking_public_models.py::test_only_a_silent_decision_can_be_final.
#[test]
fn only_a_silent_decision_can_be_final() {
    for mode in [SpeakMode::Speak, SpeakMode::Offer] {
        let error = SpeakDecision::new(
            mode,
            SpeakDecisionOptions {
                r#final: true,
                ..SpeakDecisionOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ClassedError>().unwrap().class(),
            "ValueError"
        );
        assert_eq!(error.to_string(), "only a silent decision can be final");
    }
    assert!(
        SpeakDecision::new(
            SpeakMode::Silent,
            SpeakDecisionOptions {
                r#final: true,
                ..SpeakDecisionOptions::default()
            }
        )
        .unwrap()
        .r#final
    );
}

// Python: test_speaking_public_models.py::test_hook_carriers_preserve_defaults_and_the_supplied_values.
// Python: test_speaking_public_models.py::test_public_time_carriers_do_not_reject_negative_or_fractional_numbers.
#[test]
fn hook_carriers_keep_defaults_and_unrestricted_numeric_durations() {
    let trigger = event("sms");
    let mut report = SpeakDecisionEvent::new("", "", trigger.clone(), SpeakMode::Offer.into());
    assert!(report.event.same_object(&trigger));
    assert_eq!(
        (
            report.duration_ms,
            report.asked_again,
            report.thought.clone(),
            report.late
        ),
        (0.0, false, None, false)
    );
    let mut thought = ThoughtEvent::new("", "", Thought::default());
    assert_eq!(thought.previous, Thought::default());
    assert_eq!(thought.duration_ms, None);
    for duration in [-1.0, -0.25, 0.25] {
        report.duration_ms = duration;
        thought.duration_ms = Some(duration);
        let cut = CutReply {
            text: String::new(),
            played_ms: duration,
            at: Timestamp::from_unix_millis(0).unwrap(),
        };
        assert_eq!(report.duration_ms, cut.played_ms);
        assert_eq!(thought.duration_ms, Some(cut.played_ms));
        assert_eq!(
            (
                &report.room_id,
                &report.channel_id,
                &thought.room_id,
                &thought.channel_id
            ),
            (
                &String::new(),
                &String::new(),
                &String::new(),
                &String::new()
            )
        );
    }
}

// Python: tests/test_speak_policy.py::test_a_decision_refuses_an_unknown_mode
#[test]
fn the_typed_mode_refuses_unknown_serialized_values() {
    assert!(serde_json::from_str::<SpeakMode>("\"whisper\"").is_err());
}
