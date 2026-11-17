//! What a public policy receives, before it could disclose text to a classifier.
use roomkit::model::{
    EventType, Participant, ParticipantId, ParticipantRole, ParticipantStatus, Visibility,
};
use roomkit_ai::classifiers::{
    ClassifierError, MockClassifier, MockClassifierOptions, Script, ScriptedAnswer,
};
use roomkit_ai::speaking::{ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, SpeakTurn};

use super::*;

fn said(body: &str, person: Option<&str>, channel: &str) -> RoomEvent {
    let mut source = EventSource::new(
        id(channel),
        if channel == "ai" {
            ChannelType::AI
        } else {
            ChannelType::SMS
        },
    );
    source.participant_id = person.map(|p| ParticipantId::new(p).unwrap());
    RoomEvent::new(RoomId::new("r1").unwrap(), source, TextContent::new(body))
}
fn person(id_: &str, name: &str) -> Participant {
    let mut participant = Participant::new(
        ParticipantId::new(id_).unwrap(),
        RoomId::new("r1").unwrap(),
        id("sms"),
    );
    participant.display_name = Some(name.into());
    participant
}
fn context(events: Vec<RoomEvent>) -> RoomContext {
    let (_, _, mut context) = input("");
    context.recent_events = events;
    context.participants = vec![person("p1", "Sylvain"), person("p2", "Paul")];
    context
}
fn read(event: RoomEvent, context: RoomContext) -> impl Future<Output = SpeakTurn> {
    Box::pin(read_turn(event, context))
}

async fn read_turn(event: RoomEvent, context: RoomContext) -> SpeakTurn {
    let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
        decisions: vec![SpeakMode::Silent.into()],
        ..MockSpeakPolicyOptions::default()
    }));
    let channel = AiChannel::new(
        id("ai"),
        model(false),
        AiChannelOptions {
            speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let binding = context.bindings.first().unwrap().clone();
    assert!(
        !channel
            .on_event(event, binding, context)
            .await
            .unwrap()
            .responded
    );
    channel.close().await;
    let turns = policy.turns();
    assert_eq!(turns.len(), 1);
    turns.first().unwrap().clone()
}

// Python: tests/test_speak_policy.py::test_the_policy_reads_the_conversation_when_the_turn_starts_and_who_is_there
#[tokio::test]
async fn policy_reads_all_other_visible_messages_even_after_its_trigger() {
    for with_later in [false, true] {
        let before = said("Hello", Some("p2"), "sms");
        let event = said("Nova, a question", Some("p1"), "sms");
        let later = said("Clarification", Some("p1"), "sms");
        let mut events = vec![before.clone(), event.clone()];
        if with_later {
            events.push(later.clone());
        }
        let turn = read(event.clone(), context(events)).await;
        assert_eq!(turn.event.snapshot(), event);
        let expected = if with_later {
            vec![before.id, later.id]
        } else {
            vec![before.id]
        };
        assert_eq!(
            turn.recent
                .iter()
                .map(|e| e.snapshot().id)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(turn.people, ["Sylvain", "Paul"]);
    }
}

// Python: tests/test_speak_classifier.py::test_the_turn_names_speakers_and_marks_the_agents_answers
#[tokio::test]
async fn policy_turn_names_speakers_and_marks_the_agents_answers() {
    let before = said("Hello", Some("p2"), "sms");
    let own = said("Hello Paul", None, "ai");
    let mut stamped = said("Et moi", Some("p1"), "sms");
    stamped
        .metadata
        .insert("sender_name".into(), json!("Marie"));
    let event = said("Nova ?", Some("p1"), "sms");
    let turn = read(
        event.clone(),
        context(vec![
            before.clone(),
            own.clone(),
            stamped.clone(),
            event.clone(),
        ]),
    )
    .await;
    assert_eq!(turn.channel_id, "ai");
    assert_eq!(
        turn.speakers.snapshot(),
        vec![
            (before.id.to_string(), "Paul".into()),
            (stamped.id.to_string(), "Marie".into()),
            (event.id.to_string(), "Sylvain".into())
        ]
    );
    assert!(turn.by_agent(&own.into()));
    assert!(!turn.by_agent(&before.into()));
}

// Python: tests/test_speak_classifier.py::test_people_count_a_human_agent_and_leave_out_bots_and_the_agents_channel
#[tokio::test]
async fn people_count_human_agents_but_not_departed_people_bots_or_the_channel() {
    let event = said("Nova ?", Some("p1"), "sms");
    let mut context = context(vec![event.clone()]);
    context.participants[1].status = ParticipantStatus::Left;
    let mut human = person("a1", "Claire");
    human.role = ParticipantRole::Agent;
    let mut bot = person("b1", "Codex");
    bot.role = ParticipantRole::Bot;
    let mut own = person("n1", "Nova");
    own.channel_id = id("ai");
    own.connected_via = vec![id("ai")];
    context.participants.extend([human, bot, own]);
    assert_eq!(read(event, context).await.people, ["Sylvain", "Claire"]);
}

// Python: tests/test_speak_classifier.py::test_people_count_the_voices_of_one_microphone
// This consumes sender_name on text; it neither captures nor diarizes audio.
#[tokio::test]
async fn distinct_named_text_senders_count_as_people_on_one_channel() {
    let mut first = said("Hi", Some("p1"), "sms");
    first
        .metadata
        .insert("sender_name".into(), json!("Speaker 1"));
    let mut second = said("Hello", Some("p1"), "sms");
    second
        .metadata
        .insert("sender_name".into(), json!("Speaker 2"));
    let mut event = said("Nova ?", Some("p1"), "sms");
    event
        .metadata
        .insert("sender_name".into(), json!("Speaker 1"));
    let mut context = context(vec![first, second, event.clone()]);
    context.participants = vec![person("p1", "Mic")];
    assert_eq!(
        read(event, context).await.people,
        ["Speaker 1", "Speaker 2"]
    );
}

// Python: tests/test_speak_classifier.py::test_the_turn_holds_only_what_the_channel_may_know
#[tokio::test]
async fn policy_never_receives_withheld_events_or_tool_rows() {
    let shown = said("Budget", Some("p1"), "sms");
    let mut withheld = said("SECRET", Some("p1"), "sms");
    withheld.visibility = Visibility::channels([id("sms")]).unwrap();
    let mut internal = said("Internal", Some("p1"), "sms");
    internal.visibility = Visibility::Internal;
    let mut own = said("Noted.", None, "ai");
    own.visibility = Visibility::channels([id("sms")]).unwrap();
    let mut row = said("Tool row", None, "ai");
    row.r#type = EventType::ToolCallStart;
    let event = said("Nova ?", Some("p1"), "sms");
    let turn = read(
        event.clone(),
        context(vec![
            shown.clone(),
            withheld,
            internal,
            own.clone(),
            row,
            event,
        ]),
    )
    .await;
    assert_eq!(
        turn.recent
            .iter()
            .map(|e| e.snapshot().id)
            .collect::<Vec<_>>(),
        [shown.id, own.id]
    );
}

// Python: tests/test_speak_classifier.py::test_on_the_channel_a_turn_for_someone_else_runs_nothing
// Python: tests/test_speak_classifier.py::test_on_the_channel_a_failing_classifier_lets_the_agent_speak
#[tokio::test]
async fn the_channel_obeys_real_classifier_silence_and_falls_back_on_its_error() {
    for fails in [false, true] {
        let error: Option<Arc<dyn std::error::Error + Send + Sync>> = if fails {
            Some(Arc::new(ClassifierError::new("down")))
        } else {
            None
        };
        let classifier = Arc::new(MockClassifier::new(MockClassifierOptions {
            answers: vec![Script::from_iter([(
                "directness",
                ScriptedAnswer::Number(0.1),
            )])],
            error,
        }));
        let policy =
            ClassifierSpeakPolicy::new(classifier, "Nova", ClassifierSpeakPolicyOptions::default())
                .unwrap();
        let model = model(false);
        let channel = AiChannel::new(
            id("ai"),
            model.clone(),
            AiChannelOptions {
                speak_policy: Some(Arc::new(policy)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let event = said("Paul, do you have the numbers?", Some("p1"), "sms");
        let context = context(vec![event.clone()]);
        let output = channel
            .on_event(event, context.bindings.first().unwrap().clone(), context)
            .await
            .unwrap();
        assert_eq!(output.responded, fails);
        assert_eq!(model.calls().len(), usize::from(fails));
        channel.close().await;
    }
}
