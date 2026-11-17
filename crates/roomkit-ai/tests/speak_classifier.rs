//! Public classifier policy witnesses from the Python speaking tests.

mod speak_classifier {
    mod inputs;
    mod listening;
    mod overrides;
}

use roomkit::model::{ChannelId, ChannelType, EventSource, RoomEvent, RoomId, TextContent};
use roomkit_ai::classifiers::{
    Answer, Answers, MockClassifier, MockClassifierOptions, ScoreAnswer, Script, ScriptedAnswer,
    YesNoAnswer,
};
use roomkit_ai::speaking::{ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, SpeakTurn};
use std::sync::Arc;

#[expect(clippy::unwrap_used, reason = "fixed test fixture identifiers")]
fn turn(body: &str, room: &str) -> SpeakTurn {
    let event = RoomEvent::new(
        RoomId::new(room).unwrap(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        TextContent::new(body),
    );
    let turn = SpeakTurn {
        channel_id: "ai".into(),
        people: vec!["Alice".into(), "Bob".into()],
        ..SpeakTurn::new(event)
    };
    turn.speakers
        .insert(turn.event.snapshot().id.as_str(), "Alice".into());
    turn
}

fn script(values: &[(&str, f64)]) -> Script {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), ScriptedAnswer::Number(*value)))
        .collect()
}

fn answers(values: &[(&str, f64)]) -> Answers {
    values
        .iter()
        .map(|(name, value)| {
            (
                (*name).to_owned(),
                if *name == "directness" {
                    Answer::Score(ScoreAnswer::new(*value))
                } else {
                    Answer::YesNo(YesNoAnswer {
                        probability: *value,
                    })
                },
            )
        })
        .collect()
}

#[expect(clippy::unwrap_used, reason = "default policy options are valid")]
fn policy(
    scripts: Vec<Script>,
    options: ClassifierSpeakPolicyOptions,
) -> (ClassifierSpeakPolicy, Arc<MockClassifier>) {
    let classifier = Arc::new(MockClassifier::new(MockClassifierOptions {
        answers: scripts,
        ..MockClassifierOptions::default()
    }));
    let policy = ClassifierSpeakPolicy::new(
        Arc::<MockClassifier>::clone(&classifier),
        "Assistant",
        options,
    )
    .unwrap();
    (policy, classifier)
}
