//! Public policy witnesses from Python's `tests/test_answer_only.py`.

use std::sync::{Arc, Mutex};

use roomkit::model::{ChannelId, ChannelType, EventSource, RoomEvent, RoomId, TextContent};
use roomkit::{ChannelError, ClassedError};
use roomkit_ai::speaking::{
    AlwaysSpeak, AnswerOnly, MockSpeakPolicy, MockSpeakPolicyOptions, SpeakDecision, SpeakMode,
    SpeakPolicy, SpeakTurn, Speakers, SpeakingFuture,
};

#[expect(clippy::unwrap_used, reason = "fixed test fixture identifiers")]
fn turn(name: Option<&str>, people: &[&str]) -> SpeakTurn {
    let event = RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        TextContent::new("Hello?"),
    );
    let speakers = Speakers::new();
    if let Some(name) = name {
        speakers.insert(event.id.as_str(), name.to_owned());
    }
    SpeakTurn {
        speakers,
        channel_id: "ai".into(),
        people: people.iter().map(|s| (*s).into()).collect(),
        ..SpeakTurn::new(event)
    }
}

// Python: test_a_speaker_not_answered_is_only_listened_to_without_asking_the_policy.
// Python: test_a_speaker_the_room_does_not_name_is_not_answered.
#[tokio::test]
async fn a_speaker_not_answered_is_only_listened_to_without_asking_the_policy() {
    for speaker in [Some("TV"), None, Some("")] {
        let inner = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions::default()));
        let policy = AnswerOnly::new(Arc::<MockSpeakPolicy>::clone(&inner), ["Sylvain"]).unwrap();
        let decision = policy
            .decide(&turn(speaker, &["Sylvain", "TV"]))
            .await
            .unwrap();
        assert_eq!(decision.mode, SpeakMode::Silent);
        assert_eq!(decision.reason, "only listened to");
        assert!(decision.r#final);
        assert_eq!(inner.turns(), Vec::<SpeakTurn>::new());
    }
}

// Python: test_a_speaker_answered_is_the_wrapped_policys_with_only_the_people_answered.
// Python: test_the_speaker_answered_counts_when_no_participant_record_names_them.
#[tokio::test]
async fn a_speaker_answered_is_the_wrapped_policys_with_only_the_people_answered() {
    for (people, expected) in [
        (["Sylvain", "TV"], vec!["Sylvain"]),
        (["Living room", "Paul"], vec!["Paul", "Sylvain"]),
    ] {
        let inner = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Offer.into()],
            ..MockSpeakPolicyOptions::default()
        }));
        let policy =
            AnswerOnly::new(Arc::<MockSpeakPolicy>::clone(&inner), ["Sylvain", "Paul"]).unwrap();
        let turn = turn(Some("Sylvain"), &people);
        assert_eq!(policy.decide(&turn).await.unwrap().mode, SpeakMode::Offer);
        let calls = inner.turns();
        assert_eq!(calls.len(), 1);
        let asked = calls.first().unwrap();
        assert!(asked.event.same_object(&turn.event));
        assert_eq!(asked.people, expected);
    }
}

// Python: test_names_compare_as_the_room_gives_them.
// Python: test_a_speaker_is_matched_by_the_name_without_its_rank.
#[tokio::test]
async fn names_compare_as_the_room_gives_them_and_without_their_rank() {
    for configured in ["sylvain", "  SYLVAIN ", "Sylvain:"] {
        let policy = AnswerOnly::new(Arc::new(AlwaysSpeak), [configured]).unwrap();
        assert!(policy.answers(Some("sylvain")));
        assert!(!policy.answers(Some("Paul")));
        assert!(!policy.answers(None));
        for speaker in [
            "Sylvain",
            "Sylvain (2)",
            "SYLVAIN (2)",
            "Sylvain (a participant)",
        ] {
            assert_eq!(
                policy
                    .decide(&turn(Some(speaker), &["Sylvain"]))
                    .await
                    .unwrap()
                    .mode,
                SpeakMode::Speak
            );
        }
    }
}

// Python: test_answering_no_one_is_refused.
#[test]
fn answering_no_one_is_refused() {
    for people in [vec![], vec!["", "  "], vec![":"]] {
        let error = AnswerOnly::new(Arc::new(AlwaysSpeak), people).unwrap_err();
        assert_eq!(
            error.downcast_ref::<ClassedError>().unwrap().class(),
            "ValueError"
        );
        assert_eq!(
            error.to_string(),
            "AnswerOnly needs at least one person to answer"
        );
    }
}

struct Lifecycle {
    calls: Arc<Mutex<Vec<String>>>,
    fails: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Failed(&'static str);

impl SpeakPolicy for Lifecycle {
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        AlwaysSpeak.decide(turn)
    }
    #[expect(
        clippy::unwrap_used,
        reason = "a poisoned fixture recorder fails the test"
    )]
    fn forget_room(&self, room: &str) -> Result<(), ChannelError> {
        self.calls.lock().unwrap().push(room.into());
        if self.fails {
            Err(Box::new(Failed("forget failed")))
        } else {
            Ok(())
        }
    }
    #[expect(
        clippy::unwrap_used,
        reason = "a poisoned fixture recorder fails the test"
    )]
    fn close(&self) -> SpeakingFuture<'_, ()> {
        Box::pin(async {
            self.calls.lock().unwrap().push("close".into());
            if self.fails {
                let error: ChannelError = Box::new(Failed("close failed"));
                Err(error)
            } else {
                Ok(())
            }
        })
    }
}

// Python: test_close_closes_the_wrapped_policy.
// Python: test_speaking_public_models.py::test_answer_only_preserves_the_wrapped_lifecycle_errors.
#[tokio::test]
async fn answer_only_delegates_lifecycle_and_keeps_its_errors() {
    for fails in [false, true] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let policy = AnswerOnly::new(
            Arc::new(Lifecycle {
                calls: Arc::clone(&calls),
                fails,
            }),
            ["Alice"],
        )
        .unwrap();
        let forgotten = policy.forget_room("r1");
        let closed = policy.close().await;
        if fails {
            assert_eq!(
                forgotten.unwrap_err().downcast_ref::<Failed>().unwrap().0,
                "forget failed"
            );
            assert_eq!(
                closed.unwrap_err().downcast_ref::<Failed>().unwrap().0,
                "close failed"
            );
        } else {
            forgotten.unwrap();
            closed.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), ["r1", "close"]);
    }
}
