//! The previous thought is read when a retained calculation finishes.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::InboundMessage;
use roomkit::{HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit};
use roomkit_ai::SharedAiContext;
use roomkit_ai::speaking::{SpeakingFuture, Thinker, ThoughtEvent};
use tokio::sync::Notify;

use super::*;

fn price() -> Thought {
    Thought {
        text: "They look for the price; I know it.".into(),
        want_to_say: vec!["It costs 1,200 $ a year.".into()],
        urgent: false,
    }
}
fn later() -> Thought {
    Thought {
        text: "They settled on ten licences.".into(),
        want_to_say: vec!["Ten gets the discount.".into()],
        urgent: false,
    }
}

struct SlowSecond {
    second: &'static str,
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
    returned: Notify,
}
impl Thinker for SlowSecond {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        _: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                return Ok(price());
            }
            self.entered.notify_one();
            self.release.notified().await;
            self.returned.notify_one();
            match self.second {
                "fails" => Err("model down".into()),
                "same" => Ok(previous.clone()),
                "new" => Ok(later()),
                _ => unreachable!("fixed fixture choices"),
            }
        })
    }
}

async fn say(kit: &RoomKit, body: &str) {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        kit.process_inbound(
            InboundMessage::new(id("sms"), "alice", TextContent::new(body)),
            ProcessInboundOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                ..ProcessInboundOptions::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.error.is_none());
}

// Python: tests/test_thought_reports.py::test_a_call_that_ends_after_the_agent_spoke_reports_against_what_it_spoke_with
#[tokio::test(start_paused = true)]
async fn a_late_call_replaces_the_thought_already_emptied_by_the_answer() {
    for streaming in [false, true] {
        for second in ["fails", "same", "new"] {
            let thinker = Arc::new(SlowSecond {
                second,
                calls: AtomicUsize::new(0),
                entered: Notify::new(),
                release: Notify::new(),
                returned: Notify::new(),
            });
            let model = model(streaming);
            let kit = kit::setup(
                false,
                AiChannelOptions {
                    thinker: Some(Arc::<SlowSecond>::clone(&thinker)),
                    think_wait: Duration::from_millis(100),
                    speak_policy: Some(Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
                        decisions: vec![
                            SpeakMode::Silent.into(),
                            SpeakMode::Silent.into(),
                            SpeakMode::Silent.into(),
                            SpeakMode::Speak.into(),
                        ],
                        ..MockSpeakPolicyOptions::default()
                    }))),
                    ..AiChannelOptions::default()
                },
                model.clone(),
            )
            .await;
            let seen = Arc::new(Mutex::new(Vec::<ThoughtEvent>::new()));
            let reports = Arc::clone(&seen);
            kit.hook(
                HookTrigger::OnThought,
                move |event: ThoughtEvent, _: RoomContext| {
                    reports.lock().unwrap().push(event);
                    std::future::ready(())
                },
                HookOptions {
                    execution: HookExecution::Async,
                    ..HookOptions::default()
                },
            )
            .unwrap();
            say(&kit, "a").await;
            say(&kit, "b").await;
            tokio::time::timeout(Duration::from_secs(3), thinker.entered.notified())
                .await
                .unwrap();
            say(&kit, "Nova?").await;
            assert_eq!(model.calls().len(), 1);
            assert!(
                model
                    .calls()
                    .first()
                    .unwrap()
                    .messages
                    .last()
                    .unwrap()
                    .text()
                    .contains("It costs 1,200 $ a year.")
            );
            thinker.release.notify_one();
            // This current-thread fixture wakes after think returned. The
            // worker stores the result and registers its observer in that poll.
            tokio::time::timeout(Duration::from_secs(3), thinker.returned.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), roomkit::testing::idle(&kit))
                .await
                .unwrap();
            kit.close().await.unwrap();
            assert_eq!(thinker.calls.load(Ordering::Relaxed), 2);
            let mut actual = seen
                .lock()
                .unwrap()
                .iter()
                .map(|event| {
                    (
                        event.thought.clone(),
                        event.previous.clone(),
                        event.duration_ms.is_none(),
                    )
                })
                .collect::<Vec<_>>();
            let mut expected = vec![
                (price(), Thought::default(), false),
                (price().said(), price(), true),
            ];
            if second == "new" {
                expected.push((later().said(), price().said(), false));
            }
            // Tracked observers may run in either order; each pair and its
            // duration's presence remain exact.
            actual.sort_by_key(|record| format!("{record:?}"));
            expected.sort_by_key(|record| format!("{record:?}"));
            assert_eq!(actual, expected, "{second}, streaming={streaming}");
        }
    }
}
