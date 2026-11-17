//! Real rooms isolate and coalesce the work, and channel close cuts its call.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use roomkit::model::InboundMessage;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit,
};
use roomkit_ai::SharedAiContext;
use roomkit_ai::speaking::{SpeakingFuture, Thinker, ThoughtEvent};
use tokio::sync::Notify;

use super::*;

#[derive(Default)]
struct RoomCalls {
    calls: Mutex<Vec<(Thought, SharedAiContext)>>,
    entered: Notify,
    release: Notify,
    cut: AtomicBool,
}

#[derive(Default)]
struct Held {
    rooms: Mutex<BTreeMap<String, Arc<RoomCalls>>>,
    closed: AtomicUsize,
}

impl Held {
    fn room(&self, name: &str) -> Arc<RoomCalls> {
        Arc::clone(self.rooms.lock().unwrap().entry(name.into()).or_default())
    }
}

struct Cut(Arc<RoomCalls>, bool);
impl Drop for Cut {
    fn drop(&mut self) {
        if self.1 {
            self.0.cut.store(true, Ordering::Release);
        }
    }
}

impl Thinker for Held {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            let room_id = context.snapshot().room.unwrap().room.id.to_string();
            let room = self.room(&room_id);
            let number = {
                let mut calls = room.calls.lock().unwrap();
                calls.push((previous.clone(), context.clone()));
                calls.len()
            };
            if number == 1 {
                let mut cut = Cut(Arc::clone(&room), true);
                room.entered.notify_one();
                room.release.notified().await;
                cut.1 = false;
            }
            Ok(Thought {
                text: format!("{room_id} thought {number}"),
                ..Thought::default()
            })
        })
    }
    fn close(&self) -> SpeakingFuture<'_, ()> {
        self.closed.fetch_add(1, Ordering::Release);
        Box::pin(std::future::ready(Ok(())))
    }
}

// Python: tests/test_turns_between.py::test_a_thought_at_once_reaches_the_room_before_the_turn_s_output
// Python: tests/test_turns_between.py::test_a_thinker_running_across_turns_reports_under_the_latest_turn
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thought_observers_post_under_the_turn_that_supplied_their_context() {
    for agent in [false, true] {
        for latest in [false, true] {
            let thinker = Arc::new(Held::default());
            let one = thinker.room("one");
            let (kit, _) = setup(agent, Arc::clone(&thinker)).await;
            let expected = if latest {
                "one thought 2"
            } else {
                "one thought 1"
            };
            let settled = Arc::new(Notify::new());
            let done = Arc::clone(&settled);
            let failures = Arc::new(Mutex::new(Vec::new()));
            let errors = Arc::clone(&failures);
            kit.hook(
                HookTrigger::OnThought,
                move |event: ThoughtEvent, _: RoomContext| {
                    let (done, errors) = (Arc::clone(&done), Arc::clone(&errors));
                    async move {
                        let host = roomkit::current_turn_host().unwrap();
                        let outcome = host
                            .post_status(
                                "ai",
                                "thought",
                                roomkit::orchestration::StatusLevel::Info,
                                roomkit::PostStatusOptions {
                                    detail: event.thought.text.clone(),
                                    ..roomkit::PostStatusOptions::default()
                                },
                            )
                            .await;
                        if let Err(error) = outcome {
                            errors.lock().unwrap().push(error.to_string());
                        }
                        if event.thought.text == expected {
                            done.notify_one();
                        }
                    }
                },
                HookOptions {
                    execution: HookExecution::Async,
                    ..HookOptions::default()
                },
            )
            .unwrap();
            if !latest {
                one.release.notify_one();
            }
            say(&kit, "one", "first").await;
            if latest {
                tokio::time::timeout(Duration::from_secs(3), one.entered.notified())
                    .await
                    .unwrap();
                say(&kit, "one", "latest").await;
                one.release.notify_one();
            }
            tokio::time::timeout(Duration::from_secs(3), settled.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), roomkit::testing::idle(&kit))
                .await
                .unwrap();
            let posted = kit
                .status_bus()
                .recent(roomkit::orchestration::RecentOptions::default())
                .await;
            assert_eq!(
                posted
                    .iter()
                    .filter(|entry| entry.action == "thought")
                    .map(|entry| entry.detail.as_str())
                    .collect::<Vec<_>>(),
                [expected]
            );
            assert!(failures.lock().unwrap().len() <= usize::from(latest));
            tokio::time::timeout(Duration::from_secs(3), kit.close())
                .await
                .unwrap()
                .unwrap();
        }
    }
}

async fn setup(agent: bool, thinker: Arc<Held>) -> (RoomKit, MockAiProvider) {
    let model = model(false);
    let policy = MockSpeakPolicy::new(MockSpeakPolicyOptions {
        decisions: vec![SpeakMode::Silent.into()],
        ..MockSpeakPolicyOptions::default()
    });
    let kit = kit::setup(
        agent,
        AiChannelOptions {
            thinker: Some(thinker),
            think_wait: Duration::ZERO,
            speak_policy: Some(Arc::new(policy)),
            ..AiChannelOptions::default()
        },
        model.clone(),
    )
    .await;
    for room in ["one", "two"] {
        let room = RoomId::new(room).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
        for channel in ["sms", "ai"] {
            kit.attach_channel(&room, &id(channel), AttachChannelOptions::default())
                .await
                .unwrap();
        }
    }
    (kit, model)
}

async fn say(kit: &RoomKit, room: &str, text: &str) {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        kit.process_inbound(
            InboundMessage::new(id("sms"), "alice", TextContent::new(text)),
            ProcessInboundOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                ..ProcessInboundOptions::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.error.is_none());
}

// Python: tests/test_speaking_room_lifecycle.py::test_real_messages_keep_room_thoughts_separate_and_coalesce_pending_contexts
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_messages_keep_room_thoughts_separate_and_coalesce_pending_contexts() {
    for agent in [false, true] {
        let thinker = Arc::new(Held::default());
        let (kit, model) = setup(agent, Arc::clone(&thinker)).await;
        let reports = Arc::new(Mutex::new(Vec::<ThoughtEvent>::new()));
        let reported = Arc::new(Notify::new());
        let (seen, done) = (Arc::clone(&reports), Arc::clone(&reported));
        kit.hook(
            HookTrigger::OnThought,
            move |event: ThoughtEvent, _: RoomContext| {
                let mut seen = seen.lock().unwrap();
                seen.push(event);
                if seen.len() == 3 {
                    done.notify_one();
                }
                std::future::ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        let (one, two) = (thinker.room("one"), thinker.room("two"));
        say(&kit, "one", "first").await;
        tokio::time::timeout(Duration::from_secs(3), one.entered.notified())
            .await
            .unwrap();
        say(&kit, "two", "other room").await;
        tokio::time::timeout(Duration::from_secs(3), two.entered.notified())
            .await
            .unwrap();
        say(&kit, "one", "superseded").await;
        say(&kit, "one", "latest").await;
        assert_eq!(
            (
                one.calls.lock().unwrap().len(),
                two.calls.lock().unwrap().len()
            ),
            (1, 1)
        );
        one.release.notify_one();
        two.release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), reported.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), kit.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.calls(), Vec::new());
        let (one, two) = (one.calls.lock().unwrap(), two.calls.lock().unwrap());
        assert_eq!((one.len(), two.len()), (2, 1));
        assert_eq!(one.first().unwrap().0, Thought::default());
        assert_eq!(one.last().unwrap().0.text, "one thought 1");
        assert!(
            one.last()
                .unwrap()
                .1
                .snapshot()
                .messages
                .last()
                .unwrap()
                .text()
                .contains("latest")
        );
        assert_eq!(two.first().unwrap().0, Thought::default());
        for (name, calls) in [("one", &*one), ("two", &*two)] {
            assert!(
                calls
                    .iter()
                    .all(|(_, context)| context.snapshot().room.unwrap().room.id.as_str() == name)
            );
        }
        let mut transitions = reports
            .lock()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e.room_id.clone(),
                    e.thought.text.clone(),
                    e.previous.text.clone(),
                )
            })
            .collect::<Vec<_>>();
        transitions.sort();
        assert_eq!(
            transitions,
            [
                ("one", "one thought 1", ""),
                ("one", "one thought 2", "one thought 1"),
                ("two", "two thought 1", "")
            ]
            .map(|(a, b, c)| (a.into(), b.into(), c.into()))
        );
        assert_eq!(thinker.closed.load(Ordering::Acquire), 0);
    }
}

// Python: tests/test_speaking_room_lifecycle.py::test_close_cuts_a_thinker_that_really_entered_without_reporting_a_new_thought
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_cuts_a_thinker_that_really_entered_without_reporting_a_new_thought() {
    for agent in [false, true] {
        let thinker = Arc::new(Held::default());
        let (kit, model) = setup(agent, Arc::clone(&thinker)).await;
        let reports = Arc::new(Mutex::new(Vec::<ThoughtEvent>::new()));
        let seen = Arc::clone(&reports);
        kit.hook(
            HookTrigger::OnThought,
            move |event: ThoughtEvent, _: RoomContext| {
                seen.lock().unwrap().push(event);
                std::future::ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        let one = thinker.room("one");
        say(&kit, "one", "listen to this").await;
        tokio::time::timeout(Duration::from_secs(3), one.entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), kit.close())
            .await
            .unwrap()
            .unwrap();
        assert!(one.cut.load(Ordering::Acquire));
        assert_eq!(*reports.lock().unwrap(), Vec::new());
        assert_eq!(model.calls(), Vec::new());
        assert_eq!(one.calls.lock().unwrap().len(), 1);
        assert_eq!(thinker.closed.load(Ordering::Acquire), 0);
    }
}
