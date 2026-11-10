//! ASYNC `BEFORE_BROADCAST` sees the final event only after its yielding commit.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, ModifyOptions};
use roomkit::model::{ChannelType, EventSource, HookResult, InjectedEvent, RoomEvent, TextContent};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    AttachChannelOptions, HookExecution, HookOptions, HookTrigger, HostChannel, PostOptions,
    RegenerateResponseOptions, RoomKit, SendEventOptions,
};
use roomkit_ai::{MockAiProvider, MockAiProviderOptions, Tools};
use roomkit_core::rules::transcoding::text_of;

use crate::held::{Holding, attach_agent, into, kit_of, room, said};
use crate::store::StuckStore;
use crate::support::channel;

fn observers(kit: &RoomKit<StuckStore>) {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |mut event: RoomEvent, _| async move {
            event.content =
                TextContent::new(format!("{} (labelled)", text_of(&event.content))).into();
            HookResult::modify(event, ModifyOptions::default())
        },
        HookOptions {
            name: "label".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let steps = Arc::clone(&kit.store().commit_steps);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let steps = Arc::clone(&steps);
            async move {
                steps
                    .lock()
                    .unwrap()
                    .push(format!("observed {}", text_of(&event.content)));
            }
        },
        HookOptions {
            name: "audit".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
}

fn injection(kit: &RoomKit<StuckStore>) {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| async move {
            let note = RoomEvent::new(
                room("r"),
                EventSource::new(channel("system"), ChannelType::SYSTEM),
                TextContent::new("Note."),
            );
            HookResult::allow(AllowOptions {
                injected: (text_of(&event.content) == "Hi. (labelled)").then(|| {
                    vec![InjectedEvent {
                        target_channel_ids: Some(vec![channel("ws")]),
                        ..InjectedEvent::new(note)
                    }]
                }),
            })
        },
        HookOptions {
            name: "inject".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
}

async fn enter(kit: &RoomKit<StuckStore>, door: &str) {
    match door {
        "send" => {
            kit.send_event(
                &room("r"),
                &channel("sms"),
                TextContent::new("Hi."),
                SendEventOptions::default(),
            )
            .await
            .unwrap();
            return;
        }
        "post" => {
            kit.register_channel(HostChannel::default()).unwrap();
            kit.attach_channel(
                &room("r"),
                &channel("host"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
            kit.post(&room("r"), TextContent::new("Hi."), PostOptions::default())
                .await
                .unwrap();
            return;
        }
        "agent" | "agent-stream" | "regenerate" => {
            let model = MockAiProvider::new(MockAiProviderOptions {
                responses: vec!["Hello.".to_owned(), "Again.".to_owned()],
                streaming: door == "agent-stream",
                ..MockAiProviderOptions::default()
            });
            attach_agent(kit, model, Tools::new()).await;
        }
        "response" => {
            kit.register_channel(EchoAgent::new(
                channel("agent"),
                "Hello.",
                EchoAgentOptions::default(),
            ))
            .unwrap();
            kit.attach_channel(
                &room("r"),
                &channel("agent"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
        "injected" => injection(kit),
        "inbound" => (),
        _ => unreachable!("unknown door"),
    }
    kit.process_inbound(said("Hi."), into("r")).await.unwrap();
    if door == "regenerate" {
        kit.regenerate_response(&room("r"), RegenerateResponseOptions::default())
            .await
            .unwrap()
            .unwrap();
    }
}

// Python: tests/test_kit_close_doors.py::test_a_before_broadcast_observer_starts_once_its_pass_is_over
// The eight memory doors; a yielding store commit is a native fault seam.
#[tokio::test(start_paused = true)]
async fn each_pass_commits_its_modified_event_before_starting_its_observer() {
    for (door, expected) in [
        ("inbound", vec!["Hi."]),
        ("send", vec!["Hi."]),
        ("post", vec!["Hi."]),
        ("agent", vec!["Hi.", "Hello."]),
        ("agent-stream", vec!["Hi.", "Hello."]),
        ("response", vec!["Hi.", "Hello."]),
        ("regenerate", vec!["Hi.", "Hello.", "Again."]),
        ("injected", vec!["Hi.", "Note."]),
    ] {
        let kit = kit_of(StuckStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
        kit.store().yield_commits.store(true, Ordering::SeqCst);
        observers(&kit);
        enter(&kit, door).await;
        kit.close().await.unwrap();
        let steps = kit.store().commit_steps.lock().unwrap().clone();
        let observed: Vec<_> = steps
            .iter()
            .filter_map(|step| step.strip_prefix("observed "))
            .collect();
        let expected: Vec<_> = expected
            .iter()
            .map(|body| format!("{body} (labelled)"))
            .collect();
        assert_eq!(observed, expected, "{door}: {steps:?}");
        for body in observed {
            let index = |prefix: &str| {
                steps
                    .iter()
                    .position(|step| *step == format!("{prefix} {body}"))
                    .unwrap()
            };
            assert!(index("committed") < index("observed"), "{door}: {steps:?}");
        }
    }
}
