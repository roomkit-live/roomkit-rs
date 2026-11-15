use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{ChannelType, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DeliverOptions, GetTimelineOptions, HookExecution,
    HookOptions, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiGenerationEvent, AiResponseEvent, ToolHandler, ToolResult,
};
use tokio::sync::oneshot;

use super::support::{Seen, binding, model, snapshot};

const BOUND: Duration = Duration::from_secs(10);
type Child = (oneshot::Sender<()>, tokio::task::JoinHandle<(Seen, u32)>);

pub(super) async fn kit() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        "sms1".parse().unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    for room in ["r1", "probe"] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.parse().unwrap()),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
        kit.attach_channel(
            &room.parse().unwrap(),
            &"sms1".parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}

async fn post(kit: &RoomKit, text: &str) -> u32 {
    let result = kit
        .deliver(&"probe".parse().unwrap(), text, DeliverOptions::default())
        .await
        .unwrap();
    let events = kit
        .get_timeline(&"probe".parse().unwrap(), GetTimelineOptions::default())
        .await
        .unwrap();
    events
        .iter()
        .find(|event| Some(&event.id) == result.event_id.as_ref())
        .unwrap()
        .chain_depth
}

fn child(kit: RoomKit, text: &'static str) -> Child {
    let (resume, ready) = oneshot::channel();
    let task = tokio::spawn(roomkit::keep_context(async move {
        ready.await.unwrap();
        (snapshot(text), post(&kit, text).await)
    }));
    (resume, task)
}

async fn finish(child: Child) -> (Seen, u32) {
    child.0.send(()).unwrap();
    tokio::time::timeout(BOUND, child.1).await.unwrap().unwrap()
}

fn observe(kit: &RoomKit, after: &Arc<Mutex<Vec<Seen>>>) {
    let seen = Arc::clone(after);
    kit.hook(
        HookTrigger::OnAiResponse,
        move |_: AiResponseEvent, _| {
            seen.lock().unwrap().push(snapshot("after"));
            async {}
        },
        HookOptions {
            name: "read turn".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let seen = Arc::clone(after);
    kit.hook(
        HookTrigger::BeforeAiGeneration,
        move |_: AiGenerationEvent, _| {
            seen.lock().unwrap().push(snapshot("generation"));
            async {}
        },
        HookOptions {
            name: "read preparation".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let seen = Arc::clone(after);
    kit.hook(
        HookTrigger::OnToolCall,
        move |_: roomkit::ToolCallEvent, _| {
            seen.lock().unwrap().push(snapshot("tool observer"));
            async {}
        },
        HookOptions {
            name: "read call".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
}

// Python: test_ai_turn_context_lifetime.py::test_stream_consumption_does_not_revive_preparation_children
// Python: TestLoopContextRestoration::test_the_turn_context_outlives_the_loop_within_the_turn
// The public kit consumes the stream after on_event has handed it back.
#[tokio::test]
async fn stream_consumption_does_not_revive_preparation_children() {
    for streaming in [false, true] {
        let kit = kit().await;
        let preparation = Arc::new(Mutex::new(None));
        let consumed = Arc::new(Mutex::new(None));
        let after = Arc::new(Mutex::new(Vec::new()));
        let (prepared, config_kit) = (Arc::clone(&preparation), kit.clone());
        let (served, stream_child, tool_kit) =
            (Arc::clone(&preparation), Arc::clone(&consumed), kit.clone());
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(streaming, true),
            AiChannelOptions {
                config_provider: Some(Arc::new(move |_, _| {
                    snapshot("config").check("r1", None, 3);
                    *prepared.lock().unwrap() =
                        Some(child(config_kit.clone(), "preparation child"));
                    Box::pin(async { Ok(None) })
                })),
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    let (served, stream_child, kit) = (
                        Arc::clone(&served),
                        Arc::clone(&stream_child),
                        tool_kit.clone(),
                    );
                    async move {
                        let live = snapshot("handler");
                        live.check("r1", None, 3);
                        let prepared = served.lock().unwrap().take();
                        if let Some(prepared) = prepared {
                            let (captured, depth) = finish(prepared).await;
                            captured.check("r1", None, 3);
                            assert!(Arc::ptr_eq(
                                live.room.as_ref().unwrap(),
                                captured.room.as_ref().unwrap()
                            ));
                            assert_eq!(depth, 0, "preparation ended before stream consumption");
                            *stream_child.lock().unwrap() =
                                Some(child(kit.clone(), "stream child"));
                            assert_eq!(post(&kit, "live stream").await, 3);
                        }
                        Ok(ToolResult::Text("ok".into()))
                    }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        kit.register_channel(agent).unwrap();
        kit.attach_channel(
            &"r1".parse().unwrap(),
            &"ai1".parse().unwrap(),
            AttachChannelOptions {
                metadata: binding("r1").metadata,
                ..AttachChannelOptions::default()
            },
        )
        .await
        .unwrap();
        observe(&kit, &after);
        tokio::time::timeout(
            BOUND,
            kit.send_event(
                &"r1".parse().unwrap(),
                &"sms1".parse().unwrap(),
                TextContent::new("go"),
                SendEventOptions {
                    chain_depth: 2,
                    ..SendEventOptions::default()
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let child = consumed.lock().unwrap().take().unwrap();
        let (captured, depth) = finish(child).await;
        captured.check("r1", None, 3);
        assert_eq!(depth, 0, "the consumed stream ended");
        roomkit::testing::idle(&kit).await;
        {
            let after = after.lock().unwrap();
            assert_eq!(after.len(), 4);
            for value in &*after {
                value.check("r1", None, 3);
            }
        }
        kit.close().await.unwrap();
    }
}

pub(super) async fn pending<F: Future>(mut work: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

// Python: test_ai_turn_context_lifetime.py::test_stream_consumption_does_not_revive_preparation_children
// The direct Rust channel collects its answer, but preparation must still end
// before it starts consuming that answer's stream.
#[tokio::test]
async fn direct_consumption_does_not_revive_preparation_children() {
    use roomkit::Channel;
    let kit = kit().await;
    let kept = Arc::new(Mutex::new(None));
    let (prepared, probe) = (Arc::clone(&kept), kit.clone());
    let agent = AiChannel::new(
        "ai1".parse().unwrap(),
        model(true, true),
        AiChannelOptions {
            config_provider: Some(Arc::new(move |_, _| {
                *prepared.lock().unwrap() = Some(child(probe.clone(), "direct preparation child"));
                Box::pin(async { Ok(None) })
            })),
            tool_handler: Some(ToolHandler::new(move |_, _| {
                let child = kept.lock().unwrap().take();
                async move {
                    if let Some(child) = child {
                        let (read, depth) = finish(child).await;
                        read.check("r1", Some("alice"), 3);
                        assert_eq!(depth, 0, "direct preparation ended before collecting");
                    }
                    Ok(ToolResult::Text("ok".into()))
                }
            })),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let (event, context) = super::support::event("r1", Some("alice"));
    agent.on_event(event, binding("r1"), context).await.unwrap();
    kit.close().await.unwrap();
}

// Python: test_ai_turn_context_lifetime.py::test_cancelling_a_stream_expires_its_child_depth
#[tokio::test]
async fn cancelling_a_stream_expires_its_child_depth() {
    use roomkit::Channel;
    for streaming in [false, true] {
        let kit = kit().await;
        let kept = Arc::new(Mutex::new(None));
        let (child_slot, probe) = (Arc::clone(&kept), kit.clone());
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(streaming, true),
            AiChannelOptions {
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    *child_slot.lock().unwrap() = Some(child(probe.clone(), "cancelled child"));
                    async { std::future::pending().await }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let (event, context) = super::support::event("r1", Some("alice"));
        let mut work = Box::pin(agent.on_event(event, binding("r1"), context));
        pending(work.as_mut()).await;
        assert!(roomkit_ai::current_tool_room_id().is_none());
        drop(work);
        let child = kept.lock().unwrap().take().unwrap();
        let (read, depth) = finish(child).await;
        read.check("r1", Some("alice"), 3);
        assert_eq!(depth, 0);
        assert!(roomkit_ai::current_tool_room_id().is_none());
        kit.close().await.unwrap();
    }
}
