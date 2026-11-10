//! A shared delegated answer adopts named messages without charging them twice.

use super::*;
use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, InboundMessage, TextContent};
use roomkit::{InboundResult, ProcessInboundOptions};

// Python: tests/test_reentrance_preflight_delegation.py::test_a_shared_delegated_answer_charges_each_adopted_message_once
#[tokio::test]
async fn a_shared_delegated_answer_charges_each_adopted_message_once() {
    let kit = RoomKit::new(RoomKitOptions {
        max_chain_depth: 2,
        ..Default::default()
    });
    kit.register_channel(EchoAgent::new(
        channel("worker"),
        "Found it.",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let answers: Arc<Mutex<Vec<InboundResult>>> = Arc::new(Mutex::new(Vec::new()));
    let rooms = Arc::new(Mutex::new(Vec::new()));
    let weak = kit.downgrade();
    let (seen, children) = (Arc::clone(&answers), Arc::clone(&rooms));
    let once = AtomicBool::new(false);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, context: RoomContext| {
            let selected = text_of(&event.content) == "Found it."
                && context.room.id != room()
                && !once.swap(true, Ordering::SeqCst);
            let kit = weak.upgrade().unwrap();
            let (seen, children) = (Arc::clone(&seen), Arc::clone(&children));
            async move {
                if selected {
                    children.lock().unwrap().push(context.room.id.clone());
                    // The buffered answer uses one of twenty passes. This
                    // depth prevents each nested message asking another turn.
                    for _ in 0..20 {
                        let mut message =
                            InboundMessage::new(channel("sms"), "u", TextContent::new("nested"));
                        message.chain_depth = 2;
                        let result = kit
                            .process_inbound(
                                message,
                                ProcessInboundOptions {
                                    room_id: Some(context.room.id.clone()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap();
                        seen.lock().unwrap().push(result);
                    }
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "nested-calls".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let task = tokio::time::timeout(
        BOUND,
        kit.delegate(
            &room(),
            &channel("worker"),
            "Find it.",
            DelegateOptions {
                wait: true,
                share_channels: Some(vec![channel("sms")]),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*rooms.lock().unwrap(), vec![task.child_room_id]);
    let answers = std::mem::take(&mut *answers.lock().unwrap());
    assert_eq!(answers.len(), 20);
    assert_eq!(
        answers
            .iter()
            .map(|answer| answer.blocked)
            .collect::<Vec<_>>(),
        [vec![false; 19], vec![true]].concat()
    );
    assert!(answers[..19].iter().all(|answer| answer.event.is_some()));
    assert_eq!(answers[19].reason.as_deref(), Some("reentry_loop_cap"));
    assert!(answers[19].event.is_none());
    kit.close().await.unwrap();
}
