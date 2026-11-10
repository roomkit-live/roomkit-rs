//! A pending scope read keeps its captured budget after the observer ends.

use crate::held::{into, room, said, stored};
use crate::store::StuckStore;
use crate::support::channel;
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, RoomEvent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, idle};
use roomkit::{
    AttachChannelOptions, CancelDeliveryOptions, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions, keep_context,
};
use roomkit_core::rules::transcoding::text_of;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(5);

async fn scoped_kit() -> RoomKit<StuckStore> {
    let kit = RoomKit::new(RoomKitOptions {
        max_chain_depth: 1,
        ..RoomKitOptions::with_store(StuckStore::default())
    });
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("r")),
        organization_id: Some("org".to_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room("r"), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

async fn check_key(kit: &RoomKit<StuckStore>) {
    for (org, expected) in [("other", 0), ("org", 1)] {
        assert_eq!(
            kit.cancel_delivery(
                "pending-scope",
                CancelDeliveryOptions {
                    organization_id: Some(org.to_owned()),
                    ..Default::default()
                }
            )
            .await,
            expected
        );
    }
}

async fn check(cancel: bool) {
    let kit = scoped_kit().await;
    let child = Arc::new(Mutex::new(None));
    let descendants = Arc::new(Mutex::new(Vec::new()));
    let body_returning = Arc::new(Notify::new());
    let weak = kit.downgrade();
    let (spawned, replies, returning) = (
        Arc::clone(&child),
        Arc::clone(&descendants),
        Arc::clone(&body_returning),
    );
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let kit = weak.upgrade().unwrap();
            let (spawned, replies, returning) = (
                Arc::clone(&spawned),
                Arc::clone(&replies),
                Arc::clone(&returning),
            );
            async move {
                match text_of(&event.content).as_str() {
                    "hello" => {
                        for _ in 0..9 {
                            let answer =
                                kit.process_inbound(said("again"), into("r")).await.unwrap();
                            assert!(!answer.blocked);
                        }
                        *kit.store().held_read.lock().unwrap() = Some("room");
                        let next = kit.clone();
                        *spawned.lock().unwrap() = Some(tokio::spawn(keep_context(async move {
                            let mut message = said("last");
                            message.delivery_key = Some("pending-scope".to_owned());
                            next.process_inbound(
                                message,
                                ProcessInboundOptions {
                                    organization_id: Some("org".to_owned()),
                                    ..into("r")
                                },
                            )
                            .await
                        })));
                        kit.store().reading.notified().await;
                        *kit.store().held_read.lock().unwrap() = None;
                        returning.notify_one();
                    }
                    "last" => {
                        let result = kit
                            .process_inbound(said("descendant"), into("r"))
                            .await
                            .unwrap();
                        replies.lock().unwrap().push(result);
                    }
                    _ => {}
                }
            }
        },
        HookOptions {
            name: "scope-child".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    kit.process_inbound(said("hello"), into("r")).await.unwrap();
    tokio::time::timeout(BOUND, body_returning.notified())
        .await
        .unwrap();
    // The raw child is still in preflight; idle drains the actual observer.
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    let child = child.lock().unwrap().take().unwrap();
    assert!(!child.is_finished());
    if cancel {
        check_key(&kit).await;
    }
    kit.store().release_read.notify_one();
    let answer = tokio::time::timeout(BOUND, child)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!answer.blocked && answer.event.is_some());
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    let replies = std::mem::take(&mut *descendants.lock().unwrap());
    assert_eq!(replies.len(), 1);
    let refused = replies.first().unwrap();
    assert!(refused.blocked && refused.event.is_none());
    assert_eq!(refused.reason.as_deref(), Some("reentry_loop_cap"));
    let expected: Vec<_> = std::iter::once("hello".to_owned())
        .chain(std::iter::repeat_n("again".to_owned(), 9))
        .chain(["last".to_owned()])
        .collect();
    assert_eq!(stored(&kit, "r").await, expected);
    kit.close().await.unwrap();
}

// Python: tests/test_reentrance_preflight_scope.py::test_a_scope_read_outliving_its_observer_keeps_its_budget_and_key
#[tokio::test]
async fn a_scope_read_outliving_its_observer_keeps_its_budget_and_key() {
    for cancel in [false, true] {
        check(cancel).await;
    }
}
