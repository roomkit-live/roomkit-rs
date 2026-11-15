//! Context reads survive their scope; live depth does not.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

use roomkit::model::ChannelType;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{AttachChannelOptions, CreateRoomOptions, RoomKit, RoomKitOptions};
use tokio::sync::oneshot;

use super::*;

async fn kit() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let channel = RecordingTransport::new(
        "out".parse().unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(channel).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id("r1")),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id("r1"),
        &"out".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit
}

async fn post(kit: &RoomKit, body: &str) -> u32 {
    let outcome = kit
        .deliver(&room_id("r1"), body, Default::default())
        .await
        .unwrap();
    let id = outcome.event_id.unwrap();
    let events = kit
        .get_timeline(&room_id("r1"), Default::default())
        .await
        .unwrap();
    events
        .iter()
        .find(|event| event.id == id)
        .unwrap()
        .chain_depth
}

fn options(depth: u32) -> ToolTurnContextOptions {
    ToolTurnContextOptions {
        room_id: Some(room_id("r1")),
        actor_id: Some("alice".into()),
        chain_depth: depth,
        ..Default::default()
    }
}

#[tokio::test]
async fn a_delivery_outside_a_scope_starts_at_zero() {
    let kit = kit().await;
    assert_eq!(post(&kit, "outside").await, 0);
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_kept_delivery_outside_a_scope_starts_at_zero() {
    let kit = kit().await;
    let kept = kit.clone();
    let depth = tokio::spawn(roomkit::keep_context(
        async move { post(&kept, "kept").await },
    ))
    .await
    .unwrap();
    assert_eq!(depth, 0);
    kit.close().await.unwrap();
}

// Python: tests/test_tool_turn_context.py::TestLiveDepth::test_an_inner_scope_lends_its_own_depth_then_restores_the_outer
#[tokio::test]
async fn an_inner_scope_lends_its_own_depth_then_restores_the_outer() {
    let kit = kit().await;
    tool_turn_context(options(3), async {
        assert_eq!(post(&kit, "outer").await, 3);
        tool_turn_context(options(1), async {
            assert_eq!(post(&kit, "inner").await, 1);
        })
        .await
        .unwrap();
        assert_eq!(post(&kit, "restored").await, 3);
    })
    .await
    .unwrap();
    assert_eq!(post(&kit, "outside").await, 0);
    kit.close().await.unwrap();
}

// Python: tests/test_tool_turn_context.py::TestLiveDepth::test_a_child_keeps_readers_but_stops_lending_depth_after_exit
#[tokio::test]
async fn a_child_keeps_readers_but_stops_lending_depth_after_exit() {
    let kit = kit().await;
    let (resume, ready) = oneshot::channel();
    let task = tool_turn_context(options(3), async {
        assert_eq!(post(&kit, "in scope").await, 3);
        let kit = kit.clone();
        tokio::spawn(roomkit::keep_context(async move {
            ready.await.unwrap();
            assert_eq!(current_tool_chain_depth(), 3);
            assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
            assert_eq!(post(&kit, "after scope").await, 0);
        }))
    })
    .await
    .unwrap();
    resume.send(()).unwrap();
    task.await.unwrap();
    kit.close().await.unwrap();
}

// Python: tests/test_tool_turn_context.py::TestLiveDepth::test_an_expired_inner_scope_still_masks_the_live_outer_in_its_child
#[tokio::test]
async fn an_expired_inner_scope_still_masks_the_live_outer_in_its_child() {
    let kit = kit().await;
    tool_turn_context(options(3), async {
        let (resume, ready) = oneshot::channel();
        let task = tool_turn_context(options(1), async {
            let kit = kit.clone();
            tokio::spawn(roomkit::keep_context(async move {
                ready.await.unwrap();
                assert_eq!(current_tool_chain_depth(), 1);
                assert_eq!(post(&kit, "child").await, 0);
            }))
        })
        .await
        .unwrap();
        resume.send(()).unwrap();
        task.await.unwrap();
        assert_eq!(current_tool_chain_depth(), 3);
        assert_eq!(post(&kit, "outer still live").await, 3);
    })
    .await
    .unwrap();
    kit.close().await.unwrap();
}

async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

// Rust cancellation counterpart of Python's restoring finally block.
#[tokio::test]
async fn dropping_a_pending_scope_restores_its_parent_and_expires_its_children() {
    let kit = kit().await;
    tool_turn_context(options(3), async {
        let (resume, ready) = oneshot::channel();
        let mut task = None;
        let mut scope = Box::pin(tool_turn_context(options(1), async {
            let kit = kit.clone();
            task = Some(tokio::spawn(roomkit::keep_context(async move {
                ready.await.unwrap();
                assert_eq!(current_tool_chain_depth(), 1);
                assert_eq!(post(&kit, "after drop").await, 0);
            })));
            std::future::pending::<()>().await;
        }));
        poll_pending(scope.as_mut()).await;
        assert_eq!(current_tool_chain_depth(), 3);
        drop(scope);
        resume.send(()).unwrap();
        task.unwrap().await.unwrap();
        assert_eq!(post(&kit, "outer after drop").await, 3);
    })
    .await
    .unwrap();
    kit.close().await.unwrap();
}

// Rust unwind counterpart of Python's exception restoration.
#[tokio::test]
async fn panicking_work_restores_its_parent() {
    tool_turn_context(options(3), async {
        let mut scope = Box::pin(tool_turn_context(options(1), async {
            assert_eq!(current_tool_chain_depth(), 1);
            panic!("handler failed");
        }));
        poll_fn(|cx| {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scope.as_mut().poll(cx)));
            assert!(result.is_err());
            Poll::Ready(())
        })
        .await;
        drop(scope);
        assert_eq!(current_tool_chain_depth(), 3);
    })
    .await
    .unwrap();
    outside();
}

#[tokio::test]
async fn a_borrowed_non_send_future_can_suspend_and_return_a_value() {
    let value = std::rc::Rc::new(std::cell::Cell::new(0));
    let result = tool_turn_context(options(2), async {
        tokio::task::yield_now().await;
        value.set(current_tool_chain_depth());
        "borrowed"
    })
    .await
    .unwrap();
    assert_eq!(result, "borrowed");
    assert_eq!(value.get(), 2);
    outside();
}

#[tokio::test]
async fn interleaved_scopes_do_not_change_their_consumer() {
    let mut one = Box::pin(tool_turn_context(options(1), async {
        assert_eq!(current_tool_chain_depth(), 1);
        tokio::task::yield_now().await;
        assert_eq!(current_tool_chain_depth(), 1);
    }));
    let mut two = Box::pin(tool_turn_context(options(3), async {
        assert_eq!(current_tool_chain_depth(), 3);
        tokio::task::yield_now().await;
        assert_eq!(current_tool_chain_depth(), 3);
    }));
    poll_pending(one.as_mut()).await;
    outside();
    poll_pending(two.as_mut()).await;
    outside();
    one.await.unwrap();
    two.await.unwrap();
    outside();
}
