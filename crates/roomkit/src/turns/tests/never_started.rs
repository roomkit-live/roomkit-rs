//! RFC §10.6.2: no turn work is built before the executor starts and accepts.

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::support::{channel, kit_with, of_type, room_id, says};
use crate::turn::TurnKit;
use crate::turns::observe::{Carried, Fate};
use crate::{Channel, ChannelError, ConversationStore};
use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventSource,
    RoomContext, RoomEvent, TextContent,
};

struct Unbuilt {
    calls: Arc<AtomicUsize>,
    _held: Arc<()>,
}

impl Channel for Unbuilt {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: roomkit_core::model::InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::default()))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        // Count the call itself, even if its returned future is never polled.
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(ChannelOutput::default()))
    }
}

// Python: tests/test_work_cancelled_before_start.py::test_a_turn_its_executor_never_ran_leaves_its_channel_unbuilt[closed-at-once]
#[tokio::test(start_paused = true)]
async fn an_executor_cut_before_its_first_poll_builds_no_turn_work() {
    let held = Arc::new(());
    let weak = Arc::downgrade(&held);
    let calls = Arc::new(AtomicUsize::new(0));
    let kit = kit_with(Unbuilt {
        calls: Arc::clone(&calls),
        _held: held,
    })
    .await;
    let binding = kit
        .store()
        .get_binding(&room_id(), &channel("ai"))
        .await
        .unwrap()
        .unwrap();
    let context = RoomContext::new(kit.store().get_room(&room_id()).await.unwrap().unwrap());
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(channel("sms"), ChannelType::SMS),
        TextContent::new("Hi"),
    );
    let turn_kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
    let (turn, ()) = kit
        .inner
        .turns
        .submit(&turn_kit, &binding, event, context, |_| ())
        .unwrap();
    // submit is synchronous. On this current-thread runtime neither the
    // executor nor the owner pump got a first poll before this cut.
    kit.inner.tasks.abort_left_turns();
    drop(turn);
    tokio::time::timeout(Duration::from_secs(1), kit.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(kit.inner.tasks.running(), 0);
    drop(kit);
    assert!(
        weak.upgrade().is_none(),
        "the never-started turn keeps no channel resource"
    );
}

// Python: tests/test_work_cancelled_before_start.py::test_a_turn_its_executor_never_ran_leaves_its_channel_unbuilt[accepted-not-carried]
#[tokio::test(start_paused = true)]
async fn an_executor_whose_accepted_is_refused_builds_no_turn_work() {
    let held = Arc::new(());
    let weak = Arc::downgrade(&held);
    let calls = Arc::new(AtomicUsize::new(0));
    let kit = kit_with(Unbuilt {
        calls: Arc::clone(&calls),
        _held: held,
    })
    .await;
    let refused = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&refused);
    kit.inner.turns.set_fault(Arc::new(move |family, raw| {
        if family == Carried::Frame && raw.contains(of_type("accepted")) {
            seen.fetch_add(1, Ordering::SeqCst);
            Fate {
                refused: Some("the link refused the frame"),
                ..Default::default()
            }
        } else {
            Fate::default()
        }
    }));
    let result = tokio::time::timeout(Duration::from_secs(20), says(&kit, "Hi"))
        .await
        .unwrap()
        .unwrap();
    assert!(result.error.is_some());
    assert_eq!(refused.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    kit.close().await.unwrap();
    assert_eq!(kit.inner.tasks.running(), 0);
    drop(kit);
    assert!(
        weak.upgrade().is_none(),
        "the refused turn keeps no channel resource"
    );
}

// Python: tests/test_work_cancelled_before_start.py's closed-at-once contract,
// applied to the public independent executor close. Admission is synchronous:
// close must wait for an unpolled executor without ever running its channel.
#[tokio::test(start_paused = true)]
async fn a_local_executor_closed_before_its_first_poll_builds_no_turn_work() {
    let calls = Arc::new(AtomicUsize::new(0));
    let kit = kit_with(Unbuilt {
        calls: Arc::clone(&calls),
        _held: Arc::new(()),
    })
    .await;
    let binding = kit
        .store()
        .get_binding(&room_id(), &channel("ai"))
        .await
        .unwrap()
        .unwrap();
    let context = RoomContext::new(kit.store().get_room(&room_id()).await.unwrap().unwrap());
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(channel("sms"), ChannelType::SMS),
        TextContent::new("Hi"),
    );
    let turn_kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
    let (mut turn, ()) = kit
        .inner
        .turns
        .submit(&turn_kit, &binding, event.clone(), context.clone(), |_| ())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), kit.turn_executor().close())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(turn.output(kit.inner.turns.cutting()).await.is_err());
    let previous = kit
        .inner
        .turns
        .records()
        .get(&channel("ai"), &room_id())
        .cloned()
        .unwrap();
    let refusal = kit
        .inner
        .turns
        .submit(&turn_kit, &binding, event, context, |_| ())
        .unwrap_err();
    assert_eq!(
        refusal.attributes.get("reason"),
        Some(&serde_json::json!("no_executor"))
    );
    let after = kit
        .inner
        .turns
        .records()
        .get(&channel("ai"), &room_id())
        .cloned()
        .unwrap();
    assert_eq!(after.turn_id, previous.turn_id);
    kit.close().await.unwrap();
    assert_eq!(kit.inner.tasks.running(), 0);
}
