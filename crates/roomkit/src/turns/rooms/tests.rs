//! Python: `tests/test_turns_channel_control.py`, bounded executor state.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit_core::model::{
    ChannelBinding, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use tokio::sync::Notify;

use super::*;
use crate::{Channel, RoomKitError};

#[derive(Default)]
struct Drops {
    rooms: Mutex<Vec<RoomId>>,
    failed: AtomicBool,
    dropped: Notify,
}

struct Agent(Arc<Drops>);
impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        channel()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
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
        ready(Ok(ChannelOutput::empty()))
    }
    fn on_room_detached(
        &self,
        room_id: RoomId,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        if self.0.failed.swap(false, Ordering::SeqCst) {
            return ready(Err(RoomKitError::Runtime("store down".to_owned()).into()));
        }
        lock(&self.0.rooms).push(room_id);
        self.0.dropped.notify_one();
        ready(Ok(()))
    }
}

fn channel() -> ChannelId {
    ChannelId::new("agent").unwrap()
}
fn key(room: &str) -> Key {
    (channel(), RoomId::new(room).unwrap())
}
fn previous(id: &str) -> PreviousTurn {
    PreviousTurn {
        turn_id: id.to_owned(),
        executor_id: None,
    }
}

// Python: test_a_record_that_falls_out_drops_the_state_it_vouched_for.
#[tokio::test]
async fn a_record_that_falls_out_drops_the_state_it_vouched_for() {
    let rooms = Arc::new(ExecutorRooms::default());
    lock(&rooms.state).kept = 1;
    let drops = Arc::new(Drops::default());
    let agent: Arc<dyn DynChannel> = Arc::new(Agent(Arc::clone(&drops)));
    assert!(!rooms.enter(&agent, key("r1"), "t1", None));
    assert!(!rooms.enter(&agent, key("r2"), "t2", None));
    drops.dropped.notified().await;
    assert_eq!(*lock(&drops.rooms), [key("r1").1]);
    assert_eq!(lock(&rooms.state).left.len(), 1);
}

// Python: test_a_failed_drop_is_tried_again_on_the_next_turn.
#[tokio::test]
async fn a_failed_drop_is_tried_again_on_the_next_turn() {
    let rooms = Arc::new(ExecutorRooms::default());
    let drops = Arc::new(Drops::default());
    let agent: Arc<dyn DynChannel> = Arc::new(Agent(Arc::clone(&drops)));
    rooms.enter(&agent, key("r1"), "t1", None);
    drops.failed.store(true, Ordering::SeqCst);
    let stale = rooms.enter(&agent, key("r1"), "t2", Some(&previous("elsewhere")));
    assert!(
        rooms
            .ready(&*agent, &key("r1"), Metadata::new(), stale)
            .await
            .is_err()
    );
    let stale = rooms.enter(&agent, key("r1"), "t3", Some(&previous("t2")));
    assert!(stale);
    rooms
        .ready(&*agent, &key("r1"), Metadata::new(), stale)
        .await
        .unwrap();
    assert_eq!(*lock(&drops.rooms), [key("r1").1]);
}

// Python: test_a_channel_call_no_executor_took_leaves_the_previous_turn.
#[tokio::test]
async fn a_channel_call_no_executor_took_leaves_the_previous_turn() {
    let turns = super::super::Turns::default();
    turns
        .records()
        .put(&channel(), &key("r1").1, previous("before"));
    turns.executor_lifecycle.close().await;
    let agent: Arc<dyn DynChannel> = Arc::new(Agent(Arc::new(Drops::default())));
    let call = super::super::ChannelCall {
        call_id: "refused".to_owned(),
        channel_id: channel(),
        room_id: key("r1").1,
        verb: "nope".to_owned(),
        arguments: Metadata::new(),
        organization_id: None,
        room_state: Metadata::new(),
        previous_turn: None,
    };
    assert!(
        matches!(turns.channel_call(call, agent).await, Err(RoomKitError::TurnLink { reason, .. }) if reason == "no_executor")
    );
    assert_eq!(
        turns
            .records()
            .get(&channel(), &key("r1").1)
            .unwrap()
            .turn_id,
        "before"
    );
    assert!(lock(&turns.rooms.state).left.is_empty());
}
struct ReviewInterruptedDrop {
    started: Arc<Notify>,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}
impl Channel for ReviewInterruptedDrop {
    fn channel_id(&self) -> ChannelId {
        channel()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
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
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_room_detached(&self, _room_id: RoomId) -> Result<(), ChannelError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

// Python: tests/test_channel_call_contract.py::test_cancelled_stale_drop_is_retried.
#[tokio::test]
async fn a_cancelled_stale_drop_is_tried_again_before_the_next_work() {
    let rooms = Arc::new(ExecutorRooms::default());
    let started = Arc::new(Notify::new());
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let agent: Arc<dyn DynChannel> = Arc::new(ReviewInterruptedDrop {
        started: Arc::clone(&started),
        attempts: Arc::clone(&attempts),
    });
    rooms.enter(&agent, key("r1"), "t1", None);
    let stale = rooms.enter(&agent, key("r1"), "t2", Some(&previous("elsewhere")));
    assert!(stale);
    let task_rooms = Arc::clone(&rooms);
    let task_agent = Arc::clone(&agent);
    let dropping = tokio::spawn(async move {
        task_rooms
            .ready(&*task_agent, &key("r1"), Metadata::new(), stale)
            .await
    });
    started.notified().await;
    dropping.abort();
    assert!(dropping.await.unwrap_err().is_cancelled());
    let stale = rooms.enter(&agent, key("r1"), "t3", Some(&previous("t2")));
    assert!(stale, "an interrupted drop must leave no vouched state");
    rooms
        .ready(&*agent, &key("r1"), Metadata::new(), stale)
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}
