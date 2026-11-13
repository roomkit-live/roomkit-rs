//! RFC §8.3, §10.6.5: a status a turn's host posts is at least as deep as
//! the turn's response whenever the room's owner serves the verb, before
//! the call that submitted the turn waits on it too (RMK-1552).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventSource,
    InboundMessage, RoomContext, RoomEvent, TextContent,
};
use tokio::sync::Notify;

use super::support::{channel, kit_with, room_id};
use crate::orchestration::{StatusEntry, StatusLevel};
use crate::turn::TurnKit;
use crate::{
    Channel, ChannelError, ConversationStore, HookExecution, HookOptions, HookTrigger,
    PostStatusOptions, RoomKit, SendEventOptions, current_turn_host,
};

/// An agent whose first turn posts a status on its turn's host, and
/// answers nothing.
#[derive(Default)]
struct Posting {
    posted: AtomicBool,
}

impl Channel for Posting {
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
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if !self.posted.swap(true, Ordering::SeqCst) {
            let host = current_turn_host().unwrap();
            let options = PostStatusOptions::default();
            host.post_status("ai", "lookup", StatusLevel::Info, options)
                .await?;
        }
        Ok(ChannelOutput::default())
    }
}

/// An `ON_STATUS_POSTED` hook that says it fired, then sends `again` into
/// the room by SMS, noting the depth the event is stored at.
fn relay(kit: &RoomKit, fired: Arc<Notify>, depths: Arc<Mutex<Vec<u32>>>) {
    let sending = kit.clone();
    let options = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    let relay = move |_entry: StatusEntry, _| {
        let (kit, fired, depths) = (sending.clone(), Arc::clone(&fired), Arc::clone(&depths));
        async move {
            fired.notify_one();
            let (room, sms) = (room_id(), channel("sms"));
            let again = TextContent::new("again");
            let sent = kit.send_event(&room, &sms, again, SendEventOptions::default());
            let depth = sent.await.unwrap().chain_depth;
            depths.lock().unwrap().push(depth);
        }
    };
    kit.hook(HookTrigger::OnStatusPosted, relay, options)
        .unwrap();
}

/// A turn is submitted, and no call waits on its output until its status
/// hook fired: on several workers, its executor can post a status, and the
/// room's owner serve the verb, before the call that submitted the turn
/// reaches its output. The status hook's send is as deep as the turn's
/// response, 1, whether a call waits on the turn yet or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_posted_before_its_turn_is_waited_on_is_as_deep_as_the_turn() {
    let kit = kit_with(Posting::default()).await;
    let fired = Arc::new(Notify::new());
    let depths = Arc::new(Mutex::new(Vec::new()));
    relay(&kit, Arc::clone(&fired), Arc::clone(&depths));
    let store = kit.store();
    let binding = store.get_binding(&room_id(), &channel("ai")).await;
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    let source = EventSource::new(channel("sms"), ChannelType::SMS);
    let event = RoomEvent::new(room_id(), source, TextContent::new("go"));
    let turn_kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
    let turns = &kit.inner.turns;
    let submitted = turns.submit(
        &turn_kit,
        &binding.unwrap().unwrap(),
        event,
        RoomContext::new(room),
        |_| (),
    );
    let (mut turn, ()) = submitted.unwrap();
    let bound = Duration::from_secs(10);
    let published = tokio::time::timeout(bound, fired.notified()).await.is_ok();
    turn.output(turns.cutting()).await.unwrap();
    drop(turn);
    crate::testing::idle(&kit).await;
    kit.close().await.unwrap();
    let depths = depths.lock().unwrap().clone();
    assert_eq!((published, depths), (true, vec![1]));
}
