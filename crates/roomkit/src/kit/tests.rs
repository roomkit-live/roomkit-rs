//! What the kit's calls cannot reach on their own: a call waiting for the
//! room's admission ticket or for its lock while another holder keeps them,
//! as an earlier ticketed event or a lifecycle change would, and the moment
//! a call lets its ticket go (RFC §9.5.1, §13.6). These tests hold a ticket
//! and the lock themselves, as such a holder would at a chosen moment, and
//! run the real driver against them.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{ChannelDeclaration, FrameworkEvent, InboundResult};
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};
use tokio::sync::Notify;

use super::RoomKit;
use crate::channel::{Channel, ChannelError};
use crate::store::ConversationStore;
use crate::testing::RecordingTransport;

/// The kit's `process_timeout`.
const TIMEOUT: Duration = Duration::from_millis(100);
/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(
        ChannelId::new("sms").unwrap(),
        "+15551234567",
        EventContent::text(text),
    )
}

/// A kit with an SMS transport attached to `room_1`, an off-lock check
/// that allows, and a `process_timeout`.
async fn checked_room() -> RoomKit {
    let kit = RoomKit::new();
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::Sms,
    ))
    .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &ChannelId::new("sms").unwrap())
        .await
        .unwrap();
    let spec = HookSpec {
        needs_lock: false,
        ..HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync)
    };
    kit.hook(spec, |_, _| async { Ok(HookResult::allow()) })
        .unwrap();
    kit.set_process_timeout(Some(TIMEOUT));
    kit
}

async fn send(kit: &RoomKit, text: &str) -> InboundResult {
    tokio::time::timeout(BOUND, kit.process_inbound(message(text), Some(&room_id())))
        .await
        .unwrap()
        .unwrap()
}

/// What a refusal for the timeout reads, and what the room's timeline
/// holds after it.
async fn refusal(kit: &RoomKit, result: &InboundResult) -> (bool, Option<String>, usize) {
    let timeline = kit.store().list_events(&room_id(), true).await.unwrap();
    (result.blocked, result.reason.clone(), timeline.len())
}

fn timed_out() -> (bool, Option<String>, usize) {
    (true, Some("process_timeout".to_owned()), 0)
}

/// RFC §9.5.1, Bound: waiting for a turn counts against `process_timeout`.
/// The call releases its ticket; once the earlier one goes, the next
/// message goes through.
#[tokio::test]
async fn a_call_past_its_deadline_waiting_for_its_ticket_writes_nothing_and_releases_it() {
    let kit = checked_room().await;
    let earlier = kit.inner.tickets.take(&room_id());
    let result = send(&kit, "Bonjour").await;
    let refused = refusal(&kit, &result).await;
    let held = kit.inner.tickets.held(&room_id());
    drop(earlier);
    // The next call proves what is free, not how fast it is.
    kit.set_process_timeout(None);
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refused,
            held,
            next.event.map(|event| (event.index, event.status)),
            kit.inner.tickets.held(&room_id())
        ),
        (timed_out(), 1, Some((0, EventStatus::Delivered)), 0)
    );
}

/// RFC §13.6: the wait for the room lock is bounded; a timeout before the
/// lock names the channel.
#[tokio::test]
async fn a_call_past_its_deadline_waiting_for_the_room_lock_writes_nothing() {
    let kit = checked_room().await;
    let mut events = kit.subscribe();
    let lock = kit.inner.locks.lock(&room_id()).await;
    let result = send(&kit, "Bonjour").await;
    let refused = refusal(&kit, &result).await;
    drop(lock);
    kit.set_process_timeout(None);
    let next = send(&kit, "Still there?").await;
    let mut named = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let FrameworkEvent::ProcessTimeout {
            room_id,
            channel_id,
            event_id,
            ..
        } = event
        {
            named.push((room_id, channel_id.map(String::from), event_id));
        }
    }
    assert_eq!(
        (
            refused,
            named,
            next.event.map(|event| event.index),
            kit.inner.tickets.held(&room_id())
        ),
        (
            timed_out(),
            vec![(Some(room_id()), Some("sms".to_owned()), None)],
            Some(0),
            0
        )
    );
}

/// A transport whose delivery tells `delivering` it started, then waits for
/// `go`.
struct Paused {
    declaration: ChannelDeclaration,
    delivering: Arc<Notify>,
    go: Arc<Notify>,
}

impl Channel for Paused {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (delivering, go) = (Arc::clone(&self.delivering), Arc::clone(&self.go));
        async move {
            delivering.notify_one();
            go.notified().await;
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §9.5.1: the ticket is released once the locked section decided and
/// committed, not when the call's cascade ends: a later ticket's turn
/// comes while the event is still being delivered.
#[tokio::test]
async fn a_call_releases_its_ticket_once_its_event_is_committed() {
    let kit = checked_room().await;
    kit.set_process_timeout(None);
    let (delivering, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    kit.register_channel(Paused {
        declaration: ChannelDeclaration::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::Websocket,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
        delivering: Arc::clone(&delivering),
        go: Arc::clone(&go),
    })
    .unwrap();
    kit.attach(&room_id(), &ChannelId::new("ws").unwrap())
        .await
        .unwrap();
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(message("Bonjour"), Some(&room_id()))
                .await
        })
    };
    tokio::time::timeout(BOUND, delivering.notified())
        .await
        .unwrap();
    let later = kit.inner.tickets.take(&room_id());
    let turn = tokio::time::timeout(TIMEOUT, kit.inner.tickets.turn(&later)).await;
    drop(later);
    go.notify_one();
    let delivered = tokio::time::timeout(BOUND, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        (turn.is_ok(), delivered.event.map(|event| event.status)),
        (true, Some(EventStatus::Delivered))
    );
}
