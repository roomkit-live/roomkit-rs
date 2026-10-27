//! What the kit's calls cannot reach on their own: a call waiting for the
//! room's admission ticket or for its lock while another holder keeps them,
//! as an earlier ticketed event or a lifecycle change would, and the moment
//! a call lets its ticket go (RFC §9.5.1, §13.6). These tests hold a ticket
//! and the lock themselves, as such a holder would at a chosen moment, and
//! run the real driver against them. They also run the driver on an
//! operation that breaks the protocol, and a call on a runtime that shuts
//! down under it.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{
    ChannelDeclaration, EffectId, Environment, FrameworkEvent, InboundResult, Next, Operation,
    Outcome, ProtocolError, Step,
};
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};
use tokio::sync::Notify;

use super::{RoomKit, RoomKitOptions};
use crate::channel::{Channel, ChannelError};
use crate::driver;
use crate::error::KitError;
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
    let options = RoomKitOptions {
        process_timeout: TIMEOUT,
        ..Default::default()
    };
    let kit = RoomKit::new(options);
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
    ))
    .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &ChannelId::new("sms").unwrap())
        .await
        .unwrap();
    let mut spec = HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = false;
    kit.hook(spec, |_, _| async { Ok(HookResult::allow()) })
        .unwrap();
    kit
}

/// Lifts the kit's `process_timeout`, for a call that proves what is free,
/// not how fast it is.
fn unbounded(kit: &RoomKit) {
    let lifted = kit.inner.try_change_config(|config| {
        config.process_timeout = None;
        Ok::<(), Infallible>(())
    });
    lifted.unwrap();
}

async fn send(kit: &RoomKit, text: &str) -> InboundResult {
    tokio::time::timeout(BOUND, kit.process_inbound_in(&room_id(), message(text)))
        .await
        .unwrap()
        .unwrap()
}

/// What a refusal for the timeout reads, and what the room's timeline
/// holds after it.
async fn refusal(kit: &RoomKit, result: &InboundResult) -> (bool, Option<String>, usize) {
    let timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    (result.blocked, result.reason.clone(), timeline.len())
}

/// The refusal, with the timeline holding the attach's lifecycle record
/// alone (gap G-046).
fn timed_out() -> (bool, Option<String>, usize) {
    (true, Some("process_timeout".to_owned()), 1)
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
    unbounded(&kit);
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refused,
            held,
            next.event.map(|event| (event.index, event.status)),
            kit.inner.tickets.held(&room_id())
        ),
        (timed_out(), 1, Some((1, EventStatus::Delivered)), 0)
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
    unbounded(&kit);
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
            Some(1),
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
    unbounded(&kit);
    let (delivering, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    kit.register_channel(Paused {
        declaration: ChannelDeclaration::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
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
        tokio::spawn(async move { kit.process_inbound_in(&room_id(), message("Bonjour")).await })
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

/// An operation that waits for an effect it never asked for.
struct WaitsForNothing;

impl Operation for WaitsForNothing {
    type Output = ();

    fn poll(&mut self, _environment: &dyn Environment) -> Step<()> {
        Step {
            notices: Vec::new(),
            settled: Vec::new(),
            decided: None,
            next: Next::Waiting,
        }
    }

    fn complete(&mut self, id: EffectId, _outcome: Outcome) -> Result<(), ProtocolError> {
        Err(ProtocolError::UnknownEffect(id))
    }
}

/// A run that waits for an effect the driver never received fails as a
/// stall, a defect of the operation's, not as a cancelled run.
#[tokio::test]
async fn a_run_waiting_for_an_effect_never_asked_for_stalls() {
    let kit = RoomKit::new(Default::default());
    let stalled = driver::run(&kit.inner, "test", WaitsForNothing).await;
    assert_eq!(stalled, Err(KitError::Stalled));
}

/// A call whose run the runtime cancels, as a runtime shutting down does,
/// fails as cancelled: whether its event was committed is not known.
#[test]
fn a_call_whose_runtime_shuts_down_under_it_is_cancelled() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    let first = runtime();
    let kit = first.block_on(checked_room());
    unbounded(&kit);
    kit.before_broadcast("stuck", |_, _| async {
        std::future::pending::<()>().await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let room = room_id();
    let mut call = std::pin::pin!(kit.process_inbound_in(&room, message("Bonjour")));
    let started = first.block_on(async { tokio::time::timeout(TIMEOUT, call.as_mut()).await });
    drop(first);
    let cancelled = runtime().block_on(call);
    assert_eq!(
        (started.is_err(), cancelled.map(|result| result.event)),
        (true, Err(KitError::Cancelled))
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> crate::store::EventQuery {
    crate::store::EventQuery {
        limit: u64::MAX,
        event_filter: Some(crate::store::EventFilter {
            include_blocked: true,
            ..crate::store::EventFilter::default()
        }),
        ..crate::store::EventQuery::default()
    }
}
