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
use roomkit_core::model::hook::AllowOptions;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_core::ops::{
    ChannelConfig, EffectId, Environment, Next, Operation, Outcome, ProtocolError, Step,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use tokio::sync::Notify;

use crate::HookOptions;

use super::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use crate::InboundResult;
use crate::channel::{Channel, ChannelError};
use crate::driver;
use crate::error::RoomKitError;
use crate::store::ConversationStore;
use crate::testing::{RecordingTransport, RecordingTransportOptions};

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
        TextContent::new(text),
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
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &ChannelId::new("sms").unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let spec = HookOptions {
        name: "scanner".to_owned(),
        needs_lock: false,
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async { Ok(HookResult::allow(AllowOptions::default())) },
        spec,
    )
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
    tokio::time::timeout(
        BOUND,
        kit.process_inbound(
            message(text),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

/// What a refusal for the timeout reads, and what the room's timeline
/// holds after it.
async fn refusal(kit: &RoomKit, result: &InboundResult) -> (bool, Option<String>, usize) {
    let timeline = kit
        .store()
        .list_events(&room_id(), every_row())
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
    let tap = crate::testing::EventTap::on(&kit, &["process_timeout"]);
    let lock = kit.inner.locks.lock(&room_id()).await;
    let result = send(&kit, "Bonjour").await;
    let refused = refusal(&kit, &result).await;
    drop(lock);
    unbounded(&kit);
    let next = send(&kit, "Still there?").await;
    crate::testing::idle(&kit).await;
    let named: Vec<_> = tap
        .events()
        .into_iter()
        .map(|event| {
            (
                event.room_id,
                event.channel_id.map(String::from),
                event.event_id,
            )
        })
        .collect();
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
    declaration: ChannelConfig,
    delivering: Arc<Notify>,
    go: Arc<Notify>,
}

impl Channel for Paused {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn address_config(
        &self,
    ) -> (
        roomkit_core::rules::addresses::ChannelAddressing,
        Option<roomkit_core::rules::addresses::CountryCode>,
    ) {
        let config = &self.declaration;
        let addressing = roomkit_core::rules::addresses::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
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
        declaration: ChannelConfig::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
        delivering: Arc::clone(&delivering),
        go: Arc::clone(&go),
    })
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &ChannelId::new("ws").unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(
                message("Bonjour"),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
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

/// An operation that waits for an effect it never asked for.
struct WaitsForNothing;

impl Operation for WaitsForNothing {
    type Output = ();

    fn poll(&mut self, _environment: &dyn Environment) -> Step<()> {
        Step {
            notices: Vec::new(),
            settled: Vec::new(),
            delivered: Vec::new(),
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
    let kit = RoomKit::new(RoomKitOptions::default());
    let stalled = driver::run(&kit.inner, "test", WaitsForNothing).await;
    assert_eq!(stalled, Err(driver::stalled()));
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
    let stuck = HookOptions {
        name: "stuck".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            std::future::pending::<()>().await;
            Ok(HookResult::allow(AllowOptions::default()))
        },
        stuck,
    )
    .unwrap();
    let room = room_id();
    let mut call = std::pin::pin!(kit.process_inbound(
        message("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        }
    ));
    let started = first.block_on(async { tokio::time::timeout(TIMEOUT, call.as_mut()).await });
    drop(first);
    let cancelled = runtime().block_on(call);
    assert_eq!(
        (started.is_err(), cancelled.map(|result| result.event)),
        (true, Err(RoomKitError::Cancelled))
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> crate::store::ListEventsOptions {
    crate::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(crate::store::EventFilter {
            include_blocked: true,
            ..crate::store::EventFilter::default()
        }),
        ..crate::store::ListEventsOptions::default()
    }
}

/// RFC §12.10.4: the kit's own work, a cut turn's record written on its
/// last message, waits for its room's lock through the close's grace,
/// where a call of anyone else would be cut there at once, and is written
/// once the lock is let go (killing `rmk1030-rs-review-A` mutant M23).
#[tokio::test(start_paused = true)]
async fn the_kit_s_own_write_waits_for_its_room_lock_through_the_grace() {
    let kit = checked_room().await;
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let result = kit.process_inbound(message("Hi"), options).await.unwrap();
    let event_id = result.event.unwrap().id;
    let lock = kit.inner.locks.lock(&room_id()).await;
    // Work the close gives its grace to, which the test ends.
    let running = kit
        .inner
        .tasks
        .track_unabortable(crate::tasks::Kind::Observer);
    let close = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    tokio::task::yield_now().await;
    let own = tokio::spawn({
        let (kit, event_id) = (kit.clone(), event_id.clone());
        async move {
            let mut metadata = roomkit_core::model::Metadata::new();
            metadata.insert("loop_end_reason".to_owned(), "cancelled".into());
            let updates = roomkit_core::model::EventUpdates {
                metadata: Some(metadata),
                ..Default::default()
            };
            kit.update_own(&room_id(), &event_id, updates).await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waits = !own.is_finished();
    drop(lock);
    let written = tokio::time::timeout(BOUND, own).await.unwrap().unwrap();
    drop(running);
    tokio::time::timeout(Duration::from_secs(10), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let stored = kit.inner.store.get_event(&event_id).await.unwrap().unwrap();
    assert_eq!(
        (
            waits,
            written.is_ok(),
            stored.metadata.get("loop_end_reason").cloned()
        ),
        (true, true, Some("cancelled".into()))
    );
}

/// Python's `type(store).__name__`: the store's own name, a generic one's
/// without what it wraps.
#[test]
fn a_store_s_name_in_the_warning_is_its_type_alone() {
    struct Wrapper<T>(std::marker::PhantomData<T>);
    assert_eq!(
        super::short_type_name::<crate::InMemoryStore>(),
        "InMemoryStore"
    );
    assert_eq!(
        super::short_type_name::<Wrapper<crate::InMemoryStore>>(),
        "Wrapper"
    );
}
