//! The processing timeout on the tokio driver (RFC §13.6, §8.2, gap
//! G-068): a call past its `process_timeout` during an off-lock hook or a
//! locked one writes nothing, lets go of what it held, and the next message
//! goes through; a hook's own timeout stays a hook error; a slow delivery
//! never times out a committed event, nor a later call into the room, which
//! commits within its bound and waits for its turn in the lane after it; a
//! kit clock that stands still bounds the call all the same, and a timeout
//! past the clocks' range bounds nothing.
//!
//! A refusal is deterministic: what it waits for outlasts the bound by far.
//! The call that follows, which shows what was left free, runs unbounded.
//!
//! The waits for the ticket and for the room lock are made to last by the
//! kit's own tests, which hold a ticket and the lock as an earlier holder
//! would (`kit/tests.rs`).

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::Timestamp;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConversationStore, FrameworkEvent, HookExecution,
    HookSpec, HookTrigger, InboundResult, RoomKit, SequentialEnvironment,
};
use tokio::sync::{Notify, broadcast};

/// The kit's `process_timeout`.
const TIMEOUT: Duration = Duration::from_millis(100);
/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

/// A kit with an SMS transport attached to `room_1`, and a
/// `process_timeout`.
async fn room() -> RoomKit {
    let kit = RoomKit::new();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::Sms))
        .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &channel("sms")).await.unwrap();
    kit.set_process_timeout(Some(TIMEOUT));
    kit
}

/// Registers a SYNC `BEFORE_BROADCAST` hook, off the lock or under it,
/// that outlasts the timeout the first time it runs and allows at once
/// after.
fn slow_the_first_time(kit: &RoomKit, needs_lock: bool) {
    let spec = HookSpec {
        needs_lock,
        ..HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync)
    };
    let slowed = Arc::new(AtomicBool::new(false));
    kit.hook(spec, move |_, _| {
        let first = !slowed.swap(true, Ordering::SeqCst);
        async move {
            if first {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
}

/// Runs `message` into `room_1`, failing the test past `BOUND`.
async fn send(kit: &RoomKit, text: &str) -> InboundResult {
    tokio::time::timeout(BOUND, kit.process_inbound_in(&room_id(), message(text)))
        .await
        .unwrap_or_else(|_| panic!("the call outlasted its process_timeout"))
        .unwrap()
}

/// What a refusal for the timeout reads: blocked, its reason, no event.
fn refusal(result: &InboundResult) -> (bool, Option<&str>, bool) {
    (
        result.blocked,
        result.reason.as_deref(),
        result.event.is_none(),
    )
}

fn timed_out() -> (bool, Option<&'static str>, bool) {
    (true, Some("process_timeout"), true)
}

/// The `process_timeout` events `events` holds: whether each named the
/// channel, and whether it named an event.
fn process_timeouts(events: &mut broadcast::Receiver<FrameworkEvent>) -> Vec<(bool, bool)> {
    let mut found = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let FrameworkEvent::ProcessTimeout {
            channel_id,
            event_id,
            ..
        } = event
        {
            found.push((channel_id.is_some(), event_id.is_some()));
        }
    }
    found
}

async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    kit.store().list_events(&room_id(), true).await.unwrap()
}

#[tokio::test]
async fn a_timeout_during_an_off_lock_hook_writes_nothing_and_the_next_message_goes_through() {
    let kit = room().await;
    slow_the_first_time(&kit, false);
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    let written = timeline(&kit).await.len();
    kit.set_process_timeout(None);
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refusal(&result),
            written,
            process_timeouts(&mut events),
            next.event.map(|event| (event.index, event.status))
        ),
        (
            timed_out(),
            0,
            vec![(true, false)],
            Some((0, EventStatus::Delivered))
        )
    );
}

/// The lock is released: a lifecycle change takes it at once after.
#[tokio::test]
async fn a_timeout_during_a_locked_hook_releases_the_lock_and_names_the_event() {
    let kit = room().await;
    slow_the_first_time(&kit, true);
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    let written = timeline(&kit).await.len();
    let paused = tokio::time::timeout(BOUND, kit.pause_room(&room_id())).await;
    kit.set_process_timeout(None);
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refusal(&result),
            written,
            process_timeouts(&mut events),
            paused.is_ok(),
            next.event.map(|event| (event.index, event.status))
        ),
        (
            timed_out(),
            0,
            vec![(false, true)],
            true,
            Some((0, EventStatus::Delivered))
        )
    );
}

/// RFC §9.3: a hook past its own timeout fails open, reported as a hook's
/// timeout, never as the call's.
#[tokio::test]
async fn a_hook_s_own_timeout_stays_a_hook_error() {
    let kit = room().await;
    kit.set_process_timeout(Some(BOUND));
    let spec = HookSpec {
        timeout: Duration::from_millis(20),
        ..HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync)
    };
    kit.hook(spec, |_, _| async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    let mut names = Vec::new();
    while let Ok(event) = events.try_recv() {
        if matches!(event, FrameworkEvent::HookTimeout { .. }) {
            names.push("hook_timeout");
        } else if matches!(event, FrameworkEvent::HookError { .. }) {
            names.push("hook_error");
        } else if matches!(event, FrameworkEvent::ProcessTimeout { .. }) {
            names.push("process_timeout");
        }
    }
    assert_eq!(
        (names, result.event.map(|event| event.status)),
        (
            vec!["hook_timeout", "hook_error"],
            Some(EventStatus::Delivered)
        )
    );
}

/// A transport whose deliveries each last `lasts`, the first one until
/// `release` is notified with `hangs_first`, each telling `delivering` it
/// started.
struct Slow {
    declaration: ChannelDeclaration,
    lasts: Duration,
    /// Whether a delivery hung already, or none is to.
    hung: Arc<AtomicBool>,
    delivering: Arc<Notify>,
    release: Arc<Notify>,
}

impl Slow {
    fn new(lasts: Duration, hangs_first: bool) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("slow"),
                ChannelType::Websocket,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            lasts,
            hung: Arc::new(AtomicBool::new(!hangs_first)),
            delivering: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

impl Channel for Slow {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let hangs = !self.hung.swap(true, Ordering::SeqCst);
        let (delivering, release) = (Arc::clone(&self.delivering), Arc::clone(&self.release));
        let lasts = self.lasts;
        async move {
            delivering.notify_one();
            if hangs {
                release.notified().await;
            } else {
                tokio::time::sleep(lasts).await;
            }
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §13.6: the commit is the call's decision; its delivery is never
/// bounded.
#[tokio::test]
async fn a_slow_delivery_never_times_out_the_committed_event() {
    let kit = room().await;
    // The decision has a quarter of a second; the delivery outlasts it.
    let bound = Duration::from_millis(250);
    kit.set_process_timeout(Some(bound));
    kit.register_channel(Slow::new(bound * 3, false)).unwrap();
    kit.attach(&room_id(), &channel("slow")).await.unwrap();
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    assert_eq!(
        (
            result.blocked,
            result.event.map(|event| event.status),
            process_timeouts(&mut events)
        ),
        (false, Some(EventStatus::Delivered), Vec::new())
    );
}

/// Gap G-068: the room's lane is entered at the commit, so a call behind
/// the previous call's slow delivery commits within its bound, and its own
/// delivery waits for its turn after the commit, counting nothing against
/// it: it is delivered once the previous set is over.
#[tokio::test]
async fn a_call_behind_a_slow_delivery_commits_within_its_bound() {
    let kit = room().await;
    kit.set_process_timeout(None);
    let slow = Slow::new(Duration::ZERO, true);
    let (delivering, release) = (Arc::clone(&slow.delivering), Arc::clone(&slow.release));
    kit.register_channel(slow).unwrap();
    kit.attach(&room_id(), &channel("slow")).await.unwrap();
    let first = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.process_inbound_in(&room_id(), message("Bonjour")).await })
    };
    // The first call committed and hangs in its delivery, its place first.
    tokio::time::timeout(BOUND, delivering.notified())
        .await
        .unwrap();
    kit.set_process_timeout(Some(TIMEOUT));
    let mut events = kit.subscribe();
    let second = {
        let kit = kit.clone();
        tokio::spawn(async move { send(&kit, "Still there?").await })
    };
    // Nothing tells the commit apart from the outside: the store is read.
    let deadline = tokio::time::Instant::now() + BOUND;
    while timeline(&kit).await.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the call never committed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let delivered_yet = second.is_finished();
    // The first call's delivery is over: its place is left.
    release.notify_one();
    let first = tokio::time::timeout(BOUND, first).await.unwrap().unwrap();
    let second = tokio::time::timeout(BOUND, second).await.unwrap().unwrap();
    assert_eq!(
        (
            delivered_yet,
            first.map(|first| first.event.map(|event| event.index)),
            second.event.map(|event| (event.index, event.status)),
            process_timeouts(&mut events)
        ),
        (
            false,
            Ok(Some(0)),
            Some((1, EventStatus::Delivered)),
            Vec::new()
        )
    );
}

/// The kit's clock stands still: the deadline set on it is waited for on
/// the runtime's clock all the same.
#[tokio::test]
async fn a_kit_clock_that_stands_still_bounds_the_call_all_the_same() {
    let kit = room().await;
    let now = Timestamp::from_unix_millis(1_792_677_780_000).unwrap();
    kit.set_environment(SequentialEnvironment::new(now));
    slow_the_first_time(&kit, false);
    let result = send(&kit, "Bonjour").await;
    kit.set_process_timeout(None);
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (refusal(&result), next.event.map(|event| event.status)),
        (timed_out(), Some(EventStatus::Delivered))
    );
}

/// Gap G-068: a timeout past the clocks' range bounds nothing, and never
/// panics.
#[tokio::test]
async fn a_timeout_past_the_clocks_range_bounds_nothing() {
    let kit = room().await;
    let seven_thousand_years = Duration::from_hours(7_000 * 365 * 24);
    let mut statuses = Vec::new();
    for timeout in [Duration::MAX, seven_thousand_years] {
        kit.set_process_timeout(Some(timeout));
        let result = send(&kit, "Bonjour").await;
        statuses.push(result.event.map(|event| event.status));
    }
    assert_eq!(
        statuses,
        [Some(EventStatus::Delivered), Some(EventStatus::Delivered)]
    );
}
