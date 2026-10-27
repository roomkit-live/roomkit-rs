//! The processing timeout on the tokio driver (RFC §13.6, §8.2, gap
//! G-068): a call past its `process_timeout` during an off-lock hook or a
//! locked one writes nothing, lets go of what it held, and the next message
//! goes through; a hook's own timeout stays a hook error; a slow delivery
//! never times out a committed event, nor a later call into the room, which
//! commits within its bound and waits for its turn in the lane after it; a
//! kit clock that stands still bounds the call all the same, and a timeout
//! past the clocks' range bounds nothing.
//!
//! A kit with no `process_timeout` set bounds a call to 30 seconds, as
//! the Python implementation's does.
//!
//! A refusal is deterministic: what it waits for outlasts the bound by far,
//! and the tests that refuse run on tokio's paused clock, so the call that
//! follows, which shows what was left free, keeps the same bound.
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
use roomkit::testing::{RecordingTransport, SequentialEnvironment, kit_with_environment};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, FrameworkEvent, HookExecution, HookSpec, HookTrigger, InboundResult,
    PauseRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
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
/// `process_timeout` of [`TIMEOUT`].
async fn room() -> RoomKit {
    room_bound_by(TIMEOUT).await
}

/// [`room`], its calls bounded by `process_timeout`.
async fn room_bound_by(process_timeout: Duration) -> RoomKit {
    room_with(RoomKit::new(RoomKitOptions {
        process_timeout,
        ..Default::default()
    }))
    .await
}

/// `kit` with an SMS transport attached to `room_1`.
async fn room_with(kit: RoomKit) -> RoomKit {
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// Registers a SYNC `BEFORE_BROADCAST` hook, off the lock or under it,
/// that outlasts the timeout the first time it runs and allows at once
/// after.
fn slow_the_first_time(kit: &RoomKit, needs_lock: bool) {
    let mut spec = HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = needs_lock;
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

/// The room's timeline but its lifecycle records (gap G-046).
async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    let mut timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    timeline.retain(|event| event.source.channel_type != ChannelType::SYSTEM);
    timeline
}

#[tokio::test(start_paused = true)]
async fn a_timeout_during_an_off_lock_hook_writes_nothing_and_the_next_message_goes_through() {
    let kit = room().await;
    slow_the_first_time(&kit, false);
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    let written = timeline(&kit).await.len();
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refusal(&result),
            written,
            process_timeouts(&mut events),
            next.event.map(|event| (event.index, event.status))
        ),
        // The attach's lifecycle record took index 0 (gap G-046).
        (
            timed_out(),
            0,
            vec![(true, false)],
            Some((1, EventStatus::Delivered))
        )
    );
}

/// The lock is released: a lifecycle change takes it at once after.
#[tokio::test(start_paused = true)]
async fn a_timeout_during_a_locked_hook_releases_the_lock_and_names_the_event() {
    let kit = room().await;
    slow_the_first_time(&kit, true);
    let mut events = kit.subscribe();
    let result = send(&kit, "Bonjour").await;
    let written = timeline(&kit).await.len();
    let paused = tokio::time::timeout(
        BOUND,
        kit.pause_room(&room_id(), PauseRoomOptions::default()),
    )
    .await;
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refusal(&result),
            written,
            process_timeouts(&mut events),
            paused.is_ok(),
            next.event.map(|event| (event.index, event.status))
        ),
        // The attach's lifecycle record took index 0; the pause writes
        // none (gap G-046).
        (
            timed_out(),
            0,
            vec![(false, true)],
            true,
            Some((1, EventStatus::Delivered))
        )
    );
}

/// RFC §9.3: a hook past its own timeout fails open, reported as a hook's
/// timeout, never as the call's.
#[tokio::test]
async fn a_hook_s_own_timeout_stays_a_hook_error() {
    let kit = room_bound_by(BOUND).await;
    let mut spec = HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.timeout = Duration::from_millis(20);
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
                ChannelType::WEBSOCKET,
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
    // The decision has a quarter of a second; the delivery outlasts it.
    let bound = Duration::from_millis(250);
    let kit = room_bound_by(bound).await;
    kit.register_channel(Slow::new(bound * 3, false)).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
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
    let slow = Slow::new(Duration::ZERO, true);
    let (delivering, release) = (Arc::clone(&slow.delivering), Arc::clone(&slow.release));
    kit.register_channel(slow).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let first = {
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
    // The first call committed and hangs in its delivery, its place first.
    tokio::time::timeout(BOUND, delivering.notified())
        .await
        .unwrap();
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
        // After the two attaches' lifecycle records (gap G-046).
        (
            false,
            Ok(Some(2)),
            Some((3, EventStatus::Delivered)),
            Vec::new()
        )
    );
}

/// The kit's clock stands still: the deadline set on it is waited for on
/// the runtime's clock all the same.
#[tokio::test(start_paused = true)]
async fn a_kit_clock_that_stands_still_bounds_the_call_all_the_same() {
    let now = Timestamp::from_unix_millis(1_792_677_780_000).unwrap();
    let options = RoomKitOptions {
        process_timeout: TIMEOUT,
        ..Default::default()
    };
    let kit = room_with(kit_with_environment(
        options,
        SequentialEnvironment::new(now),
    ))
    .await;
    slow_the_first_time(&kit, false);
    let result = send(&kit, "Bonjour").await;
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
    let seven_thousand_years = Duration::from_hours(7_000 * 365 * 24);
    let mut statuses = Vec::new();
    for timeout in [Duration::MAX, seven_thousand_years] {
        let kit = room_bound_by(timeout).await;
        let result = send(&kit, "Bonjour").await;
        statuses.push(result.event.map(|event| event.status));
    }
    assert_eq!(
        statuses,
        [Some(EventStatus::Delivered), Some(EventStatus::Delivered)]
    );
}

/// RFC §13.6: a kit built with no `process_timeout` bounds a call to 30
/// seconds, Python's `process_timeout=30.0`: a hook that waits 31 seconds,
/// within its own 60 seconds, has the message blocked `process_timeout`.
#[tokio::test(start_paused = true)]
async fn process_timeout_defaults_to_30_seconds() {
    let kit = room_with(RoomKit::new(RoomKitOptions::default())).await;
    let mut spec = HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.timeout = Duration::from_secs(60);
    kit.hook(spec, |_, _| async {
        tokio::time::sleep(Duration::from_secs(31)).await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let room = room_id();
    let call = kit.process_inbound(
        message("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    let result = tokio::time::timeout(Duration::from_secs(120), call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refusal(&result), timed_out());
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
