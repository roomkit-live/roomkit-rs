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
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{
    EventTap, RecordingTransport, RecordingTransportOptions, SequentialEnvironment,
    kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InboundResult, PauseRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;

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
    InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text))
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
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
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
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// Registers a SYNC `BEFORE_BROADCAST` hook, off the lock or under it,
/// that outlasts the timeout the first time it runs and allows at once
/// after.
fn slow_the_first_time(kit: &RoomKit, needs_lock: bool) {
    let spec = HookOptions {
        name: "scanner".to_owned(),
        needs_lock,
        ..Default::default()
    };
    let slowed = Arc::new(AtomicBool::new(false));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let first = !slowed.swap(true, Ordering::SeqCst);
            async move {
                if first {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
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

/// The `process_timeout` events `tap` saw once the kit's background work is
/// over: whether each named the channel, and whether it named an event.
async fn process_timeouts(kit: &RoomKit, tap: &EventTap) -> Vec<(bool, bool)> {
    roomkit::testing::idle(kit).await;
    tap.events()
        .into_iter()
        .filter(|event| event.r#type == "process_timeout")
        .map(|event| (event.channel_id.is_some(), event.event_id.is_some()))
        .collect()
}

/// The room's timeline but its lifecycle records (gap G-046).
async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    let mut timeline = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    timeline.retain(|event| event.source.channel_type != ChannelType::SYSTEM);
    timeline
}

#[tokio::test(start_paused = true)]
async fn a_timeout_during_an_off_lock_hook_writes_nothing_and_the_next_message_goes_through() {
    let kit = room().await;
    slow_the_first_time(&kit, false);
    let tap = EventTap::on(&kit, &["process_timeout"]);
    let result = send(&kit, "Bonjour").await;
    let written = timeline(&kit).await.len();
    let next = send(&kit, "Still there?").await;
    assert_eq!(
        (
            refusal(&result),
            written,
            process_timeouts(&kit, &tap).await,
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
    let tap = EventTap::on(&kit, &["process_timeout"]);
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
            process_timeouts(&kit, &tap).await,
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
    let spec = HookOptions {
        name: "scanner".to_owned(),
        timeout: Duration::from_millis(20),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(HookResult::allow(AllowOptions::default()))
        },
        spec,
    )
    .unwrap();
    let tap = EventTap::on(&kit, &["hook_timeout", "hook_error", "process_timeout"]);
    let result = send(&kit, "Bonjour").await;
    roomkit::testing::idle(&kit).await;
    let names = tap.types();
    assert_eq!(
        (names, result.event.map(|event| event.status)),
        (
            vec!["hook_timeout".to_owned(), "hook_error".to_owned()],
            Some(EventStatus::Delivered)
        )
    );
}

/// A transport whose deliveries each last `lasts`, the first one until
/// `release` is notified with `hangs_first`, each telling `delivering` it
/// started.
struct Slow {
    declaration: ChannelConfig,
    lasts: Duration,
    /// Whether a delivery hung already, or none is to.
    hung: Arc<AtomicBool>,
    delivering: Arc<Notify>,
    release: Arc<Notify>,
}

impl Slow {
    fn new(lasts: Duration, hangs_first: bool) -> Self {
        Self {
            declaration: ChannelConfig::new(
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

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
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
    let tap = EventTap::on(&kit, &["process_timeout"]);
    let result = send(&kit, "Bonjour").await;
    assert_eq!(
        (
            result.blocked,
            result.event.map(|event| event.status),
            process_timeouts(&kit, &tap).await
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
    let tap = EventTap::on(&kit, &["process_timeout"]);
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
            process_timeouts(&kit, &tap).await
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
    let spec = HookOptions {
        name: "scanner".to_owned(),
        timeout: Duration::from_secs(60),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            tokio::time::sleep(Duration::from_secs(31)).await;
            Ok(HookResult::allow(AllowOptions::default()))
        },
        spec,
    )
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
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}
