//! Off-lock checks on the tokio driver (RFC §9.5.1, §10.1 step 5a, Appendix
//! B.2): a hook registered with `needs_lock = false` keeps the room lock
//! free while it runs, the checks of two calls into one room overlap and
//! commit in ticket order, an event no check fires for is not ordered
//! against them, a call cancelled during a check leaves no ticket behind,
//! and B.2's scanner run off the lock gives the timeline it gives under it.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelId, ChannelType, EventDirection, EventSource, EventStatus, HookResult, InboundMessage,
    InjectedEvent, Observation, RoomEvent, RoomId, RoomStatus, TextContent, Timestamp,
};
use roomkit::testing::{
    RecordingTransport, RecordingTransportOptions, SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookReturn,
    HookSpec, HookTrigger, InboundResult, PauseRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

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

/// A SYNC `BEFORE_BROADCAST` hook named `name`, off the room lock or under
/// it.
fn before_broadcast(name: &str, needs_lock: bool) -> HookSpec {
    let mut spec = HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = needs_lock;
    spec
}

/// A kit with an SMS transport attached to `room_1`.
async fn room() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
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

/// Registers a hook named `scanner`, off the lock or under it, that waits
/// for `go` once it told `started` it runs; then runs a message into
/// `room_1` in a task of its own, and waits until the hook runs.
async fn message_held_in_its_hook(
    kit: &RoomKit,
    needs_lock: bool,
) -> (Arc<Notify>, tokio::task::JoinHandle<InboundResult>) {
    let (started, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (hook_started, hook_go) = (Arc::clone(&started), Arc::clone(&go));
    kit.hook(before_broadcast("scanner", needs_lock), move |_, _| {
        let (started, go) = (Arc::clone(&hook_started), Arc::clone(&hook_go));
        async move {
            started.notify_one();
            go.notified().await;
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
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
            .unwrap()
        })
    };
    tokio::time::timeout(BOUND, started.notified())
        .await
        .unwrap_or_else(|_| panic!("the hook never ran"));
    (go, call)
}

/// RFC §9.5.1, §13.5: the room's lifecycle goes on while an off-lock check
/// runs; the message, decided once the check is over, resumes the room.
#[tokio::test]
async fn a_slow_off_lock_hook_leaves_the_room_lock_free_for_a_lifecycle_change() {
    let kit = room().await;
    let (go, call) = message_held_in_its_hook(&kit, false).await;
    let paused = tokio::time::timeout(
        BOUND,
        kit.pause_room(&room_id(), PauseRoomOptions::default()),
    )
    .await;
    let status_meanwhile = kit
        .store()
        .get_room(&room_id())
        .await
        .unwrap()
        .unwrap()
        .status;
    go.notify_one();
    let result = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    let status_after = kit
        .store()
        .get_room(&room_id())
        .await
        .unwrap()
        .unwrap()
        .status;
    assert_eq!(
        (
            paused.is_ok(),
            status_meanwhile,
            result.event.map(|event| (event.index, event.status)),
            status_after
        ),
        // The attach's lifecycle record took index 0 (gap G-046).
        (
            true,
            RoomStatus::Paused,
            Some((1, EventStatus::Delivered)),
            RoomStatus::Active
        )
    );
}

/// The contrast: a hook that needs the lock holds it while it runs.
#[tokio::test]
async fn a_slow_locked_hook_holds_the_room_lock_from_a_lifecycle_change() {
    let kit = room().await;
    let (go, call) = message_held_in_its_hook(&kit, true).await;
    let paused = tokio::time::timeout(
        Duration::from_millis(100),
        kit.pause_room(&room_id(), PauseRoomOptions::default()),
    )
    .await;
    go.notify_one();
    tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert!(paused.is_err(), "the pause should wait for the room lock");
}

/// The texts of `room_1`'s timeline, in index order, its lifecycle records
/// left out (gap G-046).
async fn texts(kit: &RoomKit) -> Vec<String> {
    kit.store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .iter()
        .filter(|event| event.source.channel_type != ChannelType::SYSTEM)
        .map(|event| text_of(&event.content))
        .collect()
}

/// Runs `text` from the customer into `room_1` in a task of its own.
fn sent(kit: &RoomKit, text: &'static str) -> tokio::task::JoinHandle<InboundResult> {
    let kit = kit.clone();
    tokio::spawn(async move {
        kit.process_inbound(
            message(text),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    })
}

/// RFC §9.5.1, Order: the checks of two calls into one room overlap, the
/// second's over while the first's still runs, and the two commit in the
/// order their tickets were taken.
#[tokio::test]
async fn two_overlapping_checks_commit_in_ticket_order() {
    let kit = room().await;
    let (held, go, second_checked) = (
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    let signals = (
        Arc::clone(&held),
        Arc::clone(&go),
        Arc::clone(&second_checked),
    );
    kit.hook(before_broadcast("scanner", false), move |event, _| {
        let (held, go, second_checked) = (
            Arc::clone(&signals.0),
            Arc::clone(&signals.1),
            Arc::clone(&signals.2),
        );
        async move {
            if text_of(&event.content) == "first" {
                held.notify_one();
                go.notified().await;
            } else {
                second_checked.notify_one();
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let first = sent(&kit, "first");
    tokio::time::timeout(BOUND, held.notified()).await.unwrap();
    let second = sent(&kit, "second");
    tokio::time::timeout(BOUND, second_checked.notified())
        .await
        .unwrap_or_else(|_| panic!("the second check waited for the first"));
    let committed_meanwhile = texts(&kit).await;
    go.notify_one();
    let first = tokio::time::timeout(BOUND, first).await.unwrap().unwrap();
    let second = tokio::time::timeout(BOUND, second).await.unwrap().unwrap();
    assert_eq!(
        (
            committed_meanwhile,
            first.event.map(|event| event.index),
            second.event.map(|event| event.index),
            texts(&kit).await
        ),
        // After the attach's lifecycle record (gap G-046).
        (
            Vec::<String>::new(),
            Some(1),
            Some(2),
            vec!["first".to_owned(), "second".to_owned()]
        )
    );
}

/// RFC §9.5.1, Order: an event no off-lock hook fires for takes no ticket,
/// and is not ordered against ticketed ones: the advisor's message commits
/// while the customer's check still runs.
#[tokio::test]
async fn a_message_no_check_fires_for_commits_during_another_s_check() {
    let kit = room().await;
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    let (held, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (hook_held, hook_go) = (Arc::clone(&held), Arc::clone(&go));
    let mut spec = before_broadcast("scanner", false);
    spec.filter.channel_ids = Some([channel("sms")].into());
    kit.hook(spec, move |_, _| {
        let (held, go) = (Arc::clone(&hook_held), Arc::clone(&hook_go));
        async move {
            held.notify_one();
            go.notified().await;
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let customer = sent(&kit, "Bonjour");
    tokio::time::timeout(BOUND, held.notified()).await.unwrap();
    let advisor = InboundMessage::new(channel("ws"), "advisor", TextContent::new("Hello"));
    let during = tokio::time::timeout(
        BOUND,
        kit.process_inbound(
            advisor,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("the advisor's message waited for the customer's check"))
    .unwrap();
    go.notify_one();
    let customer = tokio::time::timeout(BOUND, customer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            during.event.map(|event| event.index),
            customer.event.map(|event| event.index)
        ),
        // After the two attaches' lifecycle records (gap G-046).
        (Some(2), Some(3))
    );
}

/// RFC §9.5.1, Release: a call dropped while its off-lock check runs
/// releases its ticket. Were it left held, the next message's check would
/// wait for it forever.
#[tokio::test]
async fn a_call_cancelled_during_its_off_lock_check_releases_its_ticket() {
    let kit = room().await;
    let hung = Arc::new(AtomicBool::new(false));
    kit.hook(before_broadcast("scanner", false), move |_, _| {
        let first = !hung.swap(true, Ordering::SeqCst);
        async move {
            if first {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let dropped = tokio::time::timeout(
        Duration::from_millis(50),
        kit.process_inbound(
            message("Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await;
    let next = tokio::time::timeout(
        BOUND,
        kit.process_inbound(
            message("Still there?"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("the next call waited on a ticket left held"))
    .unwrap();
    assert_eq!(
        (dropped.is_err(), next.event.map(|event| event.index)),
        // After the attach's lifecycle record (gap G-046).
        (true, Some(1))
    );
}

/// RFC §9.5.1, §13.5: a call dropped in a locked hook, after its off-lock
/// check, leaves the room lock, the lane and the ticket free: a lifecycle
/// change and the next message go through.
#[tokio::test]
async fn a_call_cancelled_in_a_locked_hook_after_its_check_leaves_everything_free() {
    let kit = room().await;
    kit.hook(before_broadcast("scanner", false), |_, _| async {
        Ok(HookResult::allow(AllowOptions::default()))
    })
    .unwrap();
    let hung = Arc::new(AtomicBool::new(false));
    kit.hook(before_broadcast("policy", true), move |_, _| {
        let first = !hung.swap(true, Ordering::SeqCst);
        async move {
            if first {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let dropped = tokio::time::timeout(
        Duration::from_millis(50),
        kit.process_inbound(
            message("Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await;
    let paused = tokio::time::timeout(
        BOUND,
        kit.pause_room(&room_id(), PauseRoomOptions::default()),
    )
    .await;
    let next = tokio::time::timeout(
        BOUND,
        kit.process_inbound(
            message("Still there?"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("the next call waited on what the dropped one held"))
    .unwrap();
    assert_eq!(
        (
            dropped.is_err(),
            paused.is_ok(),
            next.event.map(|event| event.index)
        ),
        // After the attach's lifecycle record; the pause writes none (gap
        // G-046).
        (true, true, Some(1))
    );
}

/// A Canadian social insurance number, as `123-456-789`.
fn has_sin(text: &str) -> bool {
    text.split_whitespace().any(|word| {
        let groups: Vec<&str> = word.split('-').collect();
        groups.len() == 3
            && groups
                .iter()
                .all(|group| group.len() == 3 && group.chars().all(|c| c.is_ascii_digit()))
    })
}

/// A notice from the framework, for the scanner to inject.
fn notice(text: &str, now: Timestamp) -> RoomEvent {
    RoomEvent {
        id: "placeholder".parse().unwrap(),
        created_at: now,
        ..RoomEvent::new(
            room_id(),
            EventSource {
                direction: EventDirection::Outbound,
                ..EventSource::new(channel("system"), ChannelType::SYSTEM)
            },
            TextContent::new(text),
        )
    }
}

/// What B.2 leaves: the timeline, what each side received, the
/// observations, and the call's result.
#[derive(Debug, PartialEq)]
struct B2 {
    timeline: Vec<RoomEvent>,
    customer: Vec<String>,
    advisor: Vec<String>,
    observations: Vec<Observation>,
    blocked: bool,
    reason: Option<String>,
}

/// RFC Appendix B.2, as the `block_and_inject` example runs it, its
/// scanner registered off the lock or under it.
/// A recording transport `id` of `channel_type`.
fn recording(id: &str, channel_type: ChannelType) -> RecordingTransport {
    RecordingTransport::new(
        channel(id),
        channel_type,
        RecordingTransportOptions::default(),
    )
}

async fn block_and_inject(needs_lock: bool) -> B2 {
    let now = Timestamp::from_unix_millis(1_792_677_780_000).unwrap();
    let kit = kit_with_environment(RoomKitOptions::default(), SequentialEnvironment::new(now));
    let customer = recording("sms_customer", ChannelType::SMS);
    let advisor = recording("ws_advisor", ChannelType::WEBSOCKET);
    kit.register_channel(customer.clone()).unwrap();
    kit.register_channel(advisor.clone()).unwrap();
    let to_customer = notice("Message blocked. Do not send your SIN by SMS.", now);
    let to_advisor = notice("The client attempted to send a SIN. Blocked.", now);
    kit.hook(
        before_broadcast("sensitivity_scanner", needs_lock),
        move |event, context| {
            let found = has_sin(&text_of(&event.content));
            let (to_customer, to_advisor) = (to_customer.clone(), to_advisor.clone());
            async move {
                if !found {
                    return HookReturn::Ok(HookResult::allow(AllowOptions::default()));
                }
                let observation = Observation {
                    created_at: event.created_at,
                    ..Observation::new(
                        "obs_sin".parse()?,
                        context.room.id.clone(),
                        channel("sms_customer"),
                        "SIN sent by SMS",
                    )
                };
                Ok(HookResult::block(
                    "SIN detected",
                    BlockOptions {
                        injected: Some(vec![
                            InjectedEvent {
                                target_channel_ids: Some(vec![channel("sms_customer")]),
                                ..InjectedEvent::new(to_customer)
                            },
                            InjectedEvent {
                                target_channel_ids: Some(vec![channel("ws_advisor")]),
                                ..InjectedEvent::new(to_advisor)
                            },
                        ]),
                        observations: Some(vec![observation]),
                        ..Default::default()
                    },
                ))
            }
        },
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        customer.channel_id(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        advisor.channel_id(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let sin = InboundMessage::new(
        customer.channel_id().clone(),
        "+15551234567",
        TextContent::new("Mon NAS est 123-456-789"),
    );
    let result = kit
        .process_inbound(
            sin,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    B2 {
        timeline: kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap(),
        customer: customer.texts(),
        advisor: advisor.texts(),
        observations: kit.store().list_observations(&room_id()).await.unwrap(),
        blocked: result.blocked,
        reason: result.reason,
    }
}

/// The timeline as `#index source status text` lines.
fn lines(timeline: &[RoomEvent]) -> Vec<String> {
    timeline
        .iter()
        .map(|event| {
            let status = match &event.blocked_by {
                Some(blocked_by) => format!("blocked by {blocked_by}"),
                None => format!("{:?}", event.status).to_lowercase(),
            };
            format!(
                "#{} {} {status} {:?}",
                event.index,
                event.source.channel_id,
                text_of(&event.content)
            )
        })
        .collect()
}

#[tokio::test]
async fn b2_with_its_scanner_off_the_lock_gives_block_and_inject_s_timeline() {
    let off_lock = block_and_inject(false).await;
    assert_eq!(
        lines(&off_lock.timeline),
        // The two attaches' lifecycle records come first (gap G-046), where
        // RFC Appendix B, which assumes none, starts at the message.
        [
            "#0 system delivered \"Channel sms_customer attached (read_write, all)\"",
            "#1 system delivered \"Channel ws_advisor attached (read_write, all)\"",
            "#2 sms_customer blocked by sensitivity_scanner \"Mon NAS est 123-456-789\"",
            "#3 system delivered \"Message blocked. Do not send your SIN by SMS.\"",
            "#4 system delivered \"The client attempted to send a SIN. Blocked.\"",
        ]
    );
}

/// Everything B.2 leaves is the same off the lock as under it: the
/// timeline record for record, the deliveries, the observation and the
/// result.
#[tokio::test]
async fn b2_leaves_the_same_room_off_the_lock_as_under_it() {
    let under_the_lock = block_and_inject(true).await;
    let off_the_lock = block_and_inject(false).await;
    assert_eq!(off_the_lock, under_the_lock);
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
