//! RFC §12.10.4, rules 4 and 5 (RMK-1030): the running work gets one grace
//! of 5 s from the close's start, then is cut, a write included, and
//! every call the close cuts returns one result, `kit_closed`, with what
//! it delivered and the answers already stored. Python's
//! `tests/test_kit_close.py`, sections 4, 5 and 7.
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType,
    DeliveryResultStatus, HookResult, RoomContext, RoomEvent, RoomStatus, TextContent,
};
use roomkit::testing::EchoAgent;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CommitEventOptions,
    GetRoomOptions, HookExecution, HookSpec, HookTrigger, InMemoryStore, PauseRoomOptions,
    SendEventOptions, text_of,
};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::held::{
    CUT_BY_CLOSE, Hold, Holding, a_send_held, closed, cut, deferred, into, kit_of, record,
    result_of, room, said, spawn_close, spawn_said, stored,
};
use crate::store::StuckStore;
use crate::support::channel;
use crate::warnings::Warnings;

/// The statuses of a call's delivery results, with a failure's code and
/// whether it is retryable.
fn statuses(
    result: &roomkit::InboundResult,
) -> BTreeMap<String, (DeliveryResultStatus, Option<(String, bool)>)> {
    result
        .delivery_results
        .iter()
        .map(|(id, delivered)| {
            let error = delivered
                .error
                .as_ref()
                .map(|error| (error.code.clone(), error.retryable));
            (id.as_str().to_owned(), (delivered.status, error))
        })
        .collect()
}

/// RFC §12.10.4: a send ending within the grace is delivered; its
/// handle, awaited while the close runs, reads the send it made, and
/// `kit_closed`, the call under way when the close started (Python's
/// `test_a_send_ending_within_the_grace_is_delivered`). On fefbfac it read
/// no reason.
#[tokio::test(start_paused = true)]
async fn a_send_ending_within_the_grace_is_delivered() {
    let held = Holding::new("ws", [("send", Hold::For(Duration::from_secs(1)))]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    let call = deferred(&kit, "send", "r").await;
    held.entered("send").await;

    let close = spawn_close(&kit);
    let result = call.delivery.unwrap().wait().await.unwrap();
    closed(close).await;

    assert_eq!(
        (statuses(&result), result.cancellation_reason.as_deref()),
        (
            BTreeMap::from([("ws".to_owned(), (DeliveryResultStatus::Sent, None))]),
            Some("kit_closed")
        )
    );
}

/// RFC §12.10.4: a send outlasting the grace is cut: its target reads
/// `failed`, code `kit_closed`, retryable, while a target delivered keeps
/// `sent` (Python's
/// `test_a_send_outlasting_the_grace_is_cut_and_reported_failed`). On
/// fefbfac the call read `lane_cancelled`, with no delivery result.
#[tokio::test(start_paused = true)]
async fn a_send_outlasting_the_grace_is_cut_and_reported_failed() {
    let quick = Holding::new("a", []);
    let held = Holding::new("b", [("both", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&quick, &held], &["r"]).await;
    let call = deferred(&kit, "both", "r").await;
    held.entered("both").await;
    let started = Instant::now();

    closed(spawn_close(&kit)).await;

    let took = started.elapsed();
    let result = call.delivery.unwrap().wait().await.unwrap();
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took < Duration::from_secs(6),
            result.cancellation_reason.as_deref(),
            statuses(&result),
            held.sent(),
            held.steps(),
        ),
        (
            true,
            Some("kit_closed"),
            BTreeMap::from([
                ("a".to_owned(), (DeliveryResultStatus::Sent, None)),
                (
                    "b".to_owned(),
                    (
                        DeliveryResultStatus::Failed,
                        Some(("kit_closed".to_owned(), true))
                    )
                ),
            ]),
            Vec::<String>::new(),
            vec!["send cut", "closed"],
        )
    );
}

/// RFC §12.10.4: a commit past its caller's decision, the caller gone,
/// gets the grace, then is cut: nothing of it is written after `close()`
/// (Python's `test_a_write_outlasting_the_grace_is_cut_and_never_lands`).
/// On fefbfac the close left the write running.
#[tokio::test(start_paused = true)]
async fn a_write_outlasting_the_grace_is_cut_and_never_lands() {
    let store = StuckStore::hanging("slow", false);
    let committing = Arc::clone(&store.committing);
    let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
    let call = spawn_said(&kit, "slow", "r");
    committing.notified().await;
    call.abort();

    closed(spawn_close(&kit)).await;

    assert_eq!(
        (kit.store().was_cut("commit"), stored(&kit, "r").await),
        (true, Vec::<String>::new())
    );
}

/// RFC §12.10.4: a room change whose write outlasts the grace is cut: the
/// call returns `KitClosed`, and the room is as it was (Python's
/// `test_a_room_change_outlasting_the_grace_is_cut_and_never_lands`, its
/// `update_room_metadata` a pause here). On fefbfac the change ran on
/// past the close.
#[tokio::test(start_paused = true)]
async fn a_room_change_outlasting_the_grace_is_cut_and_never_lands() {
    let store = StuckStore::default();
    let (committing, rooms_hang) = (Arc::clone(&store.committing), Arc::clone(&store.rooms_hang));
    let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
    rooms_hang.store(true, std::sync::atomic::Ordering::SeqCst);
    let change = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.pause_room(&room("r"), PauseRoomOptions::default())
                .await
        }
    });
    committing.notified().await;

    closed(spawn_close(&kit)).await;

    let changed = tokio::time::timeout(Duration::from_secs(1), change).await;
    rooms_hang.store(false, std::sync::atomic::Ordering::SeqCst);
    let status = kit
        .get_room(&room("r"), GetRoomOptions::default())
        .await
        .unwrap()
        .status;
    assert_eq!(
        (
            changed
                .map(|joined| format!("{:?}", joined.unwrap().err()))
                .ok(),
            kit.store().was_cut("room"),
            status
        ),
        (Some("Some(KitClosed)".to_owned()), true, RoomStatus::Active)
    );
}

/// What a call through `door` whose commit lands as the grace runs out
/// came to.
async fn a_commit_landed_as_it_was_cut(door: &str) -> (String, Vec<String>, Vec<String>) {
    let store = StuckStore::hanging("slow", true);
    let committing = Arc::clone(&store.committing);
    let held = Holding::new("ws", []);
    let kit = kit_of(store, &[&held], &["r"]).await;
    let call = tokio::spawn({
        let (kit, door) = (kit.clone(), door.to_owned());
        async move {
            let r = room("r");
            match door.as_str() {
                "process_inbound" => {
                    let result = kit.process_inbound(said("slow"), into("r")).await.unwrap();
                    format!(
                        "{:?} {:?} {}",
                        result.event.as_ref().map(|event| text_of(&event.content)),
                        cut(&result),
                        result.delivery_results.len()
                    )
                }
                "send_event" => {
                    let (content, sms) = (TextContent::new("slow"), channel("sms"));
                    let sent = kit.send_event(&r, &sms, content, SendEventOptions::default());
                    format!("{:?}", sent.await.map(|event| text_of(&event.content)))
                }
                _ => {
                    let committed =
                        kit.commit_event(&r, record("slow"), CommitEventOptions::default());
                    format!("{:?}", committed.await.map(|event| text_of(&event.content)))
                }
            }
        }
    });
    committing.notified().await;

    closed(spawn_close(&kit)).await;

    let ended = tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .map_or_else(|_| "did not end".to_owned(), Result::unwrap);
    (ended, stored(&kit, "r").await, held.sent())
}

/// RFC §12.10.4 (D7): the close cuts a call whose commit had started and
/// landed: the call reads its event back within the unwind and reports it
/// stored, never refused, delivered to no one (Python's
/// `test_a_call_whose_commit_landed_as_it_was_cut_reports_it_stored`). On
/// fefbfac the call waited for its write, which never ended.
#[tokio::test(start_paused = true)]
async fn a_call_whose_commit_landed_as_it_was_cut_reports_it_stored() {
    let mut rows = Vec::new();
    for door in ["process_inbound", "send_event", "commit_event"] {
        rows.push((door, a_commit_landed_as_it_was_cut(door).await));
    }
    let slow = || vec!["slow".to_owned()];
    assert_eq!(
        rows,
        [
            (
                "process_inbound",
                (
                    r#"Some("slow") (false, None, Some("kit_closed")) 0"#.to_owned(),
                    slow(),
                    vec![]
                )
            ),
            ("send_event", (r#"Ok("slow")"#.to_owned(), slow(), vec![])),
            ("commit_event", (r#"Ok("slow")"#.to_owned(), slow(), vec![])),
        ]
    );
}

/// RFC §12.10.4: the limit of the read-back: one that fails leaves the
/// call reported as not stored, whatever the store holds, and a warning
/// says so (Python's
/// `test_a_cut_commit_whose_read_back_fails_reports_nothing_stored_and_says_so`).
#[tokio::test(start_paused = true)]
async fn a_cut_commit_whose_read_back_fails_reports_nothing_stored_and_says_so() {
    let (logs, logging) = Warnings::capture();
    let store = StuckStore::hanging("slow", true).failing_read_back();
    let committing = Arc::clone(&store.committing);
    let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
    let call = spawn_said(&kit, "slow", "r");
    committing.notified().await;

    closed(spawn_close(&kit)).await;

    let result = result_of(call).await;
    drop(logging);
    let said_so = logs
        .containing("reading it back failed")
        .iter()
        .any(|line| line.starts_with("WARN"));
    assert_eq!((cut(&result), said_so), (CUT_BY_CLOSE, true));
}

/// RFC §12.10.4: the work outlasting the grace is cut before the channels
/// close (Python's
/// `test_the_work_outlasting_the_grace_is_cut_before_the_channels_close`).
/// On fefbfac too.
#[tokio::test(start_paused = true)]
async fn the_work_outlasting_the_grace_is_cut_before_the_channels_close() {
    let held = Holding::new("ws", [("held", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;

    closed(spawn_close(&kit)).await;

    assert_eq!(held.steps(), ["send cut", "closed"]);
}

/// RFC §12.10.4: each call the close cuts reads `kit_closed`, with what it
/// delivered: the send cut `failed`, the one queued nothing (Python's
/// `test_each_call_the_close_cuts_reads_kit_closed_with_what_it_delivered`,
/// its `m1` and `m2`). On fefbfac they read `lane_cancelled` and
/// `kit_closed`, with no delivery result.
#[tokio::test(start_paused = true)]
async fn each_call_the_close_cuts_reads_kit_closed_with_what_it_delivered() {
    let held = Holding::new("ws", [("m1", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    deferred(&kit, "m0", "r").await;
    let m1 = deferred(&kit, "m1", "r").await;
    let m2 = deferred(&kit, "m2", "r").await;
    held.entered("m1").await;

    closed(spawn_close(&kit)).await;

    let mut read = Vec::new();
    for call in [m1, m2] {
        let result = call.delivery.unwrap().wait().await.unwrap();
        read.push((result.cancellation_reason.clone(), statuses(&result)));
    }
    let kit_closed = || Some("kit_closed".to_owned());
    assert_eq!(
        read,
        [
            (
                kit_closed(),
                BTreeMap::from([(
                    "ws".to_owned(),
                    (
                        DeliveryResultStatus::Failed,
                        Some(("kit_closed".to_owned(), true))
                    )
                )])
            ),
            (kit_closed(), BTreeMap::new()),
        ]
    );
}

/// An intelligence channel whose turn on another agent's answer never
/// ends.
#[derive(Clone, Default)]
struct StuckOnAnswers {
    started: Arc<Notify>,
}

impl Channel for StuckOnAnswers {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("stuck"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let started = Arc::clone(&self.started);
        async move {
            if event.source.channel_id == channel("ai") {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §12.10.4: a call the close cuts carries the answers its cascade
/// stored (Python's `test_a_cut_call_carries_the_answers_already_stored`).
#[tokio::test(start_paused = true)]
async fn a_cut_call_carries_the_answers_already_stored() {
    let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
    let stuck = StuckOnAnswers::default();
    kit.register_channel(EchoAgent::new(channel("ai"), "hello"))
        .unwrap();
    kit.register_channel(stuck.clone()).unwrap();
    for id in ["ai", "stuck"] {
        kit.attach_channel(&room("r"), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let call = spawn_said(&kit, "hi", "r");
    stuck.started.notified().await;

    closed(spawn_close(&kit)).await;

    let result = result_of(call).await;
    let answers: Vec<String> = result
        .response_events
        .iter()
        .map(|event| text_of(&event.content))
        .collect();
    assert_eq!(
        (result.cancellation_reason.as_deref(), answers),
        (Some("kit_closed"), vec!["hello".to_owned()])
    );
}

/// RFC §12.10.4: a send, a decided write, an observer and a hook holding
/// its room's lock, none ever ending: the close cuts them all once its
/// grace ran out, and returns then (Python's
/// `test_the_close_takes_its_grace_with_every_kind_of_work_stuck`, 19.8 s
/// before RMK-1030). On fefbfac it took 15 s.
#[tokio::test(start_paused = true)]
async fn the_close_takes_its_grace_with_every_kind_of_work_stuck() {
    let store = StuckStore::hanging("slow", false);
    let committing = Arc::clone(&store.committing);
    let held = Holding::new("ws", [("held", Hold::ForEver)]);
    let kit = kit_of(store, &[&held], &["send", "write", "observer", "lock"]).await;
    let (observing, locking) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let told = Arc::clone(&observing);
    kit.observe(
        "observer",
        HookTrigger::AfterBroadcast,
        move |event, _context| {
            let told = Arc::clone(&told);
            let ours = text_of(&event.content) == "observed";
            async move {
                if ours {
                    told.notify_one();
                    std::future::pending::<()>().await;
                }
            }
        },
    )
    .unwrap();
    let told = Arc::clone(&locking);
    let spec = HookSpec::new("locker", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    kit.hook(spec, move |event, _context| {
        let told = Arc::clone(&told);
        let ours = text_of(&event.content) == "locked";
        async move {
            if ours {
                told.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    a_send_held(&kit, &held, "send").await;
    let calls = [
        spawn_said(&kit, "observed", "observer"),
        spawn_said(&kit, "locked", "lock"),
    ];
    let write = spawn_said(&kit, "slow", "write");
    observing.notified().await;
    locking.notified().await;
    committing.notified().await;
    write.abort();
    let started = Instant::now();

    closed(spawn_close(&kit)).await;

    let took = started.elapsed();
    for call in calls {
        drop(tokio::time::timeout(Duration::from_secs(1), call).await);
    }
    assert!(
        took >= Duration::from_secs(5) && took < Duration::from_millis(5500),
        "the close took {took:?}"
    );
}
