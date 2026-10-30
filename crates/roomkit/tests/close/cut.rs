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
    DeliveryResultStatus, HookResult, Observation, ObservationId, RoomContext, RoomEvent,
    RoomStatus, TextContent,
};
use roomkit::testing::EchoAgent;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CommitEventOptions,
    ConversationStore, GetRoomOptions, HookExecution, HookSpec, HookTrigger, InMemoryStore,
    PauseRoomOptions, SendEventOptions, text_of,
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

/// A transport whose `on_event()` notes an observation on each event.
#[derive(Clone)]
struct Noting;

impl Channel for Noting {
    fn declaration(&self) -> ChannelDeclaration {
        crate::support::transport("noting")
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let observation = Observation::at(
            ObservationId::from_u128(1),
            event.room_id.clone(),
            channel("noting"),
            "seen",
            event.created_at,
        );
        async move {
            Ok(ChannelOutput {
                observations: vec![observation],
                ..ChannelOutput::empty()
            })
        }
    }
}

/// RFC §12.10.4: a call the close cut past its decision, its send cut
/// once the grace ran out, writes nothing more: the observation a target
/// noted is never stored, as nothing else of a cut plan's aftermath runs in
/// the Python implementation (`_report_cut`); the call reads its cut. On
/// 01c8420 the cut run still wrote its side effects.
#[tokio::test(start_paused = true)]
async fn a_call_cut_past_its_decision_writes_nothing_more() {
    let held = Holding::new("ws", [("noted", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    kit.register_channel(Noting).unwrap();
    kit.attach_channel(
        &room("r"),
        &channel("noting"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let call = deferred(&kit, "noted", "r").await;
    held.entered("noted").await;

    closed(spawn_close(&kit)).await;

    let result = call.delivery.unwrap().wait().await.unwrap();
    let observations = kit.store().list_observations(&room("r")).await.unwrap();
    assert_eq!(
        (result.cancellation_reason.as_deref(), observations.len()),
        (Some("kit_closed"), 0)
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

/// The doors a call's commit goes through.
const COMMITTING: [&str; 3] = ["process_inbound", "send_event", "commit_event"];

/// A call of "slow" into `r` through `door`, on a task of its own: what it
/// came to.
fn commit_through(
    kit: &roomkit::RoomKit<StuckStore>,
    door: &'static str,
) -> tokio::task::JoinHandle<String> {
    let kit = kit.clone();
    tokio::spawn(async move {
        let r = room("r");
        match door {
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
                let committed = kit.commit_event(&r, record("slow"), CommitEventOptions::default());
                format!("{:?}", committed.await.map(|event| text_of(&event.content)))
            }
        }
    })
}

/// What a call through `door` whose commit lands as the grace runs out
/// came to.
async fn a_commit_landed_as_it_was_cut(door: &'static str) -> (String, Vec<String>, Vec<String>) {
    let store = StuckStore::hanging("slow", true);
    let committing = Arc::clone(&store.committing);
    let held = Holding::new("ws", []);
    let kit = kit_of(store, &[&held], &["r"]).await;
    let call = commit_through(&kit, door);
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
    for door in COMMITTING {
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

/// RFC §12.10.4: the limit of the read-back: one that fails, or that does
/// not end within the close's unwind, leaves the call reported as not
/// stored, whatever the store holds, and a warning says so, through each
/// door whose call commits (Python's
/// `test_a_cut_commit_whose_read_back_fails_reports_nothing_stored_and_says_so`).
/// On 01c8420 the close aborted a message or a `send_event` whose read-back
/// hung before it timed out, and nothing was logged
/// (`rmk1030-rs-review-A/probe1-3-5.out`).
#[tokio::test(start_paused = true)]
async fn a_cut_commit_whose_read_back_fails_reports_nothing_stored_and_says_so() {
    let mut rows = Vec::new();
    for hangs in [false, true] {
        for door in COMMITTING {
            let (logs, logging) = Warnings::capture();
            let store = StuckStore::hanging("slow", true);
            let store = if hangs {
                store.hanging_read_back()
            } else {
                store.failing_read_back()
            };
            let committing = Arc::clone(&store.committing);
            let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
            let call = commit_through(&kit, door);
            committing.notified().await;

            closed(spawn_close(&kit)).await;

            let ended = tokio::time::timeout(Duration::from_secs(1), call)
                .await
                .map_or_else(|_| "did not end".to_owned(), Result::unwrap);
            drop(logging);
            let said_so = logs
                .containing("reading it back failed")
                .iter()
                .filter(|line| line.starts_with("WARN"))
                .count();
            rows.push(((hangs, door), ended, said_so));
        }
    }
    let message = format!("None {CUT_BY_CLOSE:?} 0");
    let expected = [false, true].into_iter().flat_map(|hangs| {
        [
            ((hangs, "process_inbound"), message.clone(), 1),
            ((hangs, "send_event"), "Err(KitClosed)".to_owned(), 1),
            ((hangs, "commit_event"), "Err(KitClosed)".to_owned(), 1),
        ]
    });
    assert_eq!(rows, expected.collect::<Vec<_>>());
}

/// RFC §12.10.4: a run whose read-back hangs ends within the unwind from
/// the cut all the same when a close joined from a task its hook kept in
/// its context spares it meanwhile, so that the close's abort no longer
/// reaches it: the read-back has its own deadline, as Python's
/// `timeout_at(cut_at + CUT_UNWIND_S)`, the call reads as cut before its
/// commit, a warning says so, and the kit's work ends (review A's probe 7,
/// `rmk1030-rs-review-A/light/probes_1_6_7.out`). On d192534 the call
/// never returned and the kit never went idle.
#[tokio::test(start_paused = true)]
async fn a_spared_run_whose_read_back_hangs_ends_within_its_unwind() {
    let (logs, logging) = Warnings::capture();
    let store = StuckStore::hanging("slow", true).hanging_read_back();
    let committing = Arc::clone(&store.committing);
    let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
    let go = Arc::new(Notify::new());
    let (weak, waiting) = (kit.downgrade(), Arc::clone(&go));
    let spec = HookSpec::new(
        "late_closer",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    kit.hook(spec, move |event, _context| {
        let (weak, waiting) = (weak.clone(), Arc::clone(&waiting));
        let ours = text_of(&event.content) == "slow";
        async move {
            if ours {
                tokio::spawn(roomkit::keep_context(async move {
                    waiting.notified().await;
                    if let Some(kit) = weak.upgrade() {
                        drop(kit.close().await);
                    }
                }));
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let call = spawn_said(&kit, "slow", "r");
    committing.notified().await;
    let close = spawn_close(&kit);
    // Past the grace: the run is cut and reads its commit back, which
    // hangs; the hook's task joins the close then, sparing the run.
    tokio::time::sleep(Duration::from_millis(5300)).await;
    go.notify_one();

    closed(close).await;

    let ended = tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .map(|joined| format!("{:?}", cut(&joined.unwrap().unwrap())));
    let idle = tokio::time::timeout(Duration::from_secs(30), roomkit::testing::idle(&kit))
        .await
        .is_ok();
    drop(logging);
    let said_so = logs.containing("reading it back failed").len();
    let cut_by_close = format!("{CUT_BY_CLOSE:?}");
    assert_eq!((ended.ok(), idle, said_so), (Some(cut_by_close), true, 1));
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
/// stored (Python's `test_a_cut_call_carries_the_answers_already_stored`),
/// its run ending on its cut, or aborted once its unwind ran out, its
/// write of the room's delivery cursor never ending: its record reports
/// them then (killing `rmk1030-rs-review-A` mutant M18).
#[tokio::test(start_paused = true)]
async fn a_cut_call_carries_the_answers_already_stored() {
    let mut rows = Vec::new();
    for cursor_hangs in [false, true] {
        let store = StuckStore::default();
        let cursor = Arc::clone(&store.cursor_writes_left);
        let kit = kit_of(store, &[&Holding::new("ws", [])], &["r"]).await;
        let stuck = StuckOnAnswers::default();
        kit.register_channel(EchoAgent::new(channel("ai"), "hello"))
            .unwrap();
        kit.register_channel(stuck.clone()).unwrap();
        for id in ["ai", "stuck"] {
            kit.attach_channel(&room("r"), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        if cursor_hangs {
            // The cursor of "hi" moves; the one of the answer never does.
            *cursor.lock().unwrap() = Some(1);
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
        rows.push((cursor_hangs, result.cancellation_reason, answers));
    }
    let row = |hangs| {
        (
            hangs,
            Some("kit_closed".to_owned()),
            vec!["hello".to_owned()],
        )
    };
    assert_eq!(rows, [row(false), row(true)]);
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
