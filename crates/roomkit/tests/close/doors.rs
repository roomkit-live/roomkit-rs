//! RFC §12.10.4: a close from the kit's own work neither waits for that
//! work nor cuts it, and the work stores and delivers nothing once the kit
//! is sealed, as Python's `test_kit_close_doors.py`.
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelBinding, ChannelOutput, HookResult, RoomContext, RoomEvent, RoomId};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    HookExecution, HookOptions, HookTrigger, RoomKit, ToolCallEvent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::Barrier;
use tokio::time::Instant;

use crate::support::{
    AT_ONCE, BOUND, Closer, DOORS, Door, Gate, Witness, attach_agent, channel, ended, kit_with, ok,
    open, room_id, sealed, spawn_message, stored, transport,
};

/// RFC §12.10.4: the kit's own work closes the kit: its close neither
/// waits for nor cuts the work it runs within, and returns once every
/// channel is closed; the kit is sealed, the work's call ends as Python's
/// does (`Door::call_ends`), and nothing more is stored or delivered. On
/// fefbfac the calls under way read no reason, the SYNC hook's
/// `KitClosed`.
#[tokio::test(start_paused = true)]
async fn a_close_from_the_kit_s_own_work_returns_and_closes_the_kit() {
    let mut doors = Vec::new();
    for door in DOORS {
        let witness = Witness::new();
        let started = Instant::now();
        let (kit, closer, call) = open(door, &witness, false).await;
        let returned = closer.returned_once_closed().await;
        let at_once = started.elapsed() < AT_ONCE;
        let call = ended(call).await;
        roomkit::testing::idle(&kit).await;
        doors.push((
            door,
            (returned, at_once, sealed(&kit).await),
            call,
            stored(&kit).await,
            witness.delivered_before_its_close("Bye."),
        ));
    }
    let expected = DOORS.map(|door| {
        (
            door,
            (Some(true), true, true),
            Some(door.call_ends(false)),
            door.stores(),
            (door.delivers(), true),
        )
    });
    assert_eq!(doors, expected);
}

/// The doors a close can join another from: a tool's call is cut by the
/// running close, with no grace, before it could (RFC §10.6.8), as
/// Python's turn is.
const JOINING: [Door; 4] = [
    Door::SyncHook,
    Door::DeliveryHook,
    Door::Delivery,
    Door::Observer,
];

/// RFC §12.10.4: another close runs, and waits for the work (its grace),
/// when that work closes the kit too: from then on it is spared, and both
/// closes return once the kit is closed.
#[tokio::test(start_paused = true)]
async fn a_close_from_the_kit_s_own_work_joining_another_spares_that_work() {
    let mut doors = Vec::new();
    for door in JOINING {
        let witness = Witness::new();
        let (kit, closer, call) = open(door, &witness, true).await;
        tokio::time::timeout(BOUND, closer.entered.notified())
            .await
            .unwrap();
        let started = Instant::now();
        let first = tokio::spawn({
            let kit = kit.clone();
            async move { kit.close().await }
        });
        // The first close's grace reaches the work, and waits for it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        closer.go.notify_one();
        let returned = closer.returned_once_closed().await;
        let first = tokio::time::timeout(BOUND, first).await.unwrap().unwrap();
        let at_once = started.elapsed() < AT_ONCE;
        let call = ended(call).await;
        roomkit::testing::idle(&kit).await;
        doors.push((door, (returned, first, at_once), call, stored(&kit).await));
    }
    let expected = JOINING.map(|door| {
        (
            door,
            (Some(true), Ok(()), true),
            Some(door.call_ends(true)),
            door.stores(),
        )
    });
    assert_eq!(doors, expected);
}

/// A transport whose deliveries each close the kit, once both are in
/// their delivery.
#[derive(Clone)]
struct ClosingTransport {
    kit: Arc<Mutex<Option<RoomKit>>>,
    together: Arc<Barrier>,
    returned: Arc<AtomicUsize>,
}

impl Channel for ClosingTransport {
    fn declaration(&self) -> ChannelDeclaration {
        transport("closing")
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            this.together.wait().await;
            let kit = this.kit.lock().unwrap().clone();
            if let Some(kit) = kit
                && kit.close().await.is_ok()
            {
                this.returned.fetch_add(1, Ordering::SeqCst);
            }
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §12.10.4: two deliveries, in two rooms, each close the kit at once:
/// the second close joins the shutdown the first started, which spares
/// both, and both return once the kit is closed (Python's
/// `test_two_sessions_ending_at_once_each_close_the_kit`).
#[tokio::test(start_paused = true)]
async fn two_deliveries_closing_the_kit_at_once_each_close_it() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closing = ClosingTransport {
        kit: Arc::new(Mutex::new(Some(kit.clone()))),
        together: Arc::new(Barrier::new(2)),
        returned: Arc::default(),
    };
    kit.register_channel(closing.clone()).unwrap();
    let rooms = [room_id(), RoomId::new("room_2").unwrap()];
    for room in &rooms {
        if room != &room_id() {
            kit.create_room(CreateRoomOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        kit.attach_channel(room, &channel("closing"), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let started = Instant::now();
    let calls = rooms.map(|room| spawn_message(&kit, "Bye.", room));
    let mut ended_calls = 0;
    for call in calls {
        ended_calls += usize::from(ended(call).await.is_some());
    }
    assert_eq!(
        (
            ended_calls,
            closing.returned.load(Ordering::SeqCst),
            started.elapsed() < AT_ONCE,
            witness.closed.load(Ordering::SeqCst)
        ),
        (2, 2, true, true)
    );
}

/// A transport whose delivery runs `closer` in a task it spawns with
/// `keep_context`, and waits for that task.
#[derive(Clone)]
struct KeepingContext {
    closer: Arc<Mutex<Option<Closer>>>,
}

impl Channel for KeepingContext {
    fn declaration(&self) -> ChannelDeclaration {
        transport("keeping")
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let closer = self.closer.lock().unwrap().take();
        async move {
            if let Some(closer) = closer {
                let task = tokio::spawn(roomkit::keep_context(async move { closer.close().await }));
                task.await.unwrap();
            }
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §12.10.4: a task the kit's work spawns with `keep_context` is that
/// work: a close it calls neither waits for the delivery awaiting the task
/// nor cuts it, as `keep_context` promises, the Rust form of Python's
/// `test_a_close_from_a_held_run_does_not_wait_for_it`. The call, under
/// way as the close started, reads `kit_closed`.
#[tokio::test(start_paused = true)]
async fn a_close_from_a_task_kept_in_its_context_does_not_wait_for_its_work() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, false);
    let keeping = KeepingContext {
        closer: Arc::new(Mutex::new(Some(closer.clone()))),
    };
    kit.register_channel(keeping).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("keeping"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let started = Instant::now();
    let call = spawn_message(&kit, "Bye.", room_id());
    let returned = closer.returned_once_closed().await;
    let at_once = started.elapsed() < AT_ONCE;
    assert_eq!(
        (returned, at_once, ended(call).await, sealed(&kit).await),
        (Some(true), true, Some(ok(Some("kit_closed"), None)), true)
    );
}

/// RFC §12.10.4: an observer the kit fires and forgets is no work its run
/// waits for: a close it calls spares the observer alone, and the close
/// gives the run that fired it its grace, as a close from outside the
/// kit's work does. An ASYNC `BEFORE_BROADCAST` observer closes once its
/// run's delivery of 2 s began: the close returns once the delivery ended,
/// the witness reading its text before its close, as Python's
/// (`py_review_probe.out`, leak observer), the call under way reading
/// `kit_closed`; on 1d8796c the close returned at once and the delivery
/// came to a closed channel.
#[tokio::test(start_paused = true)]
async fn a_close_from_an_observer_waits_for_the_run_that_fired_it() {
    let gate = Gate::default();
    let witness = Witness::slow("Bye.", Duration::from_secs(2)).gated(gate.clone());
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, false);
    let work = closer.clone();
    let spec = HookOptions {
        execution: HookExecution::Async,
        name: "quit".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_event: RoomEvent, _context| {
            let (work, gate) = (work.clone(), gate.clone());
            async move {
                gate.began().await;
                gate.go();
                work.close().await;
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
    .unwrap();
    let started = Instant::now();
    let call = spawn_message(&kit, "Bye.", room_id());
    let returned = closer.returned_once_closed().await;
    let took = started.elapsed();
    assert_eq!(
        (
            returned,
            took >= Duration::from_secs(2) && took < Duration::from_secs(3),
            ended(call).await,
            witness.delivered()
        ),
        (
            Some(true),
            true,
            Some(ok(Some("kit_closed"), None)),
            vec!["Bye.".to_owned()]
        )
    );
}

/// RFC §12.10.4: a tool call's observer closes while the turn goes on to a
/// tool of 8 s: an observer the kit fires and forgets starts with fresh
/// marks, so its close spares the observer alone and cuts the turn as a
/// close from outside does, at once (RFC §10.6.8), the close returning at
/// once, the slow tool never ending and no answer stored; on 1d8796c the
/// observer's close spared the turn, whose slow tool ran to its end and
/// whose answer was committed after the seal (`review_probe_1d8796c.out`,
/// leak tool observer). The call, under way as the close started, reads
/// `kit_closed`.
#[tokio::test(start_paused = true)]
async fn a_close_from_a_tool_s_observer_cuts_its_turn() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, false);
    let slow_ended = Arc::new(AtomicUsize::new(0));
    let ended_slow = Arc::clone(&slow_ended);
    let tools = Tools::new()
        .tool(
            ToolDefinition::new("quick", "Quick.", json!({ "type": "object" })),
            |_call, _context| async { Ok(ToolOutput::Text("quick".to_owned())) },
        )
        .tool(
            ToolDefinition::new("slow", "Slow.", json!({ "type": "object" })),
            move |_call, _context| {
                let ended_slow = Arc::clone(&ended_slow);
                async move {
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    ended_slow.fetch_add(1, Ordering::SeqCst);
                    Ok(ToolOutput::Text("slow".to_owned()))
                }
            },
        );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "quick", "{}")]),
        ScriptedRound::calling("", [("c2", "slow", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    attach_agent(&kit, model, tools).await;
    let work = closer.clone();
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _context| {
            let work = work.clone();
            let quick_ended = event.name == "quick";
            async move {
                if quick_ended {
                    work.close().await;
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "quit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let started = Instant::now();
    let call = spawn_message(&kit, "Go.", room_id());
    let returned = closer.returned_once_closed().await;
    let at_once = started.elapsed() < AT_ONCE;
    let call = ended(call).await;
    roomkit::testing::idle(&kit).await;
    let stored = stored(&kit).await;
    assert_eq!(
        (
            (returned, at_once),
            call,
            slow_ended.load(Ordering::SeqCst),
            stored.iter().any(|text| text == "Done.")
        ),
        (
            (Some(true), true),
            Some(ok(Some("kit_closed"), None)),
            0,
            false
        )
    );
}

/// RFC §12.10.4: the close drops the delivery sets queued in a room's lane
/// as it starts, those of the work it spares included, as Python's sealed
/// lanes drop every queued entry. "One."'s witness delivery holds
/// `room_1`'s lane; "Two.", whose hook spawned a task kept in its context,
/// has its set queued behind it. That task closes the kit, then the
/// witness's delivery closes it too, joining: both runs are spared, and
/// "Two."'s set is dropped, never delivered, its agent never asked, its
/// call reading `kit_closed`, no error. On fefbfac the set delivered once
/// the kit was sealed, its agent's turn refused `backend_closed`, the call
/// carrying `TurnLinkError`.
#[tokio::test(start_paused = true)]
async fn a_spared_run_s_queued_set_is_dropped() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let one_closes = Closer::new(&kit, &witness, true);
    *witness.closes_on_delivery.lock().unwrap() = Some(one_closes.clone());
    let two_closes = Closer::new(&kit, &witness, true);
    let work = two_closes.clone();
    let spec = HookOptions {
        name: "keep".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let work = work.clone();
            let is_two = roomkit_core::rules::transcoding::text_of(&event.content) == "Two.";
            async move {
                if is_two {
                    drop(tokio::spawn(roomkit::keep_context(async move {
                        work.close().await;
                    })));
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
    .unwrap();
    let model = ScriptedProvider::new([
        ScriptedRound::answering("Hi."),
        ScriptedRound::answering("Hi."),
    ]);
    attach_agent(&kit, model, Tools::new()).await;
    let one = spawn_message(&kit, "One.", room_id());
    tokio::time::timeout(BOUND, one_closes.entered.notified())
        .await
        .unwrap();
    let two = spawn_message(&kit, "Two.", room_id());
    tokio::time::timeout(BOUND, two_closes.entered.notified())
        .await
        .unwrap();
    // "Two." commits and takes its place behind "One."'s set.
    tokio::time::sleep(Duration::from_millis(10)).await;
    two_closes.go.notify_one();
    // The shutdown's grace, "One."'s delivery holding the lane.
    tokio::time::sleep(Duration::from_millis(10)).await;
    one_closes.go.notify_one();
    let returned = (
        one_closes.returned_once_closed().await,
        two_closes.returned_once_closed().await,
    );
    let two_delivered = witness
        .delivered()
        .iter()
        .any(|text| text.starts_with("Two."));
    drop(one);
    assert_eq!(
        (returned, ended(two).await, two_delivered),
        (
            (Some(true), Some(true)),
            Some(ok(Some("kit_closed"), None)),
            false
        )
    );
}

/// A transport whose `on_event()` takes a second before its delivery,
/// which it notes.
#[derive(Clone, Default)]
struct Late {
    delivered: Arc<Mutex<Vec<String>>>,
}

impl Channel for Late {
    fn declaration(&self) -> ChannelDeclaration {
        transport("late")
    }

    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(ChannelOutput::default())
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.delivered
            .lock()
            .unwrap()
            .push(roomkit_core::rules::transcoding::text_of(&event.content));
        async { Ok(ChannelOutput::default()) }
    }
}

/// RFC §12.10.4: once the kit is sealed, the work a close spared delivers
/// nothing: the witness's delivery closes the kit, the close returns at
/// once, and a target of the same set whose `on_event()` ends a second
/// later is never delivered to (killing `rmk1030-rs-review-A` mutant M30).
#[tokio::test(start_paused = true)]
async fn a_spared_run_delivers_nothing_once_the_kit_is_sealed() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, false);
    *witness.closes_on_delivery.lock().unwrap() = Some(closer.clone());
    let late = Late::default();
    kit.register_channel(late.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("late"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let call = spawn_message(&kit, "Bye.", room_id());
    let returned = closer.returned_once_closed().await;
    drop(ended(call).await);
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        (
            returned,
            sealed(&kit).await,
            late.delivered.lock().unwrap().clone()
        ),
        (Some(true), true, Vec::<String>::new())
    );
}

/// How many times each tests below runs the observer door.
const OBSERVER_RUNS: usize = 20;

/// What the observer door comes to, `runs` times: an ASYNC
/// `BEFORE_BROADCAST` observer closes the kit, and each run reads what its
/// close answered, the call, what the room stores and what the witness
/// received, counted by outcome.
async fn observer_door_runs(runs: usize) -> std::collections::BTreeMap<String, usize> {
    let mut outcomes = std::collections::BTreeMap::new();
    for _ in 0..runs {
        // The witness's delivery is held until the observer's close
        // started: the call is under way as the close starts, whatever the
        // threads (review A, light pass: on threads the instant delivery
        // ended the run first in about one door in 30).
        let gate = Gate::default();
        let witness = Witness::slow("Bye.", Duration::ZERO).gated(gate.clone());
        let (kit, closer, call) = open(Door::Observer, &witness, false).await;
        tokio::time::timeout(BOUND, gate.began()).await.unwrap();
        tokio::time::timeout(BOUND, closer.closing.notified())
            .await
            .unwrap();
        gate.go();
        let returned = closer.returned_once_closed().await;
        let call = ended(call).await;
        roomkit::testing::idle(&kit).await;
        let outcome = format!(
            "{returned:?} {call:?} {:?} {:?}",
            stored(&kit).await,
            witness.delivered()
        );
        *outcomes.entry(outcome).or_insert(0) += 1;
    }
    outcomes
}

/// The one outcome of the observer door, as Python's
/// (`rmk1030-rs-review-B/py_probe.out`, observer door): the close returns,
/// the call reads `kit_closed`, "Bye." is stored and the witness receives
/// it, `runs` times.
fn delivered_every_time(runs: usize) -> std::collections::BTreeMap<String, usize> {
    let outcome = format!(
        "{:?} {:?} {:?} {:?}",
        Some(true),
        Some(ok(Some("kit_closed"), None)),
        ["Bye."],
        ["Bye."]
    );
    std::collections::BTreeMap::from([(outcome, runs)])
}

/// RFC §9.3, §12.10.4: an ASYNC `BEFORE_BROADCAST` observer that closes the
/// kit finds its event's set delivering: its hooks fire once the set took
/// its place in the idle lane, running from it, so the close gives the set
/// its grace, as Python's (maintainer's decision R1, 2026-10-30), on a
/// runtime of one thread.
#[tokio::test(start_paused = true)]
async fn an_observer_closing_the_kit_finds_its_set_delivering() {
    let outcomes = observer_door_runs(OBSERVER_RUNS).await;
    assert_eq!(outcomes, delivered_every_time(OBSERVER_RUNS));
}

/// The same on a runtime of threads, where the observer may run beside the
/// set's run: the set, placed first in the idle lane, runs from its
/// placement, so the outcome never depends on the runtime. On e5973d6 the
/// close dropped the set in some runs, its turn not yet taken
/// (`rmk1030-rs/pass1/observer_threads_before.out`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_closing_the_kit_finds_its_set_delivering_on_threads() {
    let outcomes = observer_door_runs(OBSERVER_RUNS).await;
    assert_eq!(outcomes, delivered_every_time(OBSERVER_RUNS));
}

/// What a call came to: its `cancellation_reason` and its delivery results'
/// statuses.
fn delivered_by(result: &roomkit::InboundResult) -> String {
    let statuses: Vec<String> = result
        .delivery_results
        .iter()
        .map(|(id, delivered)| format!("{}={:?}", id.as_str(), delivered.status))
        .collect();
    format!("{:?} {statuses:?}", result.cancellation_reason)
}

/// Two messages into `room_1`, "first"'s delivery held by its gate, an
/// ASYNC `BEFORE_BROADCAST` observer noting each event it observes, and,
/// when `closes`, closing the kit on "second": `runs` times, counted by
/// outcome, whether "second"'s observer ran while "first" still delivered,
/// what each call came to, what the witness received and what the room
/// stores.
async fn queued_observer_runs(
    closes: bool,
    runs: usize,
) -> std::collections::BTreeMap<String, usize> {
    let mut outcomes = std::collections::BTreeMap::new();
    for _ in 0..runs {
        let gate = Gate::default();
        let witness = Witness::slow("first", Duration::from_millis(10)).gated(gate.clone());
        let kit = kit_with(&witness).await;
        let seen = Arc::new(tokio::sync::Notify::new());
        let (weak, telling) = (kit.downgrade(), Arc::clone(&seen));
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _context| {
                let (weak, telling) = (weak.clone(), Arc::clone(&telling));
                let second = roomkit_core::rules::transcoding::text_of(&event.content) == "second";
                async move {
                    if !second {
                        return;
                    }
                    telling.notify_one();
                    if closes && let Some(kit) = weak.upgrade() {
                        drop(kit.close().await);
                    }
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                name: "obs".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
        let first = spawn_message(&kit, "first", room_id());
        tokio::time::timeout(BOUND, gate.began()).await.unwrap();
        let second = spawn_message(&kit, "second", room_id());
        let while_first = tokio::time::timeout(Duration::from_secs(2), seen.notified())
            .await
            .is_ok();
        gate.go();
        let first = tokio::time::timeout(BOUND, first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(BOUND, second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if closes {
            tokio::time::timeout(BOUND, kit.close())
                .await
                .unwrap()
                .unwrap();
        }
        roomkit::testing::idle(&kit).await;
        let outcome = format!(
            "{while_first} {} {} {:?} {:?}",
            delivered_by(&first),
            delivered_by(&second),
            witness.delivered(),
            stored(&kit).await
        );
        *outcomes.entry(outcome).or_insert(0) += 1;
        if !closes {
            kit.close().await.unwrap();
        }
    }
    outcomes
}

/// The one outcome of the queued observer, as Python's
/// (`rmk1030-rs/pass1/py_queued_observer.out`): "second"'s observer runs
/// while "first" still delivers, its set queued behind; with no close both
/// are delivered; when that observer closes the kit, "first" delivers
/// within the grace, `kit_closed`, `sent`, and "second", queued, is
/// dropped, `kit_closed` with no delivery result, both stored.
fn as_python(closes: bool, runs: usize) -> std::collections::BTreeMap<String, usize> {
    let (reason, second, delivered) = if closes {
        (Some("kit_closed"), "[]", vec!["first"])
    } else {
        (None, "[\"witness=Sent\"]", vec!["first", "second"])
    };
    let outcome = format!(
        "true {reason:?} [\"witness=Sent\"] {reason:?} {second} {delivered:?} {:?}",
        ["first", "second"]
    );
    std::collections::BTreeMap::from([(outcome, runs)])
}

/// RFC §9.3, §12.10.4: a queued set's ASYNC `BEFORE_BROADCAST` observers
/// run once its place is taken, while the set ahead still delivers, as
/// Python's (maintainer's decision R1, 2026-10-30), on a runtime of one
/// thread. On 31c3c26 they waited for the set's turn
/// (`rmk1030-rs/pass1/queued_observer_before.out`).
#[tokio::test(start_paused = true)]
async fn a_queued_set_s_observer_runs_while_the_set_ahead_delivers() {
    let outcomes = (
        queued_observer_runs(false, OBSERVER_RUNS).await,
        queued_observer_runs(true, OBSERVER_RUNS).await,
    );
    let expected = (
        as_python(false, OBSERVER_RUNS),
        as_python(true, OBSERVER_RUNS),
    );
    assert_eq!(outcomes, expected);
}

/// The same on a runtime of threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_set_s_observer_runs_while_the_set_ahead_delivers_on_threads() {
    let outcomes = (
        queued_observer_runs(false, OBSERVER_RUNS).await,
        queued_observer_runs(true, OBSERVER_RUNS).await,
    );
    let expected = (
        as_python(false, OBSERVER_RUNS),
        as_python(true, OBSERVER_RUNS),
    );
    assert_eq!(outcomes, expected);
}
