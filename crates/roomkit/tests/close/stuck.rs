//! RFC §12.10.4: what waits behind spared work is dropped at once, rather
//! than drained for the close's grace, and a turn the close cuts holds it
//! only until its reader's read is committed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{HookResult, RoomId};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookSpec, HookTrigger, KitError,
    RoomKit, text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::time::Instant;

use crate::support::{
    AT_ONCE, BOUND, Closer, Door, Witness, attach_agent, channel, ended, kit_with, ok, open,
    room_id, spawn_message, stored,
};

/// RFC §12.10.4: a delivery closes the kit while the room's next message
/// waits behind it in the room's lane: the delivery is spared, not
/// drained, so that message is never delivered, and its caller is answered
/// at once, `kit_closed`, rather than at the close's grace (5 s on
/// 1d8796c's parent, a959002). Python's test closes from an ASYNC
/// `AFTER_BROADCAST` hook, which its lane runs before the next delivery;
/// Rust's lane does not wait for such a hook, a difference of its own, so
/// the door here is the delivery itself.
#[tokio::test(start_paused = true)]
async fn the_deliveries_queued_behind_a_delivery_a_close_runs_in_are_dropped() {
    let witness = Witness::new();
    let (kit, closer, call) = open(Door::Delivery, &witness, true).await;
    tokio::time::timeout(BOUND, closer.entered.notified())
        .await
        .unwrap();
    let queued = spawn_message(&kit, "Again.", room_id());
    // Queued behind the delivery that closes the kit.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    closer.go.notify_one();
    let returned = closer.returned_once_closed().await;
    let queued = ended(queued).await;
    let at_once = started.elapsed() < AT_ONCE;
    assert_eq!(
        (
            returned,
            at_once,
            queued,
            ended(call).await,
            witness.delivered()
        ),
        (
            Some(true),
            true,
            Some(ok(Some("kit_closed"), None)),
            Some(ok(None, None)),
            vec!["Bye.".to_owned()]
        )
    );
}

/// RFC §12.10.4: a SYNC hook closes the kit while the room's next message
/// waits for the room's lock its run holds: that message is dropped at
/// once, refused as every call is once the kit is sealed, and the hook's
/// own message is refused at its commit, nothing stored, as Python's
/// callers both raise `RoomKitError` at once
/// (`rmk1017-review-A/py_review_probe.out` row ii). On 1d8796c the close
/// drained the waiting message for 5 s (`rmk1017-review-B/
/// probe_sync_hook_queued.out`).
#[tokio::test(start_paused = true)]
async fn a_message_waiting_on_the_lock_a_close_runs_under_is_refused_at_once() {
    let witness = Witness::new();
    let (kit, closer, call) = open(Door::SyncHook, &witness, true).await;
    tokio::time::timeout(BOUND, closer.entered.notified())
        .await
        .unwrap();
    let waiting = spawn_message(&kit, "Again.", room_id());
    // Waiting for the room's lock the hook's run holds.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    closer.go.notify_one();
    let returned = closer.returned_once_closed().await;
    let waiting = ended(waiting).await;
    let at_once = started.elapsed() < AT_ONCE;
    assert_eq!(
        (
            returned,
            at_once,
            waiting,
            ended(call).await,
            stored(&kit).await
        ),
        (
            Some(true),
            true,
            Some(Err(KitError::ShutDown)),
            Some(Err(KitError::ShutDown)),
            Vec::<String>::new()
        )
    );
}

/// RFC §12.10.4: a transport's delivery closes the kit in a room where an
/// agent answers, the turn of the same delivery set: the turn is the
/// spared run's work, spared with it, so the close returns at once, and,
/// the kit sealed, the turn never starts its tool nor stores anything; its
/// stream, read once the kit is sealed, is refused with the turn link's
/// `backend_closed` (RFC §10.6.8: a turn still open as its owner closes),
/// which the call's result carries, as Python's `TurnLinkError`
/// (`rmk1017-lightreview/lr_probe_py.out` P3). On 1d8796c the close
/// drained the turn for 5 s; on 9dfe09b the call read `kit_closed`, no
/// error.
#[tokio::test(start_paused = true)]
async fn the_turns_a_spared_delivery_runs_are_spared_with_it() {
    let mut cases = Vec::new();
    for calls_a_tool in [false, true] {
        let witness = Witness::new();
        let kit = kit_with(&witness).await;
        let tool_ran = Arc::new(AtomicUsize::new(0));
        let ran = Arc::clone(&tool_ran);
        let tools = Tools::new().tool(
            ToolDefinition::new("post", "Post.", json!({ "type": "object" })),
            move |_call, _context| {
                ran.fetch_add(1, Ordering::SeqCst);
                async { Ok(ToolOutput::Text("posted".to_owned())) }
            },
        );
        let model = if calls_a_tool {
            ScriptedProvider::new([
                ScriptedRound::calling("", [("c1", "post", "{}")]),
                ScriptedRound::answering("Done."),
            ])
        } else {
            ScriptedProvider::new([ScriptedRound::answering("Hi.")])
        };
        attach_agent(&kit, model, tools).await;
        let closer = Closer::new(&kit, &witness, false);
        *witness.closes_on_delivery.lock().unwrap() = Some(closer.clone());
        let started = Instant::now();
        let call = spawn_message(&kit, "Go.", room_id());
        let returned = closer.returned_once_closed().await;
        let at_once = started.elapsed() < AT_ONCE;
        let call = ended(call).await;
        roomkit::testing::idle(&kit).await;
        cases.push((
            calls_a_tool,
            (returned, at_once),
            call,
            tool_ran.load(Ordering::SeqCst),
            stored(&kit).await,
        ));
    }
    let case = |calls_a_tool| {
        (
            calls_a_tool,
            (Some(true), true),
            Some(ok(None, Some("TurnLinkError"))),
            0,
            vec!["Go.".to_owned()],
        )
    };
    assert_eq!(cases, [case(false), case(true)]);
}

/// RFC §10.6.8, §12.10.4: a turn's work, its model's rounds and its
/// tools' calls, is cut at once when the kit closes, with no grace, as
/// Python's: an agent writes "Thinking", then calls a tool of 30 s, and the
/// kit is closed from outside at 0.1 s. The turn is cut at once and what
/// its reader read is delivered as the lanes drain, so the close returns
/// at 0.1 s; when the witness takes 30 s to deliver "Thinking", the drain
/// gives that delivery its 5 s grace, so the close returns at 5.1 s. Either
/// way the call reads no reason, its own delivery over, as Python's
/// (`rmk1017-lightreview/lr_probe_py.out` c1, `lr_probe_py_p6.out`). On
/// 9dfe09b the turn had the drain's grace: the close returned at 5.1 s in
/// both rows, the call reading `kit_closed`.
#[tokio::test(start_paused = true)]
async fn a_turn_is_cut_at_once_when_the_kit_closes() {
    let mut rows = Vec::new();
    for slow_witness in [false, true] {
        let witness = if slow_witness {
            Witness::slow("Thinking", Duration::from_secs(30))
        } else {
            Witness::new()
        };
        let kit = kit_with(&witness).await;
        let tools = Tools::new().tool(
            ToolDefinition::new("slow", "Slow.", json!({ "type": "object" })),
            |_call, _context| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(ToolOutput::Text("slow".to_owned()))
            },
        );
        let model = ScriptedProvider::new([
            ScriptedRound::calling("Thinking", [("c1", "slow", "{}")]).in_pieces(["Think", "ing"]),
            ScriptedRound::answering("Done."),
        ]);
        attach_agent(&kit, model, tools).await;
        let call = spawn_message(&kit, "Go.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        rows.push((slow_witness, took, ended(call).await));
    }
    assert_eq!(
        rows,
        [
            (false, 0, Some(ok(None, None))),
            (true, 50, Some(ok(None, None)))
        ]
    );
}

/// A SYNC `BEFORE_BROADCAST` hook whose body runs `closer` for "One.",
/// under the room's lock, or before it, in the room's admission order, when
/// it does not need the lock (RFC §9.5.1).
fn closing_hook(kit: &RoomKit, closer: &Closer, needs_lock: bool) {
    let work = closer.clone();
    let mut spec = HookSpec::new("quit", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = needs_lock;
    kit.hook(spec, move |event, _context| {
        let work = work.clone();
        let is_one = text_of(&event.content) == "One.";
        async move {
            if is_one {
                work.close().await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
}

/// RFC §12.10.4: a SYNC hook closes the kit under `room_1`'s lock while
/// `room_2` drains a delivery of 2 s; "Two." comes to `room_1` 0.5 s into
/// the close: it waits behind the spared run's lock, so it is refused at
/// once, and the close returns once `room_2`'s delivery ended, at 2 s, as
/// Python's (`rmk1017-lightreview/lr_probe_py.out` P1). On 9dfe09b the
/// close found waiters only as it began and as callers joined: "Two." held
/// it until the grace, 5 s (`lr_probe_head.out`).
#[tokio::test(start_paused = true)]
async fn a_message_coming_behind_a_spared_lock_as_the_close_drains_is_refused() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let slow = Witness::slow("Slow.", Duration::from_secs(2)).named("slow");
    kit.register_channel(slow).unwrap();
    let room_2 = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_2.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "slow"] {
        kit.attach_channel(&room_2, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let closer = Closer::new(&kit, &witness, true);
    closing_hook(&kit, &closer, true);
    let started = Instant::now();
    let tenths = || (started.elapsed().as_millis() + 50) / 100;
    let slow_call = spawn_message(&kit, "Slow.", room_2);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let one = spawn_message(&kit, "One.", room_id());
    tokio::time::timeout(BOUND, closer.entered.notified())
        .await
        .unwrap();
    closer.go.notify_one();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let two = spawn_message(&kit, "Two.", room_id());
    let two = ended(two).await;
    let two_at = tenths();
    let returned = closer.returned_once_closed().await;
    let closed_at = tenths();
    assert_eq!(
        (
            (two, two_at),
            (returned, closed_at),
            ended(one).await,
            ended(slow_call).await
        ),
        (
            (Some(Err(KitError::ShutDown)), 5),
            (Some(true), 20),
            Some(Err(KitError::ShutDown)),
            Some(ok(None, None))
        )
    );
}

/// RFC §12.10.4, §9.5.1: an off-lock SYNC hook closes the kit while its
/// run holds `room_1`'s admission ticket, "Two." holding the next one: it
/// waits behind the spared run's ticket, so it is refused at once, nothing
/// stored, as Python's callers raise `RoomKitError` at 0.1 s
/// (`rmk1017-fix/py_p2.out`). Without the tickets' half of the drop, the
/// close held until its grace, 5.1 s (light review, `M1b_tickets` on P2).
#[tokio::test(start_paused = true)]
async fn a_message_behind_a_spared_admission_ticket_is_refused_at_once() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, true);
    closing_hook(&kit, &closer, false);
    let one = spawn_message(&kit, "One.", room_id());
    tokio::time::timeout(BOUND, closer.entered.notified())
        .await
        .unwrap();
    let two = spawn_message(&kit, "Two.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    closer.go.notify_one();
    let returned = closer.returned_once_closed().await;
    let at_once = started.elapsed() < AT_ONCE;
    assert_eq!(
        (
            returned,
            at_once,
            ended(two).await,
            ended(one).await,
            stored(&kit).await
        ),
        (
            Some(true),
            true,
            Some(Err(KitError::ShutDown)),
            Some(Err(KitError::ShutDown)),
            Vec::<String>::new()
        )
    );
}
