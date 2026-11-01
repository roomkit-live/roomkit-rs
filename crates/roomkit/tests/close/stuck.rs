//! RFC §12.10.4: what waits behind the work a close is called from, a
//! queued delivery, a call waiting for the room's lock or admission ticket
//! that work holds, is cut at once, as everything not started is; every
//! turn is cut at once, a spared delivery's included.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, RoomEvent};
use roomkit::{HookOptions, HookTrigger, RoomKit};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::time::Instant;

use crate::support::{
    AT_ONCE, BOUND, Closer, Door, Witness, attach_agent, ended, kit_with, ok, open, room_id,
    spawn_message, stored,
};
use roomkit_core::rules::transcoding::text_of;

/// RFC §12.10.4: a delivery closes the kit while the room's next message
/// waits behind it in the room's lane: the delivery is spared, the queued
/// set dropped at once, so that message is never delivered, and its
/// caller is answered at once, `kit_closed`, rather than at the close's
/// grace (5 s on 1d8796c's parent, a959002); the spared delivery's call,
/// under way as the close started, reads `kit_closed`. Python's test
/// closes from an ASYNC `AFTER_BROADCAST` hook, which its lane runs before
/// the next delivery; Rust's lane does not wait for such a hook, a
/// difference of its own, so the door here is the delivery itself.
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
            Some(ok(Some("kit_closed"), None)),
            vec!["Bye.".to_owned()]
        )
    );
}

/// RFC §12.10.4: a SYNC hook closes the kit while the room's next message
/// waits for the room's lock its run holds: that message is cut at once,
/// and the hook's own message is refused at its commit once the kit is
/// sealed, nothing stored, each reading as a message cut before its
/// commit, `kit_closed`, as Python's (`rmk1030-rs/py_doors.out`,
/// `sync-hook`). On fefbfac both failed `KitClosed`; on 1d8796c the close
/// drained the waiting message for 5 s (`rmk1017-review-B/
/// probe_sync_hook_queued.out`).
#[tokio::test(start_paused = true)]
async fn a_message_waiting_on_the_lock_a_close_runs_under_is_cut_at_once() {
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
            Some(ok(Some("kit_closed"), None)),
            Some(ok(Some("kit_closed"), None)),
            Vec::<String>::new()
        )
    );
}

/// RFC §12.10.4: a transport's delivery closes the kit in a room where an
/// agent answers, the turn of the same delivery set: the turn is cut at
/// once, as every turn, the delivery a close is called from spared, so the
/// close returns at once, the turn never starts its tool nor stores
/// anything, and the call reads `kit_closed`, no error, as Python's
/// (`rmk1030-rs/py_doors.out`, `spared-delivery-turns`). On fefbfac the
/// turn was spared with the delivery, its stream refused once the kit was
/// sealed, the call carrying `TurnLinkError`.
#[tokio::test(start_paused = true)]
async fn the_turns_a_spared_delivery_runs_are_cut() {
    let mut cases = Vec::new();
    for calls_a_tool in [false, true] {
        let witness = Witness::new();
        let kit = kit_with(&witness).await;
        let tool_ran = Arc::new(AtomicUsize::new(0));
        let ran = Arc::clone(&tool_ran);
        let tools = Tools::new().tool(
            AiTool {
                parameters: json!({ "type": "object" }),
                ..AiTool::new("post", "Post.").unwrap()
            },
            move |_call, _context| {
                ran.fetch_add(1, Ordering::SeqCst);
                async { Ok(ToolResult::Text("posted".to_owned())) }
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
            Some(ok(Some("kit_closed"), None)),
            0,
            vec!["Go.".to_owned()],
        )
    };
    assert_eq!(cases, [case(false), case(true)]);
}

/// RFC §10.6.8, §12.10.4: a turn's work, its model's rounds and its
/// tools' calls, is cut at once when the kit closes, with no grace, as
/// Python's: an agent writes "Thinking", then calls a tool of 30 s, and the
/// kit is closed from outside at 0.1 s. The turn is cut at once, so the
/// close returns at 0.1 s; when the witness takes 30 s to deliver
/// "Thinking", that send, running as the close starts, has the grace, so
/// the close returns at 5.1 s. Either way the call, under way as the close
/// started, reads `kit_closed`. On fefbfac it read no reason; on 9dfe09b
/// the turn had the drain's grace, the close returning at 5.1 s in both
/// rows.
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
            AiTool {
                parameters: json!({ "type": "object" }),
                ..AiTool::new("slow", "Slow.").unwrap()
            },
            |_call, _context| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(ToolResult::Text("slow".to_owned()))
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
            (false, 0, Some(ok(Some("kit_closed"), None))),
            (true, 50, Some(ok(Some("kit_closed"), None)))
        ]
    );
}

/// A SYNC `BEFORE_BROADCAST` hook whose body runs `closer` for "One.",
/// under the room's lock, or before it, in the room's admission order, when
/// it does not need the lock (RFC §9.5.1).
fn closing_hook(kit: &RoomKit, closer: &Closer, needs_lock: bool) {
    let work = closer.clone();
    let spec = HookOptions {
        name: "quit".to_owned(),
        needs_lock,
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let work = work.clone();
            let is_one = text_of(&event.content) == "One.";
            async move {
                if is_one {
                    work.close().await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
    .unwrap();
}

/// RFC §12.10.4, §9.5.1: an off-lock SYNC hook closes the kit while its
/// run holds `room_1`'s admission ticket, "Two." holding the next one: it
/// waits for its ticket's turn, so it is cut at once, and the hook's own
/// message is refused at its commit once the kit is sealed, nothing
/// stored, each reading as a message cut before its commit, `kit_closed`.
/// On fefbfac both failed `KitClosed`.
#[tokio::test(start_paused = true)]
async fn a_message_behind_a_spared_admission_ticket_is_cut_at_once() {
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
            Some(ok(Some("kit_closed"), None)),
            Some(ok(Some("kit_closed"), None)),
            Vec::<String>::new()
        )
    );
}
