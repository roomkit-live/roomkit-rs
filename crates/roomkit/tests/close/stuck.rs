//! RFC §12.10.4: what waits behind spared work is dropped at once, rather
//! than drained for the close's grace, and a turn the close cuts holds it
//! only until its reader's read is committed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::KitError;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::time::Instant;

use crate::support::{
    AT_ONCE, BOUND, Closer, Door, Witness, attach_agent, ended, kit_with, open, room_id,
    spawn_message, stored,
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
            Some(Ok(Some("kit_closed".to_owned()))),
            Some(Ok(None)),
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
/// the kit sealed, the turn never starts its tool nor stores anything, as
/// Python's (`rmk1017-review-A/py_probe_deliver_agent.out`). On 1d8796c
/// the close drained the turn for 5 s.
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
            Some(Ok(Some("kit_closed".to_owned()))),
            0,
            vec!["Go.".to_owned()],
        )
    };
    assert_eq!(cases, [case(false), case(true)]);
}

/// RFC §12.10.4, §10.6.8, lot 1's grace: an agent writes "Thinking", then
/// calls a tool of 30 s, while the witness takes 30 s to deliver that
/// text: closed from outside, the kit drains for its 5 s grace, cuts the
/// turn, waits for its run to commit what the turn's reader read, and
/// aborts the rest, the delivery under way included, so the close returns
/// at 5.1 s, as on a959002. On 1d8796c it waited for the whole run, up to
/// twice the cancel bound, and returned at 15.1 s
/// (`rmk1017-review-A/review_probe_1d8796c.out`); Python's returns near
/// 6-7 s.
#[tokio::test(start_paused = true)]
async fn a_cut_turn_s_run_is_waited_for_only_until_its_read_is_committed() {
    let witness = Witness::slow("Thinking", Duration::from_secs(30));
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
    let started = Instant::now();
    let call = spawn_message(&kit, "Go.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    let call = ended(call).await;
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took < Duration::from_secs(6),
            call.is_some_and(|ended| ended.is_ok())
        ),
        (true, true),
        "the close returned after {took:?}"
    );
}
