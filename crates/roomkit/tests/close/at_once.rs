//! RFC §12.10.4, rules 2 and 3 (RMK-1030): what has not started is cut as
//! the close starts, a queued delivery and a call waiting for its room's
//! lock, and so is every model turn, its stream and its tool calls, while
//! the other running work still has its grace. Python's
//! `tests/test_kit_close.py`, sections 2 and 3.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, EventContent, EventType, HookResult, RoomEvent, RoomStatus, TextContent,
    ToolCallStatus,
};
use roomkit::{
    AttachChannelOptions, CloseRoomOptions, CommitEventOptions, ConversationStore, EventQuery,
    GetRoomOptions, HookExecution, HookSpec, HookTrigger, InMemoryStore, ListBindingsOptions,
    RoomKit, SendEventOptions, text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{
    AT_ONCE, CUT_BY_CLOSE, Hold, Holding, Pausing, a_send_held, attach_agent, closed, closing, cut,
    deferred, into, kit_of, record, result_of, room, said, spawn_close, spawn_said, stored,
};
use crate::support::channel;

/// RFC §12.10.4: a delivery queued in a room's lane as the kit closes is
/// dropped at once, never sent, `kit_closed` with no delivery result
/// (Python's `test_a_delivery_queued_as_the_kit_closes_is_never_sent`).
/// On fefbfac the lane's drain delivered it.
#[tokio::test(start_paused = true)]
async fn a_delivery_queued_as_the_kit_closes_is_never_sent() {
    let held = Holding::new("ws", [("held", Hold::For(Duration::from_secs(1)))]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;
    let queued = deferred(&kit, "queued", "r").await;

    closed(spawn_close(&kit)).await;

    let dropped = queued.delivery.unwrap().wait().await.unwrap();
    assert_eq!(
        (
            dropped.cancellation_reason.as_deref(),
            dropped.delivery_results.len(),
            held.sent()
        ),
        (Some("kit_closed"), 0, vec!["held".to_owned()])
    );
}

/// A SYNC `BEFORE_BROADCAST` hook on "locker" that tells `entered` it
/// began and waits for `release`: under the room's lock, or before it, in
/// the room's admission order, when it does not need the lock.
fn locking_hook(kit: &RoomKit, entered: &Arc<Notify>, release: &Arc<Notify>, needs_lock: bool) {
    let (entered, release) = (Arc::clone(entered), Arc::clone(release));
    let mut spec = HookSpec::new("locker", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = needs_lock;
    kit.hook(spec, move |event, _context| {
        let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
        let ours = text_of(&event.content) == "locker";
        async move {
            if ours {
                entered.notify_one();
                release.notified().await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
}

/// What a call through `door` into `r` came to: a message's
/// `(blocked, reason, cancellation_reason)`, any other's error.
async fn waiting_call(kit: &RoomKit, door: &str) -> String {
    let r = room("r");
    let ended = match door {
        "process_inbound" => {
            let result = kit.process_inbound(said("waiter"), into("r")).await;
            return format!("{:?}", result.map(|result| format!("{:?}", cut(&result))));
        }
        "send_event" => kit
            .send_event(
                &r,
                &channel("sms"),
                TextContent::new("waiter"),
                SendEventOptions::default(),
            )
            .await
            .map(drop),
        "commit_event" => kit
            .commit_event(&r, record("waiter"), CommitEventOptions::default())
            .await
            .map(drop),
        "attach_channel" => kit
            .attach_channel(&r, &channel("late"), AttachChannelOptions::default())
            .await
            .map(drop),
        "close_room" => kit
            .close_room(&r, CloseRoomOptions::default())
            .await
            .map(drop),
        _ => unreachable!("no door {door}"),
    };
    format!("{ended:?}")
}

/// RFC §12.10.4: a call into a room, or a room change, waiting for the
/// room's lock is cut as the close starts, while the call holding the lock
/// still has its grace: nothing of it is stored (Python's
/// `test_a_call_waiting_for_its_room_lock_is_cut_at_once`). The locker,
/// committed within the grace, is delivered to no one. On fefbfac the
/// waiter waited for the lock.
#[tokio::test(start_paused = true)]
async fn a_call_waiting_for_its_room_lock_is_cut_at_once() {
    let mut rows = Vec::new();
    for door in [
        "process_inbound",
        "send_event",
        "commit_event",
        "attach_channel",
        "close_room",
    ] {
        let held = Holding::new("ws", []);
        let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
        kit.register_channel(Holding::new("late", [])).unwrap();
        let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        locking_hook(&kit, &entered, &release, true);
        let locker = spawn_said(&kit, "locker", "r");
        entered.notified().await;
        let mut waiter = tokio::spawn({
            let kit = kit.clone();
            async move { waiting_call(&kit, door).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let waits = !waiter.is_finished();

        let close = spawn_close(&kit);
        let ended = tokio::time::timeout(AT_ONCE, &mut waiter).await;
        let locker_runs = !locker.is_finished();
        release.notify_one();
        closed(close).await;
        let held_lock = result_of(locker).await;
        let room_now = kit
            .get_room(&room("r"), GetRoomOptions::default())
            .await
            .unwrap();
        let bindings = kit
            .list_bindings(&room("r"), ListBindingsOptions::default())
            .await
            .unwrap();
        rows.push((
            door,
            (waits, locker_runs),
            ended.ok().map(Result::unwrap),
            (held_lock.event.is_some(), held_lock.cancellation_reason),
            (
                room_now.status,
                bindings
                    .iter()
                    .any(|binding| binding.channel_id == channel("late")),
            ),
            stored(&kit, "r").await,
            held.sent(),
        ));
    }
    let row = |door, ended: String| {
        (
            door,
            (true, true),
            Some(ended),
            (true, Some("kit_closed".to_owned())),
            (RoomStatus::Active, false),
            vec!["locker".to_owned()],
            Vec::<String>::new(),
        )
    };
    let refused = || "Err(KitClosed)".to_owned();
    assert_eq!(
        rows,
        [
            row(
                "process_inbound",
                format!("Ok({:?})", format!("{CUT_BY_CLOSE:?}"))
            ),
            row("send_event", refused()),
            row("commit_event", refused()),
            row("attach_channel", refused()),
            row("close_room", refused()),
        ]
    );
}

/// RFC §12.10.4: a call taken before the close that reaches its room's
/// lock once the close started is cut there, as a call already waiting
/// is: nothing of it is stored (Python's
/// `test_a_call_reaching_its_room_lock_once_the_close_started_is_cut`). An
/// off-lock hook holds it before the lock. On fefbfac it was stored.
#[tokio::test(start_paused = true)]
async fn a_call_reaching_its_room_lock_once_the_close_started_is_cut() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r", "busy"]).await;
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    locking_hook(&kit, &entered, &release, false);
    a_send_held(&kit, &held, "busy").await;
    let call = spawn_said(&kit, "locker", "r");
    entered.notified().await;
    let close = closing(&kit).await;

    release.notify_one();
    let result = tokio::time::timeout(AT_ONCE, call).await;
    held.release.notify_one();
    closed(close).await;

    let result = result.unwrap().unwrap().unwrap();
    assert_eq!(
        (cut(&result), stored(&kit, "r").await),
        (CUT_BY_CLOSE, Vec::<String>::new())
    );
}

/// The events `r` stores of `channel_id`.
async fn of<S: ConversationStore>(kit: &RoomKit<S>, channel_id: &str) -> Vec<RoomEvent> {
    let query = EventQuery {
        limit: u64::MAX,
        ..EventQuery::default()
    };
    let events = kit.store().list_events(&room("r"), &query).await.unwrap();
    let channel_id = ChannelId::new(channel_id).unwrap();
    events
        .into_iter()
        .filter(|event| event.source.channel_id == channel_id)
        .collect()
}

/// RFC §12.10.4: a turn streaming as the kit closes is cut at once, while
/// a send in another room still has its grace: what it streamed is stored
/// marked `cancelled`, the turn's record `cancelled` on it though no target
/// read it, the call reading `kit_closed` (Python's
/// `test_a_streaming_turn_is_cut_at_once_its_text_stored_cancelled`, and
/// `rmk1030-rs/py_cut_turn_record.out`). On 01c8420 the row carried no
/// `loop_end_reason`.
#[tokio::test(start_paused = true)]
async fn a_streaming_turn_is_cut_at_once_its_text_stored_cancelled() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r", "busy"]).await;
    attach_agent(&kit, Pausing { first: "The " }, Tools::new()).await;
    let call = spawn_said(&kit, "Go.", "r");
    tokio::time::sleep(Duration::from_millis(100)).await;
    a_send_held(&kit, &held, "busy").await;

    let close = spawn_close(&kit);
    let result = tokio::time::timeout(AT_ONCE, call).await;
    let grace_runs = !close.is_finished();
    held.release.notify_one();
    closed(close).await;

    let result = result.unwrap().unwrap().unwrap();
    let answers: Vec<(String, serde_json::Value)> = of(&kit, "agent")
        .await
        .iter()
        .map(|event| (text_of(&event.content), json!(event.metadata)))
        .collect();
    let record = json!({ "loop_end_reason": "cancelled", "cancelled": true });
    assert_eq!(
        (grace_runs, result.cancellation_reason.as_deref(), answers),
        (true, Some("kit_closed"), vec![("The ".to_owned(), record)])
    );
}

/// Notes a tool call dropped before it ended.
struct ToolNote(Arc<Mutex<Vec<&'static str>>>);

impl Drop for ToolNote {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("cut");
    }
}

/// RFC §12.10.4: a tool call running as the kit closes is cut at once,
/// while a send in another room still has its grace: its end row is
/// stored, `failed` with the error `kit_closed`, the call reading
/// `kit_closed` (Python's
/// `test_a_running_tool_call_is_cut_at_once_its_end_row_stored`). On
/// fefbfac the end row's error was `cancelled`.
#[tokio::test(start_paused = true)]
async fn a_running_tool_call_is_cut_at_once_its_end_row_stored() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r", "busy"]).await;
    let (entered, ran) = (Arc::new(Notify::new()), Arc::new(Mutex::new(Vec::new())));
    let (entering, noting) = (Arc::clone(&entered), Arc::clone(&ran));
    let tools = Tools::new().tool(
        ToolDefinition::new("lookup", "Look up.", json!({ "type": "object" })),
        move |_call, _context| {
            let (entering, note) = (Arc::clone(&entering), ToolNote(Arc::clone(&noting)));
            async move {
                let _note = note;
                entering.notify_one();
                std::future::pending::<()>().await;
                Ok(ToolOutput::Text("found".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("Found it."),
    ]);
    attach_agent(&kit, model, tools).await;
    let call = spawn_said(&kit, "Go.", "r");
    entered.notified().await;
    a_send_held(&kit, &held, "busy").await;

    let close = spawn_close(&kit);
    let result = tokio::time::timeout(AT_ONCE, call).await;
    let grace_runs = !close.is_finished();
    held.release.notify_one();
    closed(close).await;

    let result = result.unwrap().unwrap().unwrap();
    let ends: Vec<(ToolCallStatus, Option<String>)> = of(&kit, "agent")
        .await
        .into_iter()
        .filter(|event| event.r#type == EventType::ToolCallEnd)
        .filter_map(|event| {
            let EventContent::ToolCall(call) = event.content else {
                return None;
            };
            Some((call.status, call.error))
        })
        .collect();
    assert_eq!(
        (
            grace_runs,
            ran.lock().unwrap().clone(),
            result.cancellation_reason.as_deref(),
            ends
        ),
        (
            true,
            vec!["cut"],
            Some("kit_closed"),
            vec![(ToolCallStatus::Failed, Some("kit_closed".to_owned()))]
        )
    );
}
