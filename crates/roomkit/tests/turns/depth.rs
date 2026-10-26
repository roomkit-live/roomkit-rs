//! RFC §8.3, §10.6.5: depth across the backend. A verb a turn's work makes
//! after the turn crosses as `room_call`: the owner takes it from the
//! room's last turn of the agent alone, as a new arrival at the depth the
//! verb names, which an observer of the turn's calls raises to the floor
//! it keeps. (A verb made during the turn keeps its depth and draws its
//! pass: roomkit-ai's `a_tool_writes_to_its_room_at_the_turn_s_depth` and
//! `two_agents_answering_each_other_take_at_most_the_budget_s_turns`.)

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{DirectEvent, EventContent};
use roomkit::testing::TurnTap;
use roomkit::{KitError, current_turn};
use roomkit_ai::ToolOutput;
use tokio::sync::Notify;

use crate::support::{asks, channel, kit};

/// What an observer of the turn's calls sent once the turn was over, when
/// `go` told it to: the event's depth, or the error.
type Sent = Arc<Mutex<Vec<Result<u32, String>>>>;

/// A kit whose tool answers at once, and whose observer of its calls sends
/// a note into the room through the turn once `go` tells it to.
async fn observed() -> (roomkit::RoomKit, Arc<Notify>, Sent) {
    let kit = kit(|_call, _context| async { Ok(ToolOutput::Text("42".to_owned())) }).await;
    let go = Arc::new(Notify::new());
    let sent: Sent = Arc::default();
    let (waits, noted) = (Arc::clone(&go), Arc::clone(&sent));
    kit.observe_tool_calls("follow_up", move |_event, _context| {
        let (waits, noted) = (Arc::clone(&waits), Arc::clone(&noted));
        async move {
            waits.notified().await;
            let turn = current_turn().unwrap();
            let note = DirectEvent::new(channel("sms"), EventContent::text("Following up."));
            let sent = turn.send_event(note).await;
            noted.lock().unwrap().push(match sent {
                Ok(event) => Ok(event.chain_depth),
                Err(KitError::Turn(error)) => Err(error.kind),
                Err(error) => Err(error.to_string()),
            });
        }
    })
    .unwrap();
    (kit, go, sent)
}

/// RFC §8.3, §10.6.5: an observer of a call that sends through the turn
/// once the turn is over crosses as `room_call`, and its event is at least
/// as deep as the turn's response was when the call ended: the floor the
/// observer keeps, which the verb names (the owner no longer floors it).
#[tokio::test(start_paused = true)]
async fn a_verb_after_its_turn_crosses_as_room_call_at_its_observer_s_floor() {
    let (kit, go, sent) = observed().await;
    let tap = TurnTap::on(&kit);
    let result = asks(&kit, "What is it?").await;
    go.notify_one();
    kit.idle().await;
    let room_calls = tap
        .messages()
        .iter()
        .filter(|message| message.family == "room_call")
        .count();
    assert_eq!(
        (result.error, room_calls, sent.lock().unwrap().clone()),
        (None, 1, vec![Ok(1)])
    );
}

/// RFC §10.6.3: work a turn began writes nothing once a later turn of the
/// agent was sent in the room: its `room_call` is refused.
#[tokio::test(start_paused = true)]
async fn a_verb_after_its_turn_is_refused_once_a_later_turn_was_sent() {
    let (kit, go, sent) = observed().await;
    asks(&kit, "What is it?").await;
    // The model has no round left: the later turn fails, sent all the same.
    asks(&kit, "And now?").await;
    go.notify_one();
    kit.idle().await;
    assert_eq!(
        sent.lock().unwrap().clone(),
        vec![Err("PermissionError".to_owned())]
    );
}

/// How many of the events a turn's tool sends through its turn are
/// stored, at which depths, and how many of them `BLOCKED` by the
/// cascade's budget.
async fn sends_through_the_turn(sends: usize) -> (usize, Vec<u32>, usize) {
    let kit = kit(move |_call, _context| async move {
        let turn = current_turn().unwrap();
        for index in 0..sends {
            let note = DirectEvent::new(channel("ai"), EventContent::text(format!("Note {index}")));
            turn.send_event(note)
                .await
                .map_err(|error| roomkit_ai::ToolError::raised(&error))?;
        }
        Ok(ToolOutput::Text("42".to_owned()))
    })
    .await;
    asks(&kit, "What is it?").await;
    kit.idle().await;
    let notes: Vec<roomkit::model::RoomEvent> =
        roomkit::ConversationStore::list_events(kit.store(), &crate::support::room(), false)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| roomkit::text_of(&event.content).starts_with("Note "))
            .collect();
    let capped = notes
        .iter()
        .filter(|event| event.blocked_by.as_deref() == Some("reentry_loop_cap"))
        .count();
    let mut depths: Vec<u32> = notes.iter().map(|event| event.chain_depth).collect();
    depths.dedup();
    (notes.len(), depths, capped)
}

/// RFC §8.3 "Reentry cap", §10.6.5: what a tool sends through its turn
/// crosses as `host_call`, and the owner runs it as its own code would: at
/// the turn's response depth, drawing one pass of the cascade that delivers
/// the turn's trigger, so that of sixty sends from one turn the fifty its
/// passes allow are stored, the last ten refused with nothing stored, as
/// before the backend (probe tasks/probes/step4-m7/baseline_probe.rs,
/// probe 3: 50 notes, depths [1], none stored refused).
#[tokio::test(start_paused = true)]
async fn a_tool_s_sends_through_its_turn_keep_its_depth_and_draw_its_passes() {
    assert_eq!(sends_through_the_turn(60).await, (50, vec![1], 0));
}
