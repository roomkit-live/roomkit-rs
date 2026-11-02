//! The answers of turns the caller asked itself, on an event it committed
//! outside the pipeline, committed as a room's responses (RFC §23.3 step 5,
//! §10.1 steps 9 to 14, §7.5 rule 2): `Inbound::answering`, which a
//! delegated turn takes in a child room a transport is shared into.
//!
//! The room holds the customer's `sms` and the agent `ai`; the task is a
//! system message only intelligence reads, committed before the test runs.

use pretty_assertions::assert_eq;
use roomkit_core::model::hook::ModifyOptions;
use roomkit_core::model::{
    EventStatus, HookResult, RoomContext, RoomEvent, TextContent, Visibility,
};
use roomkit_core::ops::{Inbound, InboundResult, TurnResponses};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::transcoding::text_of;

use crate::support::{Sim, channel, notice_event, response};

/// The room, and the task told to its agent.
fn room_with_a_task() -> (Sim, RoomEvent) {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.echo_agent("ai", "never asked");
    let mut task = notice_event("evt_task", "Summarise the ticket.");
    task.visibility = Visibility::Intelligence;
    task.status = EventStatus::Delivered;
    sim.seed(task);
    let task = sim.timeline.first().cloned().unwrap();
    (sim, task)
}

/// Commits `answer`, the agent's buffered response to `task`, as a room's.
fn answer(sim: &mut Sim, task: &RoomEvent, answer: &str) -> InboundResult {
    let agent = sim
        .bindings
        .iter()
        .find(|binding| binding.channel_id == channel("ai"))
        .cloned()
        .unwrap();
    let context = RoomContext {
        room: sim.room.clone(),
        bindings: sim.bindings.clone(),
        participants: Vec::new(),
        recent_events: vec![task.clone()],
        author_register: None,
    };
    let responses = TurnResponses {
        event: task.clone(),
        context,
        buffered: vec![(agent, vec![response("ai", task, answer)])],
        streamed: Vec::new(),
    };
    sim.run(Inbound::answering(sim.shared_config(), responses))
        .unwrap()
}

fn texts(events: &[RoomEvent]) -> Vec<String> {
    events.iter().map(|event| text_of(&event.content)).collect()
}

// Python: tests/tasks/test_shared_transport_delegation.py::TestASharedTransportIsToldTheAnswer::test_the_answer_crosses_the_rooms_hooks
#[test]
fn an_answer_crosses_before_broadcast_and_reaches_the_room_s_transport() {
    let (mut sim, task) = room_with_a_task();
    sim.before_broadcast(
        "redact",
        Box::new(|event| {
            let mut redacted = event.clone();
            redacted.content = TextContent::new("Call me at [phone].").into();
            HookOutcome::Returned(HookResult::modify(redacted, ModifyOptions::default()))
        }),
    );

    let result = answer(&mut sim, &task, "Call me at 555-0100.");

    assert_eq!(texts(&result.response_events), ["Call me at [phone]."]);
    let stored = &result.response_events[0];
    assert_eq!(
        (stored.chain_depth, stored.responds_to.as_ref()),
        (1, Some(&task.id))
    );
    assert_eq!(
        sim.traced("deliver"),
        [format!("deliver sms <- {}", stored.id).as_str()]
    );
    assert_eq!(
        texts(&sim.timeline),
        ["Summarise the ticket.", "Call me at [phone]."]
    );
}

/// RFC §7.5 rule 2: the agent's right to write is checked after its hooks.
#[test]
fn an_answer_its_agent_may_not_write_is_stored_blocked_and_answers_nothing() {
    let (mut sim, task) = room_with_a_task();
    sim.bindings
        .iter_mut()
        .find(|binding| binding.channel_id == channel("ai"))
        .unwrap()
        .muted = true;

    let result = answer(&mut sim, &task, "Done.");

    assert_eq!(texts(&result.response_events), Vec::<String>::new());
    assert_eq!(sim.traced("deliver"), Vec::<&str>::new());
    let stored = &sim.timeline[1];
    assert_eq!(
        (stored.status, stored.blocked_by.as_deref()),
        (EventStatus::Blocked, Some("source_muted"))
    );
}
