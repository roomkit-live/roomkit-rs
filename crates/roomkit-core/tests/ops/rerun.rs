//! The room's agents run again on a stored event, a held message's release
//! (RFC §11.7.6), effect by effect: what no scenario can order, the room
//! closing while the rerun's set waits in its lane (RFC §5.1), and the
//! room's lock held past `process_timeout` (RFC §13.6).
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventStatus, RoomStatus};
use roomkit_core::ops::{FrameworkEvent, Inbound, RefusedOperation};

use crate::support::{Sim, notice_event, room_id};

const TIMEOUT: Duration = Duration::from_secs(10);

/// `room_1` with the customer's `sms` and an agent answering `answer`, and
/// the customer's stored message `evt_0`.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.echo_agent("ai", "answer");
    let mut message = notice_event("evt_0", "Did my transfer go through?");
    message.source.channel_id = "sms".parse().unwrap();
    message.status = EventStatus::Delivered;
    sim.seed(message);
    sim
}

fn rerun(sim: &Sim) -> Inbound {
    Inbound::rerun(sim.shared_config(), room_id(), "evt_0".parse().unwrap(), 0)
}

/// The framework events of the run that name the `regenerate` operation.
fn regenerating(sim: &Sim) -> Vec<&FrameworkEvent> {
    sim.framework_events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                FrameworkEvent::RoomRefusedEvent {
                    operation: RefusedOperation::Regenerate,
                    ..
                } | FrameworkEvent::ProcessTimeout {
                    operation: Some(RefusedOperation::Regenerate),
                    ..
                }
            )
        })
        .collect()
}

// Python: core/mixins/lane_execution.py::_execute_plan, _refuse_closed_rerun
// (RMK-1189): a regeneration whose room closed while it waited in the lane
// is refused when the lane takes it, before the agent runs.
#[test]
fn a_rerun_whose_room_closed_while_it_waited_runs_no_agent() {
    let mut sim = room();
    sim.status_at_lane_turn = Some(RoomStatus::Closed);
    let operation = rerun(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            result.event.is_none(),
            sim.traced("on_event"),
            sim.timeline.len(),
        ),
        (true, Some("room_closed"), true, Vec::<&str>::new(), 1)
    );
    assert_eq!(
        regenerating(&sim),
        [&FrameworkEvent::room_refused_event(
            room_id(),
            "evt_0".parse().unwrap(),
            RoomStatus::Closed,
            RefusedOperation::Regenerate,
            roomkit_core::model::EventType::Message,
        )]
    );
}

// Python: core/lanes.py::_execute, core/mixins/lane_execution.py::
// _refuse_closed_rerun (RMK-1195): the room's read failing when the lane
// takes the regeneration, the failure is recorded on its cascade, the
// call's error, and no agent runs.
#[test]
fn a_rerun_whose_room_read_fails_at_its_turn_runs_no_agent() {
    let mut sim = room();
    sim.failing_effect = Some("unlocked load_context room_1");
    let operation = rerun(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (
            result.blocked,
            result.error.as_ref().map(|error| error.error_type.as_str()),
            sim.traced("on_event"),
            sim.timeline.len(),
            regenerating(&sim).len(),
        ),
        (false, Some("StoreUnavailable"), Vec::<&str>::new(), 1, 0)
    );
}

// The same rerun, its room still open when the lane takes it: the agent
// is handed the stored message, and its answer is committed and delivered.
#[test]
fn a_rerun_whose_room_stayed_open_runs_its_agent() {
    let mut sim = room();
    let operation = rerun(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (
            result.blocked,
            sim.traced("on_event"),
            sim.timeline.len(),
            regenerating(&sim).len(),
        ),
        (
            false,
            vec!["on_event ai <- evt_0", "on_event sms <- evt_1"],
            2,
            0
        )
    );
}

// Python: core/mixins/regenerate.py::_rerun_agents_on,
// _refuse_on_timeout(operation="regenerate") (RMK-1189): a regeneration
// past process_timeout waiting for the room's lock emits process_timeout,
// its data.operation regenerate.
#[test]
fn a_rerun_past_its_timeout_names_its_operation() {
    let mut sim = room();
    sim.config.process_timeout = Some(TIMEOUT);
    sim.durations
        .push(("lock room_1", TIMEOUT + Duration::from_secs(1)));
    let operation = rerun(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (result.blocked, result.reason.as_deref()),
        (true, Some("process_timeout"))
    );
    let mut expected = FrameworkEvent::process_timeout(Some(room_id()), None, None, 10.0);
    if let FrameworkEvent::ProcessTimeout { operation, .. } = &mut expected {
        *operation = Some(RefusedOperation::Regenerate);
    }
    assert_eq!(regenerating(&sim), [&expected]);
}
