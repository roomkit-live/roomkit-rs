//! The room's agents run again on a stored event, a held message's release
//! (RFC §11.7.6), effect by effect: what no scenario can order, the room
//! closing while the rerun's set waits in its lane (RFC §5.1), the room's
//! lock held past `process_timeout` (RFC §13.6), and a cancellation that
//! lands before the set is planned (RFC §10.1).
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventStatus, RoomStatus};
use roomkit_core::ops::{FrameworkEvent, Inbound, Operation, RefusedOperation};

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

// Python: core/mixins/regenerate.py::_rerun_agents_on (RMK-1200): the
// delivery's key names the regeneration from its start, before the lock
// (`named.attach(cascade)`), so a cancellation by the key that lands
// before its set is planned leaves the set asking no agent (RFC §10.1
// "Explicit delivery cancellation", §11.7.6).
#[test]
fn a_rerun_cut_before_its_set_is_planned_asks_no_agent() {
    let mut sim = room();
    let mut operation = rerun(&sim);
    let in_flight = operation.cancel_turns();

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (
            in_flight,
            result.blocked,
            result.event.map(|event| event.id.to_string()),
            sim.traced("on_event"),
            sim.timeline.len(),
        ),
        (
            Vec::new(),
            false,
            Some("evt_0".to_owned()),
            Vec::<&str>::new(),
            1
        )
    );
}

fn regenerate(sim: &Sim) -> Inbound {
    Inbound::regenerate(sim.shared_config(), room_id(), None, 0)
}

// Python: tests/test_regenerate_off_lock.py::test_a_room_read_past_process_timeout_refuses_the_regeneration
// (RMK-1443 F6): process_timeout bounds a regeneration's read of the room
// under the lock, as `_plan_regeneration` bounds `_regenerate_target`: past
// it the call is refused process_timeout, its operation regenerate, no agent
// asked, and the lock released.
#[test]
fn a_regeneration_whose_room_read_passes_its_timeout_is_refused() {
    let mut sim = room();
    sim.config.process_timeout = Some(TIMEOUT);
    sim.durations
        .push(("load_context room_1", TIMEOUT + Duration::from_secs(1)));
    let operation = regenerate(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (result.blocked, result.reason.as_deref(), result.event),
        (true, Some("process_timeout"), None)
    );
    let mut expected = FrameworkEvent::process_timeout(Some(room_id()), None, None, 10.0);
    if let FrameworkEvent::ProcessTimeout { operation, .. } = &mut expected {
        *operation = Some(RefusedOperation::Regenerate);
    }
    assert_eq!(regenerating(&sim), [&expected]);
    assert_eq!(sim.traced("on_event"), Vec::<&str>::new());
    assert_eq!(
        (
            sim.traced("load_context room_1 deadline passed").len(),
            sim.traced("release room_1").len()
        ),
        (1, 1)
    );
}

// Python: tests/test_regenerate_off_lock.py::test_a_held_release_read_past_process_timeout_runs_no_agent:
// `_rerun_agents_on` bounds its reads of the room and of the event with its
// wait for the lock: a held message's release whose read passes the
// deadline asks no agent, emits process_timeout naming the regenerate
// operation, and releases the lock.
#[test]
fn a_release_whose_reads_pass_their_timeout_asks_no_agent() {
    for read in ["load_context room_1", "get_event evt_0"] {
        let mut sim = room();
        sim.config.process_timeout = Some(TIMEOUT);
        sim.durations.push((read, TIMEOUT + Duration::from_secs(1)));
        let operation = rerun(&sim);

        let result = sim.run(operation).unwrap();

        assert_eq!(
            (
                result.blocked,
                result.reason.as_deref(),
                sim.traced("on_event"),
                regenerating(&sim).len(),
                sim.traced("release room_1").len(),
            ),
            (true, Some("process_timeout"), Vec::<&str>::new(), 1, 1),
            "{read}"
        );
    }
}

// Python: core/mixins/regenerate.py::regenerate_response, its
// collecting_release_failures (RMK-1443 G3): a regeneration whose room
// closed while it waited in the lane is refused room_closed, carrying the
// failed release of the lock its choice was made under as its error.
#[test]
fn a_regeneration_refused_in_the_lane_carries_its_lock_s_failed_release() {
    let mut sim = room();
    sim.status_at_lane_turn = Some(RoomStatus::Closed);
    sim.release_fails = true;
    let operation = regenerate(&sim);

    let result = sim.run(operation).unwrap();

    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            result.error.map(|error| error.message),
            sim.traced("on_event"),
        ),
        (
            true,
            Some("room_closed"),
            Some("the lock's lease was lost".to_owned()),
            Vec::<&str>::new()
        )
    );
}
