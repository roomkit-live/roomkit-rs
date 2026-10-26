//! Off-lock checks, effect by effect (RFC §9.5.1, §10.1 steps 5a and 9,
//! §26.1, gap G-067): the hooks registered with `needs_lock = false` run
//! before the room lock, on the context built before it, between the
//! admission ticket's taking and its turn; their outcome is applied at step
//! 9, after the status, idempotency and edit gates; the side effects of
//! both phases are collected once; an event no off-lock hook fires for takes
//! no ticket; a response and a hook's injected event run every hook under
//! the lock; the ticket is released on every path.
//!
//! The room holds the customer's `sms` and the advisor's `ws`. The off-lock
//! hook is `scanner`; `policy` needs the lock.

use std::cell::RefCell;
use std::rc::Rc;

use roomkit_core::model::{
    EditContent, EditSource, ErrorRecord, EventContent, EventStatus, EventType, HookResult,
    InboundMessage, InjectedEvent, Observation, ObservationId, ParticipantId, RoomContext,
    RoomEvent, RoomStatus, Task, TaskId, Timestamp,
};
use roomkit_core::ops::{FrameworkEvent, InboundError, RefusedOperation};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};
use roomkit_core::rules::transcoding::text_of;

use crate::support::{HookChange, Sim, channel, notice_event, room_id};

const SENDER: &str = "+15551234567";

/// The room with `sms` and `ws`.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.transport("ws");
    sim
}

/// Whether `event` is the customer's, the hooks' only concern: the events
/// they inject run them too (RFC §9.5.1).
fn from_customer(event: &RoomEvent) -> bool {
    event.source.channel_id.as_str() == "sms"
}

/// A hook that answers `answer` for the customer's events and allows the
/// others.
fn answering(answer: impl Fn() -> HookResult + 'static) -> crate::support::Hook {
    Box::new(move |event| {
        if from_customer(event) {
            HookOutcome::Returned(answer())
        } else {
            HookOutcome::Returned(HookResult::allow())
        }
    })
}

/// The room with an off-lock `scanner` answering `answer`, and a `policy`
/// that needs the lock and allows.
fn scanned(answer: impl Fn() -> HookResult + 'static) -> Sim {
    let mut sim = room();
    sim.off_lock_check("scanner", answering(answer));
    sim.before_broadcast("policy", answering(HookResult::allow));
    sim
}

fn bonjour() -> InboundMessage {
    Sim::message("sms", "Bonjour")
}

/// The texts of the events the hook named `name` was handed, recorded by
/// `seen`.
fn texts(seen: &Rc<RefCell<Vec<RoomEvent>>>) -> Vec<String> {
    seen.borrow()
        .iter()
        .map(|event| text_of(&event.content))
        .collect()
}

/// The correspondent the context `hook` ran with names on `sms`'s binding.
fn correspondent_seen_by(sim: &Sim, hook: &str) -> Option<String> {
    let (_, context): &(String, RoomContext) = sim
        .hook_contexts
        .iter()
        .find(|(name, _)| name == hook)
        .unwrap();
    context
        .bindings
        .iter()
        .find(|binding| binding.channel_id.as_str() == "sms")
        .and_then(|binding| binding.participant_id.as_ref())
        .map(ToString::to_string)
}

/// RFC §9.5.1, §10.1 step 5a.
mod an_off_lock_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn runs_before_the_room_lock_between_its_ticket_and_its_turn() {
        let mut sim = scanned(HookResult::allow);
        sim.send(bonjour()).unwrap();
        assert_eq!(
            sim.trace,
            [
                "load_context room_1",
                "handle_inbound sms",
                "take ticket room_1",
                "hook scanner",
                "await ticket room_1",
                "lock room_1",
                "load_context room_1",
                "hook policy",
                "load_context room_1",
                "commit #0 sms delivered \"Bonjour\"",
                "enter lane room_1 evt_1",
                "release room_1",
                "release ticket room_1",
                "await lane room_1 evt_1",
                "on_event ws <- evt_1",
                "deliver ws <- evt_1",
                "leave lane room_1 evt_1",
            ]
        );
    }

    /// RFC §9.5.1 Reentrance: the off-lock hooks, and the parse before
    /// them, run in the room's admission order; the hooks under the lock
    /// stand in it by the lock.
    #[test]
    fn runs_in_the_room_s_admission_order_as_the_parse_does() {
        let mut sim = scanned(HookResult::allow);
        sim.send(bonjour()).unwrap();
        assert_eq!(
            sim.in_admission_order,
            ["handle_inbound sms in room_1", "hook scanner in room_1"]
        );
    }

    /// The context is the one step 2 read: a sender recorded on the
    /// binding by the time the lock is taken is seen by the locked hook
    /// only.
    #[test]
    fn reads_the_context_built_before_the_lock() {
        let mut sim = scanned(HookResult::allow);
        sim.recorded_meanwhile = Some((
            room_id(),
            channel("sms"),
            ParticipantId::new(SENDER).unwrap(),
        ));
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                correspondent_seen_by(&sim, "scanner"),
                correspondent_seen_by(&sim, "policy")
            ),
            (None, Some(SENDER.to_owned()))
        );
    }

    /// Its modify reaches the locked hooks and the commit (RFC §9.5.1).
    #[test]
    fn modifying_hands_the_locked_hooks_its_payload() {
        let mut sim = room();
        sim.off_lock_check(
            "scanner",
            Box::new(|event| {
                let mut redacted = event.clone();
                if from_customer(event) {
                    redacted.content = EventContent::text("[redacted]");
                }
                HookOutcome::Returned(HookResult::modify(redacted))
            }),
        );
        let seen = Rc::new(RefCell::new(Vec::new()));
        let kept = Rc::clone(&seen);
        sim.before_broadcast(
            "policy",
            Box::new(move |event| {
                kept.borrow_mut().push(event.clone());
                HookOutcome::Returned(HookResult::allow())
            }),
        );
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (texts(&seen), sim.timeline_lines()),
            (
                vec!["[redacted]".to_owned()],
                vec!["#0 sms delivered \"[redacted]\"".to_owned()]
            )
        );
    }

    /// RFC §9.3: a fail-closed off-lock hook past its own timeout blocks
    /// the event at step 9.
    #[test]
    fn failing_closed_on_its_timeout_blocks_the_event_at_step_nine() {
        let mut sim = room();
        let mut spec = HookSpec::new("scanner", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        spec.needs_lock = false;
        spec.fail_closed = true;
        sim.sync_hook(spec, Box::new(|_| HookOutcome::TimedOut));
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (result.reason.as_deref(), sim.timeline_lines()),
            (
                Some("hook_timeout:scanner"),
                vec!["#0 sms blocked:scanner \"Bonjour\"".to_owned()]
            )
        );
    }

    /// A block stops the off-lock hooks: the next one never runs.
    #[test]
    fn blocking_leaves_the_next_off_lock_hook_unrun() {
        let mut sim = room();
        sim.off_lock_check("scanner", answering(|| HookResult::block("SIN detected")));
        sim.off_lock_check("moderation", answering(HookResult::allow));
        sim.send(bonjour()).unwrap();
        assert_eq!(sim.traced("hook"), ["hook scanner"]);
    }

    /// RFC §26.1, steps 5a and 9: an unusable result is reported when it is
    /// met, before the lock.
    #[test]
    fn that_raises_is_reported_before_the_lock_and_lets_the_event_through() {
        let mut sim = room();
        sim.off_lock_check(
            "scanner",
            Box::new(|_| HookOutcome::Raised(ErrorRecord::new("ScanError", "the scanner is down"))),
        );
        let result = sim.send(bonjour()).unwrap();
        let journal: Vec<&str> = sim
            .journal
            .iter()
            .map(String::as_str)
            .skip_while(|line| *line != "hook scanner")
            .take(3)
            .collect();
        assert_eq!(
            (journal, result.event.map(|event| event.status)),
            (
                vec!["hook scanner", "emit hook_error", "await ticket room_1"],
                Some(EventStatus::Delivered)
            )
        );
    }
}

/// RFC §9.5.1, Decision: an off-lock block is applied at step 9, after the
/// status, idempotency and edit gates, which decide first.
mod an_off_lock_block {
    use super::*;
    use pretty_assertions::assert_eq;

    fn blocking() -> Sim {
        scanned(|| HookResult::block("SIN detected"))
    }

    #[test]
    fn is_committed_under_the_lock_and_runs_no_locked_hook() {
        let mut sim = blocking();
        sim.send(bonjour()).unwrap();
        let locked: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| *line != "lock room_1")
            .collect();
        assert_eq!(
            locked,
            [
                "lock room_1",
                "load_context room_1",
                "commit #0 sms blocked:scanner \"Bonjour\"",
                "release room_1",
                "release ticket room_1",
            ]
        );
    }

    #[test]
    fn reports_the_hook_s_reason() {
        let mut sim = blocking();
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (result.blocked, result.reason.as_deref()),
            (true, Some("SIN detected"))
        );
    }

    /// The room closes while the scanner runs: the status gate refuses the
    /// event, nothing is stored, and no block is reported.
    #[test]
    fn comes_after_a_closed_room_s_refusal() {
        let mut sim = blocking();
        sim.hooks_change = Some(HookChange::CloseRoom);
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.timeline.len(),
                sim.framework_events()
            ),
            (
                Some("room_closed"),
                0,
                vec![&FrameworkEvent::room_refused_event(
                    room_id(),
                    "evt_1".parse().unwrap(),
                    RoomStatus::Closed,
                    RefusedOperation::Inbound,
                    EventType::Message
                )]
            )
        );
    }

    /// RFC §13.4: a replay answers what the first delivery did.
    #[test]
    fn comes_after_the_idempotency_check() {
        let mut sim = blocking();
        let mut first = notice_event("evt_0", "Bonjour");
        first.source.channel_id = channel("sms");
        first.idempotency_key = Some("SM1".to_owned());
        first.status = EventStatus::Delivered;
        sim.seed(first);
        let mut replay = bonjour();
        replay.idempotency_key = Some("SM1".to_owned());
        let result = sim.send(replay).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.event.map(|event| event.id.to_string()),
                sim.timeline.len()
            ),
            (false, Some("evt_0".to_owned()), 1)
        );
    }

    /// RFC §10.1 step 8a.
    #[test]
    fn comes_after_an_edit_s_refusal() {
        let mut sim = blocking();
        let mut edit = InboundMessage::new(
            channel("sms"),
            SENDER,
            EventContent::Edit(
                EditContent::new("evt_404".parse().unwrap(), EventContent::text("Bonsoir"))
                    .with_edit_source(EditSource::Sender),
            ),
        );
        edit.event_type = EventType::Edit;
        let result = sim.send(edit).unwrap();
        assert_eq!(
            (result.reason.as_deref(), sim.timeline.len()),
            (Some("target_event_not_found"), 0)
        );
    }

    /// Gap G-067: an event a gate refuses before step 9 drops the off-lock
    /// outcome with its side effects.
    #[test]
    fn refused_by_a_gate_drops_the_check_s_side_effects() {
        let mut sim = scanned(|| {
            HookResult::block("SIN detected")
                .with_task(Task::new(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Call back",
                    Timestamp::UNIX_EPOCH,
                ))
                .inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Message blocked."),
                    vec![channel("sms")],
                ))
        });
        sim.hooks_change = Some(HookChange::CloseRoom);
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (sim.tasks.len(), sim.timeline.len(), sim.traced("persist")),
            (0, 0, Vec::<&str>::new())
        );
    }
}

/// RFC §9.5.1, Decision: the side effects of both phases are collected
/// together.
mod the_side_effects_of_both_phases {
    use super::*;
    use pretty_assertions::assert_eq;

    /// An off-lock scanner that files a task and tells the advisor, and a
    /// locked policy that records an observation and tells the customer.
    fn reporting() -> Sim {
        let mut sim = room();
        sim.off_lock_check(
            "scanner",
            answering(|| {
                HookResult::allow()
                    .with_task(Task::new(
                        TaskId::new("task_1").unwrap(),
                        room_id(),
                        "Review the message",
                        Timestamp::UNIX_EPOCH,
                    ))
                    .inject(InjectedEvent::to_channels(
                        notice_event("inj_1", "Scanned"),
                        vec![channel("ws")],
                    ))
            }),
        );
        sim.before_broadcast(
            "policy",
            answering(|| {
                HookResult::allow()
                    .with_observation(Observation::new(
                        ObservationId::new("obs_1").unwrap(),
                        room_id(),
                        channel("sms"),
                        "policy checked",
                        Timestamp::UNIX_EPOCH,
                    ))
                    .inject(InjectedEvent::to_channels(
                        notice_event("inj_2", "Checked"),
                        vec![channel("sms")],
                    ))
            }),
        );
        sim
    }

    #[test]
    fn are_stored_once() {
        let mut sim = reporting();
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                sim.tasks.len(),
                sim.observations.len(),
                sim.traced("persist")
            ),
            (1, 1, vec!["persist 1 tasks, 1 observations"])
        );
    }

    /// Each injected event is committed once, in hook order (gap G-017).
    #[test]
    fn inject_each_event_once_in_hook_order() {
        let mut sim = reporting();
        sim.send(bonjour()).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 sms delivered \"Bonjour\"",
                "#1 system delivered \"Scanned\"",
                "#2 system delivered \"Checked\"",
            ]
        );
    }
}

/// RFC §9.5.1, Order: events that match no off-lock hook take no ticket.
mod an_event_no_off_lock_hook_fires_for {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn takes_no_ticket_and_runs_its_hooks_under_the_lock() {
        let mut sim = room();
        let mut spec = HookSpec::new(
            "email_scanner",
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
        );
        spec.needs_lock = false;
        spec.filter.channel_ids = Some([channel("email")].into());
        sim.sync_hook(
            spec,
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.before_broadcast("policy", answering(HookResult::allow));
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (sim.traced("take ticket"), sim.traced("hook")),
            (Vec::<&str>::new(), vec!["hook policy"])
        );
    }
}

/// RFC §9.5.1, Other commit paths: every hook runs under the lock.
mod another_pass {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The journal of the pass committing `index`, from its lock to its
    /// commit.
    fn pass_of(sim: &Sim, index: u64) -> Vec<&str> {
        let commit = format!("commit #{index} ");
        let end = sim
            .trace
            .iter()
            .position(|line| line.starts_with(&commit))
            .unwrap();
        let start = sim
            .trace
            .iter()
            .take(end)
            .rposition(|line| line == "lock room_1")
            .unwrap();
        sim.trace
            .iter()
            .skip(start)
            .take(end + 1 - start)
            .map(String::as_str)
            .collect()
    }

    fn every_hook_under_the_lock() -> Vec<&'static str> {
        vec![
            "lock room_1",
            "load_context room_1",
            "hook scanner",
            "hook policy",
            "load_context room_1",
        ]
    }

    #[test]
    fn a_response_runs_every_hook_under_the_lock() {
        let mut sim = room();
        sim.echo_agent("ai", "Hello back");
        sim.off_lock_check(
            "scanner",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.send(bonjour()).unwrap();
        let mut expected = every_hook_under_the_lock();
        expected.push("commit #1 ai delivered \"Hello back\"");
        assert_eq!(
            (pass_of(&sim, 1), sim.traced("take ticket")),
            (expected, vec!["take ticket room_1"])
        );
    }

    #[test]
    fn a_hook_s_injected_event_runs_every_hook_under_the_lock() {
        let mut sim = room();
        sim.off_lock_check(
            "scanner",
            answering(|| {
                HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Scanned"),
                    vec![channel("ws")],
                ))
            }),
        );
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.send(bonjour()).unwrap();
        let mut expected = every_hook_under_the_lock();
        expected.push("commit #1 system delivered \"Scanned\"");
        assert_eq!(pass_of(&sim, 1), expected);
    }
}

/// RFC §9.5.1, Release: a ticket is released on every path.
mod the_ticket {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The trace lines of the ticket, in order.
    fn ticket_lines(sim: &Sim) -> Vec<&str> {
        sim.trace
            .iter()
            .map(String::as_str)
            .filter(|line| line.contains("ticket"))
            .collect()
    }

    fn taken_awaited_released() -> Vec<&'static str> {
        vec![
            "take ticket room_1",
            "await ticket room_1",
            "release ticket room_1",
        ]
    }

    #[test]
    fn is_released_after_the_lock_once_the_event_is_committed() {
        let mut sim = scanned(HookResult::allow);
        sim.send(bonjour()).unwrap();
        let after_commit: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("commit"))
            .take(4)
            .collect();
        assert_eq!(
            after_commit,
            [
                "commit #0 sms delivered \"Bonjour\"",
                "enter lane room_1 evt_1",
                "release room_1",
                "release ticket room_1"
            ]
        );
    }

    #[test]
    fn is_released_after_a_locked_hook_s_block() {
        let mut sim = room();
        sim.off_lock_check("scanner", answering(HookResult::allow));
        sim.before_broadcast("policy", answering(|| HookResult::block("no")));
        sim.send(bonjour()).unwrap();
        assert_eq!(ticket_lines(&sim), taken_awaited_released());
    }

    #[test]
    fn is_released_for_a_duplicate() {
        let mut sim = scanned(HookResult::allow);
        let mut first = notice_event("evt_0", "Bonjour");
        first.source.channel_id = channel("sms");
        first.idempotency_key = Some("SM1".to_owned());
        sim.seed(first);
        let mut replay = bonjour();
        replay.idempotency_key = Some("SM1".to_owned());
        sim.send(replay).unwrap();
        assert_eq!(ticket_lines(&sim), taken_awaited_released());
    }

    #[test]
    fn is_released_when_an_edit_is_refused() {
        let mut sim = scanned(HookResult::allow);
        let mut edit = InboundMessage::new(
            channel("sms"),
            SENDER,
            EventContent::Edit(
                EditContent::new("evt_404".parse().unwrap(), EventContent::text("Bonsoir"))
                    .with_edit_source(EditSource::Sender),
            ),
        );
        edit.event_type = EventType::Edit;
        sim.send(edit).unwrap();
        assert_eq!(ticket_lines(&sim), taken_awaited_released());
    }

    /// RFC §10.1.1: an instruction is decided without a commit; its ticket
    /// goes once the lock is released.
    #[test]
    fn is_released_once_an_instruction_is_decided() {
        let mut sim = scanned(HookResult::allow);
        sim.echo_agent("ai", "Hello, I am the assistant.");
        let mut instruction = Sim::message("ws", "Introduce yourself.");
        instruction.event_type = EventType::Instruction;
        instruction.addressed_to = Some(vec![channel("ai")]);
        sim.send(instruction).unwrap();
        let decided: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| *line != "hook policy")
            .skip(1)
            .take(4)
            .collect();
        assert_eq!(
            (decided, ticket_lines(&sim)),
            (
                vec![
                    "load_context room_1",
                    "enter lane room_1 evt_1",
                    "release room_1",
                    "release ticket room_1"
                ],
                taken_awaited_released()
            )
        );
    }

    #[test]
    fn is_released_when_the_room_refuses_the_event() {
        let mut sim = scanned(HookResult::allow);
        sim.room.status = RoomStatus::Closed;
        sim.send(bonjour()).unwrap();
        assert_eq!(ticket_lines(&sim), taken_awaited_released());
    }

    #[test]
    fn is_released_when_the_commit_fails() {
        let mut sim = scanned(HookResult::allow);
        sim.failing_effect = Some("commit");
        let failed = sim.send(bonjour());
        assert_eq!(
            (ticket_lines(&sim), failed.is_err()),
            (taken_awaited_released(), true)
        );
    }

    #[test]
    fn is_released_when_the_lock_cannot_be_taken() {
        let mut sim = scanned(HookResult::allow);
        sim.failing_effect = Some("lock room_1");
        let failed = sim.send(bonjour());
        assert_eq!(
            (
                ticket_lines(&sim),
                matches!(failed, Err(InboundError::Lock(_)))
            ),
            (taken_awaited_released(), true)
        );
    }

    #[test]
    fn is_released_when_its_turn_cannot_be_awaited() {
        let mut sim = scanned(HookResult::allow);
        sim.failing_effect = Some("await ticket");
        let failed = sim.send(bonjour());
        assert_eq!(
            (
                ticket_lines(&sim),
                matches!(failed, Err(InboundError::Ticket(_)))
            ),
            (
                vec![
                    "take ticket room_1",
                    "await ticket room_1 failed",
                    "release ticket room_1"
                ],
                true
            )
        );
    }

    #[test]
    fn that_cannot_be_taken_fails_the_call_with_nothing_written() {
        let mut sim = scanned(HookResult::allow);
        sim.failing_effect = Some("take ticket");
        let failed = sim.send(bonjour());
        assert_eq!(
            (
                ticket_lines(&sim),
                sim.timeline.len(),
                sim.traced("lock"),
                matches!(failed, Err(InboundError::Ticket(_)))
            ),
            (vec!["take ticket room_1 failed"], 0, vec![], true)
        );
    }

    /// Gap G-025: a failed release changes nothing the call decided.
    #[test]
    fn whose_release_fails_leaves_the_event_committed_and_reports_it() {
        let mut sim = scanned(HookResult::allow);
        sim.failing_effect = Some("release ticket");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                result.error.map(|error| error.error_type)
            ),
            (Some(EventStatus::Delivered), Some("TicketLost".to_owned()))
        );
    }
}
