//! The processing timeout, effect by effect (RFC §13.6, §10.4, §9.5.1,
//! §8.2, gap G-068): a call past its `process_timeout` before its decision
//! writes nothing of its event, releases what it held, and is refused
//! `process_timeout`, its framework event naming the message's channel
//! before the room lock and the event under it; a hook's own timeout is
//! still a hook error; nothing after the commit is bounded.
//!
//! The kit's timeout is ten seconds; a slow effect takes eleven on the
//! simulated clock. The room holds the customer's `sms` and the advisor's
//! `ws`.
use std::time::Duration;

use roomkit_core::model::hook::AllowOptions;
use roomkit_core::model::{
    ChannelType, EditContent, EditSource, EventContent, EventStatus, EventType, HookResult,
    InboundMessage, InjectedEvent, RoomId, Task, TaskId, TextContent, Timestamp,
};
use roomkit_core::ops::{FrameworkEvent, InboundResult};
use roomkit_core::rules::hook_chain::HookOutcome;

use crate::support::{Sim, channel, notice_event, room_id};

const TIMEOUT: Duration = Duration::from_secs(10);

/// The room with `sms` and `ws`, and a ten-second `process_timeout`.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.transport("ws");
    sim.config.process_timeout = Some(TIMEOUT);
    sim
}

/// The room with an off-lock `scanner` and a locked `policy`, both
/// allowing.
fn scanned() -> Sim {
    let mut sim = room();
    sim.off_lock_check(
        "scanner",
        Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
    );
    sim.before_broadcast(
        "policy",
        Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
    );
    sim
}

/// Makes the effects whose trace line starts with `effect` outlast the
/// timeout.
fn slow(sim: &mut Sim, effect: &'static str) {
    sim.durations
        .push((effect, TIMEOUT + Duration::from_secs(1)));
}

fn bonjour() -> InboundMessage {
    Sim::message("sms", "Bonjour")
}

/// What a refusal for the timeout reads: blocked, its reason, no event.
fn refusal(result: &InboundResult) -> (bool, Option<&str>, bool) {
    (
        result.blocked,
        result.reason.as_deref(),
        result.event.is_none(),
    )
}

fn timed_out() -> (bool, Option<&'static str>, bool) {
    (true, Some("process_timeout"), true)
}

/// The `process_timeout` framework events emitted.
fn process_timeouts(sim: &Sim) -> Vec<&FrameworkEvent> {
    sim.framework_events()
        .into_iter()
        .filter(|event| matches!(event, FrameworkEvent::ProcessTimeout { .. }))
        .collect()
}

/// The event of a timeout that passed before the room lock, in `room`.
fn before_the_lock(room: Option<RoomId>) -> FrameworkEvent {
    FrameworkEvent::process_timeout(room, Some(channel("sms")), None, 10.0)
}

/// The event of a timeout that passed under the room lock.
fn under_the_lock(event_id: &str) -> FrameworkEvent {
    FrameworkEvent::process_timeout(Some(room_id()), None, Some(event_id.parse().unwrap()), 10.0)
}

/// The trace from the line starting with `from` on.
fn trace_from<'sim>(sim: &'sim Sim, from: &str) -> Vec<&'sim str> {
    sim.trace
        .iter()
        .map(String::as_str)
        .skip_while(|line| !line.starts_with(from))
        .collect()
}

/// RFC §13.6: an expiry before the decision.
mod expiring_during {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn the_channel_s_parse_writes_nothing() {
        let mut sim = room();
        slow(&mut sim, "handle_inbound sms");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (refusal(&result), sim.trace.clone(), sim.timeline.len()),
            (
                timed_out(),
                vec![
                    "load_context room_1".to_owned(),
                    "handle_inbound sms deadline passed".to_owned(),
                ],
                0
            )
        );
    }

    #[test]
    fn the_channel_s_parse_names_the_channel() {
        let mut sim = room();
        slow(&mut sim, "handle_inbound sms");
        sim.send(bonjour()).unwrap();
        assert_eq!(process_timeouts(&sim), [&before_the_lock(Some(room_id()))]);
    }

    #[test]
    fn an_off_lock_hook_releases_the_ticket_and_names_the_channel() {
        let mut sim = scanned();
        slow(&mut sim, "hook scanner");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                trace_from(&sim, "take ticket"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                vec![
                    "take ticket room_1",
                    "hook scanner deadline passed",
                    "release ticket room_1",
                ],
                vec![&before_the_lock(Some(room_id()))]
            )
        );
    }

    #[test]
    fn the_ticket_s_wait_releases_the_ticket_and_names_the_channel() {
        let mut sim = scanned();
        slow(&mut sim, "await ticket");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                trace_from(&sim, "await ticket"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                vec![
                    "await ticket room_1 deadline passed",
                    "release ticket room_1",
                ],
                vec![&before_the_lock(Some(room_id()))]
            )
        );
    }

    #[test]
    fn the_lock_s_wait_releases_the_ticket_and_names_the_channel() {
        let mut sim = scanned();
        slow(&mut sim, "lock room_1");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                trace_from(&sim, "lock room_1"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                vec!["lock room_1 deadline passed", "release ticket room_1",],
                vec![&before_the_lock(Some(room_id()))]
            )
        );
    }

    #[test]
    fn a_locked_hook_releases_the_lock_and_names_the_event() {
        let mut sim = scanned();
        slow(&mut sim, "hook policy");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                trace_from(&sim, "hook policy"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                vec![
                    "hook policy deadline passed",
                    "release room_1",
                    "release ticket room_1",
                ],
                vec![&under_the_lock("evt_1")]
            )
        );
    }

    /// Gap G-068: the injected events, tasks and observations of the hooks
    /// that ran before are dropped with the event.
    #[test]
    fn a_locked_hook_drops_what_the_earlier_hooks_produced() {
        let mut sim = room();
        sim.before_broadcast(
            "triage",
            Box::new(|_| {
                let task = Task::at(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Call back",
                    Timestamp::UNIX_EPOCH,
                );
                HookOutcome::Returned(HookResult {
                    tasks: vec![task],
                    ..HookResult::allow(AllowOptions {
                        injected: Some(vec![InjectedEvent {
                            target_channel_ids: Some(vec![channel("ws")]),
                            ..InjectedEvent::new(notice_event("inj_1", "Noted"))
                        }]),
                    })
                })
            }),
        );
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
        );
        slow(&mut sim, "hook policy");
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (sim.timeline.len(), sim.tasks.len(), sim.traced("persist")),
            (0, 0, Vec::<&str>::new())
        );
    }

    /// RFC §10.4: past the deadline, a routed message is refused, never let
    /// in unrecorded.
    #[test]
    fn routing_s_wait_for_the_lock_records_no_sender() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.attach("sms");
        sim.routes_to(Some("room_1"));
        sim.config.process_timeout = Some(TIMEOUT);
        slow(&mut sim, "lock room_1");
        let result = sim.route(None, bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                trace_from(&sim, "lock room_1"),
                sim.bindings[0].participant_id.clone(),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                vec!["lock room_1 deadline passed"],
                None,
                vec![&before_the_lock(Some(room_id()))]
            )
        );
    }

    /// Gap G-068: a call that has no room yet names none.
    #[test]
    fn the_router_names_no_room() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.config.process_timeout = Some(TIMEOUT);
        slow(&mut sim, "route sms");
        let result = sim.route(None, bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                sim.traced("enter lane"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                Vec::<&str>::new(),
                vec![&before_the_lock(None)]
            )
        );
    }

    #[test]
    fn step_2_s_read_of_the_room_names_the_channel() {
        let mut sim = room();
        slow(&mut sim, "load_context room_1");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (refusal(&result), sim.trace.clone(), process_timeouts(&sim)),
            (
                timed_out(),
                vec!["load_context room_1 deadline passed".to_owned()],
                vec![&before_the_lock(Some(room_id()))]
            )
        );
    }

    /// The locked section's reads before the hooks: the key check, an
    /// edit's target.
    fn under_the_lock_while(effect: &'static str, message: InboundMessage) -> Sim {
        let mut sim = scanned();
        slow(&mut sim, effect);
        let result = sim.send(message).unwrap();
        assert_eq!(refusal(&result), timed_out());
        sim
    }

    /// The trace from `effect`, the read that expired, on.
    fn released_after(sim: &Sim, effect: &str) -> Vec<String> {
        trace_from(sim, effect)
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    fn lock_and_ticket(expired: &str) -> Vec<String> {
        vec![
            format!("{expired} deadline passed"),
            "release room_1".to_owned(),
            "release ticket room_1".to_owned(),
        ]
    }

    #[test]
    fn the_idempotency_check_releases_the_lock_and_names_the_event() {
        let mut keyed = bonjour();
        keyed.idempotency_key = Some("SM1".to_owned());
        let sim = under_the_lock_while("find_key", keyed);
        assert_eq!(
            (released_after(&sim, "find_key"), process_timeouts(&sim)),
            (
                lock_and_ticket("find_key SM1"),
                vec![&under_the_lock("evt_1")]
            )
        );
    }

    #[test]
    fn an_edit_s_target_read_releases_the_lock_and_names_the_event() {
        let mut edit = InboundMessage::new(
            channel("sms"),
            "+15551234567",
            EventContent::Edit(EditContent {
                edit_source: Some(EditSource::Sender),
                ..EditContent::new("evt_404".parse().unwrap(), TextContent::new("Bonsoir"))
            }),
        );
        edit.event_type = EventType::Edit;
        let sim = under_the_lock_while("get_event", edit);
        assert_eq!(
            (released_after(&sim, "get_event"), process_timeouts(&sim)),
            (
                lock_and_ticket("get_event evt_404"),
                vec![&under_the_lock("evt_1")]
            )
        );
    }

    /// Gap G-070: the room read again once the hooks that ran under the
    /// lock are over, for the source check and the next index.
    #[test]
    fn the_read_after_the_hooks_releases_the_lock_and_names_the_event() {
        let mut sim = scanned();
        // The step 2 read, the gate's, then the read after the hooks.
        sim.nth_durations
            .push(("load_context", 3, TIMEOUT + Duration::from_secs(1)));
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                released_after(&sim, "hook policy")[1..].to_vec(),
                process_timeouts(&sim),
                sim.timeline.len()
            ),
            (
                timed_out(),
                lock_and_ticket("load_context room_1"),
                vec![&under_the_lock("evt_1")],
                0
            )
        );
    }

    /// Gap G-068: the identity read comes before any room.
    #[test]
    fn the_identity_read_names_no_room() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.config.process_timeout = Some(TIMEOUT);
        slow(&mut sim, "resolve_identity");
        let result = sim.route(None, bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                sim.traced("route"),
                process_timeouts(&sim)
            ),
            (
                timed_out(),
                Vec::<&str>::new(),
                vec![&before_the_lock(None)]
            )
        );
    }
}

/// RFC §13.6: nothing of the event is written, and the room goes on.
mod a_timed_out_call {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn consumes_no_index() {
        let mut sim = scanned();
        slow(&mut sim, "hook policy");
        sim.send(bonjour()).unwrap();
        sim.durations.clear();
        let next = sim.send(Sim::message("sms", "Still there?")).unwrap();
        assert_eq!(
            (next.event.map(|event| event.index), sim.timeline_lines()),
            (
                Some(0),
                vec!["#0 sms delivered \"Still there?\"".to_owned()]
            )
        );
    }

    /// Gap G-068: the room step 2 creates is attached whatever the
    /// deadline, its attach recorded; only the event is not written.
    #[test]
    fn leaves_the_room_step_2_created_attached() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.config.process_timeout = Some(TIMEOUT);
        slow(&mut sim, "lock room_1");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                refusal(&result),
                sim.room_exists,
                sim.bindings.len(),
                sim.timeline.len(),
                sim.traced("handle_inbound")
            ),
            (
                timed_out(),
                true,
                1,
                // The attach's record alone (gap G-046).
                1,
                vec!["handle_inbound sms deadline passed"]
            )
        );
    }
}

/// RFC §13.6: a hook's own timeout is the hook's, reported as such (RFC
/// §9.3, §8.2).
mod a_hook_s_own_timeout {
    use super::*;
    use pretty_assertions::assert_eq;

    fn names_emitted(sim: &Sim) -> Vec<String> {
        sim.framework_event_names()
            .into_iter()
            .filter(|name| name.starts_with("hook_") || name == "process_timeout")
            .collect()
    }

    #[test]
    fn under_the_lock_is_still_a_hook_error() {
        let mut sim = room();
        sim.before_broadcast("policy", Box::new(|_| HookOutcome::TimedOut));
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (names_emitted(&sim), result.event.map(|event| event.status)),
            (
                vec!["hook_timeout".to_owned(), "hook_error".to_owned()],
                Some(EventStatus::Delivered)
            )
        );
    }

    #[test]
    fn off_the_lock_is_still_a_hook_error() {
        let mut sim = room();
        sim.off_lock_check("scanner", Box::new(|_| HookOutcome::TimedOut));
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (names_emitted(&sim), result.event.map(|event| event.status)),
            (
                vec!["hook_timeout".to_owned(), "hook_error".to_owned()],
                Some(EventStatus::Delivered)
            )
        );
    }
}

/// RFC §13.6: the commit and everything after it are never bounded.
mod after_the_commit {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Each delivery takes thirty seconds: the cascade outlasts the
    /// timeout, and the event stays delivered.
    #[test]
    fn slow_deliveries_change_nothing() {
        let mut sim = room();
        sim.echo_agent("ai", "Hello back");
        sim.delivery_duration = Duration::from_secs(30);
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (
                result.blocked,
                sim.timeline_lines(),
                process_timeouts(&sim).len()
            ),
            (
                false,
                vec![
                    "#0 sms delivered \"Bonjour\"".to_owned(),
                    "#1 ai delivered \"Hello back\"".to_owned()
                ],
                0
            )
        );
    }

    /// Every request before the commit carries the deadline; no write, no
    /// release, nothing of the delivery and no response's pass does.
    #[test]
    fn nothing_carries_the_deadline() {
        let mut sim = scanned();
        sim.echo_agent("ai", "Hello back");
        sim.send(bonjour()).unwrap();
        assert_eq!(
            sim.bounded,
            [
                "load_context room_1",
                "handle_inbound sms",
                "hook scanner",
                "await ticket room_1",
                "lock room_1",
                "load_context room_1",
                "hook policy",
                "load_context room_1",
            ]
        );
    }

    #[test]
    fn with_no_timeout_set_nothing_is_bounded() {
        let mut sim = scanned();
        sim.config.process_timeout = None;
        slow(&mut sim, "hook policy");
        let result = sim.send(bonjour()).unwrap();
        assert_eq!(
            (sim.bounded.len(), result.event.map(|event| event.status)),
            (0, Some(EventStatus::Delivered))
        );
    }
}
