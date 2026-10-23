//! Direct injection and the reentrant lock, effect by effect (RFC §10.5,
//! §13.5, §9.5.1 Reentrance, §10.1 step 18, gaps G-069 to G-071).
//!
//! The room holds the customer's `sms` and the advisor's console `ws`; an
//! agent `ai` joins where a test needs one. An event the integrator injects
//! comes from `ws`, as a console's REST call would.

use std::cell::RefCell;
use std::rc::Rc;

use roomkit_core::model::{
    ChannelOutput, DirectEvent, EditContent, EditSource, EventContent, EventDirection, EventStatus,
    EventType, HookResult, InboundMessage, InjectedEvent, RoomEvent, RoomId, RoomStatus,
    Visibility,
};
use roomkit_core::ops::{
    Entry, FrameworkEvent, Inbound, InboundError, InboundTarget, MutationError, Next, Operation,
    RefusedOperation, SequentialEnvironment,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::transcoding::text_of;

use crate::support::{
    Answer, Called, HookChange, Reentry, Sim, calling, channel, notice_event, response, room_id,
};

/// The room with the customer's `sms` and the advisor's `ws`.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.transport("ws");
    sim
}

/// A text the advisor's console injects.
fn from_console(text: &str) -> DirectEvent {
    DirectEvent::new(channel("ws"), EventContent::text(text))
}

/// The call `send_event(room_1, ...)` of the console's `text`.
fn send_event(text: &str) -> Entry {
    Entry::Direct {
        room_id: room_id(),
        organization_id: None,
        event: Box::new(from_console(text)),
    }
}

fn hello() -> InboundMessage {
    Sim::message("sms", "hello")
}

/// What the calls of a hook returned.
type Seen = Rc<RefCell<Vec<RoomEvent>>>;
/// The indices a hook was shown.
type Shown = Rc<RefCell<Vec<u64>>>;

/// The committed event of what a call returned.
fn committed(called: &Called) -> Option<RoomEvent> {
    called.as_ref().ok().and_then(|result| result.event.clone())
}

/// A locked hook that, on the customer's events, injects the console's
/// "Heads up" into the room and allows; `seen` records what its call
/// returned, `shown` the index the hook saw.
fn notifier(sim: &mut Sim, seen: &Seen, shown: &Shown) {
    let (seen, shown) = (Rc::clone(seen), Rc::clone(shown));
    sim.reentrant_hook(
        "notifier",
        true,
        Reentry {
            entries: calling(move |event| {
                if event.source.channel_id.as_str() != "sms" {
                    return None;
                }
                shown.borrow_mut().push(event.index);
                Some(send_event("Heads up"))
            }),
            answer: Answer::Awaits(Box::new(move |called| {
                seen.borrow_mut().extend(committed(called));
                HookOutcome::Returned(HookResult::allow())
            })),
        },
    );
}

/// RFC §10.5: the event takes the pipeline from the lane on, without the
/// channel's parse.
mod an_injected_event {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn takes_the_pipeline_without_a_parse() {
        let mut sim = room();
        let result = sim
            .inject("room_1", from_console("Your card is on its way"))
            .unwrap();
        assert_eq!(
            (sim.trace.clone(), result.event.map(|event| event.index)),
            (
                [
                    "enter lane room_1",
                    "lock room_1",
                    "load_context room_1",
                    "commit #0 ws delivered \"Your card is on its way\"",
                    "release room_1",
                    "on_event sms <- evt_1",
                    "deliver sms <- evt_1",
                    "leave lane room_1",
                ]
                .map(str::to_owned)
                .to_vec(),
                Some(0)
            )
        );
    }

    /// Gap G-069: a transport's event travels in, as the message it stands
    /// for would.
    #[test]
    fn from_a_transport_is_inbound() {
        let mut sim = room();
        let result = sim.inject("room_1", from_console("Hi")).unwrap();
        assert_eq!(
            result.event.map(|event| event.source.direction),
            Some(EventDirection::Inbound)
        );
    }

    /// Gap G-069: an intelligence channel's event is its own words
    /// travelling out, as its responses do.
    #[test]
    fn from_an_intelligence_channel_is_outbound() {
        let mut sim = room();
        sim.echo_agent("ai", "Hello back");
        let event = DirectEvent::new(channel("ai"), EventContent::text("Let me check."));
        let result = sim.inject("room_1", event).unwrap();
        assert_eq!(
            result.event.map(|event| event.source.direction),
            Some(EventDirection::Outbound)
        );
    }

    #[test]
    fn reports_its_deliveries_and_its_end() {
        let mut sim = room();
        sim.inject("room_1", from_console("Hi")).unwrap();
        assert_eq!(
            sim.framework_event_names(),
            ["delivery_succeeded", "event_processed"]
        );
    }

    #[test]
    fn blocked_by_a_hook_returns_the_record() {
        let mut sim = room();
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not now"))),
        );
        let result = sim.inject("room_1", from_console("Hi")).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.reason,
                result.event.map(|event| event.status),
                sim.timeline_lines()
            ),
            (
                true,
                Some("not now".to_owned()),
                Some(EventStatus::Blocked),
                vec!["#0 ws blocked:policy \"Hi\"".to_owned()]
            )
        );
    }

    /// RFC §13.4, gap G-023: a replay returns the event its key committed.
    #[test]
    fn with_a_key_already_committed_returns_the_original() {
        let mut sim = room();
        let mut first = notice_event("evt_0", "Hi");
        first.source.channel_id = channel("ws");
        first.idempotency_key = Some("req_1".to_owned());
        first.status = EventStatus::Delivered;
        sim.seed(first);
        let mut replay = from_console("Hi");
        replay.idempotency_key = Some("req_1".to_owned());
        let result = sim.inject("room_1", replay).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.id.to_string()),
                sim.timeline.len()
            ),
            (Some("evt_0".to_owned()), 1)
        );
    }

    /// Gap G-023: a key is the channel's: another channel's event with it
    /// is no replay.
    #[test]
    fn with_another_channel_s_key_is_processed() {
        let mut sim = room();
        let mut first = notice_event("evt_0", "Hi");
        first.source.channel_id = channel("sms");
        first.idempotency_key = Some("req_1".to_owned());
        sim.seed(first);
        let mut event = from_console("Hi");
        event.idempotency_key = Some("req_1".to_owned());
        sim.inject("room_1", event).unwrap();
        assert_eq!(sim.timeline.len(), 2);
    }

    /// Gap G-069: no step 2 runs: an unbound channel is not attached, and
    /// its event is refused `source_unbound`, nothing written: the host
    /// named the channel itself.
    #[test]
    fn from_an_unbound_channel_is_refused_with_nothing_written() {
        let mut sim = room();
        sim.unbound_transport("api", roomkit_core::model::ChannelType::Webhook);
        let event = DirectEvent::new(channel("api"), EventContent::text("Order shipped"));
        let refused = sim.inject("room_1", event);
        assert_eq!(
            (refused, sim.bindings.len(), sim.timeline_lines()),
            (
                Err(InboundError::Refused {
                    reason: "source_unbound".to_owned()
                }),
                2,
                vec![]
            )
        );
    }

    /// Gap G-069: refused before its hooks, which see nothing of it.
    #[test]
    fn from_an_unbound_channel_runs_no_hook() {
        let mut sim = room();
        sim.unbound_transport("api", roomkit_core::model::ChannelType::Webhook);
        sim.before_broadcast(
            "audit",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        let event = DirectEvent::new(channel("api"), EventContent::text("Order shipped"));
        sim.inject("room_1", event).unwrap_err();
        assert_eq!(sim.traced("hook"), Vec::<&str>::new());
    }

    /// Gap G-069: refused on the room its off-lock check reads, before the
    /// check's hooks and its ticket.
    #[test]
    fn from_an_unbound_channel_runs_no_off_lock_check() {
        let mut sim = room();
        sim.unbound_transport("api", roomkit_core::model::ChannelType::Webhook);
        sim.off_lock_check(
            "scanner",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        let event = DirectEvent::new(channel("api"), EventContent::text("Order shipped"));
        let refused = sim.inject("room_1", event);
        assert_eq!(
            (refused, sim.traced("hook"), sim.traced("take ticket")),
            (
                Err(InboundError::Refused {
                    reason: "source_unbound".to_owned()
                }),
                vec![],
                vec![]
            )
        );
    }

    /// RFC §17.2, gap G-069: a room of another organization than the one
    /// the caller acts for refuses the event, nothing written.
    #[test]
    fn into_another_organization_s_room_is_refused_with_nothing_written() {
        let mut sim = room();
        sim.room.organization_id = Some("bank".to_owned());
        let refused = sim.inject_for(Some("clinic"), "room_1", from_console("Hi"));
        assert_eq!(
            (refused, sim.timeline.len(), sim.traced("hook")),
            (Err(InboundError::OtherOrganization(room_id())), 0, vec![])
        );
    }

    /// RFC §17.2: a room of no organization is not the caller's either.
    #[test]
    fn into_a_room_of_no_organization_is_refused_for_a_caller_naming_one() {
        let mut sim = room();
        let refused = sim.inject_for(Some("clinic"), "room_1", from_console("Hi"));
        assert_eq!(refused, Err(InboundError::OtherOrganization(room_id())));
    }

    #[test]
    fn into_its_organization_s_room_is_committed() {
        let mut sim = room();
        sim.room.organization_id = Some("clinic".to_owned());
        let result = sim
            .inject_for(Some("clinic"), "room_1", from_console("Hi"))
            .unwrap();
        assert_eq!(result.event.map(|event| event.index), Some(0));
    }

    /// Gap G-069: another organization's room is refused before an off-lock
    /// check's hooks read it.
    #[test]
    fn into_another_organization_s_room_runs_no_off_lock_check() {
        let mut sim = room();
        sim.room.organization_id = Some("bank".to_owned());
        sim.off_lock_check(
            "scanner",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        let refused = sim.inject_for(Some("clinic"), "room_1", from_console("Hi"));
        assert_eq!(
            (refused, sim.traced("hook")),
            (Err(InboundError::OtherOrganization(room_id())), vec![])
        );
    }

    /// RFC §9.5.1: an injected event an off-lock hook fires for takes the
    /// room's admission ticket, the hook reading the room before the lock.
    #[test]
    fn takes_a_ticket_for_its_off_lock_check() {
        let mut sim = room();
        sim.off_lock_check(
            "scanner",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.inject("room_1", from_console("Hi")).unwrap();
        assert_eq!(
            sim.trace[..7].to_vec(),
            [
                "enter lane room_1",
                "load_context room_1",
                "take ticket room_1",
                "hook scanner",
                "await ticket room_1",
                "lock room_1",
                "load_context room_1",
            ]
        );
    }

    /// RFC §10.5: the room's next event takes the index of none of its
    /// refusals.
    #[test]
    fn missing_its_room_fails_with_nothing_created() {
        let mut sim = Sim::without_rooms();
        sim.transport("ws");
        let refused = sim.inject("room_1", from_console("Hi"));
        assert_eq!(
            (refused, sim.traced("store room")),
            (
                Err(InboundError::RoomNotFound(room_id())),
                Vec::<&str>::new()
            )
        );
    }
}

/// RFC §10.5: a refusal before the commit point is raised, never returned as
/// an event that was not written; nothing of the event is written.
mod a_refusal_before_the_commit {
    use super::*;
    use pretty_assertions::assert_eq;

    fn refused(reason: &str) -> Result<(), InboundError> {
        Err(InboundError::Refused {
            reason: reason.to_owned(),
        })
    }

    /// What a refused injection left: its error, and the timeline.
    fn outcome(sim: &mut Sim, event: DirectEvent) -> (Result<(), InboundError>, usize) {
        let result = sim.inject("room_1", event).map(|_| ());
        (result, sim.timeline.len())
    }

    /// RFC §5.1: a closed room refuses it, observably, as an inbound event.
    #[test]
    fn by_a_closed_room_is_raised() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        assert_eq!(
            (
                outcome(&mut sim, from_console("Hi")),
                sim.framework_events()
            ),
            (
                (refused("room_closed"), 0),
                vec![&FrameworkEvent::RoomRefusedEvent {
                    room_id: room_id(),
                    event_id: "evt_1".parse().unwrap(),
                    status: RoomStatus::Closed,
                    operation: RefusedOperation::Inbound,
                    event_type: EventType::Message,
                }]
            )
        );
    }

    /// RFC §13.6: past its `process_timeout`, after the framework event.
    #[test]
    fn past_the_call_s_deadline_is_raised() {
        let mut sim = room();
        sim.config.process_timeout = Some(std::time::Duration::from_secs(10));
        sim.durations
            .push(("lock room_1", std::time::Duration::from_secs(11)));
        assert_eq!(
            (
                outcome(&mut sim, from_console("Hi")),
                sim.framework_event_names()
            ),
            (
                (refused("process_timeout"), 0),
                vec!["process_timeout".to_owned()]
            )
        );
    }

    /// RFC §10.1.1: an instruction that names no agent, before anything is
    /// read.
    #[test]
    fn of_an_unaddressed_instruction_is_raised_before_anything_is_read() {
        let mut sim = room();
        let mut instruction = from_console("Introduce yourself.");
        instruction.event_type = EventType::Instruction;
        assert_eq!(
            (outcome(&mut sim, instruction), sim.trace.len()),
            ((refused("instruction_unaddressed"), 0), 0)
        );
    }

    /// RFC §10.1 step 8a.
    #[test]
    fn of_an_edit_is_raised() {
        let mut sim = room();
        let mut edit = DirectEvent::new(
            channel("ws"),
            EventContent::Edit(EditContent {
                target_event_id: "evt_404".parse().unwrap(),
                new_content: Box::new(EventContent::text("Bonsoir")),
                edit_source: Some(EditSource::Sender),
            }),
        );
        edit.event_type = EventType::Edit;
        assert_eq!(
            outcome(&mut sim, edit),
            (refused("target_event_not_found"), 0)
        );
    }

    /// Gap G-041: only the framework writes `internal`.
    #[test]
    fn of_the_visibility_internal_is_raised_before_anything_is_read() {
        let mut sim = room();
        let mut event = from_console("Hi");
        event.visibility = Visibility::Internal;
        assert_eq!(
            (outcome(&mut sim, event), sim.trace.len()),
            ((Err(InboundError::InternalVisibility), 0), 0)
        );
    }

    #[test]
    fn of_an_unregistered_channel_is_raised_before_anything_is_read() {
        let mut sim = room();
        let event = DirectEvent::new(channel("fax"), EventContent::text("Hi"));
        assert_eq!(
            (outcome(&mut sim, event), sim.trace.len()),
            ((Err(InboundError::UnknownChannel(channel("fax"))), 0), 0)
        );
    }

    /// RFC §10.1.1 rule 7.
    #[test]
    fn of_standalone_on_a_message_is_raised() {
        let mut sim = room();
        let mut event = from_console("Hi");
        event.standalone = true;
        assert_eq!(
            outcome(&mut sim, event),
            (Err(InboundError::StandaloneOutsideInstruction), 0)
        );
    }
}

/// RFC §13.5, §9.5.1 Reentrance, gap G-070: an entry a locked hook makes
/// into its own room is adopted, nested in the pass that holds the lock,
/// and commits ahead of the event being processed.
mod an_entry_adopted_under_the_lock {
    use super::*;
    use pretty_assertions::assert_eq;

    fn notified() -> (Sim, Seen, Shown) {
        let mut sim = room();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let shown = Rc::new(RefCell::new(Vec::new()));
        notifier(&mut sim, &seen, &shown);
        (sim, seen, shown)
    }

    /// Its pass runs within the outer pass's section: no lock of its own,
    /// every hook run on it, its commit ahead of the outer event's, whose
    /// pass reads the room again once its hooks are over.
    #[test]
    fn commits_ahead_of_the_event_being_processed() {
        let (mut sim, _, _) = notified();
        sim.send(hello()).unwrap();
        assert_eq!(
            sim.trace,
            [
                "enter lane room_1",
                "load_context room_1",
                "handle_inbound sms",
                "lock room_1",
                "load_context room_1",
                "hook notifier calls send_event room_1 \"Heads up\"",
                "load_context room_1",
                "hook notifier",
                "load_context room_1",
                "commit #0 ws delivered \"Heads up\"",
                "hook notifier settled",
                "hook notifier returns",
                "load_context room_1",
                "commit #1 sms delivered \"hello\"",
                "release room_1",
                "on_event sms <- evt_2",
                "deliver sms <- evt_2",
                "on_event ws <- evt_1",
                "deliver ws <- evt_1",
                "leave lane room_1",
            ]
        );
    }

    /// RFC §13.4, gap G-023: an adopted entry's key is checked as any
    /// direct injection's: a replay answers the event its key committed,
    /// and nothing more is stored.
    #[test]
    fn with_a_key_already_committed_returns_the_original() {
        let mut sim = room();
        let mut first = notice_event("evt_0", "Heads up");
        first.source.channel_id = channel("ws");
        first.idempotency_key = Some("alert_1".to_owned());
        first.status = EventStatus::Delivered;
        sim.seed(first);
        let seen: Seen = Rc::new(RefCell::new(Vec::new()));
        let kept = Rc::clone(&seen);
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: calling(|event| {
                    if event.source.channel_id.as_str() != "sms" {
                        return None;
                    }
                    let mut replay = from_console("Heads up");
                    replay.idempotency_key = Some("alert_1".to_owned());
                    Some(Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(replay),
                    })
                }),
                answer: Answer::Awaits(Box::new(move |called| {
                    kept.borrow_mut().extend(committed(called));
                    HookOutcome::Returned(HookResult::allow())
                })),
            },
        );
        sim.send(hello()).unwrap();
        assert_eq!(
            (
                seen.borrow()
                    .iter()
                    .map(|event| event.id.to_string())
                    .collect::<Vec<_>>(),
                sim.timeline_lines()
            ),
            (
                vec!["evt_0".to_owned()],
                vec![
                    "#0 ws delivered \"Heads up\"".to_owned(),
                    "#1 sms delivered \"hello\"".to_owned()
                ]
            )
        );
    }

    /// Its caller receives the committed event at once, at its commit.
    #[test]
    fn returns_the_committed_event_to_its_caller() {
        let (mut sim, seen, _) = notified();
        sim.send(hello()).unwrap();
        assert_eq!(
            seen.borrow()
                .iter()
                .map(|event| (event.index, text_of(&event.content)))
                .collect::<Vec<_>>(),
            [(0, "Heads up".to_owned())]
        );
    }

    /// Gap G-070: the hooks saw a provisional index; the event takes the
    /// room's next one once they are over.
    #[test]
    fn leaves_the_outer_event_the_next_index() {
        let (mut sim, _, shown) = notified();
        let result = sim.send(hello()).unwrap();
        assert_eq!(
            (
                shown.borrow().clone(),
                result.event.map(|event| event.index)
            ),
            (vec![0], Some(1))
        );
    }

    /// RFC §10.2: the delivery sets keep index order, the adopted entry's
    /// first.
    #[test]
    fn is_delivered_first_in_index_order() {
        let (mut sim, _, _) = notified();
        sim.send(hello()).unwrap();
        assert_eq!(
            sim.traced("on_event"),
            ["on_event sms <- evt_2", "on_event ws <- evt_1"]
        );
    }

    /// RFC §8.2: the adopted entry's event reports its deliveries and its
    /// end, as an event that entered through direct injection.
    #[test]
    fn reports_its_deliveries() {
        let (mut sim, _, _) = notified();
        sim.send(hello()).unwrap();
        let processed: Vec<String> = sim
            .framework_events()
            .into_iter()
            .filter_map(|event| {
                if let FrameworkEvent::EventProcessed { event_id, .. } = event {
                    return Some(event_id.to_string());
                }
                None
            })
            .collect();
        assert_eq!(processed, ["evt_2", "evt_1"]);
    }

    /// The hook answered without waiting: its answer is held back until the
    /// entry nested under it is settled, so the pass never goes on, nor
    /// lets the lock go, while the entry works under it.
    #[test]
    fn holds_back_the_answer_of_a_hook_that_did_not_wait() {
        let mut sim = room();
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| send_event("Heads up"))
                }),
                answer: Answer::AtOnce(Box::new(|| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        let section: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("hook notifier calls"))
            .take_while(|line| !line.starts_with("release"))
            .collect();
        assert_eq!(
            section,
            [
                "hook notifier calls send_event room_1 \"Heads up\"",
                "hook notifier returns",
                "load_context room_1",
                "hook notifier",
                "load_context room_1",
                "commit #0 ws delivered \"Heads up\"",
                "hook notifier settled",
                "load_context room_1",
                "commit #1 sms delivered \"hello\"",
            ]
        );
    }

    /// RFC §13.6: the entry is committed, so it is delivered even when the
    /// event whose hook made it passes its deadline afterwards: the cascade
    /// stops, the call is refused, and the committed event's delivery set
    /// still runs before the lane is left.
    #[test]
    fn is_delivered_when_the_outer_event_passes_its_deadline() {
        let (mut sim, _, _) = notified();
        sim.config.process_timeout = Some(std::time::Duration::from_secs(10));
        // The step 2 read, the gate's, the entry's gate and its read after
        // its hook, then the outer pass's read after its hooks.
        sim.nth_durations
            .push(("load_context", 5, std::time::Duration::from_secs(11)));
        let result = sim.send(hello()).unwrap();
        let after: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.ends_with("deadline passed"))
            .collect();
        assert_eq!(
            (result.reason, sim.timeline_lines(), after),
            (
                Some("process_timeout".to_owned()),
                vec!["#0 ws delivered \"Heads up\"".to_owned()],
                vec![
                    "load_context room_1 deadline passed",
                    "release room_1",
                    "on_event sms <- evt_2",
                    "deliver sms <- evt_2",
                    "leave lane room_1",
                ]
            )
        );
    }

    /// Two entries made by one hook at once run one after the other, each
    /// within the pass's section, never as two sections within one tenure:
    /// each takes its own index.
    #[test]
    fn two_made_at_once_commit_one_after_the_other() {
        let mut sim = room();
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: Box::new(|event| {
                    if event.source.channel_id.as_str() != "sms" {
                        return Vec::new();
                    }
                    vec![send_event("First"), send_event("Second")]
                }),
                answer: Answer::AtOnce(Box::new(|| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        let section: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("hook notifier calls"))
            .take_while(|line| !line.starts_with("release"))
            .collect();
        assert_eq!(
            section,
            [
                "hook notifier calls send_event room_1 \"First\"",
                "hook notifier calls send_event room_1 \"Second\"",
                "hook notifier returns",
                "load_context room_1",
                "hook notifier",
                "load_context room_1",
                "commit #0 ws delivered \"First\"",
                "hook notifier settled",
                "load_context room_1",
                "hook notifier",
                "load_context room_1",
                "commit #1 ws delivered \"Second\"",
                "hook notifier settled",
                "load_context room_1",
                "commit #2 sms delivered \"hello\"",
            ]
        );
    }

    /// RFC §8.3: an entry draws on its caller's reentry budget, so a hook
    /// that injects on every event stops at the reentry cap; the entry past
    /// it is stored blocked, running no hook.
    #[test]
    fn draws_on_its_caller_s_reentry_budget() {
        let mut sim = room();
        sim.config.max_chain_depth = 1;
        sim.reentrant_hook(
            "echoer",
            true,
            Reentry {
                entries: calling(|_| Some(send_event("again"))),
                answer: Answer::Awaits(Box::new(|_| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        let lines = sim.timeline_lines();
        assert_eq!(
            (lines.len(), lines.first().cloned(), lines.last().cloned()),
            (
                12,
                Some("#0 ws blocked:reentry_loop_cap \"again\"".to_owned()),
                Some("#11 sms delivered \"hello\"".to_owned())
            )
        );
    }

    /// Gap G-070: a locked hook's change to the room stands for the event
    /// it decides on: a muted source blocks it (RFC §7.5 rule 2).
    #[test]
    fn a_hook_that_mutes_the_source_blocks_the_event() {
        let mut sim = room();
        sim.before_broadcast(
            "moderator",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.hooks_change = Some(HookChange::Mute(channel("sms")));
        let result = sim.send(hello()).unwrap();
        assert_eq!(
            (result.reason, sim.timeline_lines()),
            (
                Some("source_muted".to_owned()),
                vec!["#0 sms blocked:source_muted \"hello\"".to_owned()]
            )
        );
    }

    /// Gap G-070: a room a locked hook closed refuses the event it decides
    /// on, as step 6 would have (RFC §5.1): nothing is stored, the hook's
    /// injected events included.
    #[test]
    fn a_hook_that_closes_the_room_refuses_the_event() {
        let mut sim = room();
        sim.before_broadcast(
            "closer",
            Box::new(|_| {
                HookOutcome::Returned(HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Goodbye"),
                    vec![channel("sms")],
                )))
            }),
        );
        sim.hooks_change = Some(HookChange::CloseRoom);
        let result = sim.send(hello()).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.reason,
                sim.timeline.len(),
                sim.framework_events()
            ),
            (
                true,
                Some("room_closed".to_owned()),
                0,
                vec![&FrameworkEvent::RoomRefusedEvent {
                    room_id: room_id(),
                    event_id: "evt_1".parse().unwrap(),
                    status: RoomStatus::Closed,
                    operation: RefusedOperation::Inbound,
                    event_type: EventType::Message,
                }]
            )
        );
    }

    /// The answers a locked hook received from the injection `entry` it
    /// makes on the customer's events.
    fn answered(sim: &mut Sim, entry: Entry) -> Rc<RefCell<Vec<Result<(), InboundError>>>> {
        let answers = Rc::new(RefCell::new(Vec::new()));
        let kept = Rc::clone(&answers);
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: calling(move |event| {
                    (event.source.channel_id.as_str() == "sms").then(|| entry.clone())
                }),
                answer: Answer::Awaits(Box::new(move |called| {
                    kept.borrow_mut().push(called.clone().map(|_| ()));
                    HookOutcome::Returned(HookResult::allow())
                })),
            },
        );
        answers
    }

    /// Gap G-069: adopted, an injection from a channel the room does not
    /// bind is refused all the same, nothing written.
    #[test]
    fn from_an_unbound_channel_is_refused_to_its_caller() {
        let mut sim = room();
        sim.unbound_transport("api", roomkit_core::model::ChannelType::Webhook);
        let entry = Entry::Direct {
            room_id: room_id(),
            organization_id: None,
            event: Box::new(DirectEvent::new(
                channel("api"),
                EventContent::text("Shipped"),
            )),
        };
        let answers = answered(&mut sim, entry);
        sim.send(hello()).unwrap();
        assert_eq!(
            (answers.borrow().clone(), sim.timeline_lines()),
            (
                vec![Err(InboundError::Refused {
                    reason: "source_unbound".to_owned()
                })],
                vec!["#0 sms delivered \"hello\"".to_owned()]
            )
        );
    }

    /// Gap G-069: past its caller's reentry budget, an injection the room
    /// refuses is refused all the same: no capped record is stored.
    #[test]
    fn past_the_reentry_budget_from_an_unbound_channel_stores_nothing() {
        let mut sim = room();
        sim.config.max_chain_depth = 0;
        sim.unbound_transport("api", roomkit_core::model::ChannelType::Webhook);
        let entry = Entry::Direct {
            room_id: room_id(),
            organization_id: None,
            event: Box::new(DirectEvent::new(
                channel("api"),
                EventContent::text("Shipped"),
            )),
        };
        let answers = answered(&mut sim, entry);
        sim.send(hello()).unwrap();
        assert_eq!(
            (answers.borrow().clone(), sim.timeline_lines()),
            (
                vec![Err(InboundError::Refused {
                    reason: "source_unbound".to_owned()
                })],
                vec!["#0 sms delivered \"hello\"".to_owned()]
            )
        );
    }

    /// RFC §17.2, gap G-069: past its caller's reentry budget, an injection
    /// for another organization than the room's stores no capped record
    /// there.
    #[test]
    fn past_the_reentry_budget_for_another_organization_stores_nothing() {
        let mut sim = room();
        sim.config.max_chain_depth = 0;
        sim.room.organization_id = Some("bank".to_owned());
        let entry = Entry::Direct {
            room_id: room_id(),
            organization_id: Some("clinic".to_owned()),
            event: Box::new(from_console("Heads up")),
        };
        let answers = answered(&mut sim, entry);
        sim.send(hello()).unwrap();
        assert_eq!(
            (answers.borrow().clone(), sim.timeline_lines()),
            (
                vec![Err(InboundError::OtherOrganization(room_id()))],
                vec!["#0 sms delivered \"hello\"".to_owned()]
            )
        );
    }

    /// Past its caller's reentry budget, an injection the room admits is
    /// stored capped, as before (RFC §8.3).
    #[test]
    fn past_the_reentry_budget_for_the_room_s_organization_is_stored_capped() {
        let mut sim = room();
        sim.config.max_chain_depth = 0;
        sim.room.organization_id = Some("bank".to_owned());
        let entry = Entry::Direct {
            room_id: room_id(),
            organization_id: Some("bank".to_owned()),
            event: Box::new(from_console("Heads up")),
        };
        answered(&mut sim, entry);
        sim.send(hello()).unwrap();
        assert_eq!(
            sim.timeline_lines().first().cloned(),
            Some("#0 ws blocked:reentry_loop_cap \"Heads up\"".to_owned())
        );
    }

    /// RFC §17.2, gap G-069: adopted, an injection for another organization
    /// than the room's is refused, nothing written.
    #[test]
    fn for_another_organization_is_refused_to_its_caller() {
        let mut sim = room();
        sim.room.organization_id = Some("bank".to_owned());
        let entry = Entry::Direct {
            room_id: room_id(),
            organization_id: Some("clinic".to_owned()),
            event: Box::new(from_console("Heads up")),
        };
        let answers = answered(&mut sim, entry);
        sim.send(hello()).unwrap();
        assert_eq!(
            (answers.borrow().clone(), sim.timeline_lines()),
            (
                vec![Err(InboundError::OtherOrganization(room_id()))],
                vec!["#0 sms delivered \"hello\"".to_owned()]
            )
        );
    }
}

/// RFC §13.5, §10.1 step 18, gap G-070: an entry the operation's delivery
/// makes into its own room runs beside the running delivery set, takes the
/// lock for its own commit, and is delivered after the set.
mod an_entry_adopted_from_a_delivery {
    use super::*;
    use pretty_assertions::assert_eq;

    /// An agent whose turn injects "Let me check." into the room, and
    /// answers nothing more; `seen` records what its call returned.
    fn checking(seen: &Seen) -> Sim {
        let mut sim = room();
        sim.echo_agent("ai", "unused");
        let seen = Rc::clone(seen);
        sim.reentrant_channel(
            "ai",
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("ai"),
                            EventContent::text("Let me check."),
                        )),
                    })
                }),
                answer: Answer::Awaits(Box::new(move |called| {
                    seen.borrow_mut().extend(committed(called));
                    Ok(ChannelOutput::default())
                })),
            },
        );
        sim
    }

    #[test]
    fn commits_beside_the_running_set_and_is_delivered_after_it() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut sim = checking(&seen);
        sim.send(hello()).unwrap();
        let cascade: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("on_event"))
            .collect();
        assert_eq!(
            cascade,
            [
                "on_event ws <- evt_1",
                "on_event ai <- evt_1 calls send_event room_1 \"Let me check.\"",
                "deliver ws <- evt_1",
                "lock room_1",
                "load_context room_1",
                "commit #1 ai delivered \"Let me check.\"",
                "release room_1",
                "on_event ai <- evt_1 settled",
                "on_event ai <- evt_1 returns",
                "on_event sms <- evt_2",
                "on_event ws <- evt_2",
                "deliver sms <- evt_2",
                "deliver ws <- evt_2",
                "leave lane room_1",
            ]
        );
    }

    /// RFC §10.1 step 18: the call's result reports its own event's
    /// deliveries, none of the entry's.
    #[test]
    fn leaves_the_call_s_result_its_own_deliveries() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut sim = checking(&seen);
        let result = sim.send(hello()).unwrap();
        assert_eq!(
            result
                .delivery_results
                .keys()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["ws"]
        );
    }

    /// RFC §10.1 step 18: detached completion, at the commit.
    #[test]
    fn returns_at_its_commit() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut sim = checking(&seen);
        sim.send(hello()).unwrap();
        assert_eq!(
            seen.borrow()
                .iter()
                .map(|event| (event.index, event.status))
                .collect::<Vec<_>>(),
            [(1, EventStatus::Delivered)]
        );
    }

    /// A channel's call into its own room (RFC §10.1): the message arrives
    /// in the lane the operation holds, its step 2 and its parse, and its
    /// caller is answered at its commit, with no delivery result.
    #[test]
    fn a_message_arrives_and_returns_at_its_commit_with_no_delivery() {
        let mut sim = room();
        let results: Rc<RefCell<Vec<Called>>> = Rc::new(RefCell::new(Vec::new()));
        let kept = Rc::clone(&results);
        sim.reentrant_channel(
            "ws",
            Reentry {
                entries: calling(|event| {
                    (text_of(&event.content) == "hello").then(|| Entry::Message {
                        room_id: room_id(),
                        organization_id: None,
                        message: Box::new(Sim::message("sms", "follow-up")),
                    })
                }),
                answer: Answer::Awaits(Box::new(move |called| {
                    kept.borrow_mut().push(called.clone());
                    Ok(ChannelOutput::default())
                })),
            },
        );
        sim.send(hello()).unwrap();
        let result = results.borrow().first().cloned().unwrap().unwrap();
        let arrival: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.contains("calls process_inbound"))
            .skip(1)
            .take(6)
            .collect();
        assert_eq!(
            (
                arrival,
                result.event.map(|event| event.index),
                result.delivery_results.len()
            ),
            (
                vec![
                    "load_context room_1",
                    "handle_inbound sms",
                    "lock room_1",
                    "load_context room_1",
                    "commit #1 sms delivered \"follow-up\"",
                    "release room_1",
                ],
                Some(1),
                0
            )
        );
    }
}

/// Gap G-070: two sections of one operation take the room lock one after
/// the other, the second's request held back until the first's release.
mod two_sections_of_one_operation {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The agent injects without waiting and answers: its response's pass
    /// asks for the lock while the entry's pass holds it.
    #[test]
    fn take_the_lock_one_after_the_other() {
        let mut sim = room();
        sim.echo_agent("ai", "unused");
        sim.reentrant_channel(
            "ai",
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("ai"),
                            EventContent::text("Let me check."),
                        )),
                    })
                }),
                answer: Answer::AtOnce(Box::new(|| {
                    Ok(crate::support::respond(
                        "ai",
                        &notice_event("x", "x"),
                        "Found it.",
                    ))
                })),
            },
        );
        sim.send(hello()).unwrap();
        let locked: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .filter(|line| {
                line.starts_with("lock")
                    || line.starts_with("release")
                    || line.starts_with("commit")
            })
            .collect();
        assert_eq!(
            (locked, sim.timeline_lines()),
            (
                vec![
                    "lock room_1",
                    "commit #0 sms delivered \"hello\"",
                    "release room_1",
                    "lock room_1",
                    "commit #1 ai delivered \"Let me check.\"",
                    "release room_1",
                    "lock room_1",
                    "commit #2 ai delivered \"Found it.\"",
                    "release room_1",
                ],
                vec![
                    "#0 sms delivered \"hello\"".to_owned(),
                    "#1 ai delivered \"Let me check.\"".to_owned(),
                    "#2 ai delivered \"Found it.\"".to_owned(),
                ]
            )
        );
    }
}

/// Gap G-070: an entry whose caller did not wait for it is settled all the
/// same, and the call ends only once it is: its delivery set still joins the
/// lane.
mod an_entry_whose_caller_did_not_wait {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn is_delivered_before_the_call_ends() {
        let mut sim = room();
        sim.echo_agent("ai", "unused");
        sim.reentrant_channel(
            "ai",
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("ai"),
                            EventContent::text("Let me check."),
                        )),
                    })
                }),
                answer: Answer::AtOnce(Box::new(|| Ok(ChannelOutput::default()))),
            },
        );
        sim.send(hello()).unwrap();
        assert_eq!(
            (sim.traced("on_event"), sim.trace.last().map(String::as_str)),
            (
                vec![
                    "on_event ws <- evt_1",
                    "on_event ai <- evt_1 calls send_event room_1 \"Let me check.\"",
                    "on_event ai <- evt_1 returns",
                    "on_event ai <- evt_1 settled",
                    "on_event sms <- evt_2",
                    "on_event ws <- evt_2",
                ],
                Some("leave lane room_1")
            )
        );
    }

    /// RFC §10.1 step 15: the side effects of the entry's delivery are
    /// stored with the call's, before it ends.
    #[test]
    fn has_the_side_effects_of_its_delivery_stored() {
        let mut sim = room();
        sim.echo_agent("ai", "unused");
        crm_filing_a_task_on(&mut sim, "Let me check.");
        sim.reentrant_channel(
            "ai",
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("ai"),
                            EventContent::text("Let me check."),
                        )),
                    })
                }),
                answer: Answer::AtOnce(Box::new(|| Ok(ChannelOutput::default()))),
            },
        );
        sim.send(hello()).unwrap();
        assert_eq!(sim.tasks.len(), 1);
    }

    /// A `crm` transport that files a task when it reads `text`.
    fn crm_filing_a_task_on(sim: &mut Sim, text: &'static str) {
        sim.transport_with(
            "crm",
            Box::new(move |event, _| {
                if text_of(&event.content) != text {
                    return Ok(ChannelOutput::default());
                }
                let task = roomkit_core::model::Task::new(
                    "task_1".parse().unwrap(),
                    room_id(),
                    "Follow up",
                    roomkit_core::model::Timestamp::UNIX_EPOCH,
                );
                Ok(ChannelOutput {
                    tasks: vec![task],
                    ..ChannelOutput::default()
                })
            }),
        );
    }
}

/// RFC §9.5.1 Reentrance: an event an off-lock hook injects takes no
/// ticket, runs every hook under the lock, and commits ahead of the event
/// whose check emitted it.
mod an_entry_adopted_from_an_off_lock_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn takes_no_ticket_and_commits_ahead() {
        let mut sim = room();
        sim.reentrant_hook(
            "scanner",
            false,
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| send_event("Scanned"))
                }),
                answer: Answer::Awaits(Box::new(|_| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        let section: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| *line != "take ticket room_1")
            .collect();
        assert_eq!(
            (section, sim.timeline_lines()),
            (
                vec![
                    "take ticket room_1",
                    "hook scanner calls send_event room_1 \"Scanned\"",
                    "lock room_1",
                    "load_context room_1",
                    "hook scanner",
                    "load_context room_1",
                    "commit #0 ws delivered \"Scanned\"",
                    "release room_1",
                    "hook scanner settled",
                    "hook scanner returns",
                    "await ticket room_1",
                    // The entry's delivery set joined the lane at its commit,
                    // ahead of the trigger's pass (RFC §10.2).
                    "on_event sms <- evt_2",
                    "deliver sms <- evt_2",
                    "lock room_1",
                    "load_context room_1",
                    "commit #1 sms delivered \"hello\"",
                    "release room_1",
                    "release ticket room_1",
                    "on_event ws <- evt_1",
                    "deliver ws <- evt_1",
                    "leave lane room_1",
                ],
                vec![
                    "#0 ws delivered \"Scanned\"".to_owned(),
                    "#1 sms delivered \"hello\"".to_owned()
                ]
            )
        );
    }
}

/// RFC §13.6: an entry an off-lock hook made is committed before the
/// event's turn; the event passing its deadline while it waits for its turn
/// leaves the entry delivered.
#[test]
fn an_off_lock_entry_is_delivered_when_its_caller_s_event_passes_its_deadline() {
    let mut sim = room();
    sim.config.process_timeout = Some(std::time::Duration::from_secs(10));
    sim.durations
        .push(("await ticket", std::time::Duration::from_secs(11)));
    sim.reentrant_hook(
        "scanner",
        false,
        Reentry {
            entries: calling(|event| {
                (event.source.channel_id.as_str() == "sms").then(|| send_event("Scanned"))
            }),
            answer: Answer::Awaits(Box::new(|_| HookOutcome::Returned(HookResult::allow()))),
        },
    );
    let result = sim.send(hello()).unwrap();
    assert_eq!(
        (
            result.reason,
            sim.timeline_lines(),
            sim.traced("deliver"),
            sim.trace.last().cloned()
        ),
        (
            Some("process_timeout".to_owned()),
            vec!["#0 ws delivered \"Scanned\"".to_owned()],
            vec!["deliver sms <- evt_2"],
            Some("leave lane room_1".to_owned())
        )
    );
}

/// RFC §13.5, gap G-070: a call no operation adopts runs as an operation of
/// its own, and waits for the room's lane as any call does.
mod a_call_no_operation_adopts {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The customer's "hello", relayed by a locked hook into `room_2`,
    /// which the relayed message creates.
    fn relayed() -> Sim {
        let mut sim = room();
        sim.reentrant_hook(
            "relay",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.room_id == room_id() && text_of(&event.content) == "hello").then(|| {
                        Entry::Message {
                            room_id: RoomId::new("room_2").unwrap(),
                            organization_id: None,
                            message: Box::new(Sim::message("sms", "Relayed")),
                        }
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim
    }

    /// A call into another room holds that room's lane for itself, while
    /// the section whose hook made it waits for it.
    #[test]
    fn into_another_room_runs_within_its_caller_s_section() {
        let mut sim = relayed();
        sim.send(hello()).unwrap();
        let call: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("hook relay calls"))
            .take_while(|line| *line != "hook relay returns")
            .collect();
        assert_eq!(
            (call, sim.other_timeline_lines("room_2")),
            (
                vec![
                    "hook relay calls process_inbound room_2 \"Relayed\"",
                    "enter lane room_2",
                    "load_context room_2",
                    "store room room_2 Active",
                    "lock room_2",
                    "load_context room_2",
                    "store binding sms",
                    "release room_2",
                    "handle_inbound sms",
                    "lock room_2",
                    "load_context room_2",
                    "hook relay",
                    "load_context room_2",
                    "commit #0 sms delivered \"Relayed\"",
                    "release room_2",
                    "leave lane room_2",
                ],
                vec!["#0 sms delivered \"Relayed\"".to_owned()]
            )
        );
    }

    /// A call made from a task outside its caller's context finds the
    /// room's lane held by the call it was made from, waits for it, and
    /// runs once that call is over.
    #[test]
    fn made_outside_its_caller_s_context_waits_for_the_lane() {
        let mut sim = room();
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| send_event("Heads up"))
                }),
                answer: Answer::Apart(Box::new(|| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        let after: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("hook notifier calls"))
            .collect();
        assert_eq!(
            (after, sim.timeline_lines()),
            (
                vec![
                    "hook notifier calls send_event room_1 \"Heads up\"",
                    "hook notifier returns",
                    "load_context room_1",
                    "commit #0 sms delivered \"hello\"",
                    "release room_1",
                    "on_event ws <- evt_1",
                    "deliver ws <- evt_1",
                    "leave lane room_1",
                    "enter lane room_1",
                    "lock room_1",
                    "load_context room_1",
                    "hook notifier",
                    "load_context room_1",
                    "commit #1 ws delivered \"Heads up\"",
                    "release room_1",
                    "on_event sms <- evt_2",
                    "deliver sms <- evt_2",
                    "leave lane room_1",
                ],
                vec![
                    "#0 sms delivered \"hello\"".to_owned(),
                    "#1 ws delivered \"Heads up\"".to_owned()
                ]
            )
        );
    }

    /// Gap G-068: the wait for the lane counts against the call's
    /// `process_timeout`; past it, the lane is not taken and nothing is
    /// written.
    #[test]
    fn made_outside_its_caller_s_context_is_refused_past_its_deadline() {
        let mut sim = room();
        sim.config.process_timeout = Some(std::time::Duration::from_secs(10));
        sim.delivery_duration = std::time::Duration::from_secs(6);
        sim.reentrant_hook(
            "notifier",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.source.channel_id.as_str() == "sms").then(|| send_event("Heads up"))
                }),
                answer: Answer::Apart(Box::new(|| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(hello()).unwrap();
        assert_eq!(
            (
                sim.trace.last().cloned(),
                sim.timeline_lines(),
                sim.framework_event_names().last().cloned()
            ),
            (
                Some("enter lane room_1 deadline passed".to_owned()),
                vec!["#0 sms delivered \"hello\"".to_owned()],
                Some("process_timeout".to_owned())
            )
        );
    }
}

/// The operation adopts a call into the room it holds the lane of, made by
/// an effect still outstanding; any other is handed back.
mod an_adoption {
    use super::*;

    /// An inbound operation polled up to its first request.
    fn started() -> (Inbound, roomkit_core::ops::EffectId) {
        let sim = room();
        let mut inbound = Inbound::new(
            sim.shared_config(),
            InboundTarget::Room(room_id()),
            None,
            hello(),
        );
        let step = inbound.poll(&mut SequentialEnvironment::default());
        let Next::Effects(effects) = step.next else {
            panic!("the operation enters the lane first");
        };
        (inbound, effects.first().unwrap().id)
    }

    #[test]
    fn of_a_call_before_the_lane_is_held_is_refused() {
        let (mut inbound, effect) = started();
        assert!(inbound.adopt(send_event("Hi"), effect).is_err());
    }

    #[test]
    fn of_another_room_s_call_is_refused() {
        let (mut inbound, effect) = started();
        let entry = Entry::Direct {
            room_id: RoomId::new("room_2").unwrap(),
            organization_id: None,
            event: Box::new(from_console("Hi")),
        };
        inbound
            .complete(effect, roomkit_core::ops::Outcome::Entered)
            .unwrap();
        assert!(inbound.adopt(entry, effect).is_err());
    }
}

/// RFC §10.5, gap G-071: a record committed outside the pipeline.
mod a_committed_record {
    use super::*;
    use pretty_assertions::assert_eq;

    fn record(id: &str, room: &str) -> RoomEvent {
        let mut record = notice_event(id, "Snapshot");
        record.room_id = RoomId::new(room).unwrap();
        record.status = EventStatus::Delivered;
        record
    }

    #[test]
    fn takes_the_next_index_and_reaches_no_channel() {
        let mut sim = room();
        sim.seed(notice_event("evt_0", "Hi"));
        let committed = sim
            .commit_record("room_1", record("snap_1", "room_1"), None)
            .unwrap();
        assert_eq!(
            (committed.index, sim.trace.clone(), sim.notices.len()),
            (
                1,
                [
                    "lock room_1",
                    "load_context room_1",
                    "commit #1 system delivered \"Snapshot\"",
                    "release room_1",
                ]
                .map(str::to_owned)
                .to_vec(),
                0
            )
        );
    }

    #[test]
    fn naming_another_room_is_refused_before_anything_is_read() {
        let mut sim = room();
        let refused = sim.commit_record("room_1", record("snap_1", "room_2"), None);
        assert_eq!(
            (refused, sim.trace.len()),
            (
                Err(MutationError::OtherRoom {
                    room_id: room_id(),
                    record: RoomId::new("room_2").unwrap(),
                }),
                0
            )
        );
    }

    /// RFC §17.2: another organization's room is not found.
    #[test]
    fn into_another_organization_s_room_is_not_found() {
        let mut sim = room();
        sim.room.organization_id = Some("bank".to_owned());
        let refused = sim.commit_record("room_1", record("snap_1", "room_1"), Some("clinic"));
        assert_eq!(
            (refused, sim.timeline.len()),
            (Err(MutationError::RoomNotFound(room_id())), 0)
        );
    }

    /// Gap G-063: unlike a change of a stored event, a record grows a
    /// timeline, and a missing room fails it.
    #[test]
    fn into_a_missing_room_is_not_found() {
        let mut sim = room();
        sim.room_exists = false;
        let refused = sim.commit_record("room_1", record("snap_1", "room_1"), None);
        assert_eq!(refused, Err(MutationError::RoomNotFound(room_id())));
    }

    /// RFC §5.1: every point where the timeline grows refuses a closed room.
    #[test]
    fn into_a_closed_room_is_refused() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        let refused = sim.commit_record("room_1", record("snap_1", "room_1"), None);
        assert_eq!(
            (refused, sim.timeline.len()),
            (
                Err(MutationError::Refused {
                    room_id: room_id(),
                    status: RoomStatus::Closed,
                }),
                0
            )
        );
    }

    /// Gap G-071: a record is no activity: a paused room stays paused.
    #[test]
    fn leaves_a_paused_room_paused() {
        let mut sim = room();
        sim.room.status = RoomStatus::Paused;
        sim.commit_record("room_1", record("snap_1", "room_1"), None)
            .unwrap();
        assert_eq!(
            (sim.room.status, sim.room.event_count),
            (RoomStatus::Paused, 1)
        );
    }
}

/// Gap G-036: a record stored `BLOCKED` keeps the visibility step 12 would
/// have given it when its source is bound.
mod a_blocked_record_s_visibility {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn is_narrowed_to_its_source_binding_s() {
        let mut sim = room();
        sim.bindings[0].visibility = Visibility::channels([channel("ws")]).unwrap();
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not now"))),
        );
        sim.send(hello()).unwrap();
        assert_eq!(sim.timeline[0].visibility.to_string(), "ws");
    }

    /// The record of an agent not asked at the chain-depth limit is the
    /// agent's (RFC §8.3).
    #[test]
    fn of_an_agent_not_asked_is_narrowed_to_the_agent_s() {
        let mut sim = room();
        sim.echo_agent("ai", "Too deep");
        sim.bindings[2].visibility = Visibility::Transport;
        let mut deep = hello();
        deep.chain_depth = 4;
        sim.send(deep).unwrap();
        let record = sim
            .timeline
            .iter()
            .find(|event| event.blocked_by.as_deref() == Some("event_chain_depth_limit"))
            .map(|event| event.visibility.to_string());
        assert_eq!(record, Some("transport".to_owned()));
    }

    /// A hook's injected event stored past the budget keeps its own: it is
    /// no channel's.
    #[test]
    fn of_an_injected_event_past_the_budget_is_its_own() {
        let mut sim = room();
        sim.config.max_chain_depth = 1;
        // Narrowed to the binding of the channel it names as its source, it
        // would be seen by no one.
        sim.bindings[1].visibility = Visibility::Intelligence;
        sim.before_broadcast(
            "notify",
            Box::new(|_| {
                let mut notice = notice_event("inj_1", "Heads up");
                notice.source.channel_id = channel("ws");
                HookOutcome::Returned(
                    HookResult::allow()
                        .inject(InjectedEvent::to_channels(notice, vec![channel("sms")])),
                )
            }),
        );
        sim.send(hello()).unwrap();
        let capped = sim
            .timeline
            .iter()
            .find(|event| event.blocked_by.as_deref() == Some("reentry_loop_cap"))
            .map(|event| event.visibility.to_string());
        assert_eq!(capped, Some("sms".to_owned()));
    }

    /// A message from a channel the room detached is stored
    /// `source_unbound` (a direct injection from one is refused, gap G-069).
    #[test]
    fn of_an_unbound_source_is_its_own() {
        let mut sim = room();
        sim.detach("sms");
        let mut message = hello();
        message.visibility = Visibility::Transport;
        sim.send(message).unwrap();
        assert_eq!(sim.timeline[0].visibility, Visibility::Transport);
    }
}

/// Gap G-041: only the framework writes `internal`.
mod the_visibility_internal {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn on_an_inbound_message_is_refused_before_anything_is_read() {
        let mut sim = room();
        let mut message = hello();
        message.visibility = Visibility::Internal;
        let refused = sim.send(message);
        assert_eq!(
            (refused, sim.trace.len()),
            (Err(InboundError::InternalVisibility), 0)
        );
    }

    #[test]
    fn written_by_a_hook_s_modify_is_put_back() {
        let mut sim = room();
        sim.before_broadcast(
            "hider",
            Box::new(|event| {
                let mut hidden = event.clone();
                hidden.visibility = Visibility::Internal;
                HookOutcome::Returned(HookResult::modify(hidden))
            }),
        );
        sim.send(hello()).unwrap();
        assert_eq!(sim.timeline[0].visibility, Visibility::All);
    }

    /// A channel's response that carries `internal` is committed `none`:
    /// stored and shown to no channel, as the channel meant, without the
    /// framework's mark.
    #[test]
    fn on_a_channel_s_response_is_committed_none() {
        let mut sim = room();
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut answer = response("ai", event, "Noted");
                answer.visibility = Visibility::Internal;
                Ok(ChannelOutput::respond(vec![answer]))
            }),
        );
        sim.send(hello()).unwrap();
        let stored: Vec<Visibility> = sim
            .timeline
            .iter()
            .map(|event| event.visibility.clone())
            .collect();
        assert_eq!(
            (stored, sim.traced("deliver").len()),
            (vec![Visibility::All, Visibility::None], 1)
        );
    }
}
