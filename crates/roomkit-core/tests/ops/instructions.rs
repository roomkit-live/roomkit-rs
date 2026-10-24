//! Instructions, effect by effect (RFC §10.1.1, §9.4, §19.3, gaps G-020,
//! G-042, G-065, G-066): the refusals before anything is read, the
//! visibility forced and the address honoured, a pass that decides without
//! committing, a blocked instruction left unstored, the fingerprint on its
//! responses, a standalone turn handed no history, and what hooks may and
//! may not do with one.
//!
//! The room holds the application's WebSocket `app`, which sends the
//! instructions, the customer's SMS-like `sms`, and the agents `ai` and
//! `billing`. Its first event, `evt_1` at index 0, is the customer's "My card
//! was declined"; an instruction is `evt_2`.

use std::cell::RefCell;
use std::rc::Rc;

use roomkit_core::model::{
    ChannelId, ChannelOutput, EventContent, EventId, EventStatus, EventType, HookResult,
    InboundMessage, InjectedEvent, Observation, ObservationId, RoomEvent, RoomStatus, Task, TaskId,
    Visibility,
};
use roomkit_core::ops::{FrameworkEvent, InboundError, InboundResult, RefusedOperation};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookTrigger;
use roomkit_core::rules::transcoding;
use serde_json::json;

use crate::support::{Sim, channel, notice_event, read_only, response, room_id};

/// The instruction's text: `ë` is two bytes and one code point.
const TEXT: &str = "Introduce yourself to Zoë.";
/// The SHA-256 of `TEXT`'s UTF-8 bytes.
const TEXT_SHA256: &str = "e81515470feb4ba095580842fcef627ae3e8e233d73ea7a83780e27642a006ca";

/// The room, with `evt_1` committed and the trace cleared; no agent yet.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("app");
    sim.transport("sms");
    sim.send(Sim::message("sms", "My card was declined"))
        .unwrap();
    forget(&mut sim);
    sim
}

/// The room with the agent `ai`, which answers every event it is asked.
fn with_agent() -> Sim {
    let mut sim = room();
    sim.echo_agent("ai", "Hello, I am the assistant.");
    sim
}

/// The room with the agent `ai`, which reads and says nothing.
fn with_silent_agent() -> Sim {
    let mut sim = room();
    sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
    sim
}

/// Forgets what the driver did so far.
fn forget(sim: &mut Sim) {
    sim.trace.clear();
    sim.journal.clear();
    sim.notices.clear();
}

fn ids(names: &[&str]) -> Vec<ChannelId> {
    names.iter().map(|name| channel(name)).collect()
}

/// The application's instruction `text`, addressed to `to`.
fn instruction_to(to: &[&str], text: &str) -> InboundMessage {
    let mut message = Sim::message("app", text);
    message.event_type = EventType::Instruction;
    message.addressed_to = Some(ids(to));
    message
}

/// The application's instruction `TEXT`, addressed to `ai`.
fn instruction() -> InboundMessage {
    instruction_to(&["ai"], TEXT)
}

/// The events a SYNC `BEFORE_BROADCAST` hook named `name` is handed from
/// now on, each answered with `answer`.
fn hook_seeing(
    sim: &mut Sim,
    name: &str,
    answer: impl Fn(&RoomEvent) -> HookResult + 'static,
) -> Rc<RefCell<Vec<RoomEvent>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let kept = Rc::clone(&seen);
    sim.before_broadcast(
        name,
        Box::new(move |event| {
            kept.borrow_mut().push(event.clone());
            HookOutcome::Returned(answer(event))
        }),
    );
    seen
}

/// The channels `on_event()` was called on for the event `event_id`.
fn asked_about(sim: &Sim, event_id: &str) -> Vec<String> {
    let suffix = format!(" <- {event_id}");
    sim.traced("on_event")
        .into_iter()
        .filter(|line| line.ends_with(&suffix))
        .map(|line| line.trim_start_matches("on_event ").replace(&suffix, ""))
        .collect()
}

/// The journal from the instruction's lock to its first delivery.
fn locked_section(sim: &Sim) -> Vec<&str> {
    sim.journal
        .iter()
        .map(String::as_str)
        .skip_while(|line| !line.starts_with("lock"))
        .take_while(|line| !line.starts_with("on_event"))
        .collect()
}

/// The stored response of `channel_id`.
fn stored_from<'sim>(sim: &'sim Sim, channel_id: &str) -> &'sim RoomEvent {
    sim.timeline
        .iter()
        .find(|event| event.source.channel_id.as_str() == channel_id)
        .unwrap()
}

/// What a refusal before anything is read left: its result, and every
/// effect and notice of the call.
fn refusal(result: &InboundResult, sim: &Sim) -> (bool, Option<String>, bool, usize, usize) {
    (
        result.blocked,
        result.reason.clone(),
        result.event.is_none(),
        sim.trace.len(),
        sim.notices.len(),
    )
}

/// A refusal with `reason`, before any effect or notice.
fn refused_with(reason: &str) -> (bool, Option<String>, bool, usize, usize) {
    (true, Some(reason.to_owned()), true, 0, 0)
}

/// RFC §10.1.1 rules 1, 2 and 7: refused before anything is read.
mod a_refused_instruction {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn unaddressed_is_refused_before_any_effect() {
        let mut sim = with_agent();
        let mut message = instruction();
        message.addressed_to = None;
        let result = sim.send(message).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("instruction_unaddressed")
        );
    }

    #[test]
    fn addressed_to_nobody_is_refused_instruction_unaddressed() {
        let mut sim = with_agent();
        let result = sim.send(instruction_to(&[], TEXT)).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("instruction_unaddressed")
        );
    }

    /// Gap G-065: the address must name a registered intelligence channel.
    #[test]
    fn naming_no_registered_agent_is_refused_instruction_unaddressed() {
        let mut sim = with_agent();
        let result = sim.send(instruction_to(&["sms", "ghost"], TEXT)).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("instruction_unaddressed")
        );
    }

    #[test]
    fn carrying_an_idempotency_key_is_refused_before_any_effect() {
        let mut sim = with_agent();
        let mut message = instruction();
        message.idempotency_key = Some("nudge_1".to_owned());
        let result = sim.send(message).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("instruction_not_idempotent")
        );
    }

    /// Rule 1 comes before rule 2.
    #[test]
    fn unaddressed_and_carrying_a_key_is_refused_instruction_unaddressed() {
        let mut sim = with_agent();
        let mut message = instruction_to(&[], TEXT);
        message.idempotency_key = Some("nudge_1".to_owned());
        let result = sim.send(message).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("instruction_unaddressed")
        );
    }

    /// Raised, since the message would otherwise land as a participant's
    /// line without the isolation it asked for.
    #[test]
    fn standalone_on_a_message_is_raised_before_any_effect() {
        let mut sim = with_agent();
        let mut message = Sim::message("sms", "Bonjour");
        message.standalone = true;
        let result = sim.send(message);
        assert_eq!(
            (result, sim.trace.len()),
            (Err(InboundError::StandaloneOutsideInstruction), 0)
        );
    }

    /// RFC §19.3 rule 2: a registered agent the room does not bind is
    /// named, and asked nowhere.
    #[test]
    fn naming_an_agent_the_room_does_not_bind_goes_on_and_asks_nobody() {
        let mut sim = room();
        sim.unbound_agent("triage", "On it");
        let result = sim.send(instruction_to(&["triage"], TEXT)).unwrap();
        assert_eq!(
            (result.blocked, asked_about(&sim, "evt_2")),
            (false, Vec::<String>::new())
        );
    }

    /// RFC §5.1, §10.1 step 6: the room's status gate holds.
    #[test]
    fn into_a_closed_room_is_refused_room_closed_with_nothing_stored() {
        let mut sim = with_agent();
        sim.room.status = RoomStatus::Closed;
        let result = sim.send(instruction()).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.reason.as_deref(),
                result.event.is_none(),
                sim.timeline.len()
            ),
            (true, Some("room_closed"), true, 1)
        );
    }

    #[test]
    fn into_a_closed_room_emits_room_refused_event_naming_an_instruction() {
        let mut sim = with_agent();
        sim.room.status = RoomStatus::Closed;
        sim.send(instruction()).unwrap();
        assert_eq!(
            sim.framework_events(),
            [&FrameworkEvent::RoomRefusedEvent {
                room_id: room_id(),
                event_id: EventId::new("evt_2").unwrap(),
                status: RoomStatus::Closed,
                operation: RefusedOperation::Inbound,
                event_type: EventType::Instruction,
            }]
        );
    }
}

/// RFC §10.1.1 rules 3 to 5, §19.3.
mod an_accepted_instruction {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Rule 3: whatever the caller asked.
    #[test]
    fn is_visible_to_intelligence_channels_only() {
        let mut sim = with_agent();
        let mut message = instruction();
        message.visibility = Visibility::All;
        let result = sim.send(message).unwrap();
        assert_eq!(
            result.event.map(|event| event.visibility),
            Some(Visibility::Intelligence)
        );
    }

    /// Rule 3, §19.3: no transport is told, and of the agents only the one
    /// it names is asked.
    #[test]
    fn asks_only_the_agent_it_names() {
        let mut sim = with_agent();
        sim.echo_agent("billing", "Billing here.");
        sim.send(instruction()).unwrap();
        assert_eq!(asked_about(&sim, "evt_2"), ["ai"]);
    }

    /// Rule 5: the lock covers the gate, the hooks and the source check;
    /// nothing is written before it is released.
    #[test]
    fn is_not_committed() {
        let mut sim = with_agent();
        sim.send(instruction()).unwrap();
        assert_eq!(
            locked_section(&sim),
            [
                "lock room_1",
                "load_context room_1",
                // Decided, never committed, it takes its place in the lane
                // all the same, behind the room's latest event (rule 5).
                "enter lane room_1 evt_2",
                "release room_1",
                "await lane room_1 evt_2",
            ]
        );
    }

    /// Rule 5: no index consumed, no counter moved, nothing stored.
    #[test]
    fn leaves_the_room_s_counters_and_timeline_as_they_were() {
        let mut sim = with_silent_agent();
        let before = (
            sim.room.event_count,
            sim.room.latest_index,
            sim.room.timers.last_activity_at,
        );
        sim.send(instruction()).unwrap();
        assert_eq!(
            (
                (
                    sim.room.event_count,
                    sim.room.latest_index,
                    sim.room.timers.last_activity_at
                ),
                sim.timeline.len()
            ),
            (before, 1)
        );
    }

    /// Gap G-065: only a committed event resumes a paused room.
    #[test]
    fn leaves_a_paused_room_paused() {
        let mut sim = with_silent_agent();
        sim.room.status = RoomStatus::Paused;
        sim.send(instruction()).unwrap();
        assert_eq!(sim.room.status, RoomStatus::Paused);
    }

    /// Rule 5: the index step 8 showed the hooks is provisional, and the
    /// next event takes it.
    #[test]
    fn shows_its_hooks_the_index_the_next_event_takes() {
        let mut sim = with_agent();
        let seen = hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        sim.send(instruction()).unwrap();
        assert_eq!(
            (
                seen.borrow()[0].index,
                sim.traced("commit"),
                sim.timeline.len()
            ),
            (
                1,
                vec!["commit #1 ai delivered \"Hello, I am the assistant.\""],
                2
            )
        );
    }

    /// Rule 5: the result carries the instruction, which the store does
    /// not hold.
    #[test]
    fn is_the_result_s_uncommitted_event() {
        let mut sim = with_agent();
        let result = sim.send(instruction()).unwrap();
        let event = result.event.unwrap();
        assert_eq!(
            (
                event.id.as_str(),
                event.event_type,
                event.status,
                result.blocked,
                sim.timeline.iter().any(|stored| stored.id == event.id)
            ),
            (
                "evt_2",
                EventType::Instruction,
                EventStatus::Delivered,
                false,
                false
            )
        );
    }

    /// Rule 4: its hooks run on it, with the caller's metadata and the
    /// `standalone` word.
    #[test]
    fn is_handed_to_its_hooks_with_the_caller_s_metadata() {
        let mut sim = with_agent();
        let seen = hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        let mut message = instruction();
        message.metadata.insert("task_id".to_owned(), json!("t_7"));
        sim.send(message).unwrap();
        let handed = seen.borrow()[0].clone();
        assert_eq!(
            (
                handed.event_type,
                serde_json::Value::Object(handed.metadata)
            ),
            (
                EventType::Instruction,
                json!({ "task_id": "t_7", "standalone": false })
            )
        );
    }

    /// RFC §8.2, §10.1 steps 16 and 17: it ends as any event that entered
    /// through the pipeline.
    #[test]
    fn fires_after_broadcast_and_emits_event_processed() {
        let mut sim = with_silent_agent();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.send(instruction()).unwrap();
        let tail: Vec<&str> = sim
            .journal
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("on_event ai"))
            .collect();
        assert_eq!(
            tail,
            [
                "on_event ai <- evt_2",
                "leave lane room_1 evt_2",
                "fire after_broadcast",
                "emit event_processed",
            ]
        );
    }

    /// RFC §8.3: at the limit the agent is not asked; its record answers
    /// the instruction, and records no fingerprint, being no reply.
    #[test]
    fn at_the_chain_depth_limit_stores_the_agent_s_record_without_a_fingerprint() {
        let mut sim = with_agent();
        let mut message = instruction();
        message.chain_depth = 4;
        sim.send(message).unwrap();
        let record = stored_from(&sim, "ai");
        assert_eq!(
            (
                record.blocked_by.as_deref(),
                record.responds_to.as_ref().map(EventId::as_str),
                record.metadata.get("instruction"),
                sim.framework_event_names().last().cloned()
            ),
            (
                Some("event_chain_depth_limit"),
                Some("evt_2"),
                None,
                Some("chain_depth_exceeded".to_owned())
            )
        );
    }

    /// RFC §13.6, gap G-065: once decided, a later failure is reported on
    /// the result, and the instruction stands.
    #[test]
    fn reports_a_failure_after_its_decision_on_the_result() {
        let mut sim = with_agent();
        sim.failing_effect = Some("commit");
        let result = sim.send(instruction()).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.id),
                result.error.map(|error| error.error_type)
            ),
            (
                Some(EventId::new("evt_2").unwrap()),
                Some("StoreUnavailable".to_owned())
            )
        );
    }
}

/// RFC §10.1.1 rule 4: a blocked instruction is not stored.
mod a_blocked_instruction {
    use super::*;
    use pretty_assertions::assert_eq;

    fn blocking(sim: &mut Sim) {
        hook_seeing(sim, "guard", |event| {
            if event.event_type == EventType::Instruction {
                HookResult::block("no nudges tonight")
                    .with_task(Task::new(
                        TaskId::new("task_1").unwrap(),
                        room_id(),
                        "nudge the customer tomorrow",
                        event.created_at,
                    ))
                    .with_observation(Observation::new(
                        ObservationId::new("obs_1").unwrap(),
                        room_id(),
                        channel("app"),
                        "a nudge came in after hours",
                        event.created_at,
                    ))
                    .inject(InjectedEvent::to_channels(
                        notice_event("notice", "A nudge was held back"),
                        vec![channel("sms")],
                    ))
            } else {
                HookResult::allow()
            }
        });
    }

    #[test]
    fn is_not_stored_and_emits_event_blocked() {
        let mut sim = with_agent();
        blocking(&mut sim);
        sim.send(instruction()).unwrap();
        assert_eq!(
            (
                sim.traced("commit"),
                sim.framework_event_names().first().cloned()
            ),
            (
                vec!["commit #1 system delivered \"A nudge was held back\""],
                Some("event_blocked".to_owned())
            )
        );
    }

    #[test]
    fn reports_the_block_with_the_uncommitted_instruction() {
        let mut sim = with_agent();
        blocking(&mut sim);
        let result = sim.send(instruction()).unwrap();
        let event = result.event.unwrap();
        assert_eq!(
            (
                result.blocked,
                result.reason.as_deref(),
                event.status,
                event.blocked_by.as_deref()
            ),
            (
                true,
                Some("no nudges tonight"),
                EventStatus::Blocked,
                Some("guard")
            )
        );
    }

    /// Its hooks' task and observation are stored, and their injected event
    /// committed and delivered.
    #[test]
    fn keeps_its_hooks_side_effects() {
        let mut sim = with_agent();
        blocking(&mut sim);
        sim.send(instruction()).unwrap();
        assert_eq!(
            (
                sim.tasks.len(),
                sim.observations.len(),
                asked_about(&sim, "evt_3")
            ),
            (1, 1, vec!["sms".to_owned()])
        );
    }

    #[test]
    fn asks_no_agent() {
        let mut sim = with_agent();
        blocking(&mut sim);
        sim.send(instruction()).unwrap();
        assert_eq!(asked_about(&sim, "evt_2"), Vec::<String>::new());
    }

    /// The source check's refusal of `sim`'s instruction: its reason, the
    /// timeline's length and the framework events.
    fn source_refusal(sim: &mut Sim) -> (Option<String>, usize, Vec<String>) {
        let result = sim.send(instruction()).unwrap();
        (
            result.reason,
            sim.timeline.len(),
            sim.framework_event_names(),
        )
    }

    #[test]
    fn from_a_read_only_source_is_blocked_source_read_only_unstored() {
        let mut sim = with_agent();
        read_only(
            sim.bindings
                .iter_mut()
                .find(|binding| binding.channel_id.as_str() == "app")
                .unwrap(),
        );
        assert_eq!(
            source_refusal(&mut sim),
            (
                Some("source_read_only".to_owned()),
                1,
                vec!["event_blocked".to_owned()]
            )
        );
    }

    /// RFC §7.5 rule 7: a detached channel stays unbound.
    #[test]
    fn from_a_detached_source_is_blocked_source_unbound_unstored() {
        let mut sim = with_agent();
        sim.detach("app");
        assert_eq!(
            source_refusal(&mut sim),
            (
                Some("source_unbound".to_owned()),
                1,
                vec!["event_blocked".to_owned()]
            )
        );
    }

    /// Rule 4, RFC §7.5 rule 2: the source check holds, and its block
    /// stores nothing either.
    #[test]
    fn from_a_muted_source_is_blocked_source_muted_unstored() {
        let mut sim = with_agent();
        sim.bindings
            .iter_mut()
            .find(|binding| binding.channel_id.as_str() == "app")
            .unwrap()
            .muted = true;
        let result = sim.send(instruction()).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.timeline.len(),
                sim.framework_event_names()
            ),
            (Some("source_muted"), 1, vec!["event_blocked".to_owned()])
        );
    }
}

/// RFC §10.1.1 rule 6: the response records the instruction's
/// fingerprint, never its text.
mod the_response {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn records_the_instruction_s_sha256_and_length() {
        let mut sim = with_agent();
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "ai").metadata.get("instruction"),
            Some(&json!({ "sha256": TEXT_SHA256, "length": 26 }))
        );
    }

    #[test]
    fn is_stored_without_the_instruction_s_text() {
        let mut sim = with_agent();
        sim.send(instruction()).unwrap();
        let stored = serde_json::to_string(&sim.timeline).unwrap();
        assert!(!stored.contains("Zoë"));
    }

    /// The pipeline's record replaces the channel's, which held the text.
    #[test]
    fn replaces_what_its_channel_wrote_under_the_key() {
        let mut sim = room();
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut answer = response("ai", event, "Hello, I am the assistant.");
                answer
                    .metadata
                    .insert("instruction".to_owned(), json!(TEXT));
                Ok(ChannelOutput::respond(vec![answer]))
            }),
        );
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "ai").metadata.get("instruction"),
            Some(&json!({ "sha256": TEXT_SHA256, "length": 26 }))
        );
    }

    /// RFC §8.5: it answers the instruction, which its hooks saw, one hop
    /// deeper.
    #[test]
    fn answers_the_instruction_one_hop_deeper() {
        let mut sim = with_agent();
        sim.send(instruction()).unwrap();
        let stored = stored_from(&sim, "ai");
        assert_eq!(
            (
                stored.responds_to.as_ref().map(EventId::as_str),
                stored.chain_depth
            ),
            (Some("evt_2"), 1)
        );
    }

    /// The fingerprint is of the instruction the agent was handed.
    #[test]
    fn records_the_text_its_hooks_left() {
        let mut sim = with_agent();
        hook_seeing(&mut sim, "rewrite", |event| {
            let mut rewritten = event.clone();
            rewritten.content = EventContent::text("abc");
            HookResult::modify(rewritten)
        });
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "ai").metadata.get("instruction"),
            Some(&json!({
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "length": 3,
            }))
        );
    }

    /// Gap G-065: a hook on the response neither removes the fingerprint nor
    /// writes the text in its place.
    #[test]
    fn keeps_its_fingerprint_through_its_own_hooks() {
        let mut sim = with_agent();
        hook_seeing(&mut sim, "copier", |event| {
            let mut copied = event.clone();
            if event.source.channel_id.as_str() == "ai" {
                copied
                    .metadata
                    .insert("instruction".to_owned(), json!(TEXT));
            }
            HookResult::modify(copied)
        });
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "ai").metadata.get("instruction"),
            Some(&json!({ "sha256": TEXT_SHA256, "length": 26 }))
        );
    }

    /// Gap G-065: a hook that removes the fingerprint has its removal
    /// undone.
    #[test]
    fn keeps_its_fingerprint_when_a_hook_removes_it() {
        let mut sim = with_agent();
        hook_seeing(&mut sim, "redactor", |event| {
            let mut redacted = event.clone();
            redacted.metadata.remove("instruction");
            HookResult::modify(redacted)
        });
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "ai").metadata.get("instruction"),
            Some(&json!({ "sha256": TEXT_SHA256, "length": 26 }))
        );
    }

    /// A response stored refused past the cascade's reentry budget (RFC
    /// §8.3) records it too.
    #[test]
    fn refused_past_the_reentry_budget_records_the_fingerprint() {
        let mut sim = room();
        sim.config.max_chain_depth = 2;
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let answers = (0..21)
                    .map(|_| response("ai", event, "Hello, I am the assistant."))
                    .collect();
                Ok(ChannelOutput::respond(answers))
            }),
        );
        sim.send(instruction()).unwrap();
        let last = sim.timeline.last().unwrap();
        assert_eq!(
            (last.blocked_by.as_deref(), last.metadata.get("instruction")),
            (
                Some("reentry_loop_cap"),
                Some(&json!({ "sha256": TEXT_SHA256, "length": 26 }))
            )
        );
    }

    /// An answer to the response answers no instruction.
    #[test]
    fn s_own_answer_records_no_fingerprint() {
        let mut sim = with_agent();
        sim.echo_agent("billing", "Billing here.");
        sim.send(instruction()).unwrap();
        assert_eq!(
            stored_from(&sim, "billing").metadata.get("instruction"),
            None
        );
    }
}

/// Gap G-065: `metadata.instruction` is the pipeline's, written on a
/// response to an instruction only. What a channel, a parser or a hook
/// wrote under the key on any other event is not stored, and a hook that
/// removes it is not undone.
mod an_instruction_key_the_pipeline_did_not_write {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A message whose parser kept the key it came with.
    fn carrying_the_key() -> InboundMessage {
        let mut message = Sim::message("sms", "Bonjour");
        message
            .metadata
            .insert("instruction".to_owned(), json!(TEXT));
        message
    }

    /// The agent `ai`, answering every event with a response that writes
    /// the key itself.
    fn agent_writing_the_key(sim: &mut Sim) {
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut answer = response("ai", event, "Hello, I am the assistant.");
                answer
                    .metadata
                    .insert("instruction".to_owned(), json!(TEXT));
                Ok(ChannelOutput::respond(vec![answer]))
            }),
        );
    }

    /// A hook that removes the key from every event it is handed.
    fn redacting(sim: &mut Sim) {
        hook_seeing(sim, "redactor", |event| {
            let mut redacted = event.clone();
            redacted.metadata.remove("instruction");
            HookResult::modify(redacted)
        });
    }

    #[test]
    fn on_an_inbound_message_is_not_stored() {
        let mut sim = room();
        sim.send(carrying_the_key()).unwrap();
        let stored = sim.timeline.last().unwrap();
        assert_eq!(stored.metadata.get("instruction"), None);
    }

    #[test]
    fn on_an_inbound_message_is_not_handed_to_its_hooks() {
        let mut sim = room();
        let seen = hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        sim.send(carrying_the_key()).unwrap();
        assert_eq!(seen.borrow()[0].metadata.get("instruction"), None);
    }

    #[test]
    fn removed_by_a_hook_from_an_inbound_message_stays_removed() {
        let mut sim = room();
        redacting(&mut sim);
        sim.send(carrying_the_key()).unwrap();
        let stored = sim.timeline.last().unwrap();
        assert_eq!(stored.metadata.get("instruction"), None);
    }

    #[test]
    fn on_a_response_to_a_message_is_not_stored() {
        let mut sim = room();
        agent_writing_the_key(&mut sim);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(stored_from(&sim, "ai").metadata.get("instruction"), None);
    }

    #[test]
    fn removed_by_a_hook_from_a_response_to_a_message_stays_removed() {
        let mut sim = room();
        agent_writing_the_key(&mut sim);
        redacting(&mut sim);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(stored_from(&sim, "ai").metadata.get("instruction"), None);
    }

    #[test]
    fn on_an_event_a_hook_injects_is_not_stored() {
        let mut sim = room();
        hook_seeing(&mut sim, "notifier", |event| {
            if event.source.channel_id.as_str() != "sms" {
                return HookResult::allow();
            }
            let mut notice = notice_event("notice", "An advisor will call");
            notice
                .metadata
                .insert("instruction".to_owned(), json!(TEXT));
            HookResult::allow().inject(InjectedEvent::to_channels(notice, vec![channel("sms")]))
        });
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            stored_from(&sim, "system").metadata.get("instruction"),
            None
        );
    }

    /// The caller's metadata rides an instruction (RFC §10.1.1 rule 4),
    /// never under the key its responses record.
    #[test]
    fn on_an_instruction_from_its_caller_is_not_handed_to_its_hooks() {
        let mut sim = with_agent();
        let seen = hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        let mut message = instruction();
        message
            .metadata
            .insert("instruction".to_owned(), json!(TEXT));
        sim.send(message).unwrap();
        assert_eq!(seen.borrow()[0].metadata.get("instruction"), None);
    }
}

/// RFC §10.1.1 rule 7: a standalone instruction's turn reads nothing of
/// the room.
mod a_standalone_instruction {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC §10.1.1 rule 7: the pipeline empties nothing it hands the
    /// targets; reading none of the room is the turn's duty.
    #[test]
    fn s_targets_are_handed_the_room_s_context() {
        let mut sim = with_agent();
        let mut message = instruction_to(&["ai"], "Summarize the call.");
        message.standalone = true;
        sim.send(message).unwrap();
        assert_eq!(
            (asked_about(&sim, "evt_2"), sim.history_seen_by("ai")),
            (
                vec!["ai".to_owned()],
                vec!["My card was declined".to_owned()]
            )
        );
    }

    /// Gap G-065: only the turn's context: hooks read the room whole.
    #[test]
    fn s_hooks_read_the_room_whole() {
        let mut sim = with_silent_agent();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        let mut message = instruction_to(&["ai"], "Summarize the call.");
        message.standalone = true;
        sim.send(message).unwrap();
        let context = sim.async_contexts().last().copied().unwrap();
        let history: Vec<String> = context
            .recent_events
            .iter()
            .map(|event| transcoding::text_of(&event.content))
            .collect();
        assert_eq!(history, ["My card was declined"]);
    }

    /// Rule 6: an instruction that does not stand alone follows the
    /// rebuilt history.
    #[test]
    fn is_the_only_kind_whose_turn_reads_no_history() {
        let mut sim = with_agent();
        sim.send(instruction()).unwrap();
        assert_eq!(sim.history_seen_by("ai"), ["My card was declined"]);
    }

    #[test]
    fn carries_its_standalone_word_to_its_hooks() {
        let mut sim = with_agent();
        let seen = hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        let mut message = instruction();
        message.standalone = true;
        sim.send(message).unwrap();
        assert_eq!(
            seen.borrow()[0].metadata.get("standalone"),
            Some(&json!(true))
        );
    }

    /// Rule 4: the typed field's word, whatever the metadata says.
    #[test]
    fn is_not_made_by_the_caller_s_metadata() {
        let mut sim = with_agent();
        let mut message = instruction();
        message
            .metadata
            .insert("standalone".to_owned(), json!(true));
        sim.send(message).unwrap();
        assert_eq!(sim.history_seen_by("ai"), ["My card was declined"]);
    }
}

/// RFC §9.4, gap G-020: a hook's modify changes what an instruction says,
/// never whom it reaches or what its turn reads.
mod a_hook_s_modify {
    use super::*;
    use pretty_assertions::assert_eq;

    fn modifying(sim: &mut Sim, change: impl Fn(&mut RoomEvent) + 'static) {
        hook_seeing(sim, "rewrite", move |event| {
            let mut modified = event.clone();
            change(&mut modified);
            HookResult::modify(modified)
        });
    }

    #[test]
    fn cannot_widen_its_visibility() {
        let mut sim = with_agent();
        modifying(&mut sim, |event| event.visibility = Visibility::All);
        let result = sim.send(instruction()).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.visibility),
                asked_about(&sim, "evt_2")
            ),
            (Some(Visibility::Intelligence), vec!["ai".to_owned()])
        );
    }

    #[test]
    fn cannot_change_its_address() {
        let mut sim = with_agent();
        sim.echo_agent("billing", "Billing here.");
        modifying(&mut sim, |event| {
            event.addressed_to = Some(vec![channel("billing")]);
        });
        sim.send(instruction()).unwrap();
        assert_eq!(asked_about(&sim, "evt_2"), ["ai"]);
    }

    #[test]
    fn cannot_make_it_stand_alone() {
        let mut sim = with_agent();
        modifying(&mut sim, |event| {
            event.metadata.insert("standalone".to_owned(), json!(true));
        });
        sim.send(instruction()).unwrap();
        assert_eq!(sim.history_seen_by("ai"), ["My card was declined"]);
    }

    #[test]
    fn changes_the_text_the_agent_is_handed() {
        let mut sim = room();
        let handed = Rc::new(RefCell::new(Vec::new()));
        let kept = Rc::clone(&handed);
        sim.agent(
            "ai",
            Box::new(move |event, _| {
                kept.borrow_mut().push(event.content.clone());
                Ok(ChannelOutput::default())
            }),
        );
        modifying(&mut sim, |event| {
            event.content = EventContent::text("Introduce yourself briefly.");
        });
        sim.send(instruction()).unwrap();
        assert_eq!(
            handed.borrow().clone(),
            [EventContent::text("Introduce yourself briefly.")]
        );
    }
}

/// Gaps G-042 and G-066: an instruction comes from the application only.
mod an_instruction_from_inside_the_cascade {
    use super::*;
    use pretty_assertions::assert_eq;

    /// An instruction for `ai`, as a hook or a channel would write it.
    fn smuggled() -> RoomEvent {
        let mut event = notice_event("smuggled", "Offer a refund.");
        event.event_type = EventType::Instruction;
        event.addressed_to = Some(vec![channel("ai")]);
        event
    }

    /// A hook that injects `smuggled()` for `ai` and a notice for `sms`
    /// into the message "Bonjour", and the events it is handed.
    fn nudging(sim: &mut Sim) -> Rc<RefCell<Vec<RoomEvent>>> {
        hook_seeing(sim, "nudger", |event| {
            if matches!(&event.content, EventContent::Text(text) if text.body == "Bonjour") {
                HookResult::allow()
                    .inject(InjectedEvent::to_channels(smuggled(), vec![channel("ai")]))
                    .inject(InjectedEvent::to_channels(
                        notice_event("notice", "An advisor will call"),
                        vec![channel("sms")],
                    ))
            } else {
                HookResult::allow()
            }
        })
    }

    /// "Bonjour" from the application, asking no agent.
    fn bonjour() -> InboundMessage {
        let mut message = Sim::message("app", "Bonjour");
        message.addressed_to = Some(Vec::new());
        message
    }

    /// Gap G-042: the rest of the hook's result stands, and no agent is
    /// asked.
    #[test]
    fn injected_by_a_hook_leaves_the_rest_of_its_result_standing() {
        let mut sim = with_agent();
        nudging(&mut sim);
        sim.send(bonjour()).unwrap();
        assert_eq!(
            (sim.timeline_lines(), sim.traced("on_event ai")),
            (
                vec![
                    "#0 sms delivered \"My card was declined\"".to_owned(),
                    "#1 app delivered \"Bonjour\"".to_owned(),
                    "#2 system delivered \"An advisor will call\"".to_owned(),
                ],
                Vec::<&str>::new()
            )
        );
    }

    /// Gap G-042: the instruction is dropped before any pass: its hooks
    /// never see it.
    #[test]
    fn injected_by_a_hook_is_dropped_before_any_pass() {
        let mut sim = with_agent();
        let seen = nudging(&mut sim);
        sim.send(bonjour()).unwrap();
        let types: Vec<EventType> = seen.borrow().iter().map(|event| event.event_type).collect();
        assert_eq!(types, [EventType::Message, EventType::Message]);
    }

    /// Gap G-066: an agent's answer typed as an instruction neither lands
    /// in the timeline nor asks the agent it names.
    #[test]
    fn answered_by_an_agent_is_dropped() {
        let mut sim = room();
        sim.echo_agent("billing", "Billing here.");
        sim.agent(
            "ai",
            Box::new(|_, _| {
                let mut answer = smuggled();
                answer.addressed_to = Some(vec![channel("billing")]);
                Ok(ChannelOutput::respond(vec![answer]))
            }),
        );
        sim.send(instruction()).unwrap();
        assert_eq!(
            (sim.timeline.len(), sim.traced("on_event billing")),
            (1, Vec::<&str>::new())
        );
    }

    /// Gap G-066: the channel's other responses re-enter.
    #[test]
    fn answered_by_an_agent_beside_a_reply_leaves_the_reply_standing() {
        let mut sim = room();
        sim.agent(
            "ai",
            Box::new(|event, _| {
                Ok(ChannelOutput::respond(vec![
                    smuggled(),
                    response("ai", event, "Hello, I am the assistant."),
                ]))
            }),
        );
        sim.send(instruction()).unwrap();
        assert_eq!(
            sim.timeline_lines().last().map(String::as_str),
            Some("#1 ai delivered \"Hello, I am the assistant.\"")
        );
    }
}

/// RFC §10.1.1 rule 5, §10.1 step 9a, gap G-065: an instruction after its
/// hooks, when a hook ran under the lock.
mod after_a_locked_hook {
    use super::*;
    use crate::support::{Answer, HookChange, Reentry, calling};
    use pretty_assertions::assert_eq;
    use roomkit_core::model::DirectEvent;
    use roomkit_core::ops::Entry;

    /// A locked hook whose code commits an event ahead of the instruction,
    /// then answers `answer`.
    fn committing_ahead(sim: &mut Sim, answer: fn() -> HookResult) {
        sim.reentrant_hook(
            "notes",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.event_type == EventType::Instruction).then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("app"),
                            EventContent::text("Noted"),
                        )),
                    })
                }),
                answer: Answer::Awaits(Box::new(move |_| HookOutcome::Returned(answer()))),
            },
        );
    }

    /// Allowed or blocked, it carries the index the room's next event
    /// takes once its hooks are over, step 9a's.
    #[test]
    fn carries_the_room_s_next_index_once_its_hooks_are_over() {
        let blocking: fn() -> HookResult = || HookResult::block("not now");
        let mut indexes = Vec::new();
        for answer in [HookResult::allow, blocking] {
            let mut sim = with_silent_agent();
            committing_ahead(&mut sim, answer);
            let result = sim.send(instruction()).unwrap();
            indexes.push((
                result.blocked,
                result.event.map(|event| event.index),
                sim.room.event_count,
            ));
        }
        assert_eq!(indexes, [(false, Some(2), 2), (true, Some(2), 2)]);
    }

    /// A room a locked hook closed refuses it as step 6 does: no event.
    #[test]
    fn closed_by_the_hook_is_refused_room_closed_with_no_event() {
        let mut sim = with_silent_agent();
        hook_seeing(&mut sim, "audit", |_| HookResult::allow());
        sim.hooks_change = Some(HookChange::CloseRoom);
        let result = sim.send(instruction()).unwrap();
        assert_eq!(
            (result.blocked, result.reason, result.event.is_none()),
            (true, Some("room_closed".to_owned()), true)
        );
    }
}
