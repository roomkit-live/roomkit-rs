//! The inbound pipeline, effect by effect (RFC §10.1, §10.2).

use std::sync::Arc;

use roomkit_core::model::{
    ChannelOutput, DeliveryStatus, ErrorRecord, EventId, EventStatus, EventType, HookResult,
    InjectedEvent, Observation, ObservationId, RoomEvent, RoomStatus, Task, TaskId, Timestamp,
    Visibility,
};
use roomkit_core::ops::{
    FrameworkEvent, Inbound, InboundError, InboundTarget, KitConfig, Next, Operation, Outcome,
    ProtocolError, RefusedOperation, SequentialEnvironment,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};

use crate::support::{
    HookChange, Sim, channel, notice_event, read_only, respond, response, room_id,
};

/// A room with an SMS sender, a WebSocket advisor and an echo agent.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.transport("ws");
    sim.echo_agent("ai", "Hello back");
    sim
}

mod an_allowed_message {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn runs_the_pipeline_in_rfc_order() {
        let mut sim = room();
        sim.before_broadcast(
            "pii_scan",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );

        sim.send(Sim::message("sms", "Bonjour")).unwrap();

        assert_eq!(
            sim.trace,
            [
                "load_context room_1",
                "handle_inbound sms",
                "lock room_1",
                "load_context room_1",
                "hook pii_scan",
                "load_context room_1",
                "commit #0 sms delivered \"Bonjour\"",
                "enter lane room_1 evt_1",
                "release room_1",
                "await lane room_1 evt_1",
                "on_event ws <- evt_1",
                "on_event ai <- evt_1",
                "deliver ws <- evt_1",
                "leave lane room_1 evt_1",
                "lock room_1",
                "load_context room_1",
                "hook pii_scan",
                "load_context room_1",
                "commit #1 ai delivered \"Hello back\"",
                "enter lane room_1 evt_2",
                "release room_1",
                "await lane room_1 evt_2",
                "on_event sms <- evt_2",
                "on_event ws <- evt_2",
                "deliver sms <- evt_2",
                "deliver ws <- evt_2",
                "leave lane room_1 evt_2",
            ]
        );
    }

    #[test]
    fn returns_the_committed_event() {
        let mut sim = room();
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let event = result.event.unwrap();
        assert_eq!((event.index, event.id.as_str()), (0, "evt_1"));
    }

    #[test]
    fn reports_the_delivery_to_each_transport() {
        let mut sim = room();
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let delivered: Vec<(&str, DeliveryStatus)> = result
            .delivery_results
            .values()
            .map(|delivery| (delivery.channel_id.as_str(), delivery.status))
            .collect();
        assert_eq!(delivered, [("ws", DeliveryStatus::Sent)]);
    }

    #[test]
    fn gives_the_response_a_chain_depth_one_deeper_answering_the_trigger() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let reply = &sim.timeline[1];
        assert_eq!(
            (
                reply.chain_depth,
                reply.responds_to.as_ref().map(EventId::as_str)
            ),
            (1, Some("evt_1"))
        );
    }

    #[test]
    fn gives_a_response_its_own_id_whatever_the_channel_wrote() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut reply = response("ai", event, "Hello back");
                reply.id = event.id.clone();
                Ok(ChannelOutput::respond(vec![reply]))
            }),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let ids: Vec<&str> = sim.timeline.iter().map(|event| event.id.as_str()).collect();
        assert_eq!(ids, ["evt_1", "evt_2"]);
    }

    /// RFC §8.5, gap G-035: a response claiming the sender's source keeps
    /// none of its fields, its provider message id included.
    #[test]
    fn commits_a_response_claiming_the_sender_s_source_with_a_source_of_its_own() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut reply = response("ai", event, "Hello back");
                reply.source = event.source.clone();
                reply.source.provider_message_id = Some("SM42".to_owned());
                Ok(ChannelOutput::respond(vec![reply]))
            }),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let source = &sim.timeline[1].source;
        assert_eq!(
            (
                source.channel_id.as_str(),
                source.participant_id.is_some(),
                source.provider_message_id.as_deref()
            ),
            ("ai", false, None)
        );
    }

    #[test]
    fn moves_the_room_s_counters_with_each_commit() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!((sim.room.event_count, sim.room.latest_index), (2, Some(1)));
    }

    #[test]
    fn emits_delivery_succeeded_then_event_processed_for_the_trigger_only() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.framework_events(),
            [
                &FrameworkEvent::DeliverySucceeded {
                    room_id: room_id(),
                    event_id: "evt_1".parse().unwrap(),
                    channel_id: channel("ws"),
                },
                &FrameworkEvent::EventProcessed {
                    room_id: room_id(),
                    event_id: "evt_1".parse().unwrap(),
                },
            ]
        );
    }

    #[test]
    fn fires_after_broadcast_once_per_delivery_set() {
        let mut sim = room();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.async_triggers(),
            [HookTrigger::AfterBroadcast, HookTrigger::AfterBroadcast]
        );
    }

    /// A room whose hook files a task for every event it reads.
    fn triaging_room() -> Sim {
        let mut sim = room();
        sim.before_broadcast(
            "triage",
            Box::new(|_| {
                let task = Task::new(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Call back",
                    Timestamp::UNIX_EPOCH,
                );
                HookOutcome::Returned(HookResult::allow().with_task(task))
            }),
        );
        sim
    }

    #[test]
    fn persists_the_side_effects_its_hooks_produced() {
        let mut sim = triaging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.traced("persist"), ["persist 2 tasks, 0 observations"]);
    }

    #[test]
    fn fires_on_task_created_for_each_task_once_they_are_stored() {
        let mut sim = triaging_room();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.observer("follow_up", HookTrigger::OnTaskCreated);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.async_triggers(),
            [
                HookTrigger::AfterBroadcast,
                HookTrigger::AfterBroadcast,
                HookTrigger::OnTaskCreated,
                HookTrigger::OnTaskCreated,
            ]
        );
    }
}

/// RFC Appendix B.2: a hook blocks, injects and observes.
mod a_blocked_message {
    use super::*;
    use pretty_assertions::assert_eq;

    fn scanning_room() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.before_broadcast(
            "sensitivity_scanner",
            Box::new(|event| {
                let text = roomkit_core::rules::transcoding::text_of(&event.content);
                if !text.contains("123-456-789") {
                    return HookOutcome::Returned(HookResult::allow());
                }
                let observation = Observation::new(
                    ObservationId::new("obs_sin").unwrap(),
                    room_id(),
                    channel("sms"),
                    "SIN sent by SMS",
                    Timestamp::UNIX_EPOCH,
                );
                HookOutcome::Returned(
                    HookResult::block("SIN detected")
                        .inject(InjectedEvent::to_channels(
                            notice_event("inj_1", "Message blocked. Do not send SIN by SMS."),
                            vec![channel("sms")],
                        ))
                        .inject(InjectedEvent::to_channels(
                            notice_event("inj_2", "Client attempted to send SIN. Blocked."),
                            vec![channel("ws")],
                        ))
                        .with_observation(observation),
                )
            }),
        );
        sim
    }

    /// `Effect::locked`: the blocked record and the events injected after
    /// it are each committed on behalf of the section that holds the room's
    /// lock (RFC §13.5).
    #[test]
    fn commits_every_event_under_the_room_lock() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(sim.commits_under_lock(), (3, 3));
    }

    #[test]
    fn stores_the_message_blocked_and_the_injected_events_after_it() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 sms blocked:sensitivity_scanner \"Mon NAS est 123-456-789\"",
                "#1 system delivered \"Message blocked. Do not send SIN by SMS.\"",
                "#2 system delivered \"Client attempted to send SIN. Blocked.\"",
            ]
        );
    }

    #[test]
    fn delivers_each_injected_event_to_its_target_only() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(
            sim.traced("deliver"),
            ["deliver sms <- evt_2", "deliver ws <- evt_3"]
        );
    }

    #[test]
    fn never_delivers_the_blocked_message() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(sim.traced("on_event ws <- evt_1"), Vec::<&str>::new());
    }

    #[test]
    fn reports_the_block_with_the_hook_s_reason() {
        let mut sim = scanning_room();
        let result = sim
            .send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(
            (result.blocked, result.reason.as_deref()),
            (true, Some("SIN detected"))
        );
    }

    #[test]
    fn returns_the_stored_record() {
        let mut sim = scanning_room();
        let result = sim
            .send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        let record = result.event.unwrap();
        assert_eq!(
            (
                record.id.as_str(),
                record.index,
                record.blocked_by.as_deref()
            ),
            ("evt_1", 0, Some("sensitivity_scanner"))
        );
    }

    #[test]
    fn persists_the_hook_s_observation() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        let categories: Vec<&str> = sim
            .observations
            .iter()
            .map(|observation| observation.content.as_str())
            .collect();
        assert_eq!(categories, ["SIN sent by SMS"]);
    }

    #[test]
    fn emits_event_blocked_naming_the_hook() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(
            sim.framework_events().first(),
            Some(&&FrameworkEvent::EventBlocked {
                room_id: room_id(),
                event_id: "evt_1".parse().unwrap(),
                channel_id: channel("sms"),
                reason: Some("SIN detected".to_owned()),
                blocked_by: "sensitivity_scanner".to_owned(),
            })
        );
    }

    #[test]
    fn stores_the_payload_an_earlier_hook_redacted() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.before_broadcast(
            "redact",
            Box::new(|event| {
                let mut redacted = event.clone();
                redacted.content = roomkit_core::model::EventContent::text("[redacted]");
                HookOutcome::Returned(HookResult::modify(redacted))
            }),
        );
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not allowed"))),
        );
        sim.send(Sim::message("sms", "secret")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            ["#0 sms blocked:policy \"[redacted]\""]
        );
    }
}

/// RFC §9.4, gap G-021: an injected event reaches its transport targets
/// under the rules any delivery follows (§7.5 rule 5), and asks no agent.
mod an_injected_event {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A room whose hook blocks the message "trigger" and injects `injected`
    /// for `target`.
    fn injecting_room(injected: RoomEvent, target: &str) -> Sim {
        let mut sim = room();
        let target = channel(target);
        sim.before_broadcast(
            "gate",
            Box::new(move |event| {
                if roomkit_core::rules::transcoding::text_of(&event.content) != "trigger" {
                    return HookOutcome::Returned(HookResult::allow());
                }
                HookOutcome::Returned(HookResult::block("gated").inject(
                    InjectedEvent::to_channels(injected.clone(), vec![target.clone()]),
                ))
            }),
        );
        sim
    }

    fn trigger_at_depth(depth: u32) -> roomkit_core::model::InboundMessage {
        let mut message = Sim::message("sms", "trigger");
        message.chain_depth = depth;
        message
    }

    #[test]
    fn does_not_ask_a_named_agent() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "ai");
        sim.send(trigger_at_depth(0)).unwrap();
        assert_eq!(sim.traced("on_event ai"), Vec::<&str>::new());
    }

    #[test]
    fn is_stored_for_the_named_agent_to_read() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "ai");
        sim.send(trigger_at_depth(0)).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 sms blocked:gate \"trigger\"",
                "#1 system delivered \"Heads up\"",
            ]
        );
    }

    #[test]
    fn is_in_the_named_agent_s_history_at_its_next_turn() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "ai");
        sim.send(trigger_at_depth(0)).unwrap();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert!(sim.history_seen_by("ai").contains(&"Heads up".to_owned()));
    }

    #[test]
    fn stores_no_depth_record_for_a_named_agent_at_the_limit() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "ai");
        sim.send(trigger_at_depth(4)).unwrap();
        assert_eq!(sim.timeline_lines().len(), 2);
    }

    #[test]
    fn reaches_a_named_transport() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "ws");
        sim.send(trigger_at_depth(0)).unwrap();
        assert_eq!(sim.traced("on_event ws").len(), 1);
    }

    #[test]
    fn never_reaches_its_own_source() {
        let mut from_sms = notice_event("inj_1", "Echo");
        from_sms.source = roomkit_core::model::EventSource::new(
            channel("sms"),
            roomkit_core::model::ChannelType::Sms,
            roomkit_core::model::EventDirection::Outbound,
        );
        let mut sim = injecting_room(from_sms, "sms");
        sim.send(trigger_at_depth(0)).unwrap();
        assert_eq!(sim.traced("on_event sms"), Vec::<&str>::new());
    }

    #[test]
    fn collects_the_tasks_its_transport_target_files() {
        let mut sim = injecting_room(notice_event("inj_1", "Heads up"), "desk");
        sim.transport_with(
            "desk",
            Box::new(|_, _| {
                let task = Task::new(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Read it",
                    Timestamp::UNIX_EPOCH,
                );
                Ok(ChannelOutput {
                    tasks: vec![task],
                    ..ChannelOutput::default()
                })
            }),
        );
        sim.send(trigger_at_depth(0)).unwrap();
        assert_eq!(sim.traced("persist"), ["persist 1 tasks, 0 observations"]);
    }
}

/// RFC §9.4: an injected event is committed after its cause however the
/// cause ends, as a pass of its own.
mod an_injected_event_s_pass {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A room whose hook lets every event through, injecting a notice for
    /// the advisor with the message "hello" (with every event when
    /// `always`).
    fn notifying_room(always: bool) -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.before_broadcast(
            "notify",
            Box::new(move |event| {
                let text = roomkit_core::rules::transcoding::text_of(&event.content);
                if !always && text != "hello" {
                    return HookOutcome::Returned(HookResult::allow());
                }
                HookOutcome::Returned(HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Heads up"),
                    vec![channel("ws")],
                )))
            }),
        );
        sim
    }

    #[test]
    fn follows_an_allowed_message() {
        let mut sim = notifying_room(false);
        sim.send(Sim::message("sms", "hello")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 sms delivered \"hello\"",
                "#1 system delivered \"Heads up\"",
            ]
        );
    }

    #[test]
    fn follows_a_message_its_source_may_not_send() {
        let mut sim = notifying_room(false);
        sim.bindings[0].muted = true;
        sim.send(Sim::message("sms", "hello")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 sms blocked:source_muted \"hello\"",
                "#1 system delivered \"Heads up\"",
            ]
        );
    }

    #[test]
    fn may_be_blocked_by_a_hook_of_its_own_chain() {
        let mut sim = notifying_room(false);
        sim.before_broadcast(
            "quiet",
            Box::new(|event| {
                if roomkit_core::rules::transcoding::text_of(&event.content) == "Heads up" {
                    return HookOutcome::Returned(HookResult::block("no notices"));
                }
                HookOutcome::Returned(HookResult::allow())
            }),
        );
        sim.send(Sim::message("sms", "hello")).unwrap();
        assert_eq!(
            sim.timeline_lines()[1],
            "#1 system blocked:quiet \"Heads up\""
        );
    }

    /// Gap G-043: the framework's own re-injection, refused by a room that
    /// closed during its cascade, here by a hook running on it under the
    /// lock (gap G-070).
    #[test]
    fn refused_by_a_room_closed_meanwhile_emits_room_refused_event_as_reentry() {
        let mut sim = notifying_room(false);
        sim.hooks_change = Some(HookChange::CloseRoomOn("Heads up"));
        sim.send(Sim::message("sms", "hello")).unwrap();
        assert_eq!(
            (sim.timeline_lines(), sim.framework_events().last().copied()),
            (
                vec!["#0 sms delivered \"hello\"".to_owned()],
                Some(&FrameworkEvent::RoomRefusedEvent {
                    room_id: room_id(),
                    event_id: "evt_2".parse().unwrap(),
                    status: RoomStatus::Closed,
                    operation: RefusedOperation::Reentry,
                    event_type: EventType::Message,
                })
            )
        );
    }

    #[test]
    fn past_the_reentry_budget_is_stored_capped_without_its_hooks() {
        let mut sim = notifying_room(true);
        sim.config.max_chain_depth = 1;
        sim.send(Sim::message("sms", "hello")).unwrap();
        let lines = sim.timeline_lines();
        assert_eq!(
            lines.last().map(String::as_str),
            Some("#11 system blocked:reentry_loop_cap \"Heads up\"")
        );
    }
}

/// RFC §9.4, gap G-031: an injected event's visibility names the targets
/// the room binds, which the hooks of its own pass may narrow, never widen.
mod an_injected_event_s_visibility {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use pretty_assertions::assert_eq;
    use roomkit_core::rules::transcoding::text_of;

    /// A room binding `sms`, `ws` and `desk`, whose hook lets "hello"
    /// through, injecting the notice "Heads up" for `targets`.
    fn notifying(targets: &'static [&'static str]) -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.transport("desk");
        sim.before_broadcast(
            "notify",
            Box::new(move |event| {
                if text_of(&event.content) != "hello" {
                    return HookOutcome::Returned(HookResult::allow());
                }
                let targets = targets.iter().map(|id| channel(id)).collect();
                HookOutcome::Returned(HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Heads up"),
                    targets,
                )))
            }),
        );
        sim
    }

    /// A hook of the notice's own pass that rewrites its visibility to
    /// `visibility`.
    fn rewriting(sim: &mut Sim, visibility: &'static str) {
        sim.before_broadcast(
            "rewrite",
            Box::new(move |event| {
                if text_of(&event.content) != "Heads up" {
                    return HookOutcome::Returned(HookResult::allow());
                }
                let mut modified = event.clone();
                modified.visibility = visibility.parse().unwrap();
                HookOutcome::Returned(HookResult::modify(modified))
            }),
        );
    }

    /// The stored notice's visibility.
    fn stored(sim: &Sim) -> String {
        sim.timeline
            .iter()
            .find(|event| text_of(&event.content) == "Heads up")
            .map(|event| event.visibility.to_string())
            .unwrap_or_default()
    }

    /// The channels the notice was delivered to.
    fn reached(sim: &Sim) -> Vec<String> {
        let Some(notice) = sim
            .timeline
            .iter()
            .find(|event| text_of(&event.content) == "Heads up")
        else {
            return Vec::new();
        };
        let suffix = format!(" <- {}", notice.id);
        sim.traced("deliver ")
            .into_iter()
            .filter_map(|line| line.strip_suffix(suffix.as_str()))
            .filter_map(|line| line.strip_prefix("deliver "))
            .map(str::to_owned)
            .collect()
    }

    fn hello(sim: &mut Sim) {
        sim.send(Sim::message("sms", "hello")).unwrap();
    }

    #[test]
    fn names_the_targets_the_room_binds_only() {
        let mut sim = notifying(&["ws", "billing"]);
        hello(&mut sim);
        assert_eq!(stored(&sim), "ws");
    }

    #[test]
    fn is_none_when_the_room_binds_no_target() {
        let mut sim = notifying(&["billing"]);
        hello(&mut sim);
        assert_eq!(stored(&sim), "none");
    }

    #[test]
    fn is_given_to_its_hooks_naming_the_targets_the_room_binds() {
        let mut sim = notifying(&["ws", "billing"]);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seeing = Rc::clone(&seen);
        sim.before_broadcast(
            "watch",
            Box::new(move |event| {
                if text_of(&event.content) == "Heads up" {
                    seeing.borrow_mut().push(event.visibility.to_string());
                }
                HookOutcome::Returned(HookResult::allow())
            }),
        );
        hello(&mut sim);
        assert_eq!(*seen.borrow(), ["ws"]);
    }

    #[test]
    fn may_be_narrowed_by_a_hook_of_its_own_pass() {
        let mut sim = notifying(&["ws", "desk"]);
        rewriting(&mut sim, "desk");
        hello(&mut sim);
        assert_eq!(
            (stored(&sim), reached(&sim)),
            ("desk".to_owned(), vec!["desk".to_owned()])
        );
    }

    #[test]
    fn is_never_widened_by_a_hook_of_its_own_pass() {
        let mut sim = notifying(&["ws"]);
        rewriting(&mut sim, "all");
        hello(&mut sim);
        assert_eq!(
            (stored(&sim), reached(&sim)),
            ("ws".to_owned(), vec!["ws".to_owned()])
        );
    }

    /// A target a hook of its pass detaches is no longer bound: the
    /// visibility drops it, and the event does not reach it.
    #[test]
    fn loses_a_target_a_hook_of_its_own_pass_detaches() {
        let mut sim = notifying(&["ws", "desk"]);
        sim.hooks_change = Some(HookChange::DetachOn("Heads up", channel("ws")));
        hello(&mut sim);
        assert_eq!(
            (stored(&sim), reached(&sim)),
            ("desk".to_owned(), vec!["desk".to_owned()])
        );
    }

    /// Gap G-036: a blocked injected event is stored as its commit would
    /// have stored it.
    #[test]
    fn blocked_by_a_hook_of_its_own_pass_is_never_widened() {
        let mut sim = notifying(&["ws"]);
        rewriting(&mut sim, "all");
        sim.before_broadcast(
            "quiet",
            Box::new(|event| {
                if text_of(&event.content) == "Heads up" {
                    return HookOutcome::Returned(HookResult::block("no notices"));
                }
                HookOutcome::Returned(HookResult::allow())
            }),
        );
        hello(&mut sim);
        assert_eq!(stored(&sim), "ws");
    }

    /// Gap G-023: past its cascade's reentry budget it is stored with no
    /// pass, naming the targets the room binds all the same.
    #[test]
    fn past_the_reentry_budget_names_the_targets_the_room_binds_only() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.config.max_chain_depth = 1;
        sim.before_broadcast(
            "notify",
            Box::new(|_| {
                HookOutcome::Returned(HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Heads up"),
                    vec![channel("ws"), channel("billing")],
                )))
            }),
        );
        hello(&mut sim);
        let capped = sim
            .timeline
            .last()
            .map(|event| (event.blocked_by.clone(), event.visibility.to_string()));
        assert_eq!(
            capped,
            Some((Some("reentry_loop_cap".to_owned()), "ws".to_owned()))
        );
    }
}

/// RFC §6.1, §10.1 step 15: the room metadata updates a channel returns.
mod metadata_updates {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    /// A room whose agent tags the room on every event it reads, trying the
    /// implementation's revocation key too.
    fn tagging_room() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "crm",
            Box::new(|_, _| {
                let mut output = ChannelOutput::default();
                output
                    .metadata_updates
                    .insert("tier".to_owned(), json!("gold"));
                output
                    .metadata_updates
                    .insert("roomkit:detached".to_owned(), json!({ "sms": "now" }));
                Ok(output)
            }),
        );
        sim.room.metadata.insert("crm_id".to_owned(), json!("c_1"));
        sim
    }

    #[test]
    fn are_written_on_the_room_once_the_cascade_is_over() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.trace[sim.trace.len() - 2..].to_vec(),
                sim.room.metadata.get("tier")
            ),
            (
                vec![
                    "leave lane room_1 evt_1".to_owned(),
                    "update room room_1 Active".to_owned()
                ],
                Some(&json!("gold"))
            )
        );
    }

    #[test]
    fn keep_the_keys_they_do_not_name() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.room.metadata.get("crm_id"), Some(&json!("c_1")));
    }

    /// RFC §10.6.5: a channel never writes a key the implementation keeps.
    #[test]
    fn never_write_a_reserved_key() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.room.metadata.get("roomkit:detached"), None);
    }

    #[test]
    fn leave_the_counters_as_the_commits_set_them() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!((sim.room.event_count, sim.room.latest_index), (1, Some(0)));
    }
}

/// RFC §7.5 rule 2: a source that cannot write commits its event blocked.
mod a_source_that_cannot_write {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn a_muted_source_commits_its_message_blocked_after_the_hooks() {
        let mut sim = room();
        sim.bindings[0].muted = true;
        sim.before_broadcast(
            "audit",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.trace[4..],
            [
                "hook audit",
                "load_context room_1",
                "commit #0 sms blocked:source_muted \"Bonjour\"",
                "release room_1",
            ]
        );
    }

    /// Gap G-015: since step 2 attaches an unbound channel, a source with no
    /// binding is one the room's integrator detached.
    #[test]
    fn a_detached_source_commits_its_message_blocked_as_unbound() {
        let mut sim = room();
        sim.detach("sms");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (result.reason.as_deref(), sim.timeline_lines()),
            (
                Some("source_unbound"),
                vec!["#0 sms blocked:source_unbound \"Bonjour\"".to_owned()]
            )
        );
    }

    #[test]
    fn a_detached_channel_parses_its_message_seeing_no_event() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Earlier")).unwrap();
        sim.detach("sms");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.parse_contexts.last().unwrap().recent_events, []);
    }

    #[test]
    fn a_read_only_source_commits_its_message_blocked() {
        let mut sim = room();
        read_only(&mut sim.bindings[0]);
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(result.reason.as_deref(), Some("source_read_only"));
    }

    #[test]
    fn a_hook_that_mutes_the_source_blocks_the_event_it_reads() {
        let mut sim = room();
        sim.before_broadcast(
            "moderator",
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        // The context read under the lock still shows the source unmuted;
        // the hook mutes it. The source check reads the bindings as the hook
        // left them (RFC §7.5 rule 2).
        sim.hooks_change = Some(HookChange::Mute(channel("sms")));
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(result.reason.as_deref(), Some("source_muted"));
    }

    #[test]
    fn a_muted_agent_is_still_asked_and_its_response_stored_blocked() {
        let mut sim = room();
        sim.bindings[2].muted = true;
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.timeline_lines()[1],
            "#1 ai blocked:source_muted \"Hello back\""
        );
    }
}

/// Gap G-025: a lock release that fails changes nothing the locked section
/// decided.
mod a_failed_lock_release {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn keeps_the_committed_event_and_reports_the_failure() {
        let mut sim = room();
        sim.release_fails = true;
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.id.to_string()),
                result.error.map(|error| error.error_type)
            ),
            (Some("evt_1".to_owned()), Some("LockLost".to_owned()))
        );
    }

    #[test]
    fn changes_nothing_the_cascade_does() {
        let mut failing = room();
        failing.release_fails = true;
        failing.send(Sim::message("sms", "Bonjour")).unwrap();
        let mut releasing = room();
        releasing.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(failing.trace, releasing.trace);
    }

    #[test]
    fn keeps_a_blocked_message_blocked() {
        let mut sim = room();
        sim.release_fails = true;
        sim.bindings[0].muted = true;
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (result.blocked, result.reason.as_deref()),
            (true, Some("source_muted"))
        );
    }
}

/// RFC §5.1: a closed room refuses every new event.
mod a_closed_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn stores_nothing_and_releases_its_lock() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.trace[2..].to_vec(), sim.timeline.len()),
            (
                vec![
                    "lock room_1".to_owned(),
                    "load_context room_1".to_owned(),
                    "release room_1".to_owned(),
                ],
                0
            )
        );
    }

    #[test]
    fn reports_room_closed() {
        let mut sim = room();
        sim.room.status = RoomStatus::Archived;
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (result.blocked, result.reason.as_deref()),
            (true, Some("room_closed"))
        );
    }

    #[test]
    fn emits_room_refused_event() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.framework_events(),
            [&FrameworkEvent::RoomRefusedEvent {
                room_id: room_id(),
                event_id: "evt_1".parse().unwrap(),
                status: RoomStatus::Closed,
                operation: RefusedOperation::Inbound,
                event_type: EventType::Message,
            }]
        );
    }
}

/// RFC §8.2, gap G-048: a binding whose channel is not registered stays in
/// its room; a transport target is skipped, an intelligence target reported.
mod an_unregistered_channel {
    use super::*;
    use pretty_assertions::assert_eq;
    use roomkit_core::ops::CHANNEL_NOT_REGISTERED;

    /// The delivery failures reported, channel and error.
    fn failures(sim: &Sim) -> Vec<(String, String)> {
        sim.framework_events()
            .into_iter()
            .filter_map(|event| {
                let FrameworkEvent::DeliveryFailed {
                    channel_id, error, ..
                } = event
                else {
                    return None;
                };
                Some((channel_id.to_string(), error.clone()))
            })
            .collect()
    }

    fn unregistered(sim: &mut Sim, id: &str) {
        sim.config.unregister_channel(&channel(id)).unwrap();
    }

    fn gone() -> ErrorRecord {
        ErrorRecord::new(CHANNEL_NOT_REGISTERED, "no channel is registered")
    }

    #[test]
    fn is_not_called_as_a_transport_target() {
        let mut sim = room();
        unregistered(&mut sim, "ws");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("on_event ws"), failures(&sim)),
            (vec![], vec![])
        );
    }

    #[test]
    fn is_reported_as_an_intelligence_target_without_its_turn() {
        let mut sim = room();
        unregistered(&mut sim, "ai");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("on_event ai"), failures(&sim)),
            (
                vec![],
                vec![("ai".to_owned(), CHANNEL_NOT_REGISTERED.to_owned())]
            )
        );
    }

    /// Unregistered once the operation started, as the driver answers.
    #[test]
    fn the_driver_holds_no_more_is_skipped_as_a_transport_target() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport_with("ws", Box::new(|_, _| Err(gone())));
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(failures(&sim), vec![]);
    }

    /// Unregistered between its `on_event()` and its `deliver()`.
    #[test]
    fn the_driver_holds_no_more_at_its_delivery_is_skipped() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.deliver_with("ws", Box::new(|_| Err(gone())));
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(failures(&sim), vec![]);
    }

    /// Unregistered once the operation started: no turn failed, so
    /// `ON_ERROR` does not fire.
    #[test]
    fn the_driver_holds_no_more_is_reported_as_an_intelligence_target() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent("ai", Box::new(|_, _| Err(gone())));
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                failures(&sim),
                sim.async_triggers().contains(&HookTrigger::OnError)
            ),
            (
                vec![("ai".to_owned(), CHANNEL_NOT_REGISTERED.to_owned())],
                false
            )
        );
    }
}

/// RFC §13.4: a redelivered message returns the event it committed first.
mod a_duplicate_message {
    use super::*;
    use pretty_assertions::assert_eq;

    fn keyed(text: &str) -> roomkit_core::model::InboundMessage {
        let mut message = Sim::message("sms", text);
        message.idempotency_key = Some("SM123".to_owned());
        message
    }

    #[test]
    fn returns_the_first_commit_without_reprocessing() {
        let mut sim = room();
        sim.send(keyed("Bonjour")).unwrap();
        let committed = sim.timeline.len();
        let result = sim.send(keyed("Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.id.to_string()),
                sim.timeline.len()
            ),
            (Some("evt_1".to_owned()), committed)
        );
    }

    #[test]
    fn the_same_key_from_another_channel_is_a_new_message() {
        let mut sim = room();
        sim.send(keyed("Bonjour")).unwrap();
        let mut from_ws = Sim::message("ws", "Hello");
        from_ws.idempotency_key = Some("SM123".to_owned());
        let result = sim.send(from_ws).unwrap();
        assert_eq!(
            result
                .event
                .map(|event| event.source.channel_id.to_string()),
            Some("ws".to_owned())
        );
    }

    #[test]
    fn a_response_never_carries_an_idempotency_key() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let mut reply = response("ai", event, "Hello back");
                reply.idempotency_key = Some("SM123".to_owned());
                Ok(ChannelOutput::respond(vec![reply]))
            }),
        );
        sim.send(keyed("Bonjour")).unwrap();
        assert_eq!(sim.timeline[1].idempotency_key, None);
    }

    #[test]
    fn a_replay_of_a_blocked_message_is_reported_blocked() {
        let mut sim = room();
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not allowed"))),
        );
        sim.send(keyed("Bonjour")).unwrap();
        let result = sim.send(keyed("Bonjour")).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.reason.as_deref(),
                result.event.map(|event| event.id.to_string())
            ),
            (true, Some("policy"), Some("evt_1".to_owned()))
        );
    }

    #[test]
    fn checks_the_key_under_the_lock_and_releases_it() {
        let mut sim = room();
        sim.send(keyed("Bonjour")).unwrap();
        sim.trace.clear();
        sim.send(keyed("Bonjour")).unwrap();
        assert_eq!(
            sim.trace,
            [
                "load_context room_1",
                "handle_inbound sms",
                "lock room_1",
                "load_context room_1",
                "find_key SM123",
                "release room_1",
            ]
        );
    }
}

/// RFC §7.5 rule 8: what a channel parsing a message may read.
mod the_receiving_channel {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parses_with_only_the_events_it_may_see() {
        let mut sim = room();
        let mut whispered = notice_event("whisper", "advisor only");
        whispered.status = EventStatus::Delivered;
        whispered.visibility = Visibility::channels([channel("ws")]).unwrap();
        let mut refused = notice_event("refused", "blocked text");
        refused.status = EventStatus::Blocked;
        let mut seen = notice_event("seen", "for everyone");
        seen.status = EventStatus::Delivered;
        sim.seed(whispered);
        sim.seed(refused);
        sim.seed(seen);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let read: Vec<&str> = sim.parse_contexts[0]
            .recent_events
            .iter()
            .map(|event| event.id.as_str())
            .collect();
        assert_eq!(read, ["seen"]);
    }
}

/// RFC §9.3: what an unusable hook outcome decides.
mod an_unusable_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn that_raises_on_a_fail_open_hook_lets_the_message_through() {
        let mut sim = room();
        sim.before_broadcast(
            "broken_logger",
            Box::new(|_| HookOutcome::Raised(ErrorRecord::new("ValueError", "boom"))),
        );
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert!(!result.blocked);
    }

    #[test]
    fn that_raises_emits_hook_error() {
        let mut sim = room();
        sim.before_broadcast(
            "broken_logger",
            Box::new(|_| HookOutcome::Raised(ErrorRecord::new("ValueError", "boom"))),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert!(
            sim.framework_events()
                .contains(&&FrameworkEvent::HookError {
                    room_id: room_id(),
                    event_id: "evt_1".parse().unwrap(),
                    hook: "broken_logger".to_owned(),
                    error: "boom".to_owned(),
                })
        );
    }

    #[test]
    fn that_times_out_emits_hook_timeout_and_hook_error() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.before_broadcast("pii", Box::new(|_| HookOutcome::TimedOut));
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let names = sim.framework_event_names();
        let reported: Vec<&str> = names
            .iter()
            .map(String::as_str)
            .filter(|name| name.starts_with("hook_"))
            .collect();
        assert_eq!(reported, ["hook_timeout", "hook_error"]);
    }

    #[test]
    fn that_times_out_on_a_fail_closed_hook_blocks_naming_the_timeout() {
        let mut sim = room();
        let spec = HookSpec {
            fail_closed: true,
            ..HookSpec::new("pii", HookTrigger::BeforeBroadcast, HookExecution::Sync)
        };
        sim.sync_hook(spec, Box::new(|_| HookOutcome::TimedOut));
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (result.blocked, result.reason.as_deref()),
            (true, Some("hook_timeout:pii"))
        );
    }
}

/// RFC §7.3, §19.3: who receives and who is asked.
mod targets {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn a_response_reaches_only_what_its_source_binding_shows_it_to() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws_advisor");
        sim.echo_agent("assistant", "Suggest the premium plan")
            .visibility = Visibility::channels([channel("ws_advisor")]).unwrap();
        sim.send(Sim::message("sms", "Which plan?")).unwrap();
        assert_eq!(
            sim.traced("deliver"),
            ["deliver ws_advisor <- evt_1", "deliver ws_advisor <- evt_2"]
        );
    }

    #[test]
    fn an_addressed_message_asks_only_the_named_agent() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.echo_agent("billing", "Billing here");
        sim.echo_agent("triage", "Triage here");
        let mut message = Sim::message("sms", "@triage help");
        message.addressed_to = Some(vec![channel("triage")]);
        sim.send(message).unwrap();
        assert_eq!(
            sim.traced("on_event triage <- evt_1").len()
                + sim.traced("on_event billing <- evt_1").len(),
            1
        );
    }

    #[test]
    fn an_addressed_message_does_not_ask_an_agent_it_does_not_name() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.echo_agent("billing", "Billing here");
        sim.echo_agent("triage", "Triage here");
        let mut message = Sim::message("sms", "@triage help");
        message.addressed_to = Some(vec![channel("triage")]);
        sim.send(message).unwrap();
        // Triage's own unaddressed answer then solicits billing (AGENT_CHAIN,
        // RFC §19.3.1); the message itself does not.
        assert_eq!(sim.traced("on_event billing <- evt_1"), Vec::<&str>::new());
    }
}

/// RFC §5.12, gap G-073: a message's `response_visibility` is where the
/// answers to it may go.
mod a_response_visibility {
    use super::*;
    use crate::support::{Answer, OnEvent, Reentry, calling};
    use pretty_assertions::assert_eq;
    use roomkit_core::model::InboundMessage;
    use roomkit_core::ops::Entry;
    use roomkit_core::rules::transcoding::text_of;

    /// A room with the customer's `sms`, the advisor's `ws_advisor` and an
    /// agent `ai` answering with `answer`.
    fn room_with(answer: OnEvent) -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws_advisor");
        sim.agent("ai", answer);
        sim
    }

    /// An agent that answers the customer's `text` with `Re: text`, visible
    /// to `own`, and nothing else.
    fn answering_as(own: &'static str) -> OnEvent {
        Box::new(move |event, _| {
            if event.source.channel_id.as_str() != "sms" {
                return Ok(ChannelOutput::default());
            }
            let text = format!("Re: {}", text_of(&event.content));
            let mut answer = response("ai", event, &text);
            answer.visibility = own.parse().unwrap();
            Ok(ChannelOutput::respond(vec![answer]))
        })
    }

    /// The customer's `text`, whose answers may go to `within`.
    fn answered_within(text: &str, within: &str) -> InboundMessage {
        InboundMessage {
            response_visibility: Some(within.parse().unwrap()),
            ..Sim::message("sms", text)
        }
    }

    /// The stored event that says `text`.
    fn saying<'sim>(sim: &'sim Sim, text: &str) -> Option<&'sim RoomEvent> {
        sim.timeline
            .iter()
            .find(|event| text_of(&event.content) == text)
    }

    /// The stored visibility of the event that says `text`.
    fn stored(sim: &Sim, text: &str) -> Option<String> {
        saying(sim, text).map(|event| event.visibility.to_string())
    }

    /// The transports delivered the event that says `text`.
    fn delivered<'sim>(sim: &'sim Sim, text: &str) -> Vec<&'sim str> {
        let Some(event) = saying(sim, text) else {
            return Vec::new();
        };
        let suffix = format!(" <- {}", event.id);
        sim.traced("deliver ")
            .into_iter()
            .filter_map(|line| line.strip_suffix(suffix.as_str()))
            .map(|line| line.trim_start_matches("deliver "))
            .collect()
    }

    #[test]
    fn narrows_each_response_within_what_its_channel_gave_it() {
        let cases = [
            ("all", "ws_advisor", "ws_advisor", vec!["ws_advisor"]),
            ("ws_advisor", "transport", "ws_advisor", vec!["ws_advisor"]),
            ("sms", "ws_advisor", "none", vec![]),
            ("all", "all", "all", vec!["sms", "ws_advisor"]),
        ];
        for (own, within, visibility, reached) in cases {
            let mut sim = room_with(answering_as(own));
            sim.send(answered_within("Hello", within)).unwrap();
            assert_eq!(
                (stored(&sim, "Re: Hello"), delivered(&sim, "Re: Hello")),
                (Some(visibility.to_owned()), reached),
                "a response visible to {own}, its message's answers within {within}"
            );
        }
    }

    #[test]
    fn leaves_the_message_itself_as_it_asked() {
        let mut sim = room_with(answering_as("all"));
        sim.send(answered_within("Hello", "ws_advisor")).unwrap();
        assert_eq!(stored(&sim, "Hello"), Some("all".to_owned()));
    }

    /// An entry a hook's call adopts answers within its own message's
    /// response visibility, the call's message within its own.
    #[test]
    fn narrows_the_responses_of_an_adopted_message_by_its_own() {
        let mut sim = room_with(answering_as("all"));
        sim.reentrant_hook(
            "follow_up",
            true,
            Reentry {
                entries: calling(|event| {
                    (text_of(&event.content) == "Hello").then(|| Entry::Message {
                        room_id: room_id(),
                        organization_id: None,
                        message: Box::new(answered_within("Follow-up", "ws_advisor")),
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(
            (stored(&sim, "Re: Follow-up"), stored(&sim, "Re: Hello")),
            (Some("ws_advisor".to_owned()), Some("all".to_owned()))
        );
    }

    /// An answer to an answer answers no message: it keeps the visibility
    /// its channel gave it.
    #[test]
    fn leaves_a_response_to_a_response_as_its_channel_gave_it() {
        let mut sim = room_with(answering_as("all"));
        sim.agent(
            "writer",
            Box::new(|event, _| {
                if event.source.channel_id.as_str() != "ai" {
                    return Ok(ChannelOutput::default());
                }
                Ok(respond("writer", event, "Noted"))
            }),
        );
        sim.send(answered_within("Hello", "intelligence")).unwrap();
        assert_eq!(
            (stored(&sim, "Re: Hello"), stored(&sim, "Noted")),
            (Some("intelligence".to_owned()), Some("all".to_owned()))
        );
    }

    /// RFC §9.2: the payload of `ON_ERROR` is visible where the trigger's
    /// answers may go.
    #[test]
    fn is_the_visibility_of_a_failed_turn_s_on_error_payload() {
        let mut sim = room_with(Box::new(|_, _| {
            Err(ErrorRecord::new("ProviderError", "rate limited"))
        }));
        sim.observer("alerts", HookTrigger::OnError);
        sim.send(answered_within("Hello", "ws_advisor")).unwrap();
        let payloads: Vec<String> = sim
            .async_payloads()
            .iter()
            .map(|payload| payload.visibility.to_string())
            .collect();
        assert_eq!(payloads, ["ws_advisor"]);
    }
}

/// RFC Appendix B.4: two agents answer each other until the limit.
mod a_chain_of_agents {
    use super::*;
    use pretty_assertions::assert_eq;

    fn analyst_and_writer() -> Sim {
        let mut sim = Sim::new();
        sim.transport("ws");
        sim.echo_agent("analyst", "Findings");
        sim.echo_agent("writer", "Report");
        sim
    }

    fn from_human() -> roomkit_core::model::InboundMessage {
        let mut message = Sim::message("ws", "Analyse the quarter");
        message.addressed_to = Some(vec![channel("analyst")]);
        message
    }

    #[test]
    fn stops_when_the_next_response_would_reach_the_limit() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 ws delivered \"Analyse the quarter\"",
                "#1 analyst delivered \"Findings\"",
                "#2 writer delivered \"Report\"",
                "#3 analyst delivered \"Findings\"",
                "#4 writer delivered \"Report\"",
                "#5 analyst blocked:event_chain_depth_limit \"\"",
            ]
        );
    }

    /// `Effect::locked`: the record the limit stores is committed on behalf
    /// of the section that holds the room's lock, as the responses are (RFC
    /// §13.5).
    #[test]
    fn commits_the_record_under_the_room_lock() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        assert_eq!(sim.commits_under_lock(), (6, 6));
    }

    #[test]
    fn never_asks_the_agent_at_the_limit() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        let asks = sim.traced("on_event analyst").len();
        assert_eq!(asks, 2);
    }

    #[test]
    fn stores_the_record_at_the_depth_the_response_would_have_had() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        assert_eq!(sim.timeline[5].chain_depth, 5);
    }

    #[test]
    fn emits_chain_depth_exceeded_for_the_record() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        let exceeded = sim
            .framework_events()
            .into_iter()
            .filter(|event| matches!(event, FrameworkEvent::ChainDepthExceeded { .. }))
            .count();
        assert_eq!(exceeded, 1);
    }

    #[test]
    fn records_an_observation_for_the_record() {
        let mut sim = analyst_and_writer();
        sim.send(from_human()).unwrap();
        let categories: Vec<Option<&str>> = sim
            .observations
            .iter()
            .map(|observation| observation.category.as_deref())
            .collect();
        assert_eq!(categories, [Some("event_chain_depth_exceeded")]);
    }

    #[test]
    fn stores_a_transport_s_response_at_the_limit_blocked() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport_with(
            "ws",
            Box::new(|event, _| Ok(respond("ws", event, "auto-reply"))),
        );
        let mut message = Sim::message("sms", "deep");
        message.chain_depth = 4;
        sim.send(message).unwrap();
        assert_eq!(
            sim.timeline_lines()[1],
            "#1 ws blocked:event_chain_depth_limit \"auto-reply\""
        );
    }
}

/// RFC §8.3: the responses one cascade re-enters share a budget.
mod the_reentry_budget {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn stores_a_response_past_the_budget_blocked() {
        let mut sim = Sim::new();
        sim.config.max_chain_depth = 3;
        sim.transport("sms");
        sim.agent(
            "ai",
            Box::new(|event, _| {
                let responses = (0..31)
                    .map(|n| {
                        let mut reply = response("ai", event, "part");
                        reply.id = format!("part_{n}").parse().unwrap();
                        reply
                    })
                    .collect();
                Ok(ChannelOutput::respond(responses))
            }),
        );
        sim.send(Sim::message("sms", "Tell me everything")).unwrap();
        let capped = sim
            .timeline_lines()
            .into_iter()
            .filter(|line| line.contains("reentry_loop_cap"))
            .count();
        assert_eq!(capped, 1);
    }
}

/// RFC §13.6: a failure after the commit leaves the event delivered.
mod a_failure_after_the_commit {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn a_failed_delivery_is_reported_and_the_event_stays_delivered() {
        let mut sim = room();
        sim.deliver_with(
            "ws",
            Box::new(|_| Err(ErrorRecord::new("ConnectionError", "socket closed"))),
        );
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                result
                    .delivery_results
                    .get("ws")
                    .map(|delivery| delivery.status)
            ),
            (Some(EventStatus::Delivered), Some(DeliveryStatus::Failed))
        );
    }

    #[test]
    fn a_failed_delivery_emits_delivery_failed_and_a_partial_failure() {
        let mut sim = room();
        sim.deliver_with(
            "ws",
            Box::new(|_| Err(ErrorRecord::new("ConnectionError", "socket closed"))),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let names = sim.framework_event_names();
        assert_eq!(
            names,
            [
                "delivery_failed",
                "broadcast_partial_failure",
                "event_processed"
            ]
        );
    }

    #[test]
    fn a_failed_turn_fires_on_error_and_reaches_the_result() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "ai",
            Box::new(|_, _| Err(ErrorRecord::new("ProviderError", "rate limited"))),
        );
        sim.observer("alerts", HookTrigger::OnError);
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.async_triggers(),
                result.error.map(|error| error.message)
            ),
            (vec![HookTrigger::OnError], Some("rate limited".to_owned()))
        );
    }
}

/// RFC §10.1 steps 1 and 4: a message that fails before its pass.
mod a_refused_operation {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn an_unregistered_channel_fails_before_any_effect() {
        let mut sim = room();
        let result = sim.send(Sim::message("telegram", "Bonjour"));
        assert_eq!(
            (result, sim.trace.len()),
            (Err(InboundError::UnknownChannel(channel("telegram"))), 0)
        );
    }

    /// Polls `inbound`, which must ask for one effect, and answers it with
    /// `outcome`.
    fn answer(inbound: &mut Inbound, environment: &mut SequentialEnvironment, outcome: Outcome) {
        let Next::Effects(effects) = inbound.poll(environment).next else {
            panic!("expected an effect");
        };
        inbound
            .complete(effects.first().unwrap().id, outcome)
            .unwrap();
    }

    #[test]
    fn a_channel_that_cannot_parse_the_message_fails_it() {
        let sim = room();
        let mut config = KitConfig::new();
        config
            .register_channel(sim.config.channel(&channel("sms")).unwrap().clone())
            .unwrap();
        let target = InboundTarget::Room(room_id());
        let mut inbound = Inbound::new(Arc::new(config), target, None, Sim::message("sms", "Hi"));
        let mut environment = SequentialEnvironment::default();
        let context = Outcome::Context(Box::new(roomkit_core::model::RoomContext {
            room: sim.room.clone(),
            bindings: sim.bindings.clone(),
            participants: Vec::new(),
            recent_events: Vec::new(),
        }));
        let failure = Outcome::Failed(ErrorRecord::new("ParseError", "bad payload"));
        for outcome in [context, failure] {
            answer(&mut inbound, &mut environment, outcome);
        }
        assert!(matches!(
            inbound.poll(&mut environment).next,
            Next::Ready(Err(InboundError::ChannelFailed { .. }))
        ));
    }
}

/// RFC §10.2, §10.1 steps 12 to 14: an event delivered takes its place in
/// the room's delivery lane at its commit, under the lock, and its set runs
/// in that place's turn; a place is left on every path, and a lane request
/// that fails is logged, the event delivered all the same (RFC §13.6).
mod the_delivery_lane {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The lines from the first commit on, the lane's and the lock's.
    fn lane_and_lock(sim: &Sim) -> Vec<&str> {
        sim.trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("commit"))
            .filter(|line| {
                [
                    "commit",
                    "release",
                    "enter lane",
                    "await lane",
                    "leave lane",
                ]
                .iter()
                .any(|kind| line.starts_with(kind))
            })
            .collect()
    }

    #[test]
    fn takes_the_event_s_place_between_its_commit_and_the_release() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            lane_and_lock(&sim),
            [
                "commit #0 sms delivered \"Bonjour\"",
                "enter lane room_1 evt_1",
                "release room_1",
                "await lane room_1 evt_1",
                "leave lane room_1 evt_1",
            ]
        );
    }

    /// A record stored blocked has no delivery set, and no place.
    #[test]
    fn gives_a_record_stored_blocked_no_place() {
        let mut sim = room();
        sim.before_broadcast(
            "policy",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not now"))),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("enter lane"), sim.timeline_lines()),
            (vec![], vec!["#0 sms blocked:policy \"Bonjour\"".to_owned()])
        );
    }

    /// A set whose targets all fail leaves its place, and the next set
    /// takes its turn.
    #[test]
    fn is_left_by_a_set_whose_targets_fail() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport_with(
            "ws",
            Box::new(|_, _| Err(ErrorRecord::new("SocketClosed", "the socket closed"))),
        );
        sim.echo_agent("ai", "Hello back");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.traced("leave lane"),
            ["leave lane room_1 evt_1", "leave lane room_1 evt_2"]
        );
    }

    #[test]
    fn delivers_an_event_whose_place_could_not_be_taken() {
        let mut sim = room();
        sim.failing_effect = Some("enter lane room_1 evt_1");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                result.error,
                sim.traced("deliver"),
                sim.traced("await lane room_1 evt_1")
            ),
            (
                Some(EventStatus::Delivered),
                None,
                vec![
                    "deliver ws <- evt_1",
                    "deliver sms <- evt_2",
                    "deliver ws <- evt_2"
                ],
                vec![]
            )
        );
    }

    #[test]
    fn delivers_an_event_whose_turn_could_not_be_awaited_and_leaves_its_place() {
        let mut sim = room();
        sim.failing_effect = Some("await lane room_1 evt_1");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                sim.traced("deliver ws <- evt_1"),
                sim.traced("leave lane room_1 evt_1")
            ),
            (
                Some(EventStatus::Delivered),
                vec!["deliver ws <- evt_1"],
                vec!["leave lane room_1 evt_1"]
            )
        );
    }
}

/// RFC §10.1 step 18: the call's decision, published once, is what a
/// caller that does not wait for the delivery receives.
mod the_decision {
    use super::*;
    use pretty_assertions::assert_eq;
    use roomkit_core::model::DirectEvent;

    /// What the operation decided, read once: the trace line written last
    /// before it, the room, and the committed event's index or the error.
    fn decided(
        sim: &Sim,
    ) -> (
        Option<String>,
        Option<String>,
        Result<Option<u64>, InboundError>,
    ) {
        let [(line, decided)] = sim.decisions.as_slice() else {
            panic!("one decision expected, got {:?}", sim.decisions);
        };
        let index = decided
            .result
            .clone()
            .map(|result| result.event.map(|event| event.index));
        let room = decided.room_id.as_ref().map(ToString::to_string);
        (line.clone(), room, index)
    }

    /// It comes once the lock is released, before the delivery set runs,
    /// and carries no delivery.
    #[test]
    fn of_a_committed_message_comes_at_its_commit() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let deliveries = sim.decisions.first().map(|(_, decided)| {
            decided
                .result
                .as_ref()
                .map(|result| result.delivery_results.len())
        });
        assert_eq!(
            (decided(&sim), deliveries),
            (
                (
                    Some("release room_1".to_owned()),
                    Some("room_1".to_owned()),
                    Ok(Some(0))
                ),
                Some(Ok(0))
            )
        );
    }

    #[test]
    fn of_a_message_refused_before_anything_is_read_is_its_error() {
        let mut sim = room();
        sim.send(Sim::message("telegram", "Bonjour")).unwrap_err();
        assert_eq!(
            decided(&sim),
            (
                None,
                None,
                Err(InboundError::UnknownChannel(channel("telegram")))
            )
        );
    }

    /// RFC §10.5: a direct injection's refusal before its commit is
    /// raised.
    #[test]
    fn of_a_direct_injection_refused_by_its_room_raises_the_refusal() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        let event = DirectEvent::new(channel("ws"), roomkit_core::model::EventContent::text("Hi"));
        sim.inject("room_1", event).unwrap_err();
        assert_eq!(
            decided(&sim).2,
            Err(InboundError::Refused {
                reason: "room_closed".to_owned()
            })
        );
    }
}

/// The protocol a driver follows (docs/architecture.md).
mod the_protocol {
    use super::*;
    use pretty_assertions::assert_eq;

    fn started() -> (Inbound, roomkit_core::ops::EffectId) {
        let mut sim = room();
        let config = Arc::new(std::mem::take(&mut sim.config));
        let target = InboundTarget::Room(room_id());
        let mut inbound = Inbound::new(config, target, None, Sim::message("sms", "Hi"));
        let Next::Effects(effects) = inbound.poll(&mut SequentialEnvironment::default()).next
        else {
            panic!("expected step 2's read of the room");
        };
        (inbound, effects.first().unwrap().id)
    }

    #[test]
    fn an_outcome_of_another_family_is_refused() {
        let (mut inbound, effect) = started();
        assert!(matches!(
            inbound.complete(effect, Outcome::Locked),
            Err(ProtocolError::UnexpectedOutcome { .. })
        ));
    }

    #[test]
    fn an_effect_completed_twice_is_refused() {
        let (mut inbound, effect) = started();
        inbound.complete(effect, Outcome::RoomNotFound).unwrap();
        assert_eq!(
            inbound.complete(effect, Outcome::RoomNotFound),
            Err(ProtocolError::UnknownEffect(effect))
        );
    }

    #[test]
    fn polling_with_an_effect_outstanding_waits() {
        let (mut inbound, _) = started();
        assert_eq!(
            inbound.poll(&mut SequentialEnvironment::default()).next,
            Next::Waiting
        );
    }
}
