//! The inbound pipeline, effect by effect (RFC §10.1, §10.2).

use std::sync::Arc;

use roomkit_core::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
use roomkit_core::model::{
    ChannelOutput, ChannelSet, DeliveryResultStatus, ErrorRecord, EventId, EventStatus, EventType,
    HookResult, InjectedEvent, Observation, ObservationId, RoomEvent, RoomStatus, Task, TaskId,
    TextContent, Timestamp, Visibility,
};
use roomkit_core::ops::{
    ChannelConfig, FrameworkEvent, HoldOutcome, Inbound, InboundError, InboundTarget, KitConfig,
    Next, Operation, Outcome, ProtocolError, RefusedOperation, SequentialEnvironment, StoreOutcome,
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
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
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
        let delivered: Vec<(&str, DeliveryResultStatus)> = result
            .delivery_results
            .values()
            .map(|delivery| (delivery.channel_id.as_str(), delivery.status))
            .collect();
        assert_eq!(delivered, [("ws", DeliveryResultStatus::Sent)]);
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
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![reply],
                    ..ChannelOutput::empty()
                })
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
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![reply],
                    ..ChannelOutput::empty()
                })
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
                &FrameworkEvent::delivery_succeeded(
                    room_id(),
                    "evt_1".parse().unwrap(),
                    channel("ws")
                ),
                &FrameworkEvent::event_processed(room_id(), "evt_1".parse().unwrap()),
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
                let task = Task::at(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Call back",
                    Timestamp::UNIX_EPOCH,
                );
                HookOutcome::Returned(HookResult {
                    tasks: vec![task],
                    ..HookResult::allow(AllowOptions::default())
                })
            }),
        );
        sim
    }

    /// RFC §10.1 step 15: each event's set stores what its hooks produced
    /// once it is over, the message's then the answer's.
    #[test]
    fn persists_the_side_effects_its_hooks_produced_with_each_set() {
        let mut sim = triaging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.traced("persist"),
            [
                "persist 1 tasks, 0 observations",
                "persist 1 tasks, 0 observations"
            ]
        );
    }

    /// RFC §10.1 steps 15 and 16: an event's tasks are stored, and their
    /// observers started, before its `AFTER_BROADCAST` hooks, which read them
    /// stored (Python's `_post_plan_effects`).
    #[test]
    fn fires_on_task_created_for_each_task_once_stored_before_after_broadcast() {
        let mut sim = triaging_room();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.observer("follow_up", HookTrigger::OnTaskCreated);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.async_triggers(),
            [
                HookTrigger::OnTaskCreated,
                HookTrigger::AfterBroadcast,
                HookTrigger::OnTaskCreated,
                HookTrigger::AfterBroadcast,
            ]
        );
    }
}

/// RFC Appendix B.2: a hook blocks, injects and observes.
mod a_blocked_message {
    use super::*;
    use crate::support::{Answer, Reentry, calling};
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{DirectEvent, TextContent};
    use roomkit_core::ops::Entry;

    fn scanning_room() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.before_broadcast(
            "sensitivity_scanner",
            Box::new(|event| {
                let text = roomkit_core::rules::transcoding::text_of(&event.content);
                if !text.contains("123-456-789") {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let observation = Observation::at(
                    ObservationId::new("obs_sin").unwrap(),
                    room_id(),
                    channel("sms"),
                    "SIN sent by SMS",
                    Timestamp::UNIX_EPOCH,
                );
                HookOutcome::Returned(HookResult::block(
                    "SIN detected",
                    BlockOptions {
                        injected: Some(vec![
                            InjectedEvent {
                                target_channel_ids: Some(vec![channel("sms")]),
                                ..InjectedEvent::new(notice_event(
                                    "inj_1",
                                    "Message blocked. Do not send SIN by SMS.",
                                ))
                            },
                            InjectedEvent {
                                target_channel_ids: Some(vec![channel("ws")]),
                                ..InjectedEvent::new(notice_event(
                                    "inj_2",
                                    "Client attempted to send SIN. Blocked.",
                                ))
                            },
                        ]),
                        observations: Some(vec![observation]),
                        ..Default::default()
                    },
                ))
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

    /// `Effect::decides`: of the three commits, the message's record alone
    /// decides the call, as its result reads it, the hook's reason its
    /// reason; the events the hook injected decide nothing.
    #[test]
    fn decides_the_call_by_its_own_record_s_commit_only() {
        let mut sim = scanning_room();
        sim.send(Sim::message("sms", "Mon NAS est 123-456-789"))
            .unwrap();
        assert_eq!(
            sim.deciding,
            [r#"Mon NAS est 123-456-789: blocked "SIN detected""#]
        );
    }

    /// `Effect::decides`, RMK-1010 item 41: an entry a locked hook adopted
    /// into the room is committed ahead of the message, and decides
    /// nothing; the message's commit decides the call.
    #[test]
    fn an_entry_committed_ahead_of_it_decides_nothing() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.reentrant_hook(
            "notes",
            true,
            Reentry {
                entries: calling(|event| {
                    let text = roomkit_core::rules::transcoding::text_of(&event.content);
                    (text == "Bonjour").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("sms"),
                            TextContent::new("Noted"),
                        )),
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| {
                    HookOutcome::Returned(HookResult::block("not now", BlockOptions::default()))
                })),
            },
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("commit").len(), sim.deciding),
            (2, vec![r#"Bonjour: blocked "not now""#.to_owned()])
        );
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
            Some(&&FrameworkEvent::event_blocked(
                room_id(),
                "evt_1".parse().unwrap(),
                channel("sms"),
                Some("SIN detected".to_owned()),
                "sensitivity_scanner".to_owned()
            ))
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
                redacted.content = TextContent::new("[redacted]").into();
                HookOutcome::Returned(HookResult::modify(redacted, ModifyOptions::default()))
            }),
        );
        sim.before_broadcast(
            "policy",
            Box::new(|_| {
                HookOutcome::Returned(HookResult::block("not allowed", BlockOptions::default()))
            }),
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
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                HookOutcome::Returned(HookResult::block(
                    "gated",
                    BlockOptions {
                        injected: Some(vec![InjectedEvent {
                            target_channel_ids: Some(vec![target.clone()]),
                            ..InjectedEvent::new(injected.clone())
                        }]),
                        ..Default::default()
                    },
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
        from_sms.source = roomkit_core::model::EventSource {
            direction: roomkit_core::model::EventDirection::Outbound,
            ..roomkit_core::model::EventSource::new(
                channel("sms"),
                roomkit_core::model::ChannelType::SMS,
            )
        };
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
                let task = Task::at(
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
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                HookOutcome::Returned(HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent {
                        target_channel_ids: Some(vec![channel("ws")]),
                        ..InjectedEvent::new(notice_event("inj_1", "Heads up"))
                    }]),
                }))
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
                    return HookOutcome::Returned(HookResult::block(
                        "no notices",
                        BlockOptions::default(),
                    ));
                }
                HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
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
                Some(&FrameworkEvent::room_refused_event(
                    room_id(),
                    "evt_2".parse().unwrap(),
                    RoomStatus::Closed,
                    RefusedOperation::Reentry,
                    EventType::Message
                ))
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
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let targets = targets.iter().map(|id| channel(id)).collect();
                HookOutcome::Returned(HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent {
                        target_channel_ids: Some(targets),
                        ..InjectedEvent::new(notice_event("inj_1", "Heads up"))
                    }]),
                }))
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
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut modified = event.clone();
                modified.visibility = visibility.parse().unwrap();
                HookOutcome::Returned(HookResult::modify(modified, ModifyOptions::default()))
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
                HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
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
                    return HookOutcome::Returned(HookResult::block(
                        "no notices",
                        BlockOptions::default(),
                    ));
                }
                HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
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
                HookOutcome::Returned(HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent {
                        target_channel_ids: Some(vec![channel("ws"), channel("billing")]),
                        ..InjectedEvent::new(notice_event("inj_1", "Heads up"))
                    }]),
                }))
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
                    .insert("roomkit:detached_channels".to_owned(), json!(["sms"]));
                Ok(output)
            }),
        );
        sim.room.metadata.insert("crm_id".to_owned(), json!("c_1"));
        sim
    }

    /// Python: `core/mixins/helpers.py::_write_metadata_updates` (RMK-1511):
    /// once its set is over, under the room's lock, taken again off the
    /// set's place in the lane.
    #[test]
    fn are_written_on_the_room_once_their_set_is_over() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.trace[sim.trace.len() - 4..].to_vec(),
                sim.room.metadata.get("tier")
            ),
            (
                vec![
                    "leave lane room_1 evt_1".to_owned(),
                    "lock room_1".to_owned(),
                    "update room room_1 Active".to_owned(),
                    "release room_1".to_owned()
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
        assert_eq!(sim.room.metadata.get("roomkit:detached_channels"), None);
    }

    #[test]
    fn leave_the_counters_as_the_commits_set_them() {
        let mut sim = tagging_room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!((sim.room.event_count, sim.room.latest_index), (1, Some(0)));
    }

    /// RFC §10.1 step 15: a delivery set that collected a task and a
    /// metadata update writes both once it is over, the task first, then
    /// the update under the room's lock, as Python's
    /// `_persist_side_effects` writes tasks, observations, then
    /// `_write_metadata_updates` (RMK-1511); the call ends once both are,
    /// the task stored and the room tagged.
    #[test]
    fn are_written_after_the_tasks_under_the_room_s_lock() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.agent(
            "crm",
            Box::new(|_, _| {
                let task = Task::at(
                    TaskId::new("task_1").unwrap(),
                    room_id(),
                    "Call back",
                    Timestamp::UNIX_EPOCH,
                );
                let mut output = ChannelOutput {
                    tasks: vec![task],
                    ..ChannelOutput::default()
                };
                output
                    .metadata_updates
                    .insert("tier".to_owned(), json!("gold"));
                Ok(output)
            }),
        );
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.trace[sim.trace.len() - 5..].to_vec(),
                sim.tasks.len(),
                sim.room.metadata.get("tier"),
                result.error
            ),
            (
                vec![
                    "leave lane room_1 evt_1".to_owned(),
                    "persist 1 tasks, 0 observations".to_owned(),
                    "lock room_1".to_owned(),
                    "update room room_1 Active".to_owned(),
                    "release room_1".to_owned()
                ],
                1,
                Some(&json!("gold")),
                None
            )
        );
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
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
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
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
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
            [&FrameworkEvent::room_refused_event(
                room_id(),
                "evt_1".parse().unwrap(),
                RoomStatus::Closed,
                RefusedOperation::Inbound,
                EventType::Message
            )]
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

    /// RFC §13.4: a blank key, empty or whitespace only, is no key: two
    /// messages carrying one are two messages, each stored with no key.
    #[test]
    fn a_blank_key_is_no_key() {
        let mut sim = room();
        let mut stored = Vec::new();
        for blank in ["", "   "] {
            for text in ["one", "two"] {
                let mut message = Sim::message("sms", text);
                message.idempotency_key = Some(blank.to_owned());
                let event = sim.send(message).unwrap().event.unwrap();
                stored.push((
                    roomkit_core::rules::transcoding::text_of(&event.content),
                    event.idempotency_key,
                ));
            }
        }
        let one = || ("one".to_owned(), None);
        let two = || ("two".to_owned(), None);
        assert_eq!(
            (stored, sim.traced("find_key").len()),
            (vec![one(), two(), one(), two()], 0)
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
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![reply],
                    ..ChannelOutput::empty()
                })
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
            Box::new(|_| {
                HookOutcome::Returned(HookResult::block("not allowed", BlockOptions::default()))
            }),
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
        whispered.visibility = ChannelSet::new([channel("ws")])
            .map(Visibility::Channels)
            .unwrap();
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
                .contains(&&FrameworkEvent::hook_error(
                    room_id(),
                    "evt_1".parse().unwrap(),
                    "broken_logger".to_owned(),
                    "boom".to_owned()
                ))
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
        let mut spec = HookSpec::new("pii", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        spec.fail_closed = true;
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
            .visibility = ChannelSet::new([channel("ws_advisor")])
            .map(Visibility::Channels)
            .unwrap();
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
    use roomkit_core::model::{DirectEvent, InboundMessage};
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
            Ok(ChannelOutput {
                responded: true,
                response_events: vec![answer],
                ..ChannelOutput::empty()
            })
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

    /// A response's hook may change its visibility as on any event, never
    /// past where its message's answers may go: the pass narrows it back
    /// once its hooks ran.
    #[test]
    fn narrows_a_response_its_hook_widened_back_within_the_message_s() {
        let mut sim = room_with(answering_as("all"));
        sim.before_broadcast(
            "widen",
            Box::new(|event| {
                if event.source.channel_id.as_str() != "ai" {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut widened = event.clone();
                widened.visibility = Visibility::All;
                HookOutcome::Returned(HookResult::modify(widened, ModifyOptions::default()))
            }),
        );
        sim.send(answered_within("Hello", "ws_advisor")).unwrap();
        assert_eq!(
            (stored(&sim, "Re: Hello"), delivered(&sim, "Re: Hello")),
            (Some("ws_advisor".to_owned()), vec!["ws_advisor"])
        );
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
                        answers: None,
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| {
                    HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
                })),
            },
        );
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(
            (stored(&sim, "Re: Follow-up"), stored(&sim, "Re: Hello")),
            (Some("ws_advisor".to_owned()), Some("all".to_owned()))
        );
    }

    /// The scope binds every pass of the message's cascade: an answer to
    /// an answer stays within it too, so nothing derived from the message
    /// leaves the perimeter it asked for.
    #[test]
    fn narrows_a_response_to_a_response_within_the_message_s() {
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
            (
                Some("intelligence".to_owned()),
                Some("intelligence".to_owned())
            )
        );
    }

    /// A direct injection names where its answers may go as a message does
    /// (RFC §10.5): its responses, and theirs, are narrowed by it.
    #[test]
    fn narrows_the_responses_of_a_direct_injection_by_its_own() {
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
        let event = DirectEvent {
            response_visibility: Some("ws_advisor,writer".parse().unwrap()),
            ..DirectEvent::new(channel("sms"), TextContent::new("Hello"))
        };
        sim.inject("room_1", event).unwrap();
        assert_eq!(
            (
                stored(&sim, "Hello"),
                stored(&sim, "Re: Hello"),
                stored(&sim, "Noted")
            ),
            (
                Some("all".to_owned()),
                Some("writer,ws_advisor".to_owned()),
                Some("writer,ws_advisor".to_owned())
            )
        );
    }

    /// The stored `response_visibility` of the event that says `text`;
    /// `None` when it restricts nothing.
    fn scope_of(sim: &Sim, text: &str) -> Option<String> {
        saying(sim, text)
            .and_then(|event| event.response_visibility.as_ref())
            .map(ToString::to_string)
    }

    /// An agent that answers the customer's `text` with `Re: text`, visible
    /// to all, its own answers' scope `scope`.
    fn answering_within(scope: Option<&'static str>) -> OnEvent {
        Box::new(move |event, _| {
            if event.source.channel_id.as_str() != "sms" {
                return Ok(ChannelOutput::default());
            }
            let text = format!("Re: {}", text_of(&event.content));
            let mut answer = response("ai", event, &text);
            answer.response_visibility = scope.map(|scope| scope.parse().unwrap());
            Ok(ChannelOutput {
                responded: true,
                response_events: vec![answer],
                ..ChannelOutput::empty()
            })
        })
    }

    /// A `writer` agent that answers `ai`'s answers with `Noted`.
    fn noting(sim: &mut Sim) {
        sim.agent(
            "writer",
            Box::new(|event, _| {
                if event.source.channel_id.as_str() != "ai" {
                    return Ok(ChannelOutput::default());
                }
                Ok(respond("writer", event, "Noted"))
            }),
        );
    }

    /// A hook named `name` that writes `scope` as the
    /// `response_visibility` of the customer's message.
    fn rescoping(sim: &mut Sim, name: &str, scope: Option<Visibility>) {
        sim.before_broadcast(
            name,
            Box::new(move |event| {
                if event.source.channel_id.as_str() != "sms" {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut rescoped = event.clone();
                rescoped.response_visibility.clone_from(&scope);
                HookOutcome::Returned(HookResult::modify(rescoped, ModifyOptions::default()))
            }),
        );
    }

    /// RFC §5.2: the scope is a field of the event, stored with it.
    #[test]
    fn is_stored_on_the_message_s_event() {
        let mut sim = room_with(answering_as("all"));
        sim.send(answered_within("Hello", "ws_advisor")).unwrap();
        assert_eq!(scope_of(&sim, "Hello"), Some("ws_advisor".to_owned()));
    }

    /// RFC §8.5: each response carries the scope it answers within,
    /// intersected with any its channel gave it.
    #[test]
    fn is_carried_by_each_response_within_its_channel_s() {
        let cases = [
            (None, "writer,ws_advisor"),
            (Some("writer"), "writer"),
            (Some("sms"), "none"),
        ];
        for (given, carried) in cases {
            let mut sim = room_with(answering_within(given));
            noting(&mut sim);
            sim.send(answered_within("Hello", "ws_advisor,writer"))
                .unwrap();
            assert_eq!(
                scope_of(&sim, "Re: Hello"),
                Some(carried.to_owned()),
                "a response its channel scoped {given:?}"
            );
        }
    }

    /// RFC §8.5: what answers a response stays within the scope the
    /// response carries, its channel's included, though its message
    /// restricted nothing.
    #[test]
    fn binds_the_answers_to_a_response_its_channel_scoped() {
        let mut sim = room_with(answering_within(Some("ws_advisor")));
        noting(&mut sim);
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(
            (stored(&sim, "Re: Hello"), stored(&sim, "Noted")),
            (Some("all".to_owned()), Some("ws_advisor".to_owned()))
        );
    }

    /// RFC §9.4: a hook may narrow where the answers go, and the answers
    /// follow.
    #[test]
    fn narrowed_by_a_hook_binds_the_answers() {
        let mut sim = room_with(answering_as("all"));
        rescoping(&mut sim, "rescope", Some("ws_advisor".parse().unwrap()));
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(
            (scope_of(&sim, "Hello"), stored(&sim, "Re: Hello")),
            (Some("ws_advisor".to_owned()), Some("ws_advisor".to_owned()))
        );
    }

    /// RFC §9.4: a hook can neither widen nor clear it, and `internal`
    /// from a hook reads `none`.
    #[test]
    fn written_by_a_hook_never_widens_nor_clears() {
        let cases = [
            (Some(Visibility::All), "ws_advisor", "ws_advisor"),
            (None, "ws_advisor", "ws_advisor"),
            (Some(Visibility::Internal), "none", "none"),
        ];
        for (written, kept, answered) in cases {
            let mut sim = room_with(answering_as("all"));
            rescoping(&mut sim, "rescope", written.clone());
            sim.send(answered_within("Hello", "ws_advisor")).unwrap();
            assert_eq!(
                (scope_of(&sim, "Hello"), stored(&sim, "Re: Hello")),
                (Some(kept.to_owned()), Some(answered.to_owned())),
                "a hook writing {written:?}"
            );
        }
    }

    /// RFC §9.4: what an earlier hook narrowed, a later one can neither
    /// widen nor clear: each hook's modify is kept against the event that
    /// hook was handed.
    #[test]
    fn narrowed_by_one_hook_is_never_widened_by_the_next() {
        let mut kept = Vec::new();
        for written in [Some(Visibility::All), None] {
            let mut sim = room_with(answering_as("all"));
            rescoping(&mut sim, "rescope", Some("ws_advisor".parse().unwrap()));
            rescoping(&mut sim, "widen", written);
            sim.send(Sim::message("sms", "Hello")).unwrap();
            kept.push((
                scope_of(&sim, "Hello"),
                stored(&sim, "Re: Hello"),
                delivered(&sim, "Re: Hello").join(","),
            ));
        }
        let narrowed = (
            Some("ws_advisor".to_owned()),
            Some("ws_advisor".to_owned()),
            "ws_advisor".to_owned(),
        );
        assert_eq!(kept, [narrowed.clone(), narrowed]);
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
                Ok(ChannelOutput {
                    responded: true,
                    response_events: responses,
                    ..ChannelOutput::empty()
                })
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
            (
                Some(EventStatus::Delivered),
                Some(DeliveryResultStatus::Failed)
            )
        );
    }

    /// The room whose `ws` delivery fails, a message sent into it.
    fn with_a_failed_delivery() -> Sim {
        let mut sim = room();
        sim.deliver_with(
            "ws",
            Box::new(|_| Err(ErrorRecord::new("ConnectionError", "socket closed"))),
        );
        sim
    }

    /// The trace from the set's leaving its place on: its record, under the
    /// room's lock, as Python's `_record_failed_deliveries` writes it.
    #[test]
    fn a_failed_delivery_is_recorded_under_the_lock_once_its_place_is_left() {
        let mut sim = with_a_failed_delivery();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let from = sim
            .trace
            .iter()
            .position(|line| line.starts_with("leave lane"))
            .unwrap();
        let aftermath: Vec<&str> = sim
            .trace
            .iter()
            .skip(from + 1)
            .take(3)
            .map(String::as_str)
            .collect();
        assert_eq!(
            aftermath,
            [
                "lock room_1",
                "record_delivery_results evt_1",
                "release room_1"
            ]
        );
    }

    /// What the stored event's `delivery_results` say of each channel.
    fn stored_results(sim: &Sim) -> Vec<(String, serde_json::Value)> {
        let stored = sim.timeline.first().unwrap();
        stored
            .delivery_results
            .iter()
            .map(|(id, result)| (id.clone(), result["status"].clone()))
            .collect()
    }

    // Python: tests/test_delivery_outcomes.py::TestOnlyAFailingSetIsRemembered::test_the_record_keeps_the_successes_too
    #[test]
    fn the_record_keeps_every_target_s_outcome() {
        let mut sim = with_a_failed_delivery();
        sim.transport("email");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            stored_results(&sim),
            [
                ("email".to_owned(), serde_json::json!("sent")),
                ("ws".to_owned(), serde_json::json!("failed"))
            ]
        );
    }

    // Python: tests/test_delivery_outcomes.py::TestOnlyAFailingSetIsRemembered::test_a_clean_delivery_writes_nothing
    #[test]
    fn a_clean_delivery_records_nothing() {
        let mut sim = room();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("record_delivery_results"), stored_results(&sim)),
            (Vec::new(), Vec::new())
        );
    }

    /// A record that cannot be written changes nothing the set delivered:
    /// the call reports its deliveries and no error, and the lock is
    /// released, which the simulated driver checks at the end of the run.
    #[test]
    fn a_record_that_fails_changes_nothing_of_the_call() {
        let mut sim = with_a_failed_delivery();
        sim.failing_effect = Some("record_delivery_results");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.error,
                result
                    .delivery_results
                    .get("ws")
                    .map(|delivery| delivery.status),
                sim.framework_event_names().last().cloned(),
            ),
            (
                None,
                Some(DeliveryResultStatus::Failed),
                Some("event_processed".to_owned()),
            )
        );
    }

    /// The partial failure first, as Python's order (RMK-1095).
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
                "broadcast_partial_failure",
                "delivery_failed",
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

    /// RFC §10.1 step 1, §10.5: a host channel receives no external
    /// payload: a message on one is refused before anything is read,
    /// whether it names its room or is routed.
    #[test]
    fn a_message_on_a_host_channel_is_refused_before_any_effect() {
        let mut sim = room();
        sim.declare(ChannelConfig::host(channel("host")));
        let named = sim.send(Sim::message("host", "Bonjour"));
        let routed = sim.route(None, Sim::message("host", "Bonjour"));
        let refused = || Err(InboundError::InboundOnHostChannel(channel("host")));
        assert_eq!((named, routed, sim.trace.len()), (refused(), refused(), 0));
    }

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
    fn answer(inbound: &mut Inbound, environment: &SequentialEnvironment, outcome: Outcome) {
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
        let environment = SequentialEnvironment::default();
        let context = Outcome::Store(StoreOutcome::Context(Box::new(
            roomkit_core::model::RoomContext {
                room: sim.room.clone(),
                bindings: sim.bindings.clone(),
                participants: Vec::new(),
                recent_events: Vec::new(),
                history_start_index: None,
                author_register: None,
            },
        )));
        let failure = Outcome::Failed(ErrorRecord::new("ParseError", "bad payload"));
        for outcome in [context, failure] {
            answer(&mut inbound, &environment, outcome);
        }
        assert!(matches!(
            inbound.poll(&environment).next,
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
            Box::new(|_| {
                HookOutcome::Returned(HookResult::block("not now", BlockOptions::default()))
            }),
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
        let event = DirectEvent::new(channel("ws"), TextContent::new("Hi"));
        sim.inject("room_1", event).unwrap_err();
        assert_eq!(
            decided(&sim).2,
            Err(InboundError::Refused {
                reason: "room_closed".to_owned()
            })
        );
    }
}

/// RFC §13.6, gap G-076: a caller that stops waiting before the call's
/// decision expires it, as a passed `process_timeout` does, without the
/// `process_timeout` event; once anything is committed, it is delivered.
/// RFC §10.1 "Explicit delivery cancellation": once the call is decided,
/// its caller gone cuts the turns of its cascade, and its events are still
/// delivered to every other target.
mod a_caller_that_stops_waiting {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A room whose SYNC hook allows every event.
    fn scanned_room() -> Sim {
        let mut sim = room();
        sim.before_broadcast(
            "scanner",
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
        );
        sim
    }

    /// The hook a pass waits for under the lock is given up on: the pass
    /// lets the lock go, and nothing of the event is written.
    #[test]
    fn during_a_hook_under_the_lock_writes_nothing_and_lets_the_lock_go() {
        let mut sim = scanned_room();
        sim.leaves_before = Some("hook scanner");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.reason,
                sim.trace.clone(),
                sim.timeline_lines(),
                sim.framework_event_names()
            ),
            (
                Some("process_timeout".to_owned()),
                [
                    "load_context room_1",
                    "handle_inbound sms",
                    "lock room_1",
                    "load_context room_1",
                    "caller gone",
                    "release room_1",
                ]
                .map(str::to_owned)
                .to_vec(),
                Vec::<String>::new(),
                Vec::<String>::new()
            )
        );
    }

    /// A commit is never within the call's bound: the caller gone while it
    /// is in flight, it is answered, the call is decided, and the event is
    /// delivered; the agent's turn never starts, as the Python
    /// implementation's does not (probe 6 of step 3).
    #[test]
    fn during_its_commit_has_its_event_delivered_and_starts_no_turn() {
        let mut sim = scanned_room();
        sim.leaves_before = Some("commit");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let processed = sim
            .framework_event_names()
            .contains(&"event_processed".to_owned());
        assert_eq!(
            (
                result.event.map(|event| event.status),
                sim.timeline_lines(),
                sim.traced("deliver"),
                sim.traced("on_event ai"),
                processed
            ),
            (
                Some(EventStatus::Delivered),
                vec!["#0 sms delivered \"Bonjour\"".to_owned()],
                vec!["deliver ws <- evt_1"],
                Vec::<&str>::new(),
                true
            )
        );
    }

    /// RFC §10.1 "Explicit delivery cancellation", gap G-076: the caller of
    /// a decided call gone once the agent was asked, its turn in flight, or
    /// once it answered, the turn is cut, or its answer does not re-enter:
    /// no answer is committed. The event is delivered to the advisor all the
    /// same, and its `AFTER_BROADCAST` hooks fire, as the Python
    /// implementation's are (probe 6 of step 3).
    #[test]
    fn during_a_decided_call_s_turn_commits_no_answer_and_delivers_the_event() {
        let seen: Vec<_> = ["on_event ai", "deliver ws"]
            .into_iter()
            .map(|leaves_before| {
                let mut sim = room();
                sim.observer("audit", HookTrigger::AfterBroadcast);
                sim.leaves_before = Some(leaves_before);
                let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
                let observed = sim
                    .fired
                    .iter()
                    .filter(|(trigger, _)| *trigger == HookTrigger::AfterBroadcast)
                    .count();
                (
                    result.event.map(|event| event.status),
                    sim.timeline_lines(),
                    sim.traced("deliver").join(", "),
                    sim.traced("on_event ai").len(),
                    observed,
                )
            })
            .collect();
        let delivered = |answered| {
            (
                Some(EventStatus::Delivered),
                vec!["#0 sms delivered \"Bonjour\"".to_owned()],
                "deliver ws <- evt_1".to_owned(),
                answered,
                1,
            )
        };
        // The turn in flight is given up on, never answered: the simulated
        // driver never runs it.
        assert_eq!(seen, [delivered(0), delivered(1)]);
    }

    /// RFC §10.1 "Explicit delivery cancellation": a call whose turns are
    /// cut before it starts, as a delivery cancelled by its key would be,
    /// commits and delivers its event, and asks no agent.
    #[test]
    fn cut_before_its_start_commits_and_delivers_its_event_and_asks_no_agent() {
        let mut sim = room();
        let target = InboundTarget::Room(room_id());
        let mut inbound = Inbound::new(
            sim.shared_config(),
            target,
            None,
            Sim::message("sms", "Bonjour"),
        );
        let in_flight = inbound.cancel_turns();
        let result = sim.run(inbound).unwrap();
        assert_eq!(
            (
                in_flight,
                result.event.map(|event| event.status),
                sim.timeline_lines(),
                sim.traced("deliver").join(", "),
                sim.traced("on_event ai").len()
            ),
            (
                Vec::new(),
                Some(EventStatus::Delivered),
                vec!["#0 sms delivered \"Bonjour\"".to_owned()],
                "deliver ws <- evt_1".to_owned(),
                0
            )
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
        let Next::Effects(effects) = inbound.poll(&SequentialEnvironment::default()).next else {
            panic!("expected step 2's read of the room");
        };
        (inbound, effects.first().unwrap().id)
    }

    #[test]
    fn an_outcome_of_another_family_is_refused() {
        let (mut inbound, effect) = started();
        assert!(matches!(
            inbound.complete(effect, Outcome::Hold(HoldOutcome::Locked)),
            Err(ProtocolError::UnexpectedOutcome { .. })
        ));
    }

    #[test]
    fn an_effect_completed_twice_is_refused() {
        let (mut inbound, effect) = started();
        inbound
            .complete(effect, Outcome::Store(StoreOutcome::RoomNotFound))
            .unwrap();
        assert_eq!(
            inbound.complete(effect, Outcome::Store(StoreOutcome::RoomNotFound)),
            Err(ProtocolError::UnknownEffect(effect))
        );
    }

    #[test]
    fn polling_with_an_effect_outstanding_waits() {
        let (mut inbound, _) = started();
        assert_eq!(
            inbound.poll(&SequentialEnvironment::default()).next,
            Next::Waiting
        );
    }
}

/// RFC §10.1 step 8, §5.2: every path that commits an event roots its
/// thread, a parent naming a reply replaced by that reply's own parent,
/// one that, once rooted, names no event of the room dropped; a parent a
/// hook sets is rooted on every pass.
mod a_thread_parent {
    use super::*;
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{DirectEvent, InboundMessage, Room, RoomId};

    fn id(event_id: &str) -> EventId {
        EventId::new(event_id).unwrap()
    }

    /// `room()` holding `evt_root` and its reply `evt_reply`.
    fn threaded() -> Sim {
        let mut sim = room();
        sim.seed(notice_event("evt_root", "Root"));
        let mut reply = notice_event("evt_reply", "Reply");
        reply.parent_event_id = Some(id("evt_root"));
        sim.seed(reply);
        sim
    }

    /// The customer's `text`, replying to `parent`.
    fn replying(text: &str, parent: &str) -> InboundMessage {
        InboundMessage {
            parent_event_id: Some(id(parent)),
            ..Sim::message("sms", text)
        }
    }

    /// The stored parent of the event that says `text`.
    fn parent_of(sim: &Sim, text: &str) -> Option<String> {
        sim.timeline
            .iter()
            .find(|event| roomkit_core::rules::transcoding::text_of(&event.content) == text)
            .and_then(|event| event.parent_event_id.as_ref())
            .map(ToString::to_string)
    }

    #[test]
    fn naming_a_reply_is_stored_under_its_thread_s_root() {
        let mut sim = threaded();
        sim.send(replying("Thanks", "evt_reply")).unwrap();
        assert_eq!(parent_of(&sim, "Thanks"), Some("evt_root".to_owned()));
    }

    /// The parent is read under the lock, then the root the reply names,
    /// after the key check and before step 8a and the hooks.
    #[test]
    fn is_read_under_the_lock_before_the_hooks() {
        let mut sim = threaded();
        sim.before_broadcast(
            "audit",
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
        );
        sim.send(replying("Thanks", "evt_reply")).unwrap();
        let section: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("lock"))
            .take(5)
            .collect();
        assert_eq!(
            section,
            [
                "lock room_1",
                "load_context room_1",
                "get_event evt_reply",
                "get_event evt_root",
                "hook audit"
            ]
        );
    }

    /// A parent the room does not hold, another room's included, is
    /// dropped: the message is posted at the top level.
    #[test]
    fn naming_no_event_of_the_room_is_dropped() {
        let mut sim = threaded();
        sim.seed_room(Room::at(
            RoomId::new("room_2").unwrap(),
            Timestamp::UNIX_EPOCH,
        ));
        let mut elsewhere = notice_event("evt_there", "There");
        elsewhere.room_id = RoomId::new("room_2").unwrap();
        sim.seed_other("room_2", elsewhere);
        sim.send(replying("Lost", "evt_404")).unwrap();
        sim.send(replying("Astray", "evt_there")).unwrap();
        assert_eq!(
            (parent_of(&sim, "Lost"), parent_of(&sim, "Astray")),
            (None, None)
        );
    }

    #[test]
    fn of_a_direct_injection_is_rooted() {
        let mut sim = threaded();
        let event = DirectEvent {
            parent_event_id: Some(id("evt_reply")),
            ..DirectEvent::new(channel("ws"), TextContent::new("Noted"))
        };
        sim.inject("room_1", event).unwrap();
        assert_eq!(parent_of(&sim, "Noted"), Some("evt_root".to_owned()));
    }

    /// RFC §10.1 step 8: every path that commits an event roots its
    /// parent, a response's included.
    #[test]
    fn of_a_response_is_rooted() {
        let mut sim = threaded();
        sim.agent(
            "advisor",
            Box::new(|event, _| {
                if event.source.channel_id.as_str() != "sms" {
                    return Ok(ChannelOutput::default());
                }
                let mut answer = response("advisor", event, "In the reply's thread");
                answer.parent_event_id = Some(id("evt_reply"));
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![answer],
                    ..ChannelOutput::empty()
                })
            }),
        );
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(
            parent_of(&sim, "In the reply's thread"),
            Some("evt_root".to_owned())
        );
    }

    /// RFC §10.1 step 8, §9.4: an event a hook injects is rooted at its
    /// own pass.
    #[test]
    fn of_a_hook_s_injected_event_is_rooted() {
        let mut sim = threaded();
        sim.before_broadcast(
            "notifier",
            Box::new(|event| {
                if event.source.channel_id.as_str() != "sms" {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut notice = notice_event("evt_notice", "Noticed");
                notice.parent_event_id = Some(id("evt_reply"));
                HookOutcome::Returned(HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent {
                        target_channel_ids: Some(vec![channel("ws")]),
                        ..InjectedEvent::new(notice)
                    }]),
                }))
            }),
        );
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(parent_of(&sim, "Noticed"), Some("evt_root".to_owned()));
    }

    /// RFC §10.1 step 8: rooting reads the root it lands on. A reply whose
    /// root a hard delete removed, its replies kept, names no event of the
    /// room once rooted: a message, an injection and a response answering
    /// it are posted at the top level.
    #[test]
    fn naming_a_reply_whose_root_is_gone_is_dropped() {
        let mut sim = room();
        let mut orphan = notice_event("evt_reply", "Reply");
        orphan.parent_event_id = Some(id("evt_gone"));
        sim.seed(orphan);
        sim.agent(
            "advisor",
            Box::new(|event, _| {
                if event.source.channel_id.as_str() != "sms" {
                    return Ok(ChannelOutput::default());
                }
                let mut answer = response("advisor", event, "Answered");
                answer.parent_event_id = Some(id("evt_reply"));
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![answer],
                    ..ChannelOutput::empty()
                })
            }),
        );
        sim.send(replying("Thanks", "evt_reply")).unwrap();
        let event = DirectEvent {
            parent_event_id: Some(id("evt_reply")),
            ..DirectEvent::new(channel("ws"), TextContent::new("Noted"))
        };
        sim.inject("room_1", event).unwrap();
        assert_eq!(
            [
                parent_of(&sim, "Thanks"),
                parent_of(&sim, "Answered"),
                parent_of(&sim, "Noted")
            ],
            [None, None, None]
        );
    }

    /// RFC §10.1 step 9: a thread parent a hook's modify sets is rooted as
    /// at step 8.
    #[test]
    fn set_by_a_hook_is_rooted() {
        let mut sim = threaded();
        sim.before_broadcast(
            "threader",
            Box::new(|event| {
                if event.source.channel_id.as_str() != "sms" {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut threaded = event.clone();
                threaded.parent_event_id = Some(id("evt_reply"));
                HookOutcome::Returned(HookResult::modify(threaded, ModifyOptions::default()))
            }),
        );
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(parent_of(&sim, "Hello"), Some("evt_root".to_owned()));
    }
}

/// RFC §9.2, §9.3: a hook registered ASYNC on `BEFORE_BROADCAST`, a SYNC
/// trigger, runs as an observer once the chain allowed the event, on the
/// payload the SYNC hooks left; a blocked event fires none (the Python
/// implementation's `run_sync_hooks`, conformance scenario
/// `hooks/an-async-hook-on-a-sync-trigger-observes`).
mod an_async_hook_on_before_broadcast {
    use super::*;
    use pretty_assertions::assert_eq;

    fn rewritten() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.before_broadcast(
            "rewrite",
            Box::new(|event| {
                let mut rewritten = event.clone();
                rewritten.content = TextContent::new("rewritten").into();
                HookOutcome::Returned(HookResult::modify(rewritten, ModifyOptions::default()))
            }),
        );
        sim.observer("watcher", HookTrigger::BeforeBroadcast);
        sim
    }

    #[test]
    fn observes_the_payload_the_chain_left() {
        let mut sim = rewritten();
        sim.send(Sim::message("sms", "original")).unwrap();
        let observed: Vec<(HookTrigger, String)> = sim
            .async_triggers()
            .into_iter()
            .zip(sim.async_payloads())
            .map(|(trigger, event)| {
                let text = roomkit_core::rules::transcoding::text_of(&event.content);
                (trigger, text)
            })
            .filter(|(trigger, _)| *trigger == HookTrigger::BeforeBroadcast)
            .collect();
        assert_eq!(
            observed,
            [(HookTrigger::BeforeBroadcast, "rewritten".to_owned())]
        );
    }

    #[test]
    fn observes_nothing_of_a_blocked_event() {
        let mut sim = rewritten();
        sim.before_broadcast(
            "policy",
            Box::new(|_| {
                HookOutcome::Returned(HookResult::block("not allowed", BlockOptions::default()))
            }),
        );
        sim.send(Sim::message("sms", "original")).unwrap();
        assert!(!sim.async_triggers().contains(&HookTrigger::BeforeBroadcast));
    }
}
