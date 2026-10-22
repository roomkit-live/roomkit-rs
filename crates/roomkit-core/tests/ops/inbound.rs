//! The inbound pipeline, effect by effect (RFC §10.1, §10.2).

use std::sync::Arc;

use roomkit_core::model::{
    ChannelOutput, DeliveryStatus, ErrorRecord, EventId, EventType, HookResult, InjectedEvent,
    Observation, ObservationId, RoomStatus, Task, TaskId, Timestamp, Visibility,
};
use roomkit_core::ops::{
    FrameworkEvent, Inbound, InboundError, KitConfig, Next, Operation, Outcome, ProtocolError,
    RefusedOperation, SequentialEnvironment,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};

use crate::support::{Sim, channel, notice_event, read_only, respond, response, room_id};

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
                "load_bindings room_1",
                "commit #0 sms delivered \"Bonjour\"",
                "release room_1",
                "on_event ws <- evt_1",
                "on_event ai <- evt_1",
                "deliver ws <- evt_1",
                "lock room_1",
                "load_context room_1",
                "hook pii_scan",
                "load_bindings room_1",
                "commit #1 ai delivered \"Hello back\"",
                "release room_1",
                "on_event sms <- ai_re_evt_1",
                "on_event ws <- ai_re_evt_1",
                "deliver sms <- ai_re_evt_1",
                "deliver ws <- ai_re_evt_1",
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

    #[test]
    fn persists_the_side_effects_its_hooks_produced() {
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
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.traced("persist"), ["persist 2 tasks, 0 observations"]);
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
            ["deliver sms <- inj_1", "deliver ws <- inj_2"]
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
            (result.blocked, result.reason.as_deref(), result.event),
            (true, Some("SIN detected"), None)
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
                "load_bindings room_1",
                "commit #0 sms blocked:source_muted \"Bonjour\"",
                "release room_1",
            ]
        );
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
        sim.hooks_mute = Some(channel("sms"));
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
                    "release room_1".to_owned()
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
            [
                "deliver ws_advisor <- evt_1",
                "deliver ws_advisor <- assistant_re_evt_1"
            ]
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
        assert_eq!(categories, [Some("chain_depth_exceeded")]);
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
            (
                Some(roomkit_core::model::EventStatus::Delivered),
                Some(DeliveryStatus::Failed)
            )
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

/// RFC §10.1 step 1 and the parts this step does not provide.
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

    #[test]
    fn a_channel_not_bound_to_the_room_fails() {
        let mut sim = room();
        sim.bindings
            .retain(|binding| binding.channel_id.as_str() != "sms");
        assert_eq!(
            sim.send(Sim::message("sms", "Bonjour")),
            Err(InboundError::ChannelNotBound {
                channel_id: channel("sms"),
                room_id: room_id(),
            })
        );
    }

    #[test]
    fn a_missing_room_fails() {
        let mut sim = room();
        sim.room_exists = false;
        assert_eq!(
            sim.send(Sim::message("sms", "Bonjour")),
            Err(InboundError::RoomNotFound(room_id()))
        );
    }

    #[test]
    fn an_instruction_is_refused_until_its_step() {
        let mut sim = room();
        let mut message = Sim::message("sms", "Introduce yourself");
        message.event_type = EventType::Instruction;
        assert!(matches!(
            sim.send(message),
            Err(InboundError::Unsupported(_))
        ));
    }

    #[test]
    fn a_standalone_message_that_is_no_instruction_is_refused() {
        let mut sim = room();
        let mut message = Sim::message("sms", "Bonjour");
        message.standalone = true;
        assert_eq!(
            sim.send(message),
            Err(InboundError::StandaloneOutsideInstruction)
        );
    }

    #[test]
    fn a_channel_that_cannot_parse_the_message_fails_it() {
        let mut sim = room();
        sim.room_exists = true;
        let mut config = KitConfig::new();
        config
            .register_channel(sim.config.channel(&channel("sms")).unwrap().clone())
            .unwrap();
        let mut inbound = Inbound::new(Arc::new(config), room_id(), Sim::message("sms", "Hi"));
        let mut environment = SequentialEnvironment::default();
        let Next::Effects(effects) = inbound.poll(&mut environment).next else {
            panic!("expected the context read");
        };
        let context = Outcome::Context(Box::new(roomkit_core::model::RoomContext {
            room: sim.room.clone(),
            bindings: sim.bindings.clone(),
            participants: Vec::new(),
            recent_events: Vec::new(),
        }));
        inbound.complete(effects[0].id, context).unwrap();
        let Next::Effects(effects) = inbound.poll(&mut environment).next else {
            panic!("expected the channel call");
        };
        let failure = Outcome::Failed(ErrorRecord::new("ParseError", "bad payload"));
        inbound.complete(effects[0].id, failure).unwrap();
        assert!(matches!(
            inbound.poll(&mut environment).next,
            Next::Ready(Err(InboundError::ChannelFailed { .. }))
        ));
    }
}

/// The protocol a driver follows (docs/architecture.md).
mod the_protocol {
    use super::*;
    use pretty_assertions::assert_eq;

    fn started() -> (Inbound, roomkit_core::ops::EffectId) {
        let mut sim = room();
        let config = Arc::new(std::mem::take(&mut sim.config));
        let mut inbound = Inbound::new(config, room_id(), Sim::message("sms", "Hi"));
        let Next::Effects(effects) = inbound.poll(&mut SequentialEnvironment::default()).next
        else {
            panic!("expected the context read");
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
