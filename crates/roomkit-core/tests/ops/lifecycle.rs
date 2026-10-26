//! The room lifecycle, effect by effect (RFC §5.1, §7.2, §7.5 rules 6 and 7,
//! §8.2, §9.2): creating a room, attaching, detaching and updating a
//! binding, each room transition, and the timers.
//!
//! The simulated driver checks on every run that the operation ended with
//! the room lock released.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use roomkit_core::model::{
    Access, BindingUpdate, ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput,
    ChannelType, DirectEvent, ErrorRecord, EventContent, EventType, HookResult, InjectedEvent,
    Room, RoomStatus, RoomTimers, Timestamp, Visibility,
};
use roomkit_core::ops::{
    ArchiveRoom, AttachChannel, CheckTimers, CloseRoom, CreateRoom, DetachChannel, Entry,
    FrameworkEvent, InboundError, LifecycleChange, LifecycleError, LifecyclePayload, Next,
    Operation, PauseRoom, RefusedOperation, ReopenRoom, ResumeRoom, SequentialEnvironment,
    TimersChecked, TransitionReason, UpdateBinding,
};
use roomkit_core::rules::attachment;
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookTrigger;
use roomkit_core::rules::lifecycle::Transition;
use roomkit_core::rules::timers::DueTransition;
use roomkit_core::rules::transcoding::text_of;

use crate::support::{Answer, Reentry, Sim, after_start, calling, channel, notice_event, room_id};

fn sms_binding() -> ChannelBinding {
    ChannelBinding::new(
        channel("sms"),
        room_id(),
        ChannelType::SMS,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
        Timestamp::UNIX_EPOCH,
    )
}

/// The channels `bindings` bind, in order.
fn bound(bindings: &[ChannelBinding]) -> Vec<String> {
    bindings
        .iter()
        .map(|binding| binding.channel_id.to_string())
        .collect()
}

/// The trace of a locked section that wrote nothing.
fn read_only_section() -> Vec<String> {
    ["lock room_1", "load_context room_1", "release room_1"]
        .map(str::to_owned)
        .to_vec()
}

mod create_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn stores_the_room_and_emits_room_created() {
        let mut sim = Sim::without_rooms();
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        let created = sim.run(CreateRoom::new(sim.shared_config(), room.clone()));
        assert_eq!(
            (created, sim.framework_event_names()),
            (Ok(room), vec!["room_created".to_owned()])
        );
    }

    /// RFC §10.1 step 2, gap G-050: the integrator's creation waits for its
    /// hooks, as the inbound pipeline's does, so the room it returns is
    /// ready for what the host sends next; `room_created` follows them.
    #[test]
    fn awaits_its_on_room_created_hooks_then_emits_room_created() {
        let mut sim = Sim::without_rooms();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        sim.run(CreateRoom::new(sim.shared_config(), room)).unwrap();
        assert_eq!(
            (sim.async_triggers(), sim.journal),
            (
                vec![],
                [
                    "store room room_1 Active",
                    "await on_room_created welcome",
                    "emit room_created"
                ]
                .map(str::to_owned)
                .to_vec()
            )
        );
    }

    /// Gap G-050: hooks that cannot be run are logged, and the room is
    /// created all the same.
    #[test]
    fn returns_the_room_when_its_hooks_cannot_be_run() {
        let mut sim = Sim::without_rooms();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        sim.failing_effect = Some("await on_room_created");
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        let created = sim.run(CreateRoom::new(sim.shared_config(), room.clone()));
        assert_eq!(created, Ok(room));
    }

    #[test]
    fn asks_for_no_hook_when_none_observes_the_trigger() {
        let mut sim = Sim::without_rooms();
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        sim.run(CreateRoom::new(sim.shared_config(), room)).unwrap();
        assert_eq!(sim.trace, ["store room room_1 Active"]);
    }

    /// RFC §10.6.5: the integrator never writes the implementation's keys.
    #[test]
    fn with_a_reserved_metadata_key_is_refused_before_any_effect() {
        let mut sim = Sim::new();
        let mut room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        room.metadata.insert(
            attachment::REVOCATIONS_KEY.to_owned(),
            serde_json::json!([]),
        );
        let created = sim.run(CreateRoom::new(sim.shared_config(), room));
        assert_eq!(
            (created, sim.trace.len()),
            (
                Err(LifecycleError::ReservedMetadataKey(
                    attachment::REVOCATIONS_KEY.to_owned()
                )),
                0
            )
        );
    }
}

mod attach_channel {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Gap G-046: the binding, then its lifecycle record, at the room's
    /// next index, both under the lock.
    #[test]
    fn stores_the_binding_then_its_record_under_the_room_lock() {
        let mut sim = Sim::new();
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None))
            .unwrap();
        assert_eq!(
            sim.trace,
            [
                "lock room_1",
                "load_context room_1",
                "store binding sms",
                "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                "release room_1",
            ]
        );
    }

    #[test]
    fn emits_room_channel_attached_with_the_binding_s_permissions() {
        let mut sim = Sim::new();
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None))
            .unwrap();
        assert_eq!(
            sim.framework_events(),
            [&FrameworkEvent::RoomChannelAttached {
                room_id: room_id(),
                channel_id: channel("sms"),
                access: sms_binding().access,
                visibility: sms_binding().visibility,
            }]
        );
    }

    #[test]
    fn fires_on_channel_attached_with_the_room_as_the_attach_left_it() {
        let mut sim = Sim::new();
        sim.transport("ws");
        sim.observer("greeter", HookTrigger::OnChannelAttached);
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None))
            .unwrap();
        assert_eq!(bound(&sim.async_contexts()[0].bindings), ["ws", "sms"]);
    }

    /// RFC §9.2: a hook reads the channel attached from its payload.
    #[test]
    fn fires_on_channel_attached_with_a_payload_naming_the_channel() {
        let mut sim = Sim::new();
        sim.observer("greeter", HookTrigger::OnChannelAttached);
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None))
            .unwrap();
        assert_eq!(
            LifecyclePayload::of(sim.async_payloads()[0]),
            Some(LifecyclePayload {
                change: LifecycleChange::ChannelAttached,
                room_id: room_id(),
                channel_id: Some(channel("sms")),
                channel_type: None,
                reason: None,
            })
        );
    }

    #[test]
    fn whose_lock_release_fails_still_attaches_and_reports() {
        let mut sim = Sim::new();
        sim.release_fails = true;
        let attached = sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None));
        assert_eq!(
            (attached, sim.framework_event_names()),
            (Ok(sms_binding()), vec!["room_channel_attached".to_owned()])
        );
    }

    #[test]
    fn to_a_missing_room_reports_the_room_when_the_release_fails_too() {
        let mut sim = Sim::new();
        sim.room_exists = false;
        sim.release_fails = true;
        let attached = sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None));
        assert_eq!(attached, Err(LifecycleError::RoomNotFound(room_id())));
    }

    #[test]
    fn to_a_missing_room_fails_once_the_lock_is_released_writing_nothing() {
        let mut sim = Sim::new();
        sim.room_exists = false;
        let attached = sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None));
        assert_eq!(
            (attached, sim.trace),
            (
                Err(LifecycleError::RoomNotFound(room_id())),
                read_only_section()
            )
        );
    }

    /// RFC §7.5 rule 7: an explicit attach is the integrator's decision.
    #[test]
    fn after_a_detach_clears_the_revocation_before_storing_the_binding() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ))
        .unwrap();
        sim.trace.clear();
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding(), None))
            .unwrap();
        assert_eq!(
            (
                sim.trace.clone(),
                attachment::revoked(&sim.room, &channel("sms"))
            ),
            (
                vec![
                    "lock room_1".to_owned(),
                    "load_context room_1".to_owned(),
                    "update room room_1 Active".to_owned(),
                    "store binding sms".to_owned(),
                    "commit #1 system delivered \"Channel sms attached (read_write, all)\""
                        .to_owned(),
                    "release room_1".to_owned(),
                ],
                false
            )
        );
    }

    /// RFC §6.2, §10.5: a host channel is bound as a transport only: a
    /// binding of one as an intelligence channel is refused before anything
    /// is read.
    #[test]
    fn of_a_host_channel_as_an_intelligence_channel_is_refused_before_any_effect() {
        let mut sim = Sim::new();
        let binding = ChannelBinding::new(
            channel("host"),
            room_id(),
            ChannelType::HOST,
            ChannelCategory::Intelligence,
            ChannelDirection::Inbound,
            Timestamp::UNIX_EPOCH,
        );
        let attached = sim.run(AttachChannel::new(sim.shared_config(), binding, None));
        assert_eq!(
            (attached, sim.trace.len()),
            (Err(LifecycleError::HostAsIntelligence(channel("host"))), 0)
        );
    }

    /// Gap G-041: only the framework writes `internal`.
    #[test]
    fn of_visibility_internal_is_refused_before_any_effect() {
        let mut sim = Sim::new();
        let binding = ChannelBinding {
            visibility: Visibility::Internal,
            ..sms_binding()
        };
        let attached = sim.run(AttachChannel::new(sim.shared_config(), binding, None));
        assert_eq!(
            (attached, sim.trace.len()),
            (Err(LifecycleError::InternalVisibility), 0)
        );
    }
}

/// RFC §7.5 rule 7: detaching is how an integrator revokes a channel.
mod detach_channel {
    use super::*;
    use pretty_assertions::assert_eq;

    fn detached() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.transport("ws");
        sim.observer("audit", HookTrigger::OnChannelDetached);
        sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ))
        .unwrap();
        sim
    }

    /// Gap G-046: then its lifecycle record.
    #[test]
    fn records_the_revocation_then_removes_the_binding_under_the_lock() {
        assert_eq!(
            detached().trace,
            [
                "lock room_1",
                "load_context room_1",
                "update room room_1 Active",
                "delete binding sms",
                "commit #0 system delivered \"Channel sms detached\"",
                "release room_1",
            ]
        );
    }

    #[test]
    fn keeps_the_revocation_in_the_room_as_python_does() {
        assert_eq!(
            detached().room.metadata.get("roomkit:detached_channels"),
            Some(&serde_json::json!(["sms"]))
        );
    }

    #[test]
    fn leaves_the_channel_unbound() {
        assert_eq!(bound(&detached().bindings), ["ws"]);
    }

    #[test]
    fn emits_room_channel_detached() {
        assert_eq!(
            detached().framework_events(),
            [&FrameworkEvent::RoomChannelDetached {
                room_id: room_id(),
                channel_id: channel("sms"),
            }]
        );
    }

    /// Gap G-027: the context of `ON_CHANNEL_DETACHED` is the room without
    /// the detached binding.
    #[test]
    fn fires_on_channel_detached_with_the_room_without_the_binding() {
        let sim = detached();
        assert_eq!(
            (
                sim.async_triggers(),
                bound(&sim.async_contexts()[0].bindings)
            ),
            (vec![HookTrigger::OnChannelDetached], vec!["ws".to_owned()])
        );
    }

    #[test]
    fn returns_the_removed_binding() {
        let mut sim = Sim::new();
        let binding = sim.transport("sms").clone();
        let detached = sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ));
        assert_eq!(detached, Ok(binding));
    }

    /// RFC §7.5 rule 2: a detached channel's message is its own no more.
    #[test]
    fn then_stores_the_channel_s_message_blocked_as_unbound() {
        let mut sim = detached();
        sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 system delivered \"Channel sms detached\"",
                "#1 sms blocked:source_unbound \"Hello?\"",
            ]
        );
    }

    /// Gap G-047: there is no binding to revoke.
    #[test]
    fn of_an_unbound_channel_fails_and_writes_nothing() {
        let mut sim = Sim::new();
        let detached = sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ));
        assert_eq!(
            (detached, sim.trace),
            (
                Err(LifecycleError::ChannelNotBound {
                    room_id: room_id(),
                    channel_id: channel("sms"),
                }),
                read_only_section()
            )
        );
    }
}

/// RFC §7.2 and §7.5 rule 6: mute, unmute, and every other binding change.
mod update_binding {
    use super::*;
    use pretty_assertions::assert_eq;

    fn room() -> Sim {
        let mut sim = Sim::new();
        sim.echo_agent("ai", "Hello back");
        sim.observer("on_muted", HookTrigger::OnChannelMuted);
        sim.observer("on_unmuted", HookTrigger::OnChannelUnmuted);
        sim
    }

    fn update(sim: &mut Sim, update: BindingUpdate) -> Result<ChannelBinding, LifecycleError> {
        sim.run(UpdateBinding::new(
            sim.shared_config(),
            room_id(),
            channel("ai"),
            update,
            None,
        ))
    }

    /// Gap G-046: then its lifecycle record.
    #[test]
    fn muting_stores_the_binding_then_its_record_under_the_lock() {
        let mut sim = room();
        update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(
            sim.trace,
            [
                "lock room_1",
                "load_context room_1",
                "store binding ai",
                "commit #0 system delivered \"Channel ai muted\"",
                "release room_1",
            ]
        );
    }

    #[test]
    fn muting_fires_on_channel_muted_with_the_muted_binding() {
        let mut sim = room();
        update(&mut sim, BindingUpdate::mute()).unwrap();
        let muted = sim.async_contexts()[0].bindings[0].muted;
        assert_eq!(
            (sim.async_triggers(), muted),
            (vec![HookTrigger::OnChannelMuted], true)
        );
    }

    #[test]
    fn unmuting_fires_on_channel_unmuted() {
        let mut sim = room();
        sim.bindings[0].muted = true;
        update(&mut sim, BindingUpdate::unmute()).unwrap();
        assert_eq!(sim.async_triggers(), [HookTrigger::OnChannelUnmuted]);
    }

    /// RFC §26.1: a mute fires its hooks and emits no framework event.
    #[test]
    fn muting_emits_no_framework_event() {
        let mut sim = room();
        update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(sim.framework_events(), Vec::<&FrameworkEvent>::new());
    }

    /// Gap G-047: a change that does not apply changes nothing.
    #[test]
    fn muting_a_muted_channel_writes_nothing_and_fires_nothing() {
        let mut sim = room();
        sim.bindings[0].muted = true;
        update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(
            (sim.async_triggers(), sim.trace),
            (Vec::new(), read_only_section())
        );
    }

    #[test]
    fn a_visibility_change_fires_no_hook() {
        let mut sim = room();
        let visibility: Visibility = "ws_advisor".parse().unwrap();
        let updated = update(&mut sim, BindingUpdate::visibility(visibility.clone())).unwrap();
        assert_eq!(
            (updated.visibility, sim.async_triggers()),
            (visibility, Vec::new())
        );
    }

    #[test]
    fn keeps_what_the_update_does_not_name() {
        let mut sim = room();
        sim.bindings[0].access = Access::ReadOnly;
        sim.bindings[0].visibility = Visibility::None;
        let updated = update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(
            (updated.access, updated.visibility),
            (Access::ReadOnly, Visibility::None)
        );
    }

    /// Gap G-041: only the framework writes `internal`.
    #[test]
    fn to_visibility_internal_is_refused_before_any_effect() {
        let mut sim = room();
        let refused = update(&mut sim, BindingUpdate::visibility(Visibility::Internal));
        assert_eq!(
            (refused, sim.trace.len()),
            (Err(LifecycleError::InternalVisibility), 0)
        );
    }

    #[test]
    fn of_an_unbound_channel_fails() {
        let mut sim = Sim::new();
        let refused = update(&mut sim, BindingUpdate::mute());
        assert_eq!(
            refused,
            Err(LifecycleError::ChannelNotBound {
                room_id: room_id(),
                channel_id: channel("ai"),
            })
        );
    }

    /// RFC §5.1 Lifecycle records, gap G-046: one record per field the
    /// update changed, in the Python implementation's order; the record is
    /// the framework's, `internal`, and no activity of the room.
    #[test]
    fn records_each_field_it_changed() {
        let mut sim = room();
        sim.room.timers.last_activity_at = Some(after_start(0));
        let mut metadata = serde_json::Map::new();
        metadata.insert("tier".to_owned(), serde_json::json!("gold"));
        let changes = BindingUpdate {
            muted: Some(true),
            access: Some(Access::ReadOnly),
            visibility: Some(Visibility::Transport),
            metadata,
            ..BindingUpdate::default()
        };
        update(&mut sim, changes).unwrap();
        let records: Vec<(EventType, String, Visibility)> = sim
            .timeline
            .iter()
            .map(|event| {
                (
                    event.event_type,
                    event.source.channel_id.to_string(),
                    event.visibility.clone(),
                )
            })
            .collect();
        assert_eq!(
            (
                sim.timeline_lines(),
                records[0].clone(),
                sim.room.timers.last_activity_at
            ),
            (
                [
                    "#0 system delivered \"Channel ai muted\"",
                    "#1 system delivered \"Channel ai access: read_write -> read_only\"",
                    "#2 system delivered \"Channel ai visibility: all -> transport\"",
                    "#3 system delivered \"Channel ai metadata updated\"",
                ]
                .map(str::to_owned)
                .to_vec(),
                (
                    EventType::ChannelMuted,
                    "system".to_owned(),
                    Visibility::Internal
                ),
                Some(after_start(0))
            )
        );
    }

    /// RFC §5.1, gap G-046: a CLOSED room's timeline does not grow because a
    /// binding changed: the binding is written, and no record.
    #[test]
    fn of_a_closed_room_writes_no_record() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(
            (sim.trace, sim.timeline.len()),
            (
                [
                    "lock room_1",
                    "load_context room_1",
                    "store binding ai",
                    "release room_1"
                ]
                .map(str::to_owned)
                .to_vec(),
                0
            )
        );
    }

    /// RFC §5.1: a record is no activity and is never delivered: a paused
    /// room stays paused, and its channels receive nothing.
    #[test]
    fn records_a_mute_of_a_paused_room_without_resuming_it_or_delivering_it() {
        let mut sim = room();
        sim.transport("sms");
        sim.room.status = RoomStatus::Paused;
        sim.room.paused_at = Some(after_start(0));
        update(&mut sim, BindingUpdate::mute()).unwrap();
        assert_eq!(
            (
                sim.room.status,
                sim.timeline.len(),
                sim.traced("on_event"),
                sim.traced("deliver")
            ),
            (
                RoomStatus::Paused,
                1,
                Vec::<&str>::new(),
                Vec::<&str>::new()
            )
        );
    }

    /// RFC §7.2: a muted channel still reads, and its response is stored
    /// blocked.
    #[test]
    fn a_muted_agent_s_answer_is_stored_blocked() {
        let mut sim = room();
        sim.transport("sms");
        update(&mut sim, BindingUpdate::mute()).unwrap();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 system delivered \"Channel ai muted\"",
                "#1 sms delivered \"Bonjour\"",
                "#2 ai blocked:source_muted \"Hello back\"",
            ]
        );
    }
}

/// RFC §5.1: each transition, from the statuses it leaves from.
mod room_transitions {
    use super::*;
    use pretty_assertions::assert_eq;

    fn in_status(status: RoomStatus) -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.room.status = status;
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.observer("on_closed", HookTrigger::OnRoomClosed);
        sim
    }

    fn run(sim: &mut Sim, transition: Transition) -> Room {
        attempt(sim, transition).unwrap()
    }

    /// The operation of `transition` on the room, as it ended.
    fn attempt(sim: &mut Sim, transition: Transition) -> Result<Room, LifecycleError> {
        let config = sim.shared_config();
        match transition {
            Transition::Pause => sim.run(PauseRoom::new(config, room_id(), None)),
            Transition::Resume => sim.run(ResumeRoom::new(config, room_id(), None)),
            Transition::Reopen => sim.run(ReopenRoom::new(config, room_id(), None)),
            Transition::Close => sim.run(CloseRoom::new(config, room_id(), None)),
            Transition::Archive => sim.run(ArchiveRoom::new(config, room_id(), None)),
        }
    }

    #[test]
    fn a_pause_writes_the_room_under_its_lock() {
        let mut sim = in_status(RoomStatus::Active);
        run(&mut sim, Transition::Pause);
        assert_eq!(
            sim.trace,
            [
                "lock room_1",
                "load_context room_1",
                "update room room_1 Paused",
                "release room_1",
            ]
        );
    }

    #[test]
    fn a_pause_records_its_instant() {
        let mut sim = in_status(RoomStatus::Active);
        let paused = run(&mut sim, Transition::Pause);
        assert_eq!(
            (paused.status, paused.paused_at),
            (RoomStatus::Paused, Some(after_start(0)))
        );
    }

    /// RFC §26.1: `room_paused` reports the timer's pause only.
    #[test]
    fn the_integrator_s_pause_fires_on_room_paused_without_room_paused() {
        let mut sim = in_status(RoomStatus::Active);
        run(&mut sim, Transition::Pause);
        assert_eq!(
            (sim.async_triggers(), sim.framework_event_names()),
            (vec![HookTrigger::OnRoomPaused], Vec::new())
        );
    }

    #[test]
    fn a_paused_room_still_accepts_events() {
        let mut sim = in_status(RoomStatus::Active);
        run(&mut sim, Transition::Pause);
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert!(!result.blocked);
    }

    #[test]
    fn a_resume_restarts_the_inactivity_timer_and_reports_nothing() {
        let mut sim = in_status(RoomStatus::Paused);
        sim.env.advance(Duration::from_secs(30));
        let resumed = run(&mut sim, Transition::Resume);
        assert_eq!(
            (
                resumed.status,
                resumed.timers.last_activity_at,
                sim.notices.len()
            ),
            (RoomStatus::Active, Some(after_start(30)), 0)
        );
    }

    #[test]
    fn a_reopened_room_accepts_events_again() {
        let mut sim = in_status(RoomStatus::Closed);
        run(&mut sim, Transition::Reopen);
        let result = sim.send(Sim::message("sms", "I am back")).unwrap();
        assert!(!result.blocked);
    }

    #[test]
    fn a_reopen_reports_nothing() {
        let mut sim = in_status(RoomStatus::Closed);
        run(&mut sim, Transition::Reopen);
        assert_eq!(sim.notices, []);
    }

    #[test]
    fn a_close_emits_room_closed_without_a_reason_and_fires_on_room_closed() {
        let mut sim = in_status(RoomStatus::Paused);
        run(&mut sim, Transition::Close);
        assert_eq!(
            (sim.framework_events(), sim.async_triggers()),
            (
                vec![&FrameworkEvent::RoomClosed {
                    room_id: room_id(),
                    reason: None,
                }],
                vec![HookTrigger::OnRoomClosed]
            )
        );
    }

    /// RFC §26.1: an archive emits `room_archived` and fires no hook.
    #[test]
    fn an_archive_emits_room_archived_and_fires_no_hook() {
        let mut sim = in_status(RoomStatus::Closed);
        let archived = run(&mut sim, Transition::Archive);
        assert_eq!(
            (
                archived.status,
                sim.framework_events(),
                sim.async_triggers()
            ),
            (
                RoomStatus::Archived,
                vec![&FrameworkEvent::RoomArchived { room_id: room_id() }],
                Vec::new()
            )
        );
    }

    /// RFC §5.1, gap G-074: a transition called from a status it does not
    /// leave from fails, writing and reporting nothing, unless the room is
    /// in the status the transition names already. Only a CLOSED room is
    /// reopened, and ARCHIVED is never left.
    #[test]
    fn from_a_status_it_does_not_leave_from_is_refused() {
        let refused = [
            (Transition::Pause, RoomStatus::Closed),
            (Transition::Pause, RoomStatus::Archived),
            (Transition::Resume, RoomStatus::Closed),
            (Transition::Resume, RoomStatus::Archived),
            (Transition::Reopen, RoomStatus::Active),
            (Transition::Reopen, RoomStatus::Paused),
            (Transition::Reopen, RoomStatus::Archived),
            (Transition::Close, RoomStatus::Archived),
        ];
        for (transition, status) in refused {
            let mut sim = in_status(status);
            let result = attempt(&mut sim, transition);
            assert_eq!(
                (result, sim.trace, sim.notices.len()),
                (
                    Err(LifecycleError::TransitionRefused {
                        room_id: room_id(),
                        transition,
                        status,
                    }),
                    read_only_section(),
                    0
                ),
                "a {transition:?} of a room {status:?}"
            );
        }
    }

    /// Gap G-074: a transition whose room is in its status already has
    /// nothing to do, as closing a closed room does not close it twice.
    #[test]
    fn already_done_succeeds_writing_and_reporting_nothing() {
        let done = [
            (Transition::Pause, RoomStatus::Paused),
            (Transition::Resume, RoomStatus::Active),
            (Transition::Close, RoomStatus::Closed),
            (Transition::Archive, RoomStatus::Archived),
        ];
        for (transition, status) in done {
            let mut sim = in_status(status);
            let before = sim.room.clone();
            let room = attempt(&mut sim, transition);
            assert_eq!(
                (room, sim.trace, sim.notices.len()),
                (Ok(before), read_only_section(), 0),
                "a {transition:?} of a room {status:?}"
            );
        }
    }
}

mod close_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn closes_the_room_under_its_lock() {
        let mut sim = Sim::new();
        let closed = sim
            .run(CloseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(
            (closed.status, closed.closed_at.is_some(), sim.room.status),
            (RoomStatus::Closed, true, RoomStatus::Closed)
        );
    }

    #[test]
    fn fires_on_room_closed_with_the_closed_room() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.observer("archive", HookTrigger::OnRoomClosed);
        sim.run(CloseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        let context = sim.async_contexts()[0];
        assert_eq!(
            (context.room.status, context.bindings.len()),
            (RoomStatus::Closed, 1)
        );
    }

    #[test]
    fn fires_on_room_closed_with_the_recent_events() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.seed(notice_event("evt_1", "Goodbye"));
        sim.observer("summarizer", HookTrigger::OnRoomClosed);
        sim.run(CloseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(sim.async_contexts()[0].recent_events.len(), 1);
    }

    #[test]
    fn then_refuses_the_next_message() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.run(CloseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        let result = sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(result.reason.as_deref(), Some("room_closed"));
    }
}

/// A room whose timers pause it after 60 seconds without activity and close
/// it 120 seconds after the pause, its last activity at the clock's start.
fn timed_room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.room.created_at = after_start(0);
    sim.room.timers = RoomTimers {
        inactive_after_seconds: Some(60),
        closed_after_seconds: Some(120),
        last_activity_at: Some(after_start(0)),
    };
    sim
}

/// The timed room, paused at the clock's start.
fn paused_room() -> Sim {
    let mut sim = timed_room();
    sim.room.status = RoomStatus::Paused;
    sim.room.paused_at = Some(after_start(0));
    sim
}

/// RFC §5.1, gap G-045: the timers of a room nobody writes to.
mod check_timers {
    use super::*;
    use pretty_assertions::assert_eq;

    fn check(sim: &mut Sim) -> TimersChecked {
        sim.run(CheckTimers::new(sim.shared_config(), room_id()))
            .unwrap()
    }

    #[test]
    fn pauses_an_inactive_room_at_the_instant_it_fell_due() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(90));
        let checked = check(&mut sim);
        assert_eq!(
            (checked.room.status, checked.room.paused_at, sim.trace),
            (
                RoomStatus::Paused,
                Some(after_start(60)),
                vec![
                    "lock room_1".to_owned(),
                    "load_context room_1".to_owned(),
                    "update room room_1 Paused".to_owned(),
                    "commit #0 system delivered \"Room room_1 paused by timer\"".to_owned(),
                    "release room_1".to_owned(),
                ]
            )
        );
    }

    #[test]
    fn emits_room_paused_with_the_reason_timer_and_fires_on_room_paused() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.env.advance(Duration::from_secs(90));
        check(&mut sim);
        assert_eq!(
            (sim.framework_events(), sim.async_triggers()),
            (
                vec![&FrameworkEvent::RoomPaused {
                    room_id: room_id(),
                    reason: TransitionReason::Timer,
                }],
                vec![HookTrigger::OnRoomPaused]
            )
        );
    }

    #[test]
    fn closes_a_paused_room_once_its_close_falls_due() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(120));
        let checked = check(&mut sim);
        assert_eq!(
            (checked.room.status, checked.room.closed_at),
            (RoomStatus::Closed, Some(after_start(120)))
        );
    }

    #[test]
    fn emits_room_closed_with_the_reason_timer() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(120));
        check(&mut sim);
        assert_eq!(
            sim.framework_events(),
            [&FrameworkEvent::RoomClosed {
                room_id: room_id(),
                reason: Some(TransitionReason::Timer),
            }]
        );
    }

    #[test]
    fn names_the_timer_in_the_payload_of_on_room_closed() {
        let mut sim = paused_room();
        sim.observer("on_closed", HookTrigger::OnRoomClosed);
        sim.env.advance(Duration::from_secs(120));
        check(&mut sim);
        let payload = LifecyclePayload::of(sim.async_payloads()[0]);
        assert_eq!(
            payload.map(|payload| (payload.change, payload.reason)),
            Some((LifecycleChange::RoomClosed, Some(TransitionReason::Timer)))
        );
    }

    #[test]
    fn applies_both_transitions_of_a_late_check_at_the_instants_they_fell_due() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(3600));
        let checked = check(&mut sim);
        assert_eq!(
            (
                checked.transitions,
                checked.room.paused_at,
                checked.room.closed_at,
                sim.framework_event_names()
            ),
            (
                vec![
                    DueTransition {
                        transition: Transition::Pause,
                        at: after_start(60),
                    },
                    DueTransition {
                        transition: Transition::Close,
                        at: after_start(180),
                    },
                ],
                Some(after_start(60)),
                Some(after_start(180)),
                vec!["room_paused".to_owned(), "room_closed".to_owned()]
            )
        );
    }

    #[test]
    fn writes_both_transitions_of_a_late_check_at_once() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(3600));
        check(&mut sim);
        assert_eq!(sim.traced("update room"), ["update room room_1 Closed"]);
    }

    #[test]
    fn hands_the_pause_s_hooks_the_room_as_the_pause_left_it() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.env.advance(Duration::from_secs(3600));
        check(&mut sim);
        assert_eq!(sim.async_contexts()[0].room.status, RoomStatus::Paused);
    }

    #[test]
    fn changes_nothing_a_second_before_a_timer_falls_due() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(59));
        let checked = check(&mut sim);
        assert_eq!(
            (checked.transitions, sim.trace),
            (Vec::new(), read_only_section())
        );
    }
}

/// RFC §10.1 step 6, gap G-045: every status gate applies the due
/// transitions before it decides.
mod the_status_gate {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn refuses_a_message_whose_room_s_close_fell_due_after_the_timer_s_room_closed() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(121));
        let result = sim.send(Sim::message("sms", "Still there?")).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.framework_events(),
                sim.timeline.len()
            ),
            (
                Some("room_closed"),
                vec![
                    &FrameworkEvent::RoomClosed {
                        room_id: room_id(),
                        reason: Some(TransitionReason::Timer),
                    },
                    &FrameworkEvent::RoomRefusedEvent {
                        room_id: room_id(),
                        event_id: "evt_1".parse().unwrap(),
                        status: RoomStatus::Closed,
                        operation: RefusedOperation::Inbound,
                        event_type: EventType::Message,
                    },
                ],
                // The record of the close alone (gap G-046).
                1
            )
        );
    }

    /// Gap G-046: the close, then its record, which the CLOSED room takes.
    #[test]
    fn writes_the_due_close_and_its_record_under_the_lock_before_the_refusal() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(121));
        sim.send(Sim::message("sms", "Still there?")).unwrap();
        assert_eq!(
            sim.trace[2..],
            [
                "lock room_1",
                "load_context room_1",
                "update room room_1 Closed",
                "commit #0 system delivered \"Room room_1 closed by timer\"",
                "release room_1",
            ]
        );
    }

    #[test]
    fn pauses_an_inactive_room_before_its_message_resumes_it() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(90));
        let result = sim.send(Sim::message("sms", "Hello again")).unwrap();
        assert_eq!(
            (
                result.blocked,
                sim.framework_event_names().first().cloned(),
                sim.room.status,
                sim.room.paused_at
            ),
            (
                false,
                Some("room_paused".to_owned()),
                RoomStatus::Active,
                None
            )
        );
    }

    /// A refused pass of the framework's own: the room closed during its
    /// cascade (gap G-043).
    fn refused_as_reentry(event_id: &str) -> FrameworkEvent {
        FrameworkEvent::RoomRefusedEvent {
            room_id: room_id(),
            event_id: event_id.parse().unwrap(),
            status: RoomStatus::Closed,
            operation: RefusedOperation::Reentry,
            event_type: EventType::Message,
        }
    }

    #[test]
    fn refuses_a_response_whose_room_s_close_fell_due_during_its_turn() {
        let mut sim = timed_room();
        sim.echo_agent("ai", "Hello back");
        sim.delivery_duration = Duration::from_secs(200);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.timeline_lines(),
                sim.framework_event_names(),
                sim.framework_events().last().copied().cloned()
            ),
            (
                // The records of the pause and the close (gap G-046).
                vec![
                    "#0 sms delivered \"Bonjour\"".to_owned(),
                    "#1 system delivered \"Room room_1 paused by timer\"".to_owned(),
                    "#2 system delivered \"Room room_1 closed by timer\"".to_owned(),
                ],
                [
                    "event_processed",
                    "room_paused",
                    "room_closed",
                    "room_refused_event"
                ]
                .map(str::to_owned)
                .to_vec(),
                Some(refused_as_reentry("evt_2"))
            )
        );
    }

    #[test]
    fn refuses_an_injected_event_whose_room_s_close_fell_due_during_the_delivery() {
        let mut sim = timed_room();
        sim.transport("ws");
        sim.before_broadcast(
            "notify",
            Box::new(|event| {
                if event.source.channel_id.as_str() != "sms" {
                    return HookOutcome::Returned(HookResult::allow());
                }
                HookOutcome::Returned(HookResult::allow().inject(InjectedEvent::to_channels(
                    notice_event("inj_1", "Heads up"),
                    vec![channel("ws")],
                )))
            }),
        );
        sim.delivery_duration = Duration::from_secs(100);
        sim.send(Sim::message("sms", "hello")).unwrap();
        assert_eq!(
            (
                sim.timeline_lines(),
                sim.framework_event_names()[2..].to_vec(),
                sim.framework_events().last().copied().cloned()
            ),
            (
                vec![
                    "#0 sms delivered \"hello\"".to_owned(),
                    "#1 system delivered \"Room room_1 paused by timer\"".to_owned(),
                    "#2 system delivered \"Room room_1 closed by timer\"".to_owned(),
                ],
                ["room_paused", "room_closed", "room_refused_event"]
                    .map(str::to_owned)
                    .to_vec(),
                Some(refused_as_reentry("evt_2"))
            )
        );
    }

    /// RFC §8.3: the record of an agent not asked at the chain-depth limit
    /// goes through the gate too, and a closed room stores nothing; the
    /// refusal is reported as the framework's own re-injection (gap G-043).
    #[test]
    fn refuses_a_record_whose_room_s_close_fell_due_during_the_delivery() {
        let mut sim = timed_room();
        sim.transport("ws");
        sim.echo_agent("ai", "Too deep");
        sim.delivery_duration = Duration::from_secs(100);
        let mut message = Sim::message("sms", "Go deeper");
        message.chain_depth = 4;
        sim.send(message).unwrap();
        assert_eq!(
            (
                sim.timeline_lines(),
                sim.framework_event_names(),
                sim.trace[sim.trace.len() - 7..].to_vec()
            ),
            (
                vec![
                    "#0 sms delivered \"Go deeper\"".to_owned(),
                    "#1 system delivered \"Room room_1 paused by timer\"".to_owned(),
                    "#2 system delivered \"Room room_1 closed by timer\"".to_owned(),
                ],
                [
                    "delivery_succeeded",
                    "event_processed",
                    "room_paused",
                    "room_closed",
                    "room_refused_event"
                ]
                .map(str::to_owned)
                .to_vec(),
                [
                    "leave lane room_1 evt_1",
                    "lock room_1",
                    "load_context room_1",
                    "update room room_1 Closed",
                    "commit #1 system delivered \"Room room_1 paused by timer\"",
                    "commit #2 system delivered \"Room room_1 closed by timer\"",
                    "release room_1"
                ]
                .map(str::to_owned)
                .to_vec()
            )
        );
    }

    #[test]
    fn applies_a_due_close_before_a_detach_decides() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(121));
        sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ))
        .unwrap();
        assert_eq!(
            (sim.framework_event_names(), sim.trace),
            (
                vec!["room_closed".to_owned(), "room_channel_detached".to_owned()],
                // The close's record; a CLOSED room takes no record of the
                // detach (RFC §5.1, gap G-046).
                [
                    "lock room_1",
                    "load_context room_1",
                    "update room room_1 Closed",
                    "commit #0 system delivered \"Room room_1 closed by timer\"",
                    "update room room_1 Closed",
                    "delete binding sms",
                    "release room_1"
                ]
                .map(str::to_owned)
                .to_vec()
            )
        );
    }

    #[test]
    fn applies_a_due_close_before_a_binding_update_decides() {
        let mut sim = paused_room();
        sim.observer("on_closed", HookTrigger::OnRoomClosed);
        sim.observer("on_muted", HookTrigger::OnChannelMuted);
        sim.env.advance(Duration::from_secs(121));
        sim.run(UpdateBinding::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            BindingUpdate::mute(),
            None,
        ))
        .unwrap();
        let mute_context = sim.async_contexts()[1].room.status;
        assert_eq!(
            (sim.async_triggers(), mute_context),
            (
                vec![HookTrigger::OnRoomClosed, HookTrigger::OnChannelMuted],
                RoomStatus::Closed
            )
        );
    }

    #[test]
    fn applies_a_due_close_before_a_lifecycle_change() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(121));
        let ws = ChannelBinding {
            channel_id: channel("ws"),
            ..sms_binding()
        };
        sim.run(AttachChannel::new(sim.shared_config(), ws, None))
            .unwrap();
        assert_eq!(
            (sim.room.status, sim.framework_event_names()),
            (
                RoomStatus::Closed,
                vec!["room_closed".to_owned(), "room_channel_attached".to_owned()]
            )
        );
    }
}

/// RFC §5.1 When (amended by 0871aec), gap G-045: a timer transition's
/// framework event is emitted once its write is stored, before what the
/// section decides next; its trigger's hooks fire once the room lock is
/// released, as an ASYNC trigger's do.
mod a_timer_transition_s_report {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn fires_its_hooks_once_a_refused_message_s_lock_is_released() {
        let mut sim = paused_room();
        sim.observer("on_closed", HookTrigger::OnRoomClosed);
        sim.env.advance(Duration::from_secs(120));
        let result = sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.framework_event_names(),
                sim.fired.clone()
            ),
            (
                Some("room_closed"),
                vec!["room_closed".to_owned(), "room_refused_event".to_owned()],
                vec![(HookTrigger::OnRoomClosed, false)]
            )
        );
    }

    /// A locked hook that injects a note into the room, the entry's read of
    /// the room made `hook_time` into the hook: the entry, nested in the
    /// trigger's section, applies what fell due meanwhile at its own gate.
    fn note_from_a_slow_hook(sim: &mut Sim, hook_time: Duration) {
        sim.reentrant_hook(
            "slow",
            true,
            Reentry {
                entries: calling(|event| {
                    (text_of(&event.content) == "Hello?").then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("sms"),
                            EventContent::text("Note"),
                        )),
                    })
                }),
                answer: Answer::AtOnce(Box::new(|| HookOutcome::Returned(HookResult::allow()))),
            },
        );
        // The third read is the entry's, after the trigger's step 2 and gate.
        sim.nth_durations.push(("load_context", 3, hook_time));
    }

    /// An entry nested under a locked hook releases no lock of its own: the
    /// pause its gate applies fires its hooks once the trigger's section,
    /// which holds the lock, released it.
    #[test]
    fn fires_a_nested_entry_s_hooks_once_the_outer_lock_is_released() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        note_from_a_slow_hook(&mut sim, Duration::from_secs(90));
        sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(
            (sim.room.status, sim.fired.clone()),
            (RoomStatus::Active, vec![(HookTrigger::OnRoomPaused, false)])
        );
    }

    /// The same with a close falling due under the hook: the entry is
    /// refused, the trigger too on its re-read, and the close's hooks fire
    /// after the trigger's release.
    #[test]
    fn fires_a_nested_entry_s_close_hooks_once_the_outer_lock_is_released() {
        let mut sim = paused_room();
        sim.observer("on_closed", HookTrigger::OnRoomClosed);
        note_from_a_slow_hook(&mut sim, Duration::from_secs(150));
        let result = sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(
            (sim.room.status, result.reason.as_deref(), sim.fired.clone()),
            (
                RoomStatus::Closed,
                Some("room_closed"),
                vec![(HookTrigger::OnRoomClosed, false)]
            )
        );
    }

    #[test]
    fn fires_its_hooks_once_a_lifecycle_change_s_lock_is_released() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.env.advance(Duration::from_secs(90));
        sim.run(PauseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(sim.fired, vec![(HookTrigger::OnRoomPaused, false)]);
    }
}

/// Gap G-045: an explicit change decides on the room as its due timers
/// left it.
mod an_explicit_change_on_a_room_whose_timer_is_due {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn a_pause_finds_the_room_paused_by_its_timer_at_the_instant_it_fell_due() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.env.advance(Duration::from_secs(90));
        let paused = sim
            .run(PauseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(
            (
                paused.paused_at,
                sim.framework_events(),
                sim.async_triggers()
            ),
            (
                Some(after_start(60)),
                vec![&FrameworkEvent::RoomPaused {
                    room_id: room_id(),
                    reason: TransitionReason::Timer,
                }],
                vec![HookTrigger::OnRoomPaused]
            )
        );
    }

    #[test]
    fn a_close_follows_the_timer_s_pause() {
        let mut sim = timed_room();
        sim.env.advance(Duration::from_secs(90));
        let closed = sim
            .run(CloseRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(
            (closed.closed_at, sim.framework_events()),
            (
                Some(after_start(90)),
                vec![
                    &FrameworkEvent::RoomPaused {
                        room_id: room_id(),
                        reason: TransitionReason::Timer,
                    },
                    &FrameworkEvent::RoomClosed {
                        room_id: room_id(),
                        reason: None,
                    },
                ]
            )
        );
    }

    #[test]
    fn a_reopen_follows_the_timer_s_close() {
        let mut sim = paused_room();
        sim.env.advance(Duration::from_secs(121));
        let reopened = sim
            .run(ReopenRoom::new(sim.shared_config(), room_id(), None))
            .unwrap();
        assert_eq!(
            (
                reopened.status,
                reopened.closed_at,
                reopened.timers.last_activity_at,
                sim.framework_events()
            ),
            (
                RoomStatus::Active,
                None,
                Some(after_start(121)),
                vec![&FrameworkEvent::RoomClosed {
                    room_id: room_id(),
                    reason: Some(TransitionReason::Timer),
                }]
            )
        );
    }
}

/// A store write that fails under the lock: the lock is released, nothing
/// is reported, and the operation fails with the store's error.
mod a_failed_store_write {
    use super::*;
    use pretty_assertions::assert_eq;

    fn store_down() -> ErrorRecord {
        ErrorRecord::new("StoreUnavailable", "the store is down")
    }

    #[test]
    fn in_the_gate_fails_the_timer_check() {
        let mut sim = timed_room();
        sim.observer("on_paused", HookTrigger::OnRoomPaused);
        sim.failing_effect = Some("update room");
        sim.env.advance(Duration::from_secs(90));
        let checked = sim.run(CheckTimers::new(sim.shared_config(), room_id()));
        assert_eq!(
            (checked, sim.trace, sim.notices, sim.room.status),
            (
                Err(LifecycleError::Store(store_down())),
                [
                    "lock room_1",
                    "load_context room_1",
                    "update room room_1 failed",
                    "release room_1"
                ]
                .map(str::to_owned)
                .to_vec(),
                Vec::new(),
                RoomStatus::Active
            )
        );
    }

    #[test]
    fn in_the_gate_fails_a_message_before_anything_is_committed() {
        let mut sim = paused_room();
        sim.failing_effect = Some("update room");
        sim.env.advance(Duration::from_secs(121));
        let sent = sim.send(Sim::message("sms", "Still there?"));
        assert_eq!(
            (
                sent,
                sim.trace[2..].to_vec(),
                sim.notices,
                sim.timeline.len()
            ),
            (
                Err(InboundError::Store(store_down())),
                [
                    "lock room_1",
                    "load_context room_1",
                    "update room room_1 failed",
                    "release room_1",
                ]
                .map(str::to_owned)
                .to_vec(),
                Vec::new(),
                0
            )
        );
    }

    #[test]
    fn in_a_detach_leaves_the_channel_revoked_and_bound_unreported() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.observer("audit", HookTrigger::OnChannelDetached);
        sim.failing_effect = Some("delete binding");
        let detached = sim.run(DetachChannel::new(
            sim.shared_config(),
            room_id(),
            channel("sms"),
            None,
        ));
        assert_eq!(
            (
                detached,
                sim.trace.clone(),
                sim.notices.len(),
                bound(&sim.bindings),
                attachment::revoked(&sim.room, &channel("sms"))
            ),
            (
                Err(LifecycleError::Store(store_down())),
                [
                    "lock room_1",
                    "load_context room_1",
                    "update room room_1 Active",
                    "delete binding sms failed",
                    "release room_1"
                ]
                .map(str::to_owned)
                .to_vec(),
                0,
                vec!["sms".to_owned()],
                true
            )
        );
    }
}

/// Gap G-045: what resumes a paused room.
mod a_paused_room {
    use super::*;
    use pretty_assertions::assert_eq;

    fn paused() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.room.status = RoomStatus::Paused;
        sim.room.paused_at = Some(Timestamp::UNIX_EPOCH);
        sim
    }

    #[test]
    fn is_resumed_by_a_delivered_commit_in_the_same_write() {
        let mut sim = paused();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.room.status,
                sim.room.paused_at,
                sim.traced("update room")
            ),
            (RoomStatus::Active, None, Vec::<&str>::new())
        );
    }

    #[test]
    fn is_read_active_by_the_delivery_set_of_the_event_that_resumed_it() {
        let mut sim = paused();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let record = Rc::clone(&seen);
        sim.agent(
            "ai",
            Box::new(move |_, context| {
                record.borrow_mut().push(context.room.status);
                Ok(ChannelOutput::default())
            }),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(*seen.borrow(), [RoomStatus::Active]);
    }

    #[test]
    fn stays_paused_after_a_blocked_commit() {
        let mut sim = paused();
        sim.before_broadcast(
            "refuse",
            Box::new(|_| HookOutcome::Returned(HookResult::block("not now"))),
        );
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.room.status, RoomStatus::Paused);
    }
}

/// The effect protocol, for the lifecycle operations.
mod the_protocol {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn a_refused_change_ends_at_its_first_poll() {
        let sim = Sim::new();
        let mut update = UpdateBinding::new(
            sim.shared_config(),
            room_id(),
            channel("ai"),
            BindingUpdate::visibility(Visibility::Internal),
            None,
        );
        let step = update.poll(&SequentialEnvironment::default());
        assert_eq!(
            step.next,
            Next::Ready(Err(LifecycleError::InternalVisibility))
        );
    }
}
