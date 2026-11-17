//! Synchronous binding notifications retain writes and fail before records.

use roomkit_core::model::{Access, BindingUpdate, ChannelBinding, ErrorRecord};
use roomkit_core::ops::{LifecycleError, UpdateBinding};
use roomkit_core::rules::hook_registry::HookTrigger;

use crate::support::{Sim, channel, room_id};

fn changes() -> [BindingUpdate; 5] {
    [
        BindingUpdate::mute(),
        BindingUpdate::unmute(),
        BindingUpdate::mute_output(),
        BindingUpdate::unmute_output(),
        BindingUpdate {
            access: Some(Access::ReadOnly),
            ..BindingUpdate::default()
        },
    ]
}

fn setup(change: &BindingUpdate, repeated: bool) -> (Sim, ChannelBinding) {
    let mut sim = Sim::new();
    sim.echo_agent("ai", "unused");
    sim.observer("on_muted", HookTrigger::OnChannelMuted);
    sim.observer("on_unmuted", HookTrigger::OnChannelUnmuted);
    let binding = sim.bindings.first_mut().unwrap();
    if let Some(muted) = change.muted {
        binding.muted = !muted;
    }
    if let Some(muted) = change.output_muted {
        binding.output_muted = !muted;
    }
    let expected = change.apply(binding);
    if repeated {
        *binding = expected.clone();
    }
    (sim, expected)
}

fn update(sim: &mut Sim, change: BindingUpdate) -> Result<ChannelBinding, LifecycleError> {
    sim.run(UpdateBinding::new(
        sim.shared_config(),
        room_id(),
        channel("ai"),
        change,
        None,
    ))
}

// Python: tests/test_binding_updates.py::test_a_callback_refusal_keeps_storage_and_stops_records_and_hooks
#[test]
fn a_refused_notification_keeps_storage_but_stops_records_and_hooks() {
    for change in changes() {
        for repeated in [false, true] {
            let (mut sim, expected) = setup(&change, repeated);
            sim.failing_effect = Some("update_binding ai");
            let result = update(&mut sim, change.clone());
            assert_eq!(
                result,
                Err(LifecycleError::ChannelFailed {
                    channel_id: channel("ai"),
                    error: ErrorRecord::new("ChannelError", "the outside world said no"),
                })
            );
            assert_eq!(sim.bindings, [expected]);
            assert_eq!(sim.timeline.len(), 0);
            assert_eq!(sim.async_triggers().len(), 0);
            assert_eq!(sim.framework_events().len(), 0);
            let mut trace = vec!["lock room_1", "load_context room_1"];
            if !repeated {
                trace.push("store binding ai");
            }
            trace.extend(["update_binding ai failed", "release room_1"]);
            assert_eq!(sim.trace, trace);
        }
    }
}

// Python: tests/test_lifecycle_hooks.py::test_a_binding_change_that_changes_nothing_writes_and_fires_nothing
#[test]
fn every_unchanged_gate_notifies_once_without_writes_or_hooks() {
    for change in changes() {
        let (mut sim, expected) = setup(&change, true);
        assert_eq!(update(&mut sim, change), Ok(expected.clone()));
        assert_eq!(sim.bindings, [expected]);
        assert_eq!(sim.timeline.len(), 0);
        assert_eq!(sim.async_triggers().len(), 0);
        assert_eq!(
            sim.trace,
            [
                "lock room_1",
                "load_context room_1",
                "update_binding ai",
                "release room_1",
            ]
        );
    }
}

// Python: tests/test_binding_updates.py::test_a_gate_change_notifies_after_storage_before_its_hook
#[test]
fn failed_storage_never_notifies_the_channel() {
    let (mut sim, _) = setup(&BindingUpdate::mute(), false);
    sim.failing_effect = Some("store binding ai");
    assert!(update(&mut sim, BindingUpdate::mute()).is_err());
    assert!(!sim.bindings.first().unwrap().muted);
    assert_eq!(sim.traced("update_binding").len(), 0);
    assert_eq!(sim.timeline.len(), 0);
    assert_eq!(sim.async_triggers().len(), 0);
}
