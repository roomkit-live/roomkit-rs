//! Discussion selection leaves queuing outside its lock and lifetime (§13.6).

use crate::support::{Sim, notice_event, room_id};
use roomkit_core::model::EventStatus;
use roomkit_core::ops::{
    FrameworkEvent, RefusedOperation, RegenerationSelection, SelectRegeneration,
};
use std::time::Duration;

fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    let mut event = notice_event("evt_0", "hello");
    event.source.channel_id = "sms".parse().unwrap();
    event.status = EventStatus::Delivered;
    sim.seed(event);
    sim
}

#[test]
fn selection_returns_the_locked_history_without_delivering_or_rewriting_it() {
    let mut sim = room();
    sim.release_fails = true;
    let selection = SelectRegeneration::new(sim.shared_config(), room_id(), None);
    let RegenerationSelection::Selected {
        trigger,
        context,
        release_error,
    } = sim.run(selection).unwrap()
    else {
        panic!("no selection");
    };
    assert_eq!(trigger.id.as_str(), "evt_0");
    assert_eq!(context.recent_events, vec![*trigger]);
    assert_eq!(release_error.unwrap().message, "the lock's lease was lost");
    assert_eq!(sim.timeline.len(), 1);
    assert_eq!(sim.traced("on_event"), Vec::<&str>::new());
    assert_eq!(sim.traced("enter lane"), Vec::<&str>::new());
    assert_eq!(sim.traced("release room_1").len(), 1);
}

#[test]
fn selection_bounds_both_lock_wait_and_context_read() {
    for step in ["lock room_1", "load_context room_1"] {
        let mut sim = room();
        sim.config.process_timeout = Some(Duration::from_secs(1));
        sim.durations.push((step, Duration::from_secs(2)));
        let selection = SelectRegeneration::new(sim.shared_config(), room_id(), None);
        let RegenerationSelection::Refused(result) = sim.run(selection).unwrap() else {
            panic!("not refused");
        };
        assert!(result.blocked);
        assert_eq!(result.reason.as_deref(), Some("process_timeout"));
        assert_eq!(sim.timeline.len(), 1);
        assert_eq!(
            sim.traced("release room_1").len(),
            usize::from(step.starts_with("load"))
        );
        assert_eq!(
            sim.framework_events()
                .iter()
                .filter(|event| matches!(
                    event,
                    FrameworkEvent::ProcessTimeout {
                        operation: Some(RefusedOperation::Regenerate),
                        ..
                    }
                ))
                .count(),
            1
        );
    }
}
