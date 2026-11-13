//! The empty-queue handoff must not let two tasks drain the same turn.

use super::*;

fn event() -> EphemeralEvent {
    EphemeralEvent::new(
        RoomId::new("room").unwrap(),
        EphemeralEventType::ToolCallDelta,
        "agent",
    )
}

// Shared contract: tool-composition/fallback-failure-closes-composition (RMK-1687).
#[test]
fn a_finished_drain_does_not_release_its_replacement() {
    let publisher = Arc::new(Publisher::default());
    assert!(publisher.add(event()));
    let mut old = Draining {
        publisher: Arc::clone(&publisher),
        finished: false,
    };
    assert!(old.next().is_some());
    assert!(old.next().is_none());

    // Another worker queues an event after the old task observed an empty
    // queue, but before that task's guard is dropped.
    assert!(publisher.add(event()));
    let mut replacement = Draining {
        publisher: Arc::clone(&publisher),
        finished: false,
    };
    drop(old);
    assert!(
        !publisher.add(event()),
        "the replacement already owns the drain"
    );
    assert!(replacement.next().is_some());
    assert!(replacement.next().is_some());
    assert!(replacement.next().is_none());
    drop(replacement);
}

#[test]
fn an_interrupted_drain_allows_the_next_event_to_restart_it() {
    let publisher = Arc::new(Publisher::default());
    assert!(publisher.add(event()));
    let mut interrupted = Draining {
        publisher: Arc::clone(&publisher),
        finished: false,
    };
    assert!(interrupted.next().is_some());
    drop(interrupted);
    assert!(publisher.add(event()));
}
