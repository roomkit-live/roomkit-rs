//! A post, the host's own voice, effect by effect (RFC §10.5): the room
//! read, the host channel attached on the room's first post in one section
//! under the lock, then the injection; the call's bound over both.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventContent, Post};
use roomkit_core::ops::{ChannelDeclaration, Inbound, InboundError, InboundResult};

use crate::support::{Sim, channel, room_id};

/// The room with the customer's `sms`, its host channel `host` registered
/// and not bound.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.declare(ChannelDeclaration::host(channel("host")));
    sim
}

fn post(sim: &mut Sim, text: &str) -> Result<InboundResult, InboundError> {
    let operation = Inbound::post(
        sim.shared_config(),
        room_id(),
        None,
        Post::new(EventContent::text(text)),
    );
    sim.run(operation)
}

/// RFC §10.5: the room is read, then, as it never bound the host channel,
/// the channel is attached under the room lock, then the post is injected
/// under a lock of its own.
#[test]
fn a_first_post_attaches_its_host_channel_under_the_lock_then_injects() {
    let mut sim = room();
    post(&mut sim, "Your order shipped.").unwrap();
    let first: Vec<&str> = sim.trace.iter().map(String::as_str).take(9).collect();
    assert_eq!(
        first,
        [
            "load_context room_1",
            "lock room_1",
            "load_context room_1",
            "store binding host",
            "commit #0 system delivered \"Channel host attached (read_write, all)\"",
            "release room_1",
            "lock room_1",
            "load_context room_1",
            "commit #1 host delivered \"Your order shipped.\"",
        ]
    );
}

/// A room that binds the host channel takes the post at once: no section
/// for an attach.
#[test]
fn a_later_post_takes_no_lock_for_an_attach() {
    let mut sim = room();
    post(&mut sim, "Your order shipped.").unwrap();
    sim.trace.clear();
    post(&mut sim, "It arrives tomorrow.").unwrap();
    let first: Vec<&str> = sim.trace.iter().map(String::as_str).take(4).collect();
    assert_eq!(
        first,
        [
            "load_context room_1",
            "lock room_1",
            "load_context room_1",
            "commit #2 host delivered \"It arrives tomorrow.\"",
        ]
    );
}

/// RFC §10.5, §13.6: the first post's wait for the room lock is within the
/// call's `process_timeout`: past it, the post is refused, nothing written.
#[test]
fn a_first_post_waits_for_the_lock_within_the_call_s_bound() {
    let mut sim = room();
    sim.config.process_timeout = Some(Duration::from_secs(10));
    sim.durations.push(("lock room_1", Duration::from_secs(11)));
    let refused = post(&mut sim, "Your order shipped.");
    assert_eq!(
        (
            refused,
            sim.bindings
                .iter()
                .any(|binding| binding.channel_id == channel("host")),
            sim.timeline.len()
        ),
        (
            Err(InboundError::Refused {
                reason: "process_timeout".to_owned()
            }),
            false,
            0
        )
    );
}

/// RFC §10.5, §13.6: once the lock is held, the first post's attach runs to
/// its end whatever becomes of the caller: a caller that leaves meanwhile
/// leaves the host channel bound and its attach recorded, and nothing of
/// its post written.
#[test]
fn a_first_post_whose_caller_leaves_once_the_lock_is_held_attaches_to_its_end() {
    let mut sim = room();
    sim.leaves_before = Some("store binding host");
    post(&mut sim, "Your order shipped.").ok();
    assert_eq!(
        (
            sim.bindings
                .iter()
                .any(|binding| binding.channel_id == channel("host")),
            sim.timeline.len()
        ),
        // The attach's record alone (gap G-046).
        (true, 1)
    );
}
