//! The record of a turn's author and the room's register of authors (RFC
//! §6.4, §10.1 step 12, §14.1, gap G-089), effect by effect. The digests and
//! ranks are the Python implementation's (roomkit 2d6e6267,
//! `core/_authors.py`), computed for the same room, sources and names.

use roomkit_core::model::{
    ChannelType, EventContent, EventDirection, EventSource, EventType, EventUpdates, HookResult,
    Metadata, Participant, ParticipantId, RoomEvent, Timestamp,
};
use roomkit_core::ops::UpdateEvent;
use roomkit_core::rules::hook_chain::HookOutcome;
use serde_json::{Value, json};

use crate::support::{Sim, channel, room_id};

/// The digest of the SMS sender `+15551234567` in `room_1`.
const SENDER: &str = "294003ef2a5ca40b";
/// The digest of the SMS sender `+15550009` in `room_1`.
const OTHER: &str = "b1e7f4a565057ab7";
/// The digest of the participant `p_1` in `room_1`.
const PARTICIPANT: &str = "2e59d2b5af0de09b";
/// What `Alice` reads as, hashed with `room_1`.
const ALICE: &str = "0342973908f267c0";
/// What `ALICE` reads as besides `Alice`'s (`allce`), hashed with `room_1`.
const ALLCE: &str = "87bd064c9cadb523";

/// A message from `+15551234567` on SMS, its sender named `name`.
fn named(name: &str) -> roomkit_core::model::InboundMessage {
    let mut message = Sim::message("sms", "Bonjour");
    message
        .metadata
        .insert("sender_name".to_owned(), name.into());
    message
}

fn author(name: &str, rank: u32, source: &str) -> Value {
    json!({ "name": name, "rank": rank, "source": source })
}

fn register(names: &Value) -> Metadata {
    let Value::Object(register) = json!({ "version": 3, "names": names }) else {
        unreachable!()
    };
    register
}

/// The record of the timeline's last event.
fn last_record(sim: &Sim) -> Option<Value> {
    sim.timeline.last()?.metadata.get("author").cloned()
}

/// RFC §10.1 step 12: the event carries its author's name, rank and
/// source, and the register is written in the commit, which writes nothing
/// else: no effect of its own.
#[test]
fn a_named_sender_s_message_carries_its_author_and_the_commit_writes_the_register() {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.send(named("Alice")).unwrap();
    assert_eq!(
        (
            last_record(&sim),
            sim.author_register.clone(),
            sim.trace.iter().any(|line| line.starts_with("list_events"))
        ),
        (
            Some(author("Alice", 1, SENDER)),
            Some(register(&json!({ ALICE: format!("{SENDER}:1") }))),
            false
        )
    );
}

/// RFC §6.4: a participant the room registered keeps the name; a sender
/// whose name reads like it ranks after it.
#[test]
fn a_registered_participant_keeps_the_name_a_sender_reads_like() {
    let mut sim = Sim::new();
    sim.transport("sms");
    let mut alice = Participant::new(
        ParticipantId::new("p_1").unwrap(),
        room_id(),
        channel("ws"),
        Timestamp::UNIX_EPOCH,
    );
    alice.display_name = Some("Alice".to_owned());
    sim.participants.push(alice);
    sim.send(named("ALICE")).unwrap();
    assert_eq!(
        (last_record(&sim), sim.author_register.clone()),
        (
            Some(author("ALICE", 2, SENDER)),
            Some(register(&json!({
                ALICE: format!("{PARTICIPANT}:1 {SENDER}:2"),
                ALLCE: format!("{SENDER}:2"),
            })))
        )
    );
}

/// RFC §10.1 step 12: a register the room's metadata still holds is merged
/// into the store's, whose ranks it keeps, and leaves the metadata.
#[test]
fn a_register_the_metadata_holds_is_merged_into_the_store_s() {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.room.metadata.insert(
        "author_register".to_owned(),
        json!({ "version": 3, "names": { ALICE: format!("{OTHER}:1") } }),
    );
    sim.send(named("Alice")).unwrap();
    assert_eq!(
        (
            last_record(&sim),
            sim.author_register.clone(),
            sim.room.metadata.contains_key("author_register")
        ),
        (
            Some(author("Alice", 2, SENDER)),
            Some(register(&json!({ ALICE: format!("{OTHER}:1 {SENDER}:2") }))),
            false
        )
    );
}

/// RFC §10.1 step 12, the review of step 3 milestone 2: a copy of the
/// register the room's metadata holds, identical to the store's, leaves
/// the metadata at the room's next named turn, one that changes no rank,
/// as the Python implementation moves it for a store that keeps the
/// register apart.
#[test]
fn an_identical_copy_the_metadata_holds_leaves_it() {
    let mut sim = Sim::new();
    sim.transport("sms");
    let held = register(&json!({ ALICE: format!("{SENDER}:1") }));
    sim.author_register = Some(held.clone());
    sim.room
        .metadata
        .insert("author_register".to_owned(), Value::Object(held));
    sim.send(named("Alice")).unwrap();
    assert_eq!(
        (
            last_record(&sim),
            sim.room.metadata.contains_key("author_register")
        ),
        (Some(author("Alice", 1, SENDER)), false)
    );
}

/// RFC §10.1 step 12: a room with no register has it rebuilt from its
/// timeline, read whole under the lock before the commit when the
/// context's window does not hold it: an earlier author with no record
/// keeps the first rank.
#[test]
fn a_room_with_no_register_has_it_rebuilt_from_its_whole_timeline() {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.config.context_window = 1;
    for (id, sender, name) in [
        ("old_1", "+15550009", "Alice"),
        ("old_2", "+15550009", "Alice"),
    ] {
        let mut source =
            EventSource::new(channel("sms"), ChannelType::SMS, EventDirection::Inbound);
        source.participant_id = Some(ParticipantId::new(sender).unwrap());
        let mut earlier = RoomEvent::new(
            id.parse().unwrap(),
            room_id(),
            EventType::Message,
            source,
            EventContent::text("Hi"),
            Timestamp::UNIX_EPOCH,
        );
        earlier
            .metadata
            .insert("sender_name".to_owned(), name.into());
        sim.seed(earlier);
    }
    sim.send(named("Alice")).unwrap();
    let commit = sim.trace.iter().position(|line| line.starts_with("commit"));
    let read = sim
        .trace
        .iter()
        .position(|line| line == "list_events room_1");
    assert_eq!(
        (
            last_record(&sim),
            sim.author_register.clone(),
            read.is_some() && read < commit
        ),
        (
            Some(author("Alice", 2, SENDER)),
            Some(register(&json!({ ALICE: format!("{OTHER}:1 {SENDER}:2") }))),
            true
        )
    );
}

/// RFC §10.1 step 12: a record of an author the event came with is the
/// runtime's alone, dropped; a record stored `BLOCKED` takes none.
#[test]
fn a_blocked_message_takes_no_author_and_loses_the_one_it_came_with() {
    let mut sim = Sim::new();
    sim.transport("sms");
    sim.before_broadcast(
        "no_entry",
        Box::new(|_| HookOutcome::Returned(HookResult::block("closed"))),
    );
    let mut message = named("Alice");
    message
        .metadata
        .insert("author".to_owned(), json!({ "name": "Admin", "rank": 1 }));
    sim.send(message).unwrap();
    assert_eq!(
        (last_record(&sim), sim.author_register.clone()),
        (None, None)
    );
}

/// RFC §10.5, §10.1 step 12: a record committed outside the pipeline
/// carries its author too.
#[test]
fn a_record_committed_outside_the_pipeline_carries_its_author() {
    let mut sim = Sim::new();
    sim.transport("sms");
    let mut source = EventSource::new(channel("sms"), ChannelType::SMS, EventDirection::Inbound);
    source.participant_id = Some(ParticipantId::new("+15551234567").unwrap());
    let mut record = RoomEvent::new(
        "rec_1".parse().unwrap(),
        room_id(),
        EventType::Message,
        source,
        EventContent::text("Noted"),
        Timestamp::UNIX_EPOCH,
    );
    record
        .metadata
        .insert("sender_name".to_owned(), "Alice".into());
    let committed = sim.commit_record("room_1", record, None).unwrap();
    assert_eq!(
        (
            committed.metadata.get("author").cloned(),
            sim.author_register.clone()
        ),
        (
            Some(author("Alice", 1, SENDER)),
            Some(register(&json!({ ALICE: format!("{SENDER}:1") })))
        )
    );
}

/// RFC §10.1 step 12, gap G-089: the record of an author is fixed at the
/// commit; a direct update that writes metadata keeps the record the event
/// was committed with, and never writes one of its own.
#[test]
fn a_direct_update_keeps_the_record_the_event_was_committed_with() {
    let mut sim = Sim::new();
    sim.transport("sms");
    let sent = sim.send(named("Alice")).unwrap().event.unwrap();
    let mut forged = sent.metadata.clone();
    forged.insert(
        "author".to_owned(),
        json!({ "name": "Admin", "rank": 1, "source": OTHER }),
    );
    forged.insert("tag".to_owned(), json!("vip"));
    let updates = EventUpdates {
        metadata: Some(forged),
        ..EventUpdates::default()
    };
    let update = UpdateEvent::new(
        sim.shared_config(),
        room_id(),
        sent.id.clone(),
        updates,
        None,
    );
    sim.run(update).unwrap();
    let stored = sim.timeline.last().unwrap();
    assert_eq!(
        (
            stored.metadata.get("author").cloned(),
            stored.metadata.get("tag").cloned()
        ),
        (Some(author("Alice", 1, SENDER)), Some(json!("vip")))
    );
}
