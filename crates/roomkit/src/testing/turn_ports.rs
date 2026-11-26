//! What a broker and a registry give a distributed turn backend, checked
//! on any implementation of the two ports: the core's own, in this process,
//! and a service's, by its own tests.
//!
//! Each check panics, saying what, where the contract is not kept. It
//! writes under names of its own (the nodes `n1`, `n2` and `waiting`, the
//! agent `agent`): give it a broker or a registry nothing else writes to,
//! as a namespace of the test's own on a shared server.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a check fails its test by panicking, as an assertion does"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::turns::{Beat, BrokerMessage, InboxEntry, TurnBroker, TurnRegistry};

const BLOCK: Duration = Duration::from_millis(20);

/// Checks that `broker` keeps the contract of [`TurnBroker`]: an inbox is
/// read in the order it was posted in, the messages of one post in theirs,
/// after any entry, until acknowledged;
/// a node reads its own inbox alone; a read that waits takes a message
/// posted meanwhile, and waits no longer than its block.
///
/// # Panics
/// Where `broker` does not keep the contract, saying what.
pub async fn check_turn_broker(broker: Arc<dyn TurnBroker>) {
    an_inbox_is_read_in_the_order_posted_until_acknowledged(&*broker).await;
    a_waiting_read_takes_a_message_posted_meanwhile(broker).await;
}

/// Checks that `registry` keeps the contract of [`TurnRegistry`]: a beat
/// is written at the registry's time and viewed until withdrawn or
/// forgotten, a beat older than its node's last one changes nothing; a room
/// has one holder until forgotten; a presence is claimed once a change.
///
/// # Panics
/// Where `registry` does not keep the contract, saying what.
pub async fn check_turn_registry(registry: Arc<dyn TurnRegistry>) {
    a_beat_is_viewed_until_withdrawn(&*registry).await;
    a_room_has_one_holder_until_forgotten(&*registry).await;
    a_presence_is_claimed_once_a_change(&*registry).await;
}

fn frame(body: &str) -> BrokerMessage {
    BrokerMessage::new("frame", "sender", body.to_owned(), "turn", "")
}

fn bodies(entries: &[InboxEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.message.as_ref().unwrap().body.as_str())
        .collect()
}

async fn an_inbox_is_read_in_the_order_posted_until_acknowledged(broker: &dyn TurnBroker) {
    broker
        .post("n1", &[frame("one"), frame("two")])
        .await
        .unwrap();
    broker.post("n2", &[frame("another node's")]).await.unwrap();
    let mut inbox = broker.inbox("n1", BLOCK).await.unwrap();
    let entries = inbox.read("").await.unwrap();
    assert_eq!(bodies(&entries), ["one", "two"]);
    let ids: Vec<String> = entries.into_iter().map(|entry| entry.id).collect();

    // After the last entry nothing is read, and the read waits no longer
    // than its block; after the first, the second again.
    assert_eq!(inbox.read(&ids[1]).await.unwrap(), []);
    assert_eq!(bodies(&inbox.read(&ids[0]).await.unwrap()), ["two"]);

    broker.post("n1", &[frame("three")]).await.unwrap();
    assert_eq!(bodies(&inbox.read(&ids[1]).await.unwrap()), ["three"]);

    // A reader that starts over reads what was not acknowledged.
    broker.acknowledge("n1", &ids).await.unwrap();
    let mut again = broker.inbox("n1", BLOCK).await.unwrap();
    assert_eq!(bodies(&again.read("").await.unwrap()), ["three"]);
}

async fn a_waiting_read_takes_a_message_posted_meanwhile(broker: Arc<dyn TurnBroker>) {
    let mut inbox = broker
        .inbox("waiting", Duration::from_secs(5))
        .await
        .unwrap();
    let reading = tokio::spawn(async move { inbox.read("").await.unwrap() });
    tokio::time::sleep(BLOCK).await;
    broker.post("waiting", &[frame("late")]).await.unwrap();
    let entries = tokio::time::timeout(Duration::from_secs(2), reading)
        .await
        .expect("the read waited its whole block")
        .unwrap();
    assert_eq!(bodies(&entries), ["late"]);
}

fn beat(generation: u64) -> Beat {
    Beat {
        node: "n1".to_owned(),
        generation,
        ..Beat::default()
    }
}

async fn a_beat_is_viewed_until_withdrawn(registry: &dyn TurnRegistry) {
    registry.connect().await.unwrap();
    assert!(registry.connected());
    registry
        .beat(&Beat {
            declared: BTreeMap::from([("n1:a".to_owned(), "its declaration".to_owned())]),
            undeclared: vec!["n1:b".to_owned()],
            running: vec!["turn-1".to_owned()],
            owner: true,
            ..beat(2)
        })
        .await
        .unwrap();
    let view = registry.view(60_000).await.unwrap();
    let (written, declared) = view.executors.get("n1:a").cloned().unwrap();
    assert_eq!(declared.as_deref(), Some("its declaration"));
    assert_eq!(view.executors.get("n1:b").unwrap().1, None);
    assert!(
        view.now >= written,
        "a beat is written at the registry's time"
    );
    assert!(view.owners.contains_key("n1"));

    // A beat older than the node's last one changes nothing.
    let older = Beat {
        gone: vec!["n1:a".to_owned()],
        ..beat(1)
    };
    registry.beat(&older).await.unwrap();
    assert!(
        registry
            .view(60_000)
            .await
            .unwrap()
            .executors
            .contains_key("n1:a")
    );

    // A node's last beat and the turns it runs are read together.
    let running = [("n1", vec!["turn-1", "turn-2"])];
    let beats = registry
        .beats(&["n1:a", "n1:unknown"], &running)
        .await
        .unwrap();
    assert_eq!(beats.last, [Some(written), None]);
    assert_eq!(beats.running, [[true, false]]);

    // The turns a node runs are read with no executor's beat asked, and a
    // beat with no turn.
    let beats = registry.beats(&[], &running).await.unwrap();
    assert_eq!(
        (beats.last, beats.running),
        (vec![], vec![vec![true, false]])
    );
    let beats = registry.beats(&["n1:a"], &[("n1", vec![])]).await.unwrap();
    assert_eq!(
        (beats.last, beats.running),
        (vec![Some(written)], vec![vec![]])
    );

    // The heartbeat's own writer writes a beat as the registry does.
    let mut beater = registry.beater().await.unwrap();
    let withdrawing = Beat {
        undeclared: vec!["n1:b".to_owned()],
        gone: vec!["n1:a".to_owned()],
        owner: true,
        ..beat(3)
    };
    beater.beat(&withdrawing).await.unwrap();
    let view = registry.view(60_000).await.unwrap();
    assert_eq!(view.executors.keys().collect::<Vec<_>>(), ["n1:b"]);
    let beats = registry.beats(&["n1:b"], &running).await.unwrap();
    assert_eq!(
        beats.running,
        [[false, false]],
        "the turns of its last beat"
    );

    registry.forget_owner("n1").await.unwrap();
    assert_eq!(registry.view(60_000).await.unwrap().owners, BTreeMap::new());

    // A view forgets what last beat too long ago, for the next ones too.
    assert_eq!(registry.view(0).await.unwrap().executors, BTreeMap::new());
    assert_eq!(
        registry.view(60_000).await.unwrap().executors,
        BTreeMap::new()
    );
}

async fn a_room_has_one_holder_until_forgotten(registry: &dyn TurnRegistry) {
    assert_eq!(registry.holder("agent", "room").await.unwrap(), None);
    registry.hold("agent", "room", "n1:a").await.unwrap();
    registry.hold("agent", "room", "n2:b").await.unwrap();
    let held = registry.holder("agent", "room").await.unwrap();
    assert_eq!(held.as_deref(), Some("n2:b"));

    // A channel and a room are told apart whatever their ids hold.
    registry.hold("a:b", "c", "n1:a").await.unwrap();
    assert_eq!(registry.holder("a", "b:c").await.unwrap(), None);

    registry.forget_holder("agent", "room").await.unwrap();
    assert_eq!(registry.holder("agent", "room").await.unwrap(), None);
}

async fn a_presence_is_claimed_once_a_change(registry: &dyn TurnRegistry) {
    let claim = |online| registry.claim_presence("agent", "room", online);
    assert!(claim(Some(true)).await.unwrap());
    assert!(!claim(Some(true)).await.unwrap(), "already so");
    assert!(claim(Some(false)).await.unwrap());
    assert!(!claim(None).await.unwrap(), "forgetting is no change");
    assert!(
        claim(Some(false)).await.unwrap(),
        "a change again, once forgotten"
    );
    assert!(
        registry
            .claim_presence("agent", "another room", Some(false))
            .await
            .unwrap()
    );
}
