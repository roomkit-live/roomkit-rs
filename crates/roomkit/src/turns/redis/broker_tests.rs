//! What a service gives the backend, checked on each one: the one of this
//! process always, Redis when `REDIS_URL` names a server.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::broker::{Entry, TurnBroker, TurnRegistry};
use super::memory_broker::MemoryBroker;
use super::redis_broker::RedisBroker;
use super::registry::Beat;
use super::stream::Message;
use crate::TurnTimeouts;

const BLOCK: Duration = Duration::from_millis(20);

fn redis() -> Option<RedisBroker> {
    let url = std::env::var("REDIS_URL").ok()?;
    Some(RedisBroker::new(
        redis::Client::open(url.as_str()).unwrap(),
        None,
        format!("roomkit:broker:{}", uuid::Uuid::new_v4()),
        TurnTimeouts::default(),
    ))
}

fn frame(body: &str) -> Message {
    Message::new("frame", "sender", body.to_owned(), "turn", "")
}

fn bodies(entries: &[Entry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.message.as_ref().unwrap().body.as_str())
        .collect()
}

async fn an_inbox_is_read_in_the_order_posted_until_acknowledged(broker: &dyn TurnBroker) {
    broker.post("n1", &frame("one")).await.unwrap();
    broker.post("n1", &frame("two")).await.unwrap();
    broker.post("n2", &frame("another node's")).await.unwrap();
    let mut inbox = broker.inbox("n1", BLOCK).await.unwrap();
    let entries = inbox.read("").await.unwrap();
    assert_eq!(bodies(&entries), ["one", "two"]);
    let ids: Vec<String> = entries.into_iter().map(|entry| entry.id).collect();

    // After the last entry nothing is read, and the read waits no longer
    // than its block; after the first, the second again.
    assert!(inbox.read(&ids[1]).await.unwrap().is_empty());
    assert_eq!(bodies(&inbox.read(&ids[0]).await.unwrap()), ["two"]);

    broker.post("n1", &frame("three")).await.unwrap();
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
    broker.post("waiting", &frame("late")).await.unwrap();
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
    assert!(registry.view(60_000).await.unwrap().owners.is_empty());

    // A view forgets what last beat too long ago, for the next ones too.
    assert!(registry.view(0).await.unwrap().executors.is_empty());
    assert!(registry.view(60_000).await.unwrap().executors.is_empty());
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

async fn keeps_the_ports<B: TurnBroker + TurnRegistry>(broker: Arc<B>) {
    an_inbox_is_read_in_the_order_posted_until_acknowledged(&*broker).await;
    a_waiting_read_takes_a_message_posted_meanwhile(Arc::<B>::clone(&broker)).await;
    a_beat_is_viewed_until_withdrawn(&*broker).await;
    a_room_has_one_holder_until_forgotten(&*broker).await;
    a_presence_is_claimed_once_a_change(&*broker).await;
}

#[tokio::test]
async fn the_broker_of_this_process_keeps_the_ports() {
    keeps_the_ports(Arc::new(MemoryBroker::new())).await;
}

#[tokio::test]
async fn the_redis_broker_keeps_the_ports() {
    let Some(broker) = redis() else {
        return;
    };
    keeps_the_ports(Arc::new(broker)).await;
}
