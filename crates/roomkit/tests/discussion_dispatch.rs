//! A discussion's dispatch policy where a scenario cannot reach (RFC §19.7.5
//! rule 18): a decision another process took first, a close while the
//! policy decides, more messages waiting than the queue keeps, and a store
//! read failing before the policy is asked.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "strategies/faulty_store.rs"]
mod faulty_store;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;

use roomkit::model::*;
use roomkit::orchestration::{DiscussionOptions, DispatchFuture};
use roomkit::store::{self, PatchRoomMetadataOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, Discussion, DispatchDecision,
    DispatchDecisionEvent, DispatchPolicy, DispatchTurn, HookOptions, HookTrigger, InMemoryStore,
    InstallStrategyOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};

const AGENTS: [&str; 3] = ["investigator", "dev", "sre"];

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// Picks `sre` once let go: `deciding` is told as each decision starts.
#[derive(Default)]
struct Held {
    deciding: Notify,
    release: Notify,
    asked: AtomicU32,
}

impl DispatchPolicy for Held {
    fn decide(&self, _turn: DispatchTurn) -> DispatchFuture<'_> {
        Box::pin(async move {
            self.asked.fetch_add(1, Ordering::SeqCst);
            self.deciding.notify_one();
            self.release.notified().await;
            Ok(DispatchDecision {
                agents: vec![id("sre")],
                reason: "scripted".to_owned(),
                ..DispatchDecision::default()
            })
        })
    }
}

/// A room of the three agents' discussion deciding by `policy`, `ops`
/// bound, on `store`.
async fn installed(store: &Arc<InMemoryStore>, policy: Arc<Held>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(store),
        ..RoomKitOptions::default()
    });
    discussing(kit, policy).await
}

/// `kit` holding `ops` and the three agents, `r1` given to their
/// discussion deciding by `policy`, `ops` bound.
async fn discussing<S: ConversationStore>(
    kit: RoomKit<S>,
    policy: Arc<dyn DispatchPolicy>,
) -> RoomKit<S> {
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    for agent in AGENTS {
        let says = format!("{agent} here");
        kit.register_channel(EchoAgent::new(
            id(agent),
            &says,
            EchoAgentOptions::default(),
        ))
        .unwrap();
    }
    let created = CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room(), &id("ops"), AttachChannelOptions::default())
        .await
        .unwrap();
    let options = DiscussionOptions {
        dispatch: Some(policy),
        ..DiscussionOptions::default()
    };
    let agents = AGENTS.map(|agent| id(agent).into()).to_vec();
    let discussion = Discussion::new(agents, options).unwrap();
    kit.install_strategy(
        &room(),
        Arc::new(discussion),
        InstallStrategyOptions::default(),
    )
    .await
    .unwrap();
    kit
}

async fn say<S: ConversationStore>(kit: &RoomKit<S>, text: &str) -> RoomEvent {
    let message = InboundMessage::new(id("ops"), "oncall", TextContent::new(text));
    let said = kit
        .process_inbound(message, ProcessInboundOptions::default())
        .await
        .unwrap();
    said.event.unwrap()
}

/// The stored speak queue of `r1`.
async fn stored_queue(store: &InMemoryStore) -> Value {
    let stored = store.get_room(&room()).await.unwrap().unwrap();
    stored
        .metadata
        .get("_speak_queue")
        .cloned()
        .unwrap_or_default()
}

/// The agents that answered in `r1`, in order.
async fn answers(store: &InMemoryStore) -> Vec<ChannelId> {
    let options = store::ListEventsOptions::default();
    let events = store.list_events(&room(), options).await.unwrap();
    events
        .into_iter()
        .filter(|event| AGENTS.contains(&event.source.channel_id.as_str()))
        .map(|event| event.source.channel_id)
        .collect()
}

/// Returns once the policy is asked; fails past 5 s.
async fn decision_starts(policy: &Held) {
    tokio::time::timeout(Duration::from_secs(5), policy.deciding.notified())
        .await
        .unwrap();
}

// Python: tests/orchestration/test_discussion_dispatch.py::test_a_decision_another_process_took_first_is_neither_applied_nor_reported
#[tokio::test]
async fn a_decision_another_process_took_first_is_neither_applied_nor_reported() {
    let store = Arc::new(InMemoryStore::default());
    let policy = Arc::new(Held::default());
    let kit = installed(&store, Arc::clone(&policy)).await;
    let told = Arc::new(AtomicU32::new(0));
    let telling = Arc::clone(&told);
    kit.hook(
        HookTrigger::OnDispatchDecision,
        move |_event: DispatchDecisionEvent, _context: RoomContext| {
            telling.fetch_add(1, Ordering::SeqCst);
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();

    say(&kit, "anyone?").await;
    decision_starts(&policy).await;
    // As a process that took the lease over would: the message is decided.
    let mut queue = stored_queue(&store).await;
    queue["dispatching"] = Value::Array(Vec::new());
    let patch = Metadata::from_iter([("_speak_queue".to_owned(), queue)]);
    store
        .patch_room_metadata(&room(), patch, PatchRoomMetadataOptions::default())
        .await
        .unwrap();
    policy.release.notify_one();
    idle(&kit).await;

    assert_eq!(told.load(Ordering::SeqCst), 0);
    assert_eq!(answers(&store).await, Vec::<ChannelId>::new());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_discussion_dispatch.py::test_closing_while_deciding_leaves_the_message_waiting
#[tokio::test]
async fn closing_while_deciding_leaves_the_message_waiting() {
    let store = Arc::new(InMemoryStore::default());
    let policy = Arc::new(Held::default());
    let kit = installed(&store, Arc::clone(&policy)).await;

    let said = say(&kit, "anyone?").await;
    decision_starts(&policy).await;
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .expect("a close waits for no decision")
        .unwrap();

    let queue = stored_queue(&store).await;
    let waiting: Vec<&str> = queue["dispatching"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|pending| pending["event_id"].as_str())
        .collect();
    assert_eq!(waiting, [said.id.as_str()]);
}

// Python: tests/orchestration/test_discussion_dispatch.py::test_past_the_messages_that_may_wait_the_oldest_asks_its_candidates,
// in one process: its policy holds the first decision while the rest come.
#[tokio::test]
async fn past_the_messages_that_may_wait_the_oldest_asks_its_candidates() {
    let store = Arc::new(InMemoryStore::default());
    let policy = Arc::new(Held::default());
    let kit = installed(&store, Arc::clone(&policy)).await;

    let first = say(&kit, "one").await;
    decision_starts(&policy).await;
    for at in 0..16 {
        say(&kit, &format!("then {at}")).await;
    }

    let queue = stored_queue(&store).await;
    let waiting: Vec<&str> = queue["dispatching"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|pending| pending["event_id"].as_str())
        .collect();
    assert_eq!(waiting.len(), 16);
    assert!(!waiting.contains(&first.id.as_str()));
    let queued: Vec<&str> = queue["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["agent"].as_str())
        .collect();
    assert_eq!(queued, AGENTS);
    // The close cuts the decision under way.
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_discussion_dispatch.py::test_a_decision_whose_message_cannot_be_read_asks_every_candidate_unreported
#[tokio::test]
async fn a_decision_whose_message_cannot_be_read_asks_every_candidate_unreported() {
    let policy = Arc::new(Held::default());
    let kit = RoomKit::new(RoomKitOptions::with_store(
        faulty_store::AbstractOnly::default(),
    ));
    let deciding: Arc<Held> = Arc::clone(&policy);
    let kit = discussing(kit, deciding).await;
    let told = Arc::new(AtomicU32::new(0));
    let telling = Arc::clone(&told);
    kit.hook(
        HookTrigger::OnDispatchDecision,
        move |_event: DispatchDecisionEvent, _context: RoomContext| {
            telling.fetch_add(1, Ordering::SeqCst);
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();

    kit.store()
        .fail_next_event_read
        .store(true, Ordering::SeqCst);
    say(&kit, "anyone?").await;
    idle(&kit).await;

    let mut answered = answers(&kit.store().store).await;
    answered.sort();
    assert_eq!(answered, [id("dev"), id("investigator"), id("sre")]);
    assert_eq!(policy.asked.load(Ordering::SeqCst), 0);
    assert_eq!(told.load(Ordering::SeqCst), 0);
    kit.close().await.unwrap();
}
