//! RFC §6.4 "The turn's record", §10.6.3: the record a channel's stream
//! ends on crosses whole. The loop's end carries a reason and a usage
//! alone; a record it cannot carry so crosses in a `record` frame before
//! it, and the owner's reader ends the turn on that record.

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventType, Metadata};
use serde_json::{Value, json};

use super::support::{Streaming, kit_with, room_id, says};
use crate::ConversationStore;

/// The turn's error, and the metadata of the agent's last message, for an
/// agent of its own whose stream ends on `end`.
async fn ending_on(end: Metadata) -> (Option<String>, Option<Value>) {
    let kit = kit_with(Streaming::ending(2, end)).await;
    let result = says(&kit, "Hi").await.unwrap();
    crate::testing::idle(&kit).await;
    let last = kit
        .store()
        .list_events(&room_id(), received())
        .await
        .unwrap()
        .into_iter()
        .rfind(|event| {
            event.r#type == EventType::Message && event.source.channel_id.as_str() == "ai"
        })
        .map(|event| Value::Object(event.metadata));
    (result.error.map(|error| error.message), last)
}

fn record(value: Value) -> Metadata {
    let Value::Object(record) = value else {
        panic!("a record is an object");
    };
    record
}

/// An empty record: no reason to name, nothing written on the message.
/// (It failed every turn: "a turn's end names its `loop_end_reason`".)
#[tokio::test(start_paused = true)]
async fn an_end_with_an_empty_record_ends_the_turn_with_nothing_written() {
    assert_eq!(ending_on(Metadata::new()).await, (None, Some(json!({}))));
}

/// A record with a key of the channel's own beside its reason: written as
/// it is, nothing lost and nothing added. (It lost the key and gained
/// `ai_usage: {}`.)
#[tokio::test(start_paused = true)]
async fn an_end_with_a_key_of_its_own_writes_the_record_whole() {
    let end = json!({ "loop_end_reason": "completed", "my_model": "m-1" });
    assert_eq!(ending_on(record(end.clone())).await, (None, Some(end)));
}

/// Every event of a room's timeline the room received.
fn received() -> crate::store::ListEventsOptions {
    crate::store::ListEventsOptions {
        limit: u64::MAX,
        ..crate::store::ListEventsOptions::default()
    }
}
