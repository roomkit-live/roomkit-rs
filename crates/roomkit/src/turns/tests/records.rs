//! RFC §6.4 "The turn's record", §10.6.3: the loop's end a channel's
//! stream ends on crosses whole. The wire's loop end carries Python's
//! `LoopEndMarker` fields; roomkit-rs's own (RMK-1029) cross in a `record`
//! frame before it, and the owner's reader ends the turn on the marker.

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventType, Metadata};
use serde_json::{Value, json};

use crate::model::{LoopEndMarker, LoopEndReason};

use super::support::{Streaming, kit_with, room_id, says};
use crate::ConversationStore;

/// The turn's error, and the metadata of the agent's last message, for an
/// agent of its own whose stream ends on `end`.
async fn ending_on(end: LoopEndMarker) -> (Option<String>, Option<Value>) {
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

/// `value`, a JSON object, as metadata.
fn usage(value: Value) -> Metadata {
    let Value::Object(usage) = value else {
        panic!("a usage is an object");
    };
    usage
}

/// Python's marker writes Python's record, its two keys alone; one carrying
/// roomkit-rs's own fields crosses them in a `record` frame, written whole.
/// (The loop's end carried a record: an empty one wrote nothing, a key of
/// the channel's own crossed with it.)
#[tokio::test(start_paused = true)]
async fn a_loop_end_writes_its_record_whatever_crosses_the_backend() {
    let python = LoopEndMarker {
        rounds: 1,
        usage: usage(json!({ "input_tokens": 4 })),
        max_rounds: Some(3),
        ..LoopEndMarker::new(LoopEndReason::MaxRounds)
    };
    let rust = LoopEndMarker {
        last_usage: Some(usage(json!({ "input_tokens": 1 }))),
        usage_complete: Some(true),
        model: Some("m-1".to_owned()),
        provider: Some("p".to_owned()),
        ..python.clone()
    };
    assert_eq!(
        (ending_on(python).await, ending_on(rust).await),
        (
            (
                None,
                Some(json!({ "loop_end_reason": "max_rounds", "ai_usage": { "input_tokens": 4 } }))
            ),
            (
                None,
                Some(json!({
                    "loop_end_reason": "max_rounds",
                    "ai_usage": { "input_tokens": 4 },
                    "ai_last_usage": { "input_tokens": 1 },
                    "ai_usage_complete": true,
                    "ai_model": "m-1",
                    "ai_provider": "p"
                }))
            )
        )
    );
}

/// Every event of a room's timeline the room received.
fn received() -> crate::store::ListEventsOptions {
    crate::store::ListEventsOptions {
        limit: u64::MAX,
        ..crate::store::ListEventsOptions::default()
    }
}
