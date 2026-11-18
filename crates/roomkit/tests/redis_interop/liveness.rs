//! Observe a slow or lost remote turn through the Rust owner's public result.

use roomkit::model::{EventContent, EventType, InboundMessage, TextContent};
use roomkit::store::ListEventsOptions;
use roomkit::{ConversationStore, ProcessInboundOptions, RoomKit};
use serde_json::{Value, json};

use super::{TestResult, open_room};

pub(super) async fn observe(kit: &RoomKit) -> TestResult<Value> {
    let (room, tx) = open_room(kit).await?;
    let prompt = std::env::var("ROOMKIT_INTEROP_PROMPT")?;
    let message = InboundMessage::new(tx, "u", TextContent::new(prompt));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await?;
    let rows = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await?;
    let mut answers = Vec::new();
    for row in rows.into_iter().filter(|row| {
        row.r#type == EventType::Message && row.source.channel_id.as_str() == "worker"
    }) {
        let EventContent::Text(text) = row.content else {
            return Err("worker answer must contain text".into());
        };
        answers.push(text.body);
    }
    Ok(json!({
        "error": result.error,
        "answers": answers,
        "end": result.response_metadata.get("turns").and_then(|turns| turns.pointer("/worker/loop_end_reason").cloned()),
    }))
}
