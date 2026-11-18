//! The owner lives across the replacement of its only worker.

use std::io::{self, Write};

use roomkit::model::{EventContent, EventType};
use roomkit::store::ListEventsOptions;
use roomkit::{ConversationStore, RoomKit};
use serde_json::{Value, json};

use super::{TestResult, open_room, send};

async fn await_worker(expected: &'static str) -> TestResult {
    let word = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        Ok::<_, io::Error>(line)
    })
    .await??;
    if word.trim_end() != expected {
        return Err(format!("expected worker handover {expected}, got {word:?}").into());
    }
    Ok(())
}

pub(super) async fn replay(kit: &RoomKit) -> TestResult<Value> {
    await_worker("start").await?;
    let (room, tx) = open_room(kit).await?;
    send(kit, &room, tx.clone(), "Look it up.").await?;
    writeln!(io::stdout(), "stored")?;
    io::stdout().flush()?;
    await_worker("next").await?;
    send(kit, &room, tx, "What did it return?").await?;
    let mut answers = Vec::new();
    for row in kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await?
    {
        if row.r#type == EventType::Message && row.source.channel_id.as_str() == "worker" {
            let EventContent::Text(text) = row.content else {
                return Err("worker answer must contain text".into());
            };
            answers.push(json!([text.body, row.metadata.get("executor_id")]));
        }
    }
    Ok(Value::Array(answers))
}
