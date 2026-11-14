//! Session notification envelopes, including sessions idle between prompts.

use agent_client_protocol_schema::v1::{ContentBlock, SessionUpdate};
use roomkit::realtime::EphemeralEventType;
use serde_json::{Value, json};

use super::ChannelClient;
use crate::channel::{config, publication, usage};

impl ChannelClient {
    pub(super) async fn observe_metadata(&self, session: &str, update: &SessionUpdate) {
        let Ok(wire) = serde_json::to_value(update) else {
            return;
        };
        match update {
            SessionUpdate::UsageUpdate(_) => {
                let metadata = self.usage_metadata(session, &wire);
                if self.sessions.contains(session) {
                    self.publish(
                        session,
                        EphemeralEventType::Custom,
                        json!({
                            "type":"acp_usage", "session_id":session,
                            "usage":wire, "usage_metadata":metadata,
                        }),
                    )
                    .await;
                }
            }
            SessionUpdate::ConfigOptionUpdate(update) if self.sessions.contains(session) => {
                if let Ok(options) = config::descriptors(update.config_options.clone()) {
                    self.sessions.update(session, options.clone());
                    self.publish(
                        session,
                        EphemeralEventType::Custom,
                        publication::configuration(session, &options),
                    )
                    .await;
                }
            }
            SessionUpdate::Plan(_) => {
                self.publish(
                    session,
                    EphemeralEventType::Custom,
                    json!({
                        "type":"acp_plan_update", "session_id":session, "update":wire,
                    }),
                )
                .await;
            }
            SessionUpdate::AgentThoughtChunk(chunk) if self.turns.updates(session).is_none() => {
                if let ContentBlock::Text(text) = &chunk.content
                    && !text.text.is_empty()
                {
                    self.idle_thought(session, &text.text).await;
                }
            }
            SessionUpdate::UserMessageChunk(_)
            | SessionUpdate::AgentMessageChunk(_)
            | SessionUpdate::AgentThoughtChunk(_)
            | SessionUpdate::ToolCall(_)
            | SessionUpdate::ToolCallUpdate(_)
            | SessionUpdate::AvailableCommandsUpdate(_)
            | SessionUpdate::CurrentModeUpdate(_)
            | SessionUpdate::ConfigOptionUpdate(_)
            | SessionUpdate::SessionInfoUpdate(_)
            | SessionUpdate::Notice(_)
            | SessionUpdate::CompactionUpdate(_)
            | SessionUpdate::CompactionSummaryChunk(_)
            | _ => {}
        }
    }

    fn usage_metadata(&self, session: &str, wire: &Value) -> Value {
        if let Some(usage) = self.turns.usage(session) {
            return usage.observe(wire, self.sessions.model(session));
        }
        usage::envelope(wire, self.trusted_usage).map_or_else(
            || json!({"session_id":session,"usage_report":usage::observe(wire, self.sessions.model(session))}),
            |envelope| json!(envelope),
        )
    }

    async fn idle_thought(&self, session: &str, text: &str) {
        let mut data = serde_json::Map::from_iter([
            ("thinking_length".to_owned(), json!(text.chars().count())),
            ("round".to_owned(), json!(0)),
        ]);
        if self.publication.publishes_text {
            data.insert(
                "thinking".to_owned(),
                json!(text.chars().take(1000).collect::<String>()),
            );
        }
        self.publish(
            session,
            EphemeralEventType::ThinkingDelta,
            Value::Object(data),
        )
        .await;
    }

    pub(super) async fn progress(&self, session: &str, wire: Value) {
        let tool = wire
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !tool.is_empty()
            && !matches!(
                wire.get("status").and_then(Value::as_str),
                Some("completed" | "failed")
            )
        {
            self.publish(
                session,
                EphemeralEventType::Custom,
                json!({
                    "type":"acp_tool_progress", "session_id":session, "tool_call":wire,
                }),
            )
            .await;
        }
    }

    async fn publish(&self, session: &str, kind: EphemeralEventType, data: Value) {
        let Some(room) = self.turns.room(session) else {
            return;
        };
        let event = publication::event(&room, &self.publication.channel_id, kind, data);
        if let Some(sink) = self.turns.updates(session) {
            sink.scope.publish(&self.publication.bridge, event).await;
        } else {
            // Python's _for_session uses an empty Context for an idle session.
            // A custom transport may call back from another turn's task.
            let publisher = self.publication.bridge.clone();
            let mut outside = tokio::task::JoinSet::new();
            outside.spawn(async move {
                publisher.publish_outside(event).await;
            });
            let _finished = outside.join_next().await;
        }
    }
}
