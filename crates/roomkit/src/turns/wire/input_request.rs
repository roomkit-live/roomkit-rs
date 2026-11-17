//! The request fields the executor preserves for its owner (RFC §10.6.5).

use roomkit_core::model::{Metadata, Timestamp};
use serde::{Deserialize, Serialize};

/// Python's private link record, including fields older executors omit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct InputRequest {
    pub(crate) request_id: String,
    pub(crate) tool_name: String,
    #[serde(default)]
    pub(crate) arguments: Metadata,
    #[serde(default)]
    pub(crate) room_id: Option<String>,
    #[serde(default)]
    pub(crate) channel_id: Option<String>,
    #[serde(default)]
    pub(crate) tool_call_id: Option<String>,
    #[serde(default)]
    pub(crate) actor_id: Option<String>,
    #[serde(default)]
    pub(crate) created_at: Option<Timestamp>,
}

impl From<&crate::human_input::PendingInput> for InputRequest {
    fn from(pending: &crate::human_input::PendingInput) -> Self {
        Self {
            request_id: pending.pending_id.clone(),
            tool_name: pending.tool_name.clone(),
            arguments: pending.arguments.clone(),
            room_id: Some(pending.room_id.clone()),
            channel_id: Some(pending.channel_id.clone()),
            tool_call_id: Some(pending.tool_call_id.clone()),
            actor_id: pending.actor_id.clone(),
            created_at: Some(pending.created_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::human_input::{PendingInput, PendingInputOptions};
    use crate::turns::wire::Frame;
    use serde_json::json;

    // Python's InputRequest.model_dump_json(), with the original request's time.
    #[test]
    fn the_private_frame_preserves_the_pending_request_in_python_s_flat_form() {
        let pending = PendingInput::new(
            "p1",
            "ask_user",
            [("question".to_owned(), json!("Color?"))]
                .into_iter()
                .collect(),
            "r1",
            "c1",
            "agent",
            PendingInputOptions {
                created_at: Some("2026-11-17T00:00:00Z".parse().unwrap()),
                actor_id: Some("alice".to_owned()),
                ..PendingInputOptions::default()
            },
        );
        let frame = Frame::InputRequest {
            request: Box::new(InputRequest::from(&pending)),
        };
        let serialized = serde_json::to_value(&frame).unwrap();
        assert_eq!(
            serialized,
            json!({
                "type": "input_request", "request_id": "p1", "tool_name": "ask_user",
                "arguments": {"question": "Color?"}, "room_id": "r1", "channel_id": "agent",
                "tool_call_id": "c1", "actor_id": "alice", "created_at": "2026-11-17T00:00:00Z",
            })
        );
        assert_eq!(serde_json::from_value::<Frame>(serialized).unwrap(), frame);
    }
}
