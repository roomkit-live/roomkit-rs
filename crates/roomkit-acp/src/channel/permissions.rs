//! The handler's permission decision and the outcome the channel must preserve.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit::model::{RoomId, ToolCall};
use roomkit_ai::{ExternalToolHandler, ProcessToolCallOptions, ToolDecision, ToolError};

use crate::AcpError;

/// One service per channel; reconnecting its ACP pipe does not start it twice.
pub(super) struct HandlerService {
    pub(super) handler: Option<Arc<dyn ExternalToolHandler>>,
    started: AtomicBool,
}

impl HandlerService {
    pub(super) fn new(handler: Option<Arc<dyn ExternalToolHandler>>) -> Self {
        Self {
            handler,
            started: AtomicBool::new(false),
        }
    }

    /// The connector serializes handshakes, including this await.
    pub(super) async fn start(&self) -> Result<(), AcpError> {
        if let Some(handler) = &self.handler
            && !self.started.load(Ordering::Acquire)
        {
            handler.start().await.map_err(AcpError::ExternalTool)?;
            self.started.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub(super) async fn stop(&self) {
        if self.started.swap(false, Ordering::AcqRel)
            && let Some(handler) = &self.handler
        {
            let _failed = handler.stop().await;
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct Decision {
    pub(super) approved: bool,
    pub(super) failure: Option<String>,
    pub(super) failure_error: Option<String>,
    pub(super) channel_refused: bool,
    pub(super) refusal: Option<String>,
    pub(super) refusal_detail: Option<String>,
}

pub(super) async fn decide(
    handler: Option<&dyn ExternalToolHandler>,
    call: &ToolCall,
    session: &str,
    room: Option<RoomId>,
) -> Decision {
    let Some(handler) = handler else {
        return Decision::default();
    };
    let options = ProcessToolCallOptions {
        tool_call_id: call.id.clone(),
        session_id: Some(session.to_owned()),
        room_id: room,
        ..ProcessToolCallOptions::default()
    };
    match handler
        .process_tool_call(&call.name, &call.arguments, options)
        .await
    {
        Ok(decision) => read(decision),
        Err(error) => {
            let (kind, message) = error_parts(&error);
            tracing::error!(target: "roomkit_acp", %error, "ACP external permission handler failed");
            Decision {
                failure: Some(format!("{kind}: {message}")),
                failure_error: Some(roomkit_ai::dumps(
                    &serde_json::json!({"error": format!("Tool '{}' failed ({kind})", call.name)}),
                )),
                ..Decision::default()
            }
        }
    }
}

fn read(decision: ToolDecision) -> Decision {
    if decision.modified_input.is_some() || decision.result.is_some() {
        return Decision {
            channel_refused: true,
            refusal: Some(
                "ACP cannot apply an approval that rewrites the call's input or result".to_owned(),
            ),
            ..Decision::default()
        };
    }
    if decision.approved {
        return Decision {
            approved: true,
            ..Decision::default()
        };
    }
    Decision {
        approved: false,
        refusal: Some(decision.reason),
        refusal_detail: decision.detail,
        ..Decision::default()
    }
}

pub(super) fn error_parts(error: &ToolError) -> (&str, &str) {
    crate::error::external_error(error)
}
