//! The AI side of the handoff (RFC §19.6): what Python's
//! `orchestration/handoff.py` gives as an `AITool` or over a memory
//! provider. The handler is `roomkit::orchestration`'s, which cannot name
//! an [`AiTool`]: it keeps the tool's definition as JSON, read here.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use roomkit::model::{RoomContext, RoomEvent, RoomId};
use roomkit::orchestration::{
    HandoffHandler, StrategyAgent, get_conversation_state, handoff_tool_definition,
};
use roomkit::store::ConversationStore;
use roomkit::{ChannelError, RoomKitError};
use roomkit_core::rules::transcript::{RUNTIME_RECORD, fence, identifier, without_mark_copies};
use serde_json::{Value, json};

use crate::memory::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::provider::{AiMessage, AiRole};
use crate::tools::AiTool;

/// How the context a previous agent hands on opens: Python's
/// `HANDED_ON_CONTEXT`.
const HANDED_ON_CONTEXT: &str = "[Context from previous agent";

/// The fence of a summary one model wrote for another: Python's
/// `CONVERSATION_SUMMARY_TAG`.
const CONVERSATION_SUMMARY_TAG: &str = "conversation_summary";

/// The generic handoff tool, its target free: Python's `HANDOFF_TOOL`.
pub static HANDOFF_TOOL: LazyLock<AiTool> =
    LazyLock::new(|| tool_of(&handoff_tool_definition(&[])));

/// A handoff tool whose target can only be one of `targets`, each an agent
/// id and its description; the generic [`HANDOFF_TOOL`] for none. Python's
/// `build_handoff_tool(targets)`.
#[must_use]
pub fn build_handoff_tool(targets: &[(String, Option<String>)]) -> AiTool {
    tool_of(&handoff_tool_definition(targets))
}

/// `definition`, `{name, description, parameters}`, as a tool.
fn tool_of(definition: &Value) -> AiTool {
    let text = |key: &str| definition[key].as_str().unwrap_or_default().to_owned();
    AiTool {
        name: text("name"),
        description: text("description"),
        parameters: definition["parameters"].clone(),
        tags: Vec::new(),
        changes_state: false,
    }
}

/// What [`setup_handoff`] takes beside the agent and the handler: Python's
/// `setup_handoff(channel, handler, *, tool=None, room_id=None)`.
#[derive(Clone, Debug, Default)]
pub struct SetupHandoffOptions {
    /// The tool declared, as [`build_handoff_tool`] builds one; `None`, the
    /// generic [`HANDOFF_TOOL`].
    pub tool: Option<AiTool>,
    /// The one room the handoff serves, when a strategy installs it for that
    /// room (RFC §19.7); `None` serves every room.
    pub room_id: Option<RoomId>,
}

/// Serves the handoff tool on `channel`, live or named by the id of one the
/// kit holds, with `handler`, in the room of each call (RFC §19.6): one
/// agent serves every room it is attached to. Python's `setup_handoff`.
///
/// # Errors
///
/// [`RoomKitError::Runtime`] when the handoff is already set up where it
/// would be declared; [`RoomKitError::ChannelNotRegistered`] for an id
/// naming no channel of the kit.
pub fn setup_handoff<S: ConversationStore>(
    channel: impl Into<StrategyAgent>,
    handler: &HandoffHandler<S>,
    options: SetupHandoffOptions,
) -> Result<(), RoomKitError> {
    let SetupHandoffOptions { tool, room_id } = options;
    let definition = tool.map(|tool| {
        json!({
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        })
    });
    handler.serve_tool(&channel.into(), definition, room_id.as_ref())
}

/// Injects a handoff's context, its summary and who wrote it, into the next
/// agent's prompt when the room's conversation state holds one, wrapping
/// another provider, whose window, ingestion and lifecycle stay its own:
/// Python's `HandoffMemoryProvider(inner)`.
#[derive(Clone)]
pub struct HandoffMemoryProvider {
    inner: Arc<dyn MemoryProvider>,
}

impl HandoffMemoryProvider {
    /// The provider over `inner`.
    #[must_use]
    pub fn new(inner: Arc<dyn MemoryProvider>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for HandoffMemoryProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HandoffMemoryProvider")
            .field("inner", &self.inner.name())
            .finish()
    }
}

/// The message handing `context`'s room's handoff on, if its state holds
/// one: a model wrote the summary, set apart, a copy of a mark in it
/// replaced, since this message keeps its own (RFC §6.4).
fn handed_on(context: &RoomContext) -> Result<Option<AiMessage>, ChannelError> {
    let state = get_conversation_state(&context.room).map_err(ChannelError::from)?;
    let summary = state
        .context
        .get("handoff_summary")
        .and_then(said)
        .filter(|summary| !summary.is_empty());
    let from = state
        .context
        .get("handoff_from")
        .and_then(Value::as_str)
        .filter(|from| !from.is_empty());
    let (Some(summary), Some(from)) = (summary, from) else {
        return Ok(None);
    };
    let text = format!(
        "{HANDED_ON_CONTEXT} ({})]\n{}",
        identifier(from, "agent"),
        fence(CONVERSATION_SUMMARY_TAG, &without_mark_copies(&summary))
    );
    let mut message = AiMessage::new(AiRole::User, text);
    message
        .metadata
        .insert(RUNTIME_RECORD.to_owned(), json!("handed_on_context"));
    Ok(Some(message))
}

/// `value` as Python's `str(value)` reads a summary a model handed on: none
/// for what Python's truth test refuses. A list or a map is written as JSON,
/// where Python writes its `repr`.
fn said(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Bool(false) => None,
        Value::Bool(true) => Some("True".to_owned()),
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => (number.as_f64() != Some(0.0)).then(|| number.to_string()),
        Value::Array(items) => (!items.is_empty()).then(|| value.to_string()),
        Value::Object(members) => (!members.is_empty()).then(|| value.to_string()),
    }
}

impl MemoryProvider for HandoffMemoryProvider {
    fn name(&self) -> String {
        format!("Handoff({})", self.inner.name())
    }

    fn recent_events_window(&self) -> usize {
        self.inner.recent_events_window()
    }

    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = self
                .inner
                .retrieve(room_id, current_event, context, options)
                .await?;
            if let Some(message) = handed_on(context)? {
                result.messages.insert(0, message);
            }
            Ok(result)
        })
    }

    fn ingest<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: &'a RoomEvent,
        options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.ingest(room_id, event, options)
    }

    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.clear(room_id)
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.close()
    }
}
