//! A scripted model's `chat_dialect` (README, "Scripted agents"): each of its
//! requests as this implementation's Chat Completions rendering writes it
//! for the dialect. The model still answers from its rounds; the rendering
//! is read once the run is over.
//!
//! The rendering is the core's (`roomkit_ai::chat_messages`), and a dialect
//! is what its `ChatDialect` states: where a server reads a round's earlier
//! reasoning (RFC §6.4). A vendor's provider is no part of this repository,
//! Anthropic's native Messages rendering among them: a scenario that names
//! `anthropic` fails here, and the runner lists it as a known failure.

use roomkit_ai::{AiContext, ChatDialect, ChatDialectOptions};
use serde_json::Value;

/// The field DeepSeek, MiniMax and Kimi read a round's earlier reasoning in.
const REASONING_CONTENT: &str = "reasoning_content";

/// What the Chat Completions dialect `name` states, as the vendor's
/// provider of RoomKit's Python implementation states it (its
/// `ChatDialect`); `None` for a rendering that is no Chat Completions
/// dialect.
fn options(name: &str) -> Option<ChatDialectOptions> {
    let reads_reasoning = ChatDialectOptions {
        thinking_field: Some(REASONING_CONTENT.to_owned()),
        ..ChatDialectOptions::default()
    };
    let options = match name {
        // OpenAI's own rendering sends no reasoning back.
        "openai" => ChatDialectOptions::default(),
        // OpenAI's, told by its configuration the field the server reads;
        // MiniMax's: the field on a round or an answer that reasoned.
        "openai_thinking_field" | "minimax" => reads_reasoning,
        // DeepSeek's: the field on every round that called tools, empty for
        // one that did not reason.
        "deepseek" => ChatDialectOptions {
            round_thinking_required: true,
            ..reads_reasoning
        },
        // Kimi's: a single space for a round that did not reason, since an
        // empty field reads as a missing one.
        "kimi" => ChatDialectOptions {
            round_thinking_required: true,
            round_thinking_placeholder: " ".to_owned(),
            ..reads_reasoning
        },
        _ => return None,
    };
    Some(options)
}

/// What `context` is on the wire under `dialect`: the request's `messages`.
///
/// # Errors
///
/// A dialect this implementation does not have, or messages that do not
/// render.
pub(crate) fn chat_messages(dialect: &str, context: &AiContext) -> Result<Value, String> {
    let no_rendering = || format!("chat_dialect {dialect}: roomkit-rs has no such rendering");
    let stated = options(dialect).ok_or_else(no_rendering)?;
    let stated = ChatDialect::new(stated).map_err(|error| error.to_string())?;
    let prompt = context.system_prompt.as_deref();
    let messages = roomkit_ai::chat_messages(&context.messages, prompt, &stated, "scripted");
    messages
        .map(Value::Array)
        .map_err(|error| error.to_string())
}
