//! A scripted model's `chat_dialect` (README, "Scripted agents"): each of its
//! requests as the core's chat rendering writes it under the options the
//! scenario states. The model still answers from its rounds; the rendering
//! is read once the run is over.
//!
//! The rendering is the core's (`roomkit_ai::chat_messages`), and the
//! options are its `ChatDialectOptions`: where a server reads a round's
//! earlier reasoning (RFC §6.4), whether a tool message names its tool,
//! whether text goes flat. No vendor is named here.

use roomkit_ai::{AiContext, ChatDialect, ChatDialectOptions};
use serde_json::Value;

/// The options `stated` names, as the core's `ChatDialectOptions`: each
/// member absent keeps its default.
///
/// # Errors
///
/// A value that is no object, a member the core does not have, or a member
/// of another type.
fn options(stated: &Value) -> Result<ChatDialectOptions, String> {
    let members = stated
        .as_object()
        .ok_or_else(|| format!("chat_dialect {stated}: not an object of options"))?;
    let mut options = ChatDialectOptions::default();
    for (name, value) in members {
        let wrong = || format!("chat_dialect.{name}: {value} is not of its type");
        match name.as_str() {
            "thinking_field" => {
                options.thinking_field = Some(value.as_str().ok_or_else(wrong)?.to_owned());
            }
            "names_tool_results" => {
                options.names_tool_results = value.as_bool().ok_or_else(wrong)?;
            }
            "flattens_text" => options.flattens_text = value.as_bool().ok_or_else(wrong)?,
            "round_thinking_required" => {
                options.round_thinking_required = value.as_bool().ok_or_else(wrong)?;
            }
            "round_thinking_placeholder" => {
                value
                    .as_str()
                    .ok_or_else(wrong)?
                    .clone_into(&mut options.round_thinking_placeholder);
            }
            _ => return Err(format!("chat_dialect.{name}: no such option")),
        }
    }
    Ok(options)
}

/// The core's dialect under the options `stated` names.
///
/// # Errors
///
/// Options that do not read, or that the core refuses.
pub(crate) fn dialect(stated: &Value) -> Result<ChatDialect, String> {
    ChatDialect::new(options(stated)?).map_err(|error| error.to_string())
}

/// What `context` is on the wire under `dialect`: the request's `messages`.
///
/// # Errors
///
/// Messages that do not render.
pub(crate) fn chat_messages(dialect: &ChatDialect, context: &AiContext) -> Result<Value, String> {
    let prompt = context.system_prompt.as_deref();
    let messages = roomkit_ai::chat_messages(&context.messages, prompt, dialect, "scripted");
    messages
        .map(Value::Array)
        .map_err(|error| error.to_string())
}
