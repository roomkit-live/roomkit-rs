//! A turn's own configuration (RFC Appendix A.9, per-turn settings):
//! Python's `AIChannelTurnConfig`, what an AI channel's `config_provider`
//! returns at the start of each turn, and the turn's base toolset, its
//! configuration's or its binding's (Python's `_resolve_turn`,
//! `_turn_base_tools`).

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{ChannelBinding, Metadata, RoomContext};
use roomkit::{ChannelError, ClassedError};
use serde_json::{Value, json};

use crate::tools::{AiTool, ToolHandler};

/// One turn's configuration, which an AI channel's
/// [`config_provider`](crate::AiChannelOptions::config_provider) returns
/// at the start of each turn (RFC Appendix A.9, per-turn settings). Each
/// setting sits under the one the turn's binding's metadata sets and over
/// the channel's own; `None` is not set, the next level's standing.
/// Python's `AIChannelTurnConfig`.
#[derive(Clone, Default)]
pub struct AiChannelTurnConfig {
    /// What the turn's system prompt opens with; an empty text opens none.
    pub system_prompt: Option<String>,
    /// The turn's toolset, served by the turn's `tool_handler`, else the
    /// channel's: it replaces the one the binding's metadata declares, the
    /// channel's own tools still declared after it.
    pub tools: Option<Vec<AiTool>>,
    /// How freely the model samples.
    pub temperature: Option<f64>,
    /// The most tokens a generation writes.
    pub max_tokens: Option<u32>,
    /// How hard a reasoning model thinks (RFC §6.7).
    pub reasoning_effort: Option<String>,
    /// The turn's budget of billed tokens (RFC §6.4), more than zero.
    pub turn_budget_tokens: Option<u64>,
    /// The turn's cost budget at the model's catalogue price (RFC §6.4),
    /// above 0.
    pub turn_budget_usd: Option<f64>,
    /// What serves the host's tools for this turn alone, in place of the
    /// channel's handler and of its tools serving themselves; the
    /// channel's own tools (a person's, the skills') keep their own. It is
    /// never installed on the channel, nor read by another turn.
    pub tool_handler: Option<ToolHandler>,
}

impl fmt::Debug for AiChannelTurnConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiChannelTurnConfig")
            .field("system_prompt", &self.system_prompt)
            .field("tools", &self.tools)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .field("reasoning_effort", &self.reasoning_effort)
            .field("turn_budget_tokens", &self.turn_budget_tokens)
            .field("turn_budget_usd", &self.turn_budget_usd)
            .field("tool_handler", &self.tool_handler.is_some())
            .finish()
    }
}

/// What a channel's `config_provider` answers: the turn's configuration,
/// `None` for none, or the error that fails the turn.
pub(crate) type TurnConfigFuture =
    Pin<Box<dyn Future<Output = Result<Option<AiChannelTurnConfig>, ChannelError>> + Send>>;

/// A channel's `config_provider`, called with the turn's binding and room
/// context.
pub(crate) type ConfigProvider =
    Arc<dyn Fn(ChannelBinding, RoomContext) -> TurnConfigFuture + Send + Sync>;

/// How an AI channel configures each turn: its `config_provider`, and the
/// host tools it reported declared under a name already taken, each once.
#[derive(Clone, Default)]
pub(crate) struct TurnConfigs {
    provider: Option<ConfigProvider>,
    collisions: Arc<Collisions>,
}

impl TurnConfigs {
    pub(crate) fn new(provider: Option<ConfigProvider>) -> Self {
        Self {
            provider,
            collisions: Arc::default(),
        }
    }

    /// The configuration of a turn on `binding` in `context`'s room, read
    /// fresh at the start of every turn; none without a provider.
    ///
    /// # Errors
    ///
    /// The provider's error, which fails the turn.
    pub(crate) async fn resolve(
        &self,
        binding: &ChannelBinding,
        context: &RoomContext,
    ) -> Result<Option<AiChannelTurnConfig>, ChannelError> {
        match &self.provider {
            None => Ok(None),
            Some(provider) => provider(binding.clone(), context.clone()).await,
        }
    }

    pub(crate) fn collisions(&self) -> &Collisions {
        &self.collisions
    }
}

/// The host tools a channel was given under a name already declared, each
/// reported once: a wiring diagnostic, not a turn event (Python's
/// `CollisionLog`).
#[derive(Default)]
pub(crate) struct Collisions {
    reported: Mutex<HashSet<String>>,
}

impl Collisions {
    /// Reports, once, that `channel` declares its own `name` in place of
    /// the host tool a turn gave under it.
    pub(crate) fn served(&self, channel: &str, name: &str) {
        if self.first("served", name) {
            tracing::warn!(target: "roomkit::ai", channel, tool = name, "the channel serves this tool itself: the host's tool under its name is not declared");
        }
    }

    /// Reports, once, that `channel` was given `name` twice, the later
    /// kept.
    pub(crate) fn duplicate(&self, channel: &str, name: &str) {
        if self.first("duplicate", name) {
            tracing::warn!(target: "roomkit::ai", channel, tool = name, "the channel declares this tool once: it was given twice, the later is kept");
        }
    }

    fn first(&self, kind: &str, name: &str) -> bool {
        self.reported
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(format!("{kind}:{name}"))
    }
}

/// A turn's base toolset, before the channel's own tools: its
/// configuration's `tools`, else the definitions its binding's metadata
/// declares under `tools`, a `null` or an empty value declaring none (RFC
/// Appendix A.9; Python's `_turn_base_tools`).
///
/// # Errors
///
/// A metadata `tools` that is no list of definitions fails the turn:
/// Python's `KeyError` for a definition with no `name`, its `TypeError`
/// for one that is no mapping or a `tools` that is no list.
pub(crate) fn base_tools(
    turn: Option<&AiChannelTurnConfig>,
    metadata: &Metadata,
) -> Result<Vec<AiTool>, ChannelError> {
    if let Some(tools) = turn.and_then(|turn| turn.tools.as_ref()) {
        return Ok(tools.clone());
    }
    let declared = match metadata.get("tools") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(declared)) => declared,
        Some(other) if is_falsy(other) => return Ok(Vec::new()),
        Some(_) => return Err(refused(TYPE_ERROR, "the binding's tools is no list")),
    };
    declared.iter().map(definition).collect()
}

/// Python's class of a definition with no name.
const KEY_ERROR: &str = "builtins:KeyError";

/// Python's class of a value read as a mapping or a list it is not.
const TYPE_ERROR: &str = "builtins:TypeError";

/// A definition of the binding's metadata as Python's `AITool` reads it:
/// a `description` of `""` and `parameters` of `{}` when absent.
fn definition(tool: &Value) -> Result<AiTool, ChannelError> {
    let Value::Object(fields) = tool else {
        return Err(refused(TYPE_ERROR, "a binding's tool is no mapping"));
    };
    let Some(name) = fields.get("name") else {
        return Err(refused(KEY_ERROR, "'name'"));
    };
    let Some(name) = name.as_str() else {
        return Err(refused(TYPE_ERROR, "a binding's tool name is no text"));
    };
    Ok(AiTool {
        name: name.to_owned(),
        description: fields
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        parameters: fields
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| json!({})),
    })
}

/// Whether Python reads `value` as false: `binding.metadata.get("tools")
/// or []` declares nothing for it.
fn is_falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(flag) => !flag,
        Value::Number(number) => number.as_f64() == Some(0.0),
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
    }
}

fn refused(class: &'static str, words: &str) -> ChannelError {
    Box::new(ClassedError::new(class, Refused(words.to_owned())))
}

/// A binding's toolset refused, in Python's words.
#[derive(Debug)]
struct Refused(String);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// The class and the words of what `base_tools` refuses `tools`, a
    /// binding's metadata `tools`, with.
    fn refusal(tools: Value) -> (String, String) {
        let metadata = Metadata::from_iter([("tools".to_owned(), tools)]);
        let error = base_tools(None, &metadata).unwrap_err();
        let classed = error.downcast_ref::<ClassedError>().unwrap();
        (classed.class().to_owned(), error.to_string())
    }

    // Python: channels/_ai_context.py _turn_base_tools reads a binding's
    // tools as `AITool(name=t["name"], description=t.get("description",
    // ""), parameters=t.get("parameters", {}))`, `or []` for a falsy one;
    // no Python test pins its errors.
    #[test]
    fn a_binding_s_tools_read_as_python_reads_them() {
        let metadata = Metadata::from_iter([("tools".to_owned(), json!([{ "name": "outlook" }]))]);
        let read = base_tools(None, &metadata).unwrap();
        assert_eq!(
            read,
            [AiTool {
                name: "outlook".to_owned(),
                description: String::new(),
                parameters: json!({}),
            }]
        );
        for falsy in [json!(null), json!(false), json!(""), json!({}), json!([])] {
            let metadata = Metadata::from_iter([("tools".to_owned(), falsy)]);
            assert_eq!(base_tools(None, &metadata).unwrap(), []);
        }
        assert_eq!(refusal(json!([{ "description": "mail" }])).0, "KeyError");
        assert_eq!(refusal(json!(["outlook"])).0, "TypeError");
        assert_eq!(refusal(json!({ "name": "outlook" })).0, "TypeError");
    }
}
