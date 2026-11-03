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
use crate::turn_settings::{Refusal, type_name, validation_error};

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
    /// The tokens the model may reason with (RFC §6.7), 0 stating off.
    pub thinking_budget: Option<i64>,
    /// Whether the model reasons (RFC §6.7): a false turns off a budget a
    /// less specific level set.
    pub enable_thinking: Option<bool>,
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
            .field("thinking_budget", &self.thinking_budget)
            .field("enable_thinking", &self.enable_thinking)
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
/// A metadata `tools` that is no list of definitions fails the turn as
/// Python's reading of it raises: its `TypeError` for a value it cannot
/// iterate or a definition it cannot index by name, its `KeyError` for a
/// definition with no `name`, and pydantic's `ValidationError` for one
/// `AITool` refuses.
pub(crate) fn base_tools(
    turn: Option<&AiChannelTurnConfig>,
    metadata: &Metadata,
) -> Result<Vec<AiTool>, ChannelError> {
    if let Some(tools) = turn.and_then(|turn| turn.tools.as_ref()) {
        for tool in tools {
            if let Some(refusal) = name_refusal(&tool.name, &json!(tool.name)) {
                return Err(validation_error("AITool", &[refusal]));
            }
        }
        return Ok(tools.clone());
    }
    match metadata.get("tools") {
        None => Ok(Vec::new()),
        Some(declared) if is_falsy(declared) => Ok(Vec::new()),
        Some(Value::Array(declared)) => declared.iter().map(definition).collect(),
        // Iterated, a text gives its characters and a mapping its keys,
        // each a text indexed by "name".
        Some(Value::String(_) | Value::Object(_)) => Err(not_indexed(&json!(""))),
        Some(other) => Err(refused(
            TYPE_ERROR,
            format!("'{}' object is not iterable", type_name(other)),
        )),
    }
}

/// Python's class of a definition with no name.
const KEY_ERROR: &str = "builtins:KeyError";

/// Python's class of a value read as a mapping or a list it is not.
const TYPE_ERROR: &str = "builtins:TypeError";

/// A definition of the binding's metadata as Python's `AITool(name=
/// t["name"], description=t.get("description", ""), parameters=
/// t.get("parameters", {}), tags=t.get("tags", []) or [])` reads it.
fn definition(tool: &Value) -> Result<AiTool, ChannelError> {
    let Value::Object(fields) = tool else {
        return Err(not_indexed(tool));
    };
    let Some(name) = fields.get("name") else {
        return Err(refused(KEY_ERROR, "'name'".to_owned()));
    };
    let mut refusals = Vec::new();
    match name.as_str() {
        None => refusals.push(text_refused(name).of("name")),
        Some(text) => refusals.extend(name_refusal(text, name)),
    }
    let description = fields.get("description");
    if let Some(value) = description.filter(|value| !value.is_string()) {
        refusals.push(text_refused(value).of("description"));
    }
    let parameters = fields.get("parameters");
    if let Some(value) = parameters.filter(|value| !value.is_object()) {
        let refusal = Refusal::new("dict_type", "Input should be a valid dictionary", value);
        refusals.push(refusal.of("parameters"));
    }
    let tags = tags_of(fields.get("tags"), &mut refusals);
    if !refusals.is_empty() {
        return Err(validation_error("AITool", &refusals));
    }
    Ok(AiTool {
        name: name.as_str().unwrap_or_default().to_owned(),
        description: description
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        parameters: parameters.cloned().unwrap_or_else(|| json!({})),
        tags,
    })
}

/// A definition's `tags`, nothing for what Python reads as false, with what
/// pydantic refuses of them added to `refusals`.
fn tags_of(tags: Option<&Value>, refusals: &mut Vec<Refusal>) -> Vec<String> {
    let Some(tags) = tags.filter(|tags| !is_falsy(tags)) else {
        return Vec::new();
    };
    let Value::Array(tags) = tags else {
        refusals.push(Refusal::new("list_type", "Input should be a valid list", tags).of("tags"));
        return Vec::new();
    };
    tags.iter()
        .enumerate()
        .filter_map(|(at, tag)| {
            if tag.is_string() {
                tag.as_str().map(str::to_owned)
            } else {
                refusals.push(text_refused(tag).of(format!("tags.{at}")));
                None
            }
        })
        .collect()
}

/// pydantic's refusal of `name`, written `value`, when no vendor accepts
/// it, as Python's `AITool` checks it (RFC §6.7).
fn name_refusal(name: &str, value: &Value) -> Option<Refusal> {
    let error = AiTool::checked(name).err()?;
    let message = format!("Value error, {error}");
    Some(Refusal::new("value_error", message, value).of("name"))
}

/// pydantic's refusal of `value` as a text.
fn text_refused(value: &Value) -> Refusal {
    Refusal::new("string_type", "Input should be a valid string", value)
}

/// Python's `TypeError` for `value["name"]`, `value` no mapping.
fn not_indexed(value: &Value) -> ChannelError {
    let words = match value {
        Value::String(_) => "string indices must be integers, not 'str'".to_owned(),
        Value::Array(_) => "list indices must be integers or slices, not str".to_owned(),
        other @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_)) => {
            format!("'{}' object is not subscriptable", type_name(other))
        }
    };
    refused(TYPE_ERROR, words)
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

fn refused(class: &'static str, words: String) -> ChannelError {
    Box::new(ClassedError::new(class, Refused(words)))
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
    // no Python test pins its errors, read here from Python's own.
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
                tags: Vec::new(),
            }]
        );
        for falsy in [
            json!(null),
            json!(false),
            json!(0),
            json!(""),
            json!({}),
            json!([]),
        ] {
            let metadata = Metadata::from_iter([("tools".to_owned(), falsy)]);
            assert_eq!(base_tools(None, &metadata).unwrap(), []);
        }
        let indices = "string indices must be integers, not 'str'";
        let refused = [
            (json!([{ "description": "mail" }]), ("KeyError", "'name'")),
            (json!(["outlook"]), ("TypeError", indices)),
            (json!("abc"), ("TypeError", indices)),
            (json!({ "name": "outlook" }), ("TypeError", indices)),
            (json!(5), ("TypeError", "'int' object is not iterable")),
            (
                json!([5]),
                ("TypeError", "'int' object is not subscriptable"),
            ),
            (
                json!([["outlook"]]),
                (
                    "TypeError",
                    "list indices must be integers or slices, not str",
                ),
            ),
        ];
        for (tools, (class, words)) in refused {
            assert_eq!(refusal(tools), (class.to_owned(), words.to_owned()));
        }
    }

    // Python: AITool's fields as pydantic reads them, its name checked by
    // `_a_name_some_vendor_accepts` (RFC §6.7), every refusal named.
    // Python: pydantic's refusal of AITool(tags=["a", 2]), which names the place
    #[test]
    fn a_refused_tag_names_its_place_in_the_list() {
        let (class, words) = refusal(json!([{ "name": "lookup", "tags": ["a", 2] }]));
        assert!(class.contains("ValidationError"), "{class}");
        assert!(
            words.contains("\ntags.1\n  Input should be a valid string"),
            "{words}"
        );
    }

    // Python: AITool(tags=...) reads a list of texts, and _turn_base_tools what is false as none
    #[test]
    fn a_binding_s_tool_reads_its_tags() {
        let metadata = |tags: Value| {
            let mut metadata = Metadata::new();
            metadata.insert(
                "tools".to_owned(),
                json!([{ "name": "lookup", "tags": tags }]),
            );
            metadata
        };
        for (tags, read) in [
            (json!(["send", "sms"]), vec!["send", "sms"]),
            (json!([]), Vec::new()),
            (json!(null), Vec::new()),
            (json!(""), Vec::new()),
        ] {
            let tools = base_tools(None, &metadata(tags)).unwrap();
            assert_eq!(tools[0].tags, read);
        }
        assert!(
            refusal(json!([{ "name": "lookup", "tags": "send" }]))
                .1
                .contains("tags\n")
        );
    }

    #[test]
    fn a_binding_s_tool_that_aitool_refuses_fails_the_turn() {
        let (class, words) =
            refusal(json!([{ "name": 5, "description": null, "parameters": null }]));
        assert_eq!(class, "ValidationError");
        assert_eq!(
            words,
            "3 validation errors for AITool\n\
             name\n  Input should be a valid string [type=string_type, input_value=5, input_type=int]\n\
             description\n  Input should be a valid string [type=string_type, input_value=None, input_type=NoneType]\n\
             parameters\n  Input should be a valid dictionary [type=dict_type, input_value=None, input_type=NoneType]"
        );
        let (class, words) = refusal(json!([{ "name": "bad name!" }]));
        assert_eq!(class, "ValidationError");
        // The same name in a turn's configuration, which Python's AITool
        // refuses as it is built.
        let config = AiChannelTurnConfig {
            tools: Some(vec![AiTool {
                name: "bad name!".to_owned(),
                description: String::new(),
                parameters: json!({}),
                tags: Vec::new(),
            }]),
            ..AiChannelTurnConfig::default()
        };
        let error = base_tools(Some(&config), &Metadata::new()).unwrap_err();
        assert_eq!(error.to_string(), words);
        assert!(
            words.starts_with("1 validation error for AITool\nname\n  Value error, tool name 'bad name!' is accepted by no provider"),
            "{words}"
        );
    }
}
