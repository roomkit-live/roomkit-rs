//! A tool call's intent (RFC §6.4): the short sentence a model writes with a
//! call to tell the person what the call does.
//!
//! An agent with `tool_call_intents` on shows its model each tool's schema
//! with one optional property more, [`INTENT_PROPERTY`]. The tool loop takes
//! it out of a call's arguments before anything reads them and carries its
//! value, normalized, as the call's `intent`: the tool never receives it. A
//! tool whose own schema declares the property keeps it as its argument. The
//! model reads its calls back with their intent, as normalized, among their
//! arguments.

use serde_json::{Map, Value, json};

use crate::model::ToolCall;

/// The reserved property a call's intent rides in.
pub const INTENT_PROPERTY: &str = "_roomkit_intent";

/// The longest intent kept, in characters (Unicode scalar values).
pub const INTENT_MAX_CHARS: usize = 200;

/// What the model is told of the property.
const INTENT_DESCRIPTION: &str = "One short sentence, in the language of the conversation, \
    telling the user what this call does. Shown to the user; never passed to the tool.";

/// The bidirectional formatting characters: they can make the text a person
/// reads differ from the text that was written.
const BIDI_CONTROLS: [char; 12] = [
    '\u{061C}', '\u{200E}', '\u{200F}', '\u{202A}', '\u{202B}', '\u{202C}', '\u{202D}', '\u{202E}',
    '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}',
];

/// `value` as a call's intent: a string with each control character a
/// space, the bidirectional controls removed, its whitespace runs one space,
/// trimmed and cut at [`INTENT_MAX_CHARS`]; `None` when nothing usable is
/// left, or `value` is not a string.
///
/// ```
/// use roomkit_core::rules::tool_intent::normalized_intent;
/// use serde_json::json;
///
/// assert_eq!(normalized_intent(&json!("  I look\n for files ")).as_deref(), Some("I look for files"));
/// assert_eq!(normalized_intent(&json!(42)), None);
/// ```
pub fn normalized_intent(value: &Value) -> Option<String> {
    let Value::String(text) = value else {
        return None;
    };
    let kept: String = text
        .chars()
        .filter(|c| !BIDI_CONTROLS.contains(c))
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let joined = kept.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = joined.chars().take(INTENT_MAX_CHARS).collect();
    let intent = cut.trim_end();
    (!intent.is_empty()).then(|| intent.to_owned())
}

/// `parameters`, a tool's schema, as an agent with intents shows it to its
/// model: with the reserved property; `None` when it cannot take it, being
/// no object schema or declaring the property already.
pub fn presented_parameters(parameters: &Value) -> Option<Value> {
    let Value::Object(schema) = parameters else {
        return None;
    };
    if schema.get("type").is_some_and(|kind| kind != "object") {
        return None;
    }
    let mut properties = match schema.get("properties") {
        None => Map::new(),
        Some(Value::Object(properties)) if !properties.contains_key(INTENT_PROPERTY) => {
            properties.clone()
        }
        Some(_) => return None,
    };
    properties.insert(
        INTENT_PROPERTY.to_owned(),
        json!({ "type": "string", "description": INTENT_DESCRIPTION }),
    );
    let mut shown = schema.clone();
    shown.insert("type".to_owned(), Value::from("object"));
    shown.insert("properties".to_owned(), Value::Object(properties));
    Some(Value::Object(shown))
}

/// Whether a tool's own schema declares the reserved property: its calls
/// keep it as their argument.
pub fn declares_intent(parameters: &Value) -> bool {
    parameters
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| properties.contains_key(INTENT_PROPERTY))
}

/// Takes the reserved property out of `call`'s arguments, its value
/// normalized as the call's intent; leaves a call without it as it is.
pub fn take_intent(call: &mut ToolCall) {
    if let Some(value) = call.arguments.remove(INTENT_PROPERTY) {
        call.intent = normalized_intent(&value);
    }
}

/// `call` as a replay shows the model its call: its intent back among its
/// arguments when it has one, and no intent beside them.
pub fn as_written(call: &ToolCall) -> ToolCall {
    let mut written = call.clone();
    if let Some(intent) = written.intent.take() {
        written
            .arguments
            .insert(INTENT_PROPERTY.to_owned(), Value::String(intent));
    }
    written
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn an_intent_is_normalized_or_none() {
        let long = "é".repeat(INTENT_MAX_CHARS + 5);
        let before_space = format!("{} b", "a".repeat(INTENT_MAX_CHARS - 1));
        let cases = [
            (
                json!("I look for the files."),
                Some("I look for the files.".to_owned()),
            ),
            (
                json!("  I  look\n\tfor files "),
                Some("I look for files".to_owned()),
            ),
            (
                json!("I\u{0}look\u{1b}[31m"),
                Some("I look [31m".to_owned()),
            ),
            (json!("\u{202E}I look\u{2066}"), Some("I look".to_owned())),
            (json!(long), Some("é".repeat(INTENT_MAX_CHARS))),
            (json!(before_space), Some("a".repeat(INTENT_MAX_CHARS - 1))),
            (json!("   "), None),
            (json!(""), None),
            (Value::Null, None),
            (json!(42), None),
            (json!(["I look"]), None),
            (json!({ "text": "I look" }), None),
        ];
        for (value, intent) in cases {
            assert_eq!(normalized_intent(&value), intent, "{value}");
        }
    }

    #[test]
    fn a_schema_is_shown_with_the_property_unless_it_cannot_take_it() {
        let closed = json!({
            "type": "object",
            "properties": { "command": { "type": "string" } },
            "required": ["command"],
            "additionalProperties": false,
        });
        let shown = presented_parameters(&closed).unwrap();
        assert_eq!(
            (
                &shown["required"],
                &shown["additionalProperties"],
                &shown["properties"][INTENT_PROPERTY]["type"]
            ),
            (&json!(["command"]), &json!(false), &json!("string"))
        );
        assert_eq!(
            presented_parameters(&json!({}))
                .unwrap()
                .pointer("/properties/_roomkit_intent/type"),
            Some(&json!("string"))
        );
        let owner = json!({ "properties": { INTENT_PROPERTY: { "type": "string" } } });
        assert_eq!(
            [
                presented_parameters(&owner),
                presented_parameters(&json!({ "type": "array" })),
            ],
            [None, None]
        );
        assert!(declares_intent(&owner) && !declares_intent(&closed));
    }

    #[test]
    fn a_call_reads_back_as_the_model_wrote_it() {
        let mut arguments = crate::model::Metadata::new();
        arguments.insert("command".to_owned(), json!("ls"));
        arguments.insert(INTENT_PROPERTY.to_owned(), json!(" I list. "));
        let mut call = ToolCall::new("c1", "shell", arguments);
        take_intent(&mut call);
        assert_eq!(
            (
                Value::Object(call.arguments.clone()),
                call.intent.as_deref()
            ),
            (json!({ "command": "ls" }), Some("I list."))
        );
        let written = as_written(&call);
        assert_eq!(
            (Value::Object(written.arguments), written.intent),
            (
                json!({ "command": "ls", (INTENT_PROPERTY): "I list." }),
                None
            )
        );
    }
}
