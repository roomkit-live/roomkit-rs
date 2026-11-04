//! What the model reads of a call that did not serve (RFC §6.4, §9.3,
//! §21.1): one text per cause, the same on every door, as the reference
//! implementation writes them.

/// `reason` as the model reads a refusal or a failure: `{"error":
/// "<reason>"}`.
pub(crate) fn error_envelope(reason: &str) -> String {
    format!("{{\"error\": {}}}", quoted(reason))
}

/// A call under an id another call of its round holds (RFC §12.4). Python's
/// `call_id_in_flight_error`.
pub(crate) fn call_id_in_flight(id: &str) -> String {
    error_envelope(&format!("Tool call '{id}' has not had its result yet"))
}

/// A call to a name the turn does not carry (RFC §21.1).
pub(crate) fn not_declared(name: &str) -> String {
    error_envelope(&format!("Tool '{name}' is not declared."))
}

/// A call to a name no tool carries while Tool Search hides a catalogue: none
/// exists, and how to find the right one (RFC §21.1). Python's
/// `unknown_tool_error(name, searching=True)`.
pub(crate) fn no_such_tool(name: &str) -> String {
    let error = quoted(&format!("No tool named '{name}' exists."));
    let hint = quoted(
        "Check the spelling, or call find_tools(query=<the task>) to discover the right tool.",
    );
    format!("{{\"error\": {error}, \"hint\": {hint}}}")
}

/// A call to `name` repeated with identical arguments, already made `repeats`
/// times this turn (RFC §6.4). Python's `_repeated_call_guard`.
pub(crate) fn repeated_call(name: &str, repeats: u32) -> String {
    let error = quoted(&format!(
        "You already called '{name}' with these EXACT arguments {repeats} time(s) this turn \u{2014} repeating it cannot yield anything new."
    ));
    let hint = quoted(
        "STOP repeating this call. Use the results you already have, try genuinely different arguments, or answer the user now with what you know.",
    );
    format!("{{\"error\": {error}, \"hint\": {hint}}}")
}

/// A call to a tool withdrawn for the turn, by `BEFORE_AI_GENERATION` or
/// `AFTER_TOOL_ROUND` (RFC §6.4).
pub(crate) fn not_available(name: &str) -> String {
    error_envelope(&format!("Tool '{name}' is not available in this turn."))
}

/// A call to a tool the policy denies (RFC §21.1).
pub(crate) fn not_permitted(name: &str) -> String {
    error_envelope(&format!(
        "Tool '{name}' is not permitted by the agent's tool policy."
    ))
}

/// A call whose arguments do not fit the tool's schema (RFC §21.1).
pub(crate) fn invalid_arguments(name: &str, why: &str) -> String {
    error_envelope(&format!("Invalid arguments for '{name}': {why}"))
}

/// A call whose arguments a `BEFORE_TOOL_USE` hook left that do not fit the
/// tool's schema (RFC §21.1).
pub(crate) fn invalid_rewritten_arguments(name: &str, why: &str) -> String {
    error_envelope(&format!("Invalid rewritten arguments for '{name}': {why}"))
}

/// A call nothing served, neither its tool nor a hook (RFC §9.3).
pub(crate) fn unserved(name: &str) -> String {
    error_envelope(&format!("No handler for tool {name}"))
}

/// A call whose tool raised an error of `kind` (RFC §9.3).
pub(crate) fn failed(name: &str, kind: &str) -> String {
    error_envelope(&format!("Tool '{name}' failed ({kind})"))
}

/// A call the response cut short over its arguments, which never ran (RFC
/// §6.4): the model may call again, with less if it ran out of room.
pub(crate) fn cut(name: &str) -> String {
    envelope_with_hint(
        "Tool call cut off",
        name,
        "This call was cut off before its arguments were complete, so it did not run. \
         Call it again if it is still needed, with shorter arguments if the response ran out \
         of room.",
    )
}

/// A call the model wrote with arguments that read as no object, which never
/// ran (RFC §6.4).
pub(crate) fn unreadable(name: &str) -> String {
    envelope_with_hint(
        "Tool call arguments unreadable",
        name,
        "This call's arguments are not a JSON object, so it did not run. Call it again with \
         arguments that are a valid JSON object matching the tool's parameters.",
    )
}

/// `{"error": ..., "tool": ..., "hint": ...}`, in that order.
fn envelope_with_hint(error: &str, name: &str, hint: &str) -> String {
    format!(
        "{{\"error\": {}, \"tool\": {}, \"hint\": {}}}",
        quoted(error),
        quoted(name),
        quoted(hint)
    )
}

/// `text` as a JSON string, as Python's `json.dumps` writes it: every
/// character past ASCII escaped.
fn quoted(text: &str) -> String {
    crate::skills::dumps(&serde_json::Value::String(text.to_owned()))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn each_refusal_reads_its_cause() {
        assert_eq!(
            [
                not_declared("no_such_tool"),
                not_permitted("admin_wipe"),
                invalid_arguments("lookup", "missing required argument 'order'"),
                failed("lookup", "RuntimeError"),
            ],
            [
                r#"{"error": "Tool 'no_such_tool' is not declared."}"#,
                r#"{"error": "Tool 'admin_wipe' is not permitted by the agent's tool policy."}"#,
                r#"{"error": "Invalid arguments for 'lookup': missing required argument 'order'"}"#,
                r#"{"error": "Tool 'lookup' failed (RuntimeError)"}"#,
            ]
        );
    }

    /// Python's `json.dumps` escapes every character past ASCII in the
    /// words the model reads, a tool's name included.
    #[test]
    fn a_refusal_escapes_what_is_past_ascii() {
        assert_eq!(
            not_declared("réserver"),
            r#"{"error": "Tool 'r\u00e9server' is not declared."}"#
        );
    }

    #[test]
    fn a_cut_call_reads_as_json_with_its_hint() {
        let text = cut("lookup");
        let read: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (read["error"].as_str(), read["tool"].as_str()),
            (Some("Tool call cut off"), Some("lookup"))
        );
    }
}
