//! A turn's settings as its binding's metadata sets them over the
//! channel's (RFC §6.4, Appendix A.9), read as Python reads them: the
//! temperature and the max tokens as pydantic's lax mode reads
//! `AIContext`'s `float` and `int` fields, the token budget as Python's
//! `turn_budget` checks it (Python's `_turn_settings`, `_turn_budget`).
//!
//! A `null` there is not set, the channel's value standing. A value read
//! otherwise fails the turn: the token budget with Python's `ValueError`,
//! the context's fields with pydantic's `ValidationError`, in its words
//! without the documentation link pydantic adds under each error.

use std::fmt::{self, Write as _};
use std::num::IntErrorKind;

use roomkit::model::Metadata;
use roomkit::{ChannelError, ClassedError};
use serde_json::Value;

use crate::memory::repr::repr;

/// Python's class of a context's refused field.
const VALIDATION_ERROR: &str = "pydantic_core:ValidationError";

/// Python's class of a refused token budget.
const VALUE_ERROR: &str = "builtins:ValueError";

/// The longest text pydantic reads as an integer.
const MAX_INT_TEXT: usize = 4300;

/// The settings one turn runs with, each its binding's over its channel's.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TurnSettings {
    /// How freely the model samples.
    pub(crate) temperature: f64,
    /// The most tokens a generation writes; `None` leaves it to the
    /// provider.
    pub(crate) max_tokens: Option<u32>,
    /// The turn's budget of billed tokens; `None` sets none.
    pub(crate) turn_budget_tokens: Option<u64>,
    /// The context's fields the binding sets to a value pydantic refuses,
    /// in `AIContext`'s order: the turn fails once its context is built.
    refused: Vec<Refusal>,
}

impl TurnSettings {
    /// The settings `metadata`, a binding's, sets over the channel's own
    /// `temperature`, `max_tokens` and `turn_budget_tokens`. A field the
    /// binding sets to a value pydantic refuses keeps the channel's, and
    /// [`Self::validated`] then fails the turn.
    ///
    /// # Errors
    ///
    /// A system prompt that is no text, with every field refused, as
    /// Python's `_turn_settings` reads it as `AIContext` does before the
    /// turn reads it as a text (RMK-1237); then a token budget that is not
    /// a positive integer, as Python's `turn_budget` raises `ValueError`
    /// before the turn reads anything.
    pub(crate) fn read(
        metadata: &Metadata,
        temperature: f64,
        max_tokens: Option<u32>,
        turn_budget_tokens: Option<u64>,
    ) -> Result<Self, ChannelError> {
        let mut settings = Self {
            temperature,
            max_tokens,
            turn_budget_tokens,
            refused: Vec::new(),
        };
        let prompt = set(metadata, "system_prompt").filter(|prompt| !prompt.is_string());
        if let Some(prompt) = prompt {
            let refusal = Refusal::new("string_type", "Input should be a valid string", prompt);
            settings.refused.push(refusal.of("system_prompt"));
        }
        if let Some(value) = set(metadata, "temperature") {
            match lax_float(value) {
                Ok(read) => settings.temperature = read,
                Err(refusal) => settings.refused.push(refusal.of("temperature")),
            }
        }
        if let Some(value) = set(metadata, "max_tokens") {
            match lax_int(value).and_then(|whole| within_u32(whole, value)) {
                Ok(read) => settings.max_tokens = Some(read),
                Err(refusal) => settings.refused.push(refusal.of("max_tokens")),
            }
        }
        if prompt.is_some() {
            settings.validated()?;
        }
        if let Some(budget) = set(metadata, "turn_budget_tokens") {
            settings.turn_budget_tokens = Some(positive_budget(budget)?);
        }
        Ok(settings)
    }

    /// Whether the turn's context takes the settings, as Python builds
    /// `AIContext` once the turn's conversation is read.
    ///
    /// # Errors
    ///
    /// pydantic's `ValidationError` naming each field refused.
    pub(crate) fn validated(&self) -> Result<(), ChannelError> {
        if self.refused.is_empty() {
            return Ok(());
        }
        let count = self.refused.len();
        let plural = if count == 1 { "" } else { "s" };
        let mut text = format!("{count} validation error{plural} for AIContext");
        for refusal in &self.refused {
            let _infallible = write!(text, "\n{refusal}");
        }
        Err(Box::new(ClassedError::new(VALIDATION_ERROR, Refused(text))))
    }
}

/// What `metadata` sets `key` to: `None` for a `null` there, not set at
/// any level (Python's `_turn_source`).
fn set<'value>(metadata: &'value Metadata, key: &str) -> Option<&'value Value> {
    metadata.get(key).filter(|value| !value.is_null())
}

/// `value` as Python's `turn_budget` takes a token budget: an integer, no
/// boolean, above zero.
fn positive_budget(value: &Value) -> Result<u64, ChannelError> {
    match value.as_u64() {
        Some(budget) if budget > 0 => Ok(budget),
        _ => Err(Box::new(ClassedError::new(
            VALUE_ERROR,
            Refused(format!(
                "turn_budget_tokens must be a positive number, got {}",
                repr(value)
            )),
        ))),
    }
}

/// A field's value pydantic refuses: its error type, its words and the
/// value.
#[derive(Clone, Debug, PartialEq)]
struct Refusal {
    field: &'static str,
    kind: &'static str,
    message: String,
    value: Value,
}

impl Refusal {
    /// The refusal of `value` under `kind`, in pydantic's `message`.
    fn new(kind: &'static str, message: impl Into<String>, value: &Value) -> Self {
        Self {
            field: "",
            kind,
            message: message.into(),
            value: value.clone(),
        }
    }

    /// The same refusal, of the field `field`.
    fn of(mut self, field: &'static str) -> Self {
        self.field = field;
        self
    }
}

impl fmt::Display for Refusal {
    /// As pydantic writes one error: the field, then its words, type,
    /// input and input type.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}\n  {} [type={}, input_value={}, input_type={}]",
            self.field,
            self.message,
            self.kind,
            truncated(&repr(&self.value)),
            type_name(&self.value)
        )
    }
}

/// A refusal's text, raised under its Python class.
#[derive(Debug)]
struct Refused(String);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// `value` as pydantic's lax mode reads a `float`: a number, a boolean, or
/// a text that parses as one.
fn lax_float(value: &Value) -> Result<f64, Refusal> {
    let refused = |kind, message| Refusal::new(kind, message, value);
    match value {
        Value::Bool(flag) => Ok(f64::from(u8::from(*flag))),
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| refused("float_type", "Input should be a valid number")),
        Value::String(text) => text_float(text).ok_or_else(|| {
            refused(
                "float_parsing",
                "Input should be a valid number, unable to parse string as a number",
            )
        }),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            Err(refused("float_type", "Input should be a valid number"))
        }
    }
}

/// `text` as pydantic-core's `str_as_float` reads it: with blanks around
/// it dropped, else with underscores between digits dropped, its blanks
/// then kept.
fn text_float(text: &str) -> Option<f64> {
    text.trim()
        .parse()
        .ok()
        .or_else(|| without_underscores(text)?.parse().ok())
}

/// `value` as pydantic's lax mode reads an `int`: an integer, a boolean, a
/// float with no fraction, or a text that parses as one. An integer past
/// `i128` reads as its bound.
fn lax_int(value: &Value) -> Result<i128, Refusal> {
    let refused = |kind, message| Refusal::new(kind, message, value);
    match value {
        Value::Bool(flag) => Ok(i128::from(*flag)),
        Value::Number(number) => {
            if let Some(whole) = number.as_i64() {
                return Ok(i128::from(whole));
            }
            if let Some(whole) = number.as_u64() {
                return Ok(i128::from(whole));
            }
            float_int(number.as_f64().unwrap_or(f64::NAN))
                .map_err(|(kind, message)| refused(kind, message))
        }
        Value::String(text) => text_int(text).map_err(|(kind, message)| refused(kind, message)),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            Err(refused("int_type", "Input should be a valid integer"))
        }
    }
}

/// A float as pydantic-core's `float_as_int` reads it.
fn float_int(float: f64) -> Result<i128, (&'static str, &'static str)> {
    if !float.is_finite() {
        return Err(("finite_number", "Input should be a finite number"));
    }
    if float.fract() != 0.0 {
        return Err((
            "int_from_float",
            "Input should be a valid integer, got a number with a fractional part",
        ));
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "pydantic-core bounds the float by i64's bounds as floats"
    )]
    let within = (i64::MIN as f64) < float && float < (i64::MAX as f64);
    if !within {
        return Err((
            "int_parsing_size",
            "Unable to parse input string as an integer, exceeded maximum size",
        ));
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a whole float within i64's bounds"
    )]
    Ok(i128::from(float as i64))
}

/// `text` as pydantic-core's `str_as_int` reads it: blanks around it
/// dropped, then underscores between digits, then a fraction of zeros.
fn text_int(text: &str) -> Result<i128, (&'static str, &'static str)> {
    let text = text.trim();
    if text.len() > MAX_INT_TEXT {
        return Err((
            "int_parsing_size",
            "Unable to parse input string as an integer, exceeded maximum size",
        ));
    }
    let whole = |digits: &str| {
        parsed_int(digits).or_else(|| without_underscores(digits).as_deref().and_then(parsed_int))
    };
    whole(text)
        .or_else(|| {
            let (digits, fraction) = text.split_once('.')?;
            let zeros = !fraction.is_empty() && fraction.chars().all(|digit| digit == '0');
            zeros.then(|| whole(digits)).flatten()
        })
        .ok_or((
            "int_parsing",
            "Input should be a valid integer, unable to parse string as an integer",
        ))
}

/// `digits` as an integer, its sign included; one past `i128` reads as its
/// bound.
fn parsed_int(digits: &str) -> Option<i128> {
    match digits.parse::<i128>() {
        Ok(whole) => Some(whole),
        Err(error) if *error.kind() == IntErrorKind::PosOverflow => Some(i128::MAX),
        Err(error) if *error.kind() == IntErrorKind::NegOverflow => Some(i128::MIN),
        Err(_) => None,
    }
}

/// `text` without its underscores, each between two characters and none
/// beside another, as Python's numbers write them; `None` otherwise.
fn without_underscores(text: &str) -> Option<String> {
    if text.starts_with('_') || text.ends_with('_') || text.contains("__") {
        return None;
    }
    Some(text.replace('_', ""))
}

/// `whole`, read from `value`, as a `u32`: Python's `int` has no bound, a
/// Rust max tokens has u32's (HITL f89861c3), refused past them in
/// pydantic's words for a bounded integer.
fn within_u32(whole: i128, value: &Value) -> Result<u32, Refusal> {
    u32::try_from(whole).map_err(|_| {
        if whole < 0 {
            Refusal::new(
                "greater_than_equal",
                "Input should be greater than or equal to 0",
                value,
            )
        } else {
            Refusal::new(
                "less_than_equal",
                format!("Input should be less than or equal to {}", u32::MAX),
                value,
            )
        }
    })
}

/// `written`, a value's `repr`, as pydantic shows an input: past 50 bytes,
/// its first 25 and its last 24 around `...`, each cut at a character's
/// edge inside them.
fn truncated(written: &str) -> String {
    if written.len() <= 50 {
        return written.to_owned();
    }
    let mut head = 25;
    while !written.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = written.len() - 24;
    while !written.is_char_boundary(tail) {
        tail += 1;
    }
    let (start, end) = (
        written.get(..head).unwrap_or_default(),
        written.get(tail..).unwrap_or_default(),
    );
    format!("{start}...{end}")
}

/// The name of the Python type `value` reads as.
fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_f64() => "float",
        Value::Number(_) => "int",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// The settings a binding's `metadata` sets over a channel's 0.7, no
    /// max tokens and no budget.
    fn read(metadata: Value) -> Result<TurnSettings, String> {
        let Value::Object(members) = metadata else {
            return Err("no object".to_owned());
        };
        let metadata: Metadata = members.into_iter().collect();
        TurnSettings::read(&metadata, 0.7, None, None).map_err(|error| error.to_string())
    }

    /// What the turn's context makes of `metadata`: its temperature and
    /// max tokens, or the turn's failure.
    fn validated(metadata: Value) -> Result<(f64, Option<u32>), String> {
        let settings = read(metadata)?;
        settings.validated().map_err(|error| error.to_string())?;
        Ok((settings.temperature, settings.max_tokens))
    }

    // Python: pydantic's lax mode on AIContext(temperature=..., max_tokens=...),
    // measured on pydantic 2.13.5.
    #[test]
    fn a_binding_s_values_read_as_pydantic_reads_them() {
        let cases = [
            (
                json!({"temperature": "0.3", "max_tokens": "100"}),
                (0.3, Some(100)),
            ),
            (
                json!({"temperature": " 1e-1 ", "max_tokens": " 100 "}),
                (0.1, Some(100)),
            ),
            (
                json!({"temperature": "1_0.5_0", "max_tokens": " +1_0.00 "}),
                (10.5, Some(10)),
            ),
            (
                json!({"temperature": ".5", "max_tokens": "0100"}),
                (0.5, Some(100)),
            ),
            (
                json!({"temperature": "1.", "max_tokens": "64.000"}),
                (1.0, Some(64)),
            ),
            (
                json!({"temperature": 1, "max_tokens": 64.0}),
                (1.0, Some(64)),
            ),
            (
                json!({"temperature": true, "max_tokens": false}),
                (1.0, Some(0)),
            ),
            (
                json!({"temperature": false, "max_tokens": true}),
                (0.0, Some(1)),
            ),
            (
                json!({"temperature": "-inf", "max_tokens": "-0"}),
                (f64::NEG_INFINITY, Some(0)),
            ),
            (json!({"max_tokens": -0.0}), (0.7, Some(0))),
            (
                json!({"temperature": null, "max_tokens": null}),
                (0.7, None),
            ),
        ];
        for (metadata, expected) in cases {
            assert_eq!(validated(metadata.clone()), Ok(expected), "{metadata}");
        }
    }

    // Python: pydantic's lax mode on AIContext(temperature=..., max_tokens=...),
    // measured on pydantic 2.13.5: the type each refusal is reported under.
    #[test]
    fn a_value_pydantic_refuses_fails_the_turn_under_its_type() {
        let cases = [
            (json!({"temperature": ""}), "float_parsing"),
            (json!({"temperature": "0x10"}), "float_parsing"),
            (json!({"temperature": "1__0"}), "float_parsing"),
            (json!({"temperature": "_1"}), "float_parsing"),
            (json!({"temperature": " 1_0 "}), "float_parsing"),
            (json!({"temperature": " _1"}), "float_parsing"),
            (json!({"temperature": "1_ "}), "float_parsing"),
            (json!({"temperature": "1e"}), "float_parsing"),
            (json!({"temperature": {"a": 1}}), "float_type"),
            (json!({"max_tokens": "1e2"}), "int_parsing"),
            (json!({"max_tokens": "100."}), "int_parsing"),
            (json!({"max_tokens": ".0"}), "int_parsing"),
            (json!({"max_tokens": "1.0_0"}), "int_parsing"),
            (json!({"max_tokens": "1 0"}), "int_parsing"),
            (json!({"max_tokens": "0x1f"}), "int_parsing"),
            (json!({"max_tokens": 64.5}), "int_from_float"),
            (json!({"max_tokens": 1e20}), "int_parsing_size"),
            (json!({"max_tokens": [1]}), "int_type"),
        ];
        for (metadata, kind) in cases {
            let failed = validated(metadata.clone()).unwrap_err();
            assert!(
                failed.contains(&format!("[type={kind}, ")),
                "{metadata}: {failed}"
            );
        }
    }

    // Python: the ValidationError's text, pydantic 2.13.5, its documentation
    // links left out.
    #[test]
    fn a_refusal_reads_as_pydantic_writes_it() {
        assert_eq!(
            validated(json!({"temperature": [1], "max_tokens": "it's"})),
            Err("2 validation errors for AIContext\n\
                 temperature\n  Input should be a valid number [type=float_type, \
                 input_value=[1], input_type=list]\n\
                 max_tokens\n  Input should be a valid integer, unable to parse string as an \
                 integer [type=int_parsing, input_value=\"it's\", input_type=str]"
                .to_owned())
        );
        assert_eq!(
            validated(json!({"temperature": "x".repeat(80)})),
            Err(
                "1 validation error for AIContext\ntemperature\n  Input should be a valid \
                 number, unable to parse string as a number [type=float_parsing, \
                 input_value='xxxxxxxxxxxxxxxxxxxxxxxx...xxxxxxxxxxxxxxxxxxxxxxx', \
                 input_type=str]"
                    .to_owned()
            )
        );
    }

    // Python: pydantic-core's truncation of an input's repr, by its UTF-8
    // bytes, measured on pydantic 2.13.5.
    #[test]
    fn a_long_input_is_cut_at_its_bytes() {
        let failed = validated(json!({"temperature": "é".repeat(56)})).unwrap_err();
        let shown = format!("input_value='{}...{}'", "é".repeat(12), "é".repeat(11));
        assert!(failed.contains(&shown), "{failed}");
    }

    // Rust only (HITL f89861c3): Python's max_tokens is an int with no bound,
    // and sends -5; a Rust one is a u32, and refuses it.
    #[test]
    fn a_max_tokens_outside_u32_fails_the_turn() {
        assert_eq!(
            validated(json!({"max_tokens": -5})),
            Err(
                "1 validation error for AIContext\nmax_tokens\n  Input should be greater \
                 than or equal to 0 [type=greater_than_equal, input_value=-5, input_type=int]"
                    .to_owned()
            )
        );
        let past = validated(json!({"max_tokens": "99999999999999999999999999999999999999999"}));
        assert!(
            past.unwrap_err()
                .contains("Input should be less than or equal to 4294967295 [type=less_than_equal")
        );
    }

    // Python: tests/test_channels/test_turn_config.py::TestABindingPromptThatIsNoText::test_a_prompt_that_is_no_text_fails_the_turn
    #[test]
    fn a_prompt_that_is_no_text_fails_the_turn() {
        for (prompt, shown, kind) in [
            (json!(5), "5", "int"),
            (json!(0), "0", "int"),
            (json!(false), "False", "bool"),
            (json!([]), "[]", "list"),
            (json!({"a": 1}), "{'a': 1}", "dict"),
        ] {
            let metadata = json!({
                "system_prompt": prompt,
                "temperature": "hot",
                "turn_budget_tokens": "1200",
            });
            assert_eq!(
                read(metadata).map(|settings| settings.temperature),
                Err(format!(
                    "2 validation errors for AIContext\nsystem_prompt\n  Input should be a valid \
                     string [type=string_type, input_value={shown}, input_type={kind}]\n\
                     temperature\n  Input should be a valid number, unable to parse string as a \
                     number [type=float_parsing, input_value='hot', input_type=str]"
                ))
            );
        }
        assert_eq!(
            read(json!({"system_prompt": "A room's own."})).map(|settings| settings.temperature),
            Ok(0.7)
        );
    }

    // Python: channels/_turn_budget.py turn_budget, _check_positive.
    #[test]
    fn a_binding_s_budget_is_a_positive_integer() {
        assert_eq!(
            read(json!({"turn_budget_tokens": 1200})).map(|settings| settings.turn_budget_tokens),
            Ok(Some(1200))
        );
        for (budget, shown) in [
            (json!("1200"), "'1200'"),
            (json!(64.0), "64.0"),
            (json!(0), "0"),
            (json!(-3), "-3"),
            (json!(true), "True"),
        ] {
            assert_eq!(
                read(json!({"turn_budget_tokens": budget}))
                    .map(|settings| settings.turn_budget_tokens),
                Err(format!(
                    "turn_budget_tokens must be a positive number, got {shown}"
                ))
            );
        }
    }
}
