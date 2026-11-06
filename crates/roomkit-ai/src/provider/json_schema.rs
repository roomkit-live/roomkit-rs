//! The portable JSON Schema subset a response schema stays within (RFC
//! §6.7): Python's `roomkit.providers.ai.json_schema`.
//!
//! Every provider that constrains its output reads its own dialect of JSON
//! Schema; the intersection checked here runs on any provider that takes a
//! response schema. It is small enough to check an answer against without a
//! JSON Schema library, which [`schema_mismatch`] does. The words of each
//! refusal and of each mismatch are Python's.

use serde_json::{Map, Value};

use crate::error::AiError;
use crate::memory::repr::repr;

/// The keywords allowed on any subschema: guidance for the model.
const ANNOTATIONS: [&str; 2] = ["title", "description"];

/// The types a subschema takes, in Python's order, and the keywords each
/// allows beside the annotations.
const KEYWORDS_BY_TYPE: [(&str, &[&str]); 6] = [
    (
        "object",
        &["type", "properties", "required", "additionalProperties"],
    ),
    ("array", &["type", "items"]),
    ("string", &["type", "enum"]),
    ("number", &["type"]),
    ("integer", &["type"]),
    ("boolean", &["type"]),
];

/// Refuses a response schema outside the portable subset (RFC §6.7):
/// Python's `check_portable_schema(schema)`. The root is an object; every
/// object lists `properties`, names each of them in `required` and sets
/// `additionalProperties` to `false`; every array has `items`; only a string
/// carries `enum`, a non-empty list of distinct strings; `title` and
/// `description`, strings, are allowed anywhere.
///
/// ```
/// use roomkit_ai::check_portable_schema;
/// use serde_json::json;
///
/// let verdict = json!({
///     "type": "object",
///     "properties": { "label": { "type": "string", "enum": ["yes", "no"] } },
///     "required": ["label"],
///     "additionalProperties": false,
/// });
/// assert!(check_portable_schema(&verdict).is_ok());
/// let open = json!({ "type": "object", "properties": {}, "required": [] });
/// assert!(check_portable_schema(&open).is_err());
/// ```
///
/// # Errors
///
/// [`AiError::ResponseSchema`], Python's `ValueError`, naming the path of
/// the first subschema that is not portable.
pub fn check_portable_schema(schema: &Value) -> Result<(), AiError> {
    if schema.get("type") != Some(&Value::from("object")) {
        return Err(AiError::ResponseSchema {
            reason: "a response schema must be a JSON Schema whose root type is 'object'"
                .to_owned(),
        });
    }
    check(schema, "$").map_err(|reason| AiError::ResponseSchema { reason })
}

/// What [`schema_mismatch`] takes beside the schema and the value:
/// Python's `path='$'`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaMismatchOptions {
    /// Where the value sits in the document, as the mismatch names it.
    pub path: String,
}

impl Default for SchemaMismatchOptions {
    fn default() -> Self {
        Self {
            path: "$".to_owned(),
        }
    }
}

/// Where `value` departs from a portable `schema`, `None` when it fits:
/// Python's `schema_mismatch(schema, value, path='$')`. `value` is a parsed
/// JSON document and `schema` one [`check_portable_schema`] took. An object
/// carries exactly its properties, an `integer` is a whole number, a whole
/// float included, and a boolean is never a number.
///
/// ```
/// use roomkit_ai::{SchemaMismatchOptions, schema_mismatch};
/// use serde_json::json;
///
/// let schema = json!({
///     "type": "object",
///     "properties": { "count": { "type": "integer" } },
///     "required": ["count"],
///     "additionalProperties": false,
/// });
/// let options = SchemaMismatchOptions::default;
/// assert_eq!(schema_mismatch(&schema, &json!({ "count": 2.0 }), options()), None);
/// assert_eq!(
///     schema_mismatch(&schema, &json!({ "count": 1.5 }), options()).as_deref(),
///     Some("$.count: expected integer, got float"),
/// );
/// ```
#[must_use]
pub fn schema_mismatch(
    schema: &Value,
    value: &Value,
    options: SchemaMismatchOptions,
) -> Option<String> {
    let SchemaMismatchOptions { path } = options;
    mismatch(schema, value, &path)
}

/// Where `value`, at `path`, departs from `schema`.
fn mismatch(schema: &Value, value: &Value, path: &str) -> Option<String> {
    match schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "object" => object_mismatch(schema, value, path),
        "array" => {
            let Value::Array(items) = value else {
                return Some(format!("{path}: expected an array"));
            };
            let subschema = schema.get("items").unwrap_or(&Value::Null);
            items
                .iter()
                .enumerate()
                .find_map(|(index, item)| mismatch(subschema, item, &format!("{path}[{index}]")))
        }
        kind => scalar_mismatch(schema, kind, value, path),
    }
}

/// Where an object `value` departs from an object `schema`.
fn object_mismatch(schema: &Value, value: &Value, path: &str) -> Option<String> {
    let Value::Object(members) = value else {
        return Some(format!("{path}: expected an object"));
    };
    let empty = Map::new();
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let missing: Vec<Value> = properties
        .keys()
        .filter(|name| !members.contains_key(*name))
        .map(|name| Value::from(name.as_str()))
        .collect();
    let unexpected: Vec<Value> = members
        .keys()
        .filter(|name| !properties.contains_key(*name))
        .map(|name| Value::from(name.as_str()))
        .collect();
    if !missing.is_empty() || !unexpected.is_empty() {
        return Some(format!(
            "{path}: missing {}, unexpected {}",
            repr(&Value::Array(missing)),
            repr(&Value::Array(unexpected))
        ));
    }
    properties.iter().find_map(|(name, subschema)| {
        let member = members.get(name).unwrap_or(&Value::Null);
        mismatch(subschema, member, &format!("{path}.{name}"))
    })
}

/// Where a scalar `value` departs from a schema of type `kind`.
fn scalar_mismatch(schema: &Value, kind: &str, value: &Value, path: &str) -> Option<String> {
    let fits = match kind {
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "number" => value.is_number(),
        "integer" => value
            .as_f64()
            .is_some_and(|number| value.is_i64() || value.is_u64() || number.fract() == 0.0),
        _ => false,
    };
    if !fits {
        return Some(format!(
            "{path}: expected {kind}, got {}",
            python_type(value)
        ));
    }
    let allowed = schema.get("enum").and_then(Value::as_array)?;
    if allowed.contains(value) {
        return None;
    }
    Some(format!(
        "{path}: {} is not one of {}",
        repr(value),
        repr(&Value::Array(allowed.clone()))
    ))
}

/// The name of the class Python's `json.loads` reads `value` as.
fn python_type(value: &Value) -> &'static str {
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

/// Refuses the subschema `schema` at `path` when it is not portable, in
/// Python's words (`_check`).
fn check(schema: &Value, path: &str) -> Result<(), String> {
    let Value::Object(members) = schema else {
        return Err(format!(
            "{path}: a subschema must be an object with string keys"
        ));
    };
    let kind = members.get("type").and_then(Value::as_str);
    let Some((kind, allowed)) = kind.and_then(|kind| {
        KEYWORDS_BY_TYPE
            .iter()
            .find(|(name, _)| *name == kind)
            .copied()
    }) else {
        let types: Vec<&str> = KEYWORDS_BY_TYPE.iter().map(|(name, _)| *name).collect();
        return Err(format!(
            "{path}: 'type' must be one of {}, as a single string",
            types.join(", ")
        ));
    };
    let mut extra: Vec<&str> = members
        .keys()
        .map(String::as_str)
        .filter(|key| !allowed.contains(key) && !ANNOTATIONS.contains(key))
        .collect();
    if !extra.is_empty() {
        extra.sort_unstable();
        let extra = Value::Array(extra.into_iter().map(Value::from).collect());
        return Err(format!(
            "{path}: {} not portable on a {} schema",
            repr(&extra),
            repr(&Value::from(kind))
        ));
    }
    for annotation in ANNOTATIONS {
        if members
            .get(annotation)
            .is_some_and(|given| !given.is_string())
        {
            return Err(format!(
                "{path}: {} must be a string",
                repr(&Value::from(annotation))
            ));
        }
    }
    match kind {
        "object" => check_object(members, path),
        "array" => match members.get("items") {
            Some(items) => check(items, &format!("{path}[]")),
            None => Err(format!("{path}: an array must declare 'items'")),
        },
        "string" => members
            .get("enum")
            .map_or(Ok(()), |values| check_enum(values, path)),
        _ => Ok(()),
    }
}

/// Refuses an object subschema that does not declare its properties, all
/// required, none other allowed (`_check_object`).
fn check_object(schema: &Map<String, Value>, path: &str) -> Result<(), String> {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return Err(format!(
            "{path}: an object must declare 'properties', keyed by name"
        ));
    };
    if schema.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(format!(
            "{path}: an object must set 'additionalProperties' to false"
        ));
    }
    let required = schema.get("required").and_then(Value::as_array);
    let required: Option<Vec<&str>> =
        required.and_then(|names| names.iter().map(Value::as_str).collect());
    let mut declared: Vec<&str> = properties.keys().map(String::as_str).collect();
    declared.sort_unstable();
    let listed = required.is_some_and(|mut names| {
        names.sort_unstable();
        names == declared
    });
    if !listed {
        return Err(format!(
            "{path}: 'required' must list every property, each once"
        ));
    }
    properties
        .iter()
        .try_for_each(|(name, subschema)| check(subschema, &format!("{path}.{name}")))
}

/// Refuses an `enum` that is not a non-empty list of distinct strings
/// (`_check_enum`).
fn check_enum(values: &Value, path: &str) -> Result<(), String> {
    let texts: Option<Vec<&str>> = values
        .as_array()
        .and_then(|values| values.iter().map(Value::as_str).collect());
    let distinct = texts.is_some_and(|texts| {
        let mut sorted = texts.clone();
        sorted.sort_unstable();
        sorted.dedup();
        !texts.is_empty() && sorted.len() == texts.len()
    });
    if distinct {
        return Ok(());
    }
    Err(format!(
        "{path}: 'enum' must be a non-empty list of distinct strings"
    ))
}

#[cfg(test)]
#[path = "json_schema_tests.rs"]
mod tests;
