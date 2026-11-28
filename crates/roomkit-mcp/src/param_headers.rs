//! MCP 2026-07-28's `x-mcp-header` annotations (RFC §21.2), read as
//! Python's MCP SDK reads them (`mcp.shared.inbound`): which listed tools a
//! client leaves out, and the HTTP headers a request carries. rmcp keeps
//! its own reading of them crate-private.

use roomkit_core::rules::python_repr;
use serde_json::Value;

/// The schema annotation that mirrors an argument into a header.
const ANNOTATION: &str = "x-mcp-header";

/// The schema types an annotated property may have.
const PRIMITIVE_TYPES: [&str; 3] = ["string", "integer", "boolean"];

/// The JSON Schema keywords whose value is one schema, which the walk
/// enters off the `properties` chain.
const SUBSCHEMA_SINGLE: [&str; 11] = [
    "items",
    "contains",
    "unevaluatedItems",
    "additionalProperties",
    "propertyNames",
    "unevaluatedProperties",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
];

/// The keywords whose value is a list of schemas.
const SUBSCHEMA_LIST: [&str; 4] = ["allOf", "anyOf", "oneOf", "prefixItems"];

/// The keywords whose value maps names to schemas.
const SUBSCHEMA_MAP: [&str; 4] = [
    "patternProperties",
    "dependentSchemas",
    "$defs",
    "definitions",
];

/// An argument's path from the arguments' root, and the header token it is
/// mirrored into.
pub(crate) type HeaderMap = Vec<(Vec<String>, String)>;

/// A schema position, with the chain of `properties` keys leading to it,
/// `None` once another keyword was crossed.
type Position<'a> = (Option<Vec<String>>, &'a serde_json::Map<String, Value>);

/// Every schema position of `root`, the root's chain empty, as the SDK walks
/// them: last child first, never entering instance data or a `$ref`.
fn positions(root: &Value) -> Vec<Position<'_>> {
    let mut found = Vec::new();
    let mut stack: Vec<(Option<Vec<String>>, &Value)> = vec![(Some(Vec::new()), root)];
    while let Some((path, node)) = stack.pop() {
        let Some(schema) = node.as_object() else {
            continue;
        };
        for (keyword, value) in schema {
            let keyword = keyword.as_str();
            if keyword == "properties"
                && let Some(properties) = value.as_object()
            {
                for (name, sub) in properties {
                    let chain = path.as_ref().map(|path| {
                        let mut chain = path.clone();
                        chain.push(name.clone());
                        chain
                    });
                    stack.push((chain, sub));
                }
            } else if SUBSCHEMA_SINGLE.contains(&keyword) {
                stack.push((None, value));
            } else if SUBSCHEMA_LIST.contains(&keyword)
                && let Some(items) = value.as_array()
            {
                stack.extend(items.iter().map(|sub| (None, sub)));
            } else if SUBSCHEMA_MAP.contains(&keyword)
                && let Some(map) = value.as_object()
            {
                stack.extend(map.values().map(|sub| (None, sub)));
            }
        }
        found.push((path, schema));
    }
    found
}

/// Why the `x-mcp-header` annotations of `schema` are invalid, if they are:
/// each must sit on a property reached from the root through `properties`
/// keys alone, name an RFC 9110 token, unique whatever its case, and
/// annotate a string, integer or boolean property. Python's
/// `find_invalid_x_mcp_header`, its reasons word for word.
pub(crate) fn invalid(schema: &Value) -> Option<String> {
    let mut seen: Vec<(String, String)> = Vec::new();
    for (path, position) in positions(schema) {
        let Some(header) = position.get(ANNOTATION) else {
            continue;
        };
        let Some(path) = path.filter(|path| !path.is_empty()) else {
            return Some(format!(
                "{ANNOTATION} found at a schema position not reachable via a pure `properties` chain"
            ));
        };
        let place = python_repr(&path.join("."));
        let Some(header) = header.as_str() else {
            return Some(format!(
                "property {place}: {ANNOTATION} must be a string, not {}",
                type_name(header)
            ));
        };
        if !is_token(header) {
            return Some(format!(
                "property {place}: {ANNOTATION} {} is not an RFC 9110 token",
                python_repr(header)
            ));
        }
        let permitted = "is only permitted on integer/string/boolean properties";
        match position.get("type") {
            Some(Value::String(kind)) if PRIMITIVE_TYPES.contains(&kind.as_str()) => {}
            Some(Value::String(kind)) => {
                return Some(format!(
                    "property {place}: {ANNOTATION} {permitted} (got {})",
                    python_repr(kind)
                ));
            }
            other => {
                return Some(format!(
                    "property {place}: {ANNOTATION} {permitted} (the type keyword is {}, not a string)",
                    other.map_or("NoneType", type_name)
                ));
            }
        }
        let lower = header.to_ascii_lowercase();
        if let Some((_, first)) = seen.iter().find(|(seen, _)| *seen == lower) {
            return Some(format!(
                "{ANNOTATION} {} on property {place} duplicates property {}",
                python_repr(header),
                python_repr(first)
            ));
        }
        seen.push((lower, path.join(".")));
    }
    None
}

/// The arguments `schema` mirrors into headers, by their path, for a schema
/// [`invalid`] accepts: Python's `x_mcp_header_map`.
pub(crate) fn header_map(schema: &Value) -> HeaderMap {
    positions(schema)
        .into_iter()
        .filter_map(|(path, position)| {
            let path = path.filter(|path| !path.is_empty())?;
            let token = position.get(ANNOTATION)?.as_str()?;
            Some((path, token.to_owned()))
        })
        .collect()
}

/// Whether a call of a tool listed with `schema` mirrors an argument into a
/// header: an object's schema whose annotations are valid and name one at
/// least (what Python's provider shows its SDK of a listing).
pub(crate) fn headed(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("object")
        && invalid(schema).is_none()
        && !header_map(schema).is_empty()
}

/// An RFC 9110 token: what an HTTP field name may hold.
fn is_token(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

/// The Python type a JSON value reads as.
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

/// The `Mcp-Param-*` headers a call with `arguments` carries, by
/// `headers`: each argument present and not `null`, rendered and encoded.
/// Python's `mcp_param_headers`.
#[cfg(feature = "http")]
pub(crate) fn param_headers(headers: &HeaderMap, arguments: &Value) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(path, token)| {
            let value = path
                .iter()
                .try_fold(arguments, |node, key| node.as_object()?.get(key))?;
            let rendered = rendered(value)?;
            Some((format!("Mcp-Param-{token}"), encoded(&rendered)))
        })
        .collect()
}

/// An argument as it is mirrored into a header: a boolean as `true` or
/// `false`, a number or a text as Python writes it, nothing for another
/// value.
#[cfg(feature = "http")]
fn rendered(value: &Value) -> Option<String> {
    match value {
        Value::Bool(flag) => Some(flag.to_string()),
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(match number.as_f64() {
            Some(float) if number.is_f64() => python_float(float),
            _ => number.to_string(),
        }),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

/// A float as Python's `str` writes it: the shortest digits that read back,
/// in positional notation from 1e-4 to below 1e16, `.0` after a whole
/// number, and in exponent notation otherwise (`1e+16`, `1.5e-05`).
#[cfg(feature = "http")]
fn python_float(value: f64) -> String {
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |rest| ("-", rest));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if !(-4..16).contains(&exponent) {
        let tail = digits.get(1..).unwrap_or_default();
        let fraction = if tail.is_empty() {
            String::new()
        } else {
            format!(".{tail}")
        };
        let lead = digits.get(..1).unwrap_or("0");
        let sign_e = if exponent < 0 { '-' } else { '+' };
        return format!("{sign}{lead}{fraction}e{sign_e}{:02}", exponent.abs());
    }
    let point = exponent + 1;
    let text = if point <= 0 {
        let zeros = "0".repeat(usize::try_from(-point).unwrap_or(0));
        format!("0.{zeros}{digits}")
    } else {
        let point = usize::try_from(point).unwrap_or(0);
        let whole = if digits.len() <= point {
            digits.clone() + &"0".repeat(point - digits.len())
        } else {
            digits.get(..point).unwrap_or_default().to_owned()
        };
        let fraction = digits.get(point..).unwrap_or_default();
        format!(
            "{whole}.{}",
            if fraction.is_empty() { "0" } else { fraction }
        )
    };
    format!("{sign}{text}")
}

/// `value` as it travels in a header: as it is when it is printable ASCII
/// with no space at either end and no look of the base64 wrapping, wrapped
/// as `=?base64?…?=` otherwise. Python's `encode_header_value`.
#[cfg(feature = "http")]
pub(crate) fn encoded(value: &str) -> String {
    use base64::Engine as _;
    let printable = value.bytes().all(|byte| (b' '..=b'~').contains(&byte));
    let trimmed = !value.starts_with(' ') && !value.ends_with(' ');
    let wrapped = value.len() >= 11 && value.starts_with("=?base64?") && value.ends_with("?=");
    if printable && trimmed && !wrapped {
        return value.to_owned();
    }
    let payload = base64::engine::general_purpose::STANDARD.encode(value);
    format!("=?base64?{payload}?=")
}

/// The parameter whose value a name-bearing request's `Mcp-Name` header
/// repeats, by method.
#[cfg(feature = "http")]
pub(crate) fn name_parameter(method: &str) -> Option<&'static str> {
    match method {
        "tools/call" | "prompts/get" => Some("name"),
        "resources/read" => Some("uri"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn schema(properties: &Value) -> Value {
        json!({"type": "object", "properties": properties})
    }

    // Python: mcp.shared.inbound.find_invalid_x_mcp_header, through
    // tests/test_mcp_discovery.py::test_a_tool_whose_header_annotation_is_invalid_is_left_out_on_2026_07_28
    #[test]
    fn an_annotation_is_valid_only_on_a_primitive_property_of_the_chain() {
        for (schema, reason) in [
            (
                schema(&json!({"region": {"type": "string", "x-mcp-header": "Region"}})),
                None,
            ),
            (
                schema(&json!({"blob": {"type": "object", "x-mcp-header": "Blob"}})),
                Some(
                    "property 'blob': x-mcp-header is only permitted on integer/string/boolean properties (got 'object')",
                ),
            ),
            (
                schema(&json!({"n": {"x-mcp-header": "N"}})),
                Some(
                    "property 'n': x-mcp-header is only permitted on integer/string/boolean properties (the type keyword is NoneType, not a string)",
                ),
            ),
            (
                schema(&json!({"r": {"type": "string", "x-mcp-header": "bad:name"}})),
                Some("property 'r': x-mcp-header 'bad:name' is not an RFC 9110 token"),
            ),
            (
                schema(&json!({"r": {"type": "string", "x-mcp-header": 7}})),
                Some("property 'r': x-mcp-header must be a string, not int"),
            ),
            (
                json!({"type": "object", "x-mcp-header": "Root"}),
                Some(
                    "x-mcp-header found at a schema position not reachable via a pure `properties` chain",
                ),
            ),
            (
                json!({"type": "object", "$defs": {"r": {"type": "string", "x-mcp-header": "R"}}}),
                Some(
                    "x-mcp-header found at a schema position not reachable via a pure `properties` chain",
                ),
            ),
            // A keyword the SDK does not walk holds no annotation it reads.
            (
                json!({"type": "object", "x-vendor": {"x-mcp-header": "Vendor"}}),
                None,
            ),
            (
                schema(&json!({
                    "a": {"type": "string", "x-mcp-header": "Region"},
                    "b": {"type": "string", "x-mcp-header": "region"},
                })),
                Some("x-mcp-header 'Region' on property 'a' duplicates property 'b'"),
            ),
        ] {
            assert_eq!(invalid(&schema).as_deref(), reason, "{schema}");
        }
    }

    #[test]
    fn a_nested_annotation_is_mapped_by_its_path() {
        let nested = schema(&json!({
            "where": {"type": "object", "properties": {"zone": {"type": "string", "x-mcp-header": "Zone"}}},
        }));
        assert_eq!(
            header_map(&nested),
            [(
                vec!["where".to_owned(), "zone".to_owned()],
                "Zone".to_owned()
            )]
        );
        assert!(headed(&nested));
        let untyped = json!({"properties": {"zone": {"type": "string", "x-mcp-header": "Zone"}}});
        assert!(!headed(&untyped), "the SDK reads only an object's schema");
    }

    // Python: mcp.shared.inbound.encode_header_value and
    // _render_header_scalar, read on SDK 2.3.
    #[cfg(feature = "http")]
    #[test]
    fn a_header_value_is_rendered_and_encoded_as_python_does() {
        for (value, expected) in [
            ("eu", "eu"),
            ("", ""),
            ("=?base64?=", "=?base64?="),
            ("=?base64??=", "=?base64?PT9iYXNlNjQ/Pz0=?="),
            (" a", "=?base64?IGE=?="),
            ("é", "=?base64?w6k=?="),
            ("a\tb", "=?base64?YQli?="),
        ] {
            assert_eq!(encoded(value), expected, "{value:?}");
        }
        for (value, expected) in [
            (json!(1.0), "1.0"),
            (json!(1e16), "1e+16"),
            (json!(1.5e-5), "1.5e-05"),
            (json!(0.0001), "0.0001"),
            (json!(123_456_789.0), "123456789.0"),
            (json!(-2.5), "-2.5"),
            (json!(7), "7"),
            (json!(true), "true"),
        ] {
            assert_eq!(rendered(&value).as_deref(), Some(expected), "{value}");
        }
        let headers = vec![(vec!["region".to_owned()], "Region".to_owned())];
        assert_eq!(
            param_headers(&headers, &json!({"region": "eu"})),
            [("Mcp-Param-Region".to_owned(), "eu".to_owned())]
        );
        assert_eq!(
            param_headers(&headers, &json!({"region": null})),
            Vec::<(String, String)>::new()
        );
    }
}
