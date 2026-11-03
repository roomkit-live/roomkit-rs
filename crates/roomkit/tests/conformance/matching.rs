//! How a scenario's expectation matches what a run produced: roomkit-specs
//! `conformance/README.md`, "Matching". An object names the members it
//! checks, a list matches element by element, and a few operators
//! (`"$any"`, `$unordered`, `$absent`, `$present`, `$contains`, `$lacks`)
//! loosen a member.

use serde_json::{Map, Value};

/// Every place where `actual` (`None`: a member the actual object does not
/// hold) does not match `expected`, as `path: why`.
pub(crate) fn mismatches(expected: &Value, actual: Option<&Value>, path: &str) -> Vec<String> {
    match expected {
        Value::String(text) if text == "$any" => Vec::new(),
        Value::Object(object) => object_mismatches(object, actual, path),
        Value::Array(items) => list_mismatches(items, actual, path),
        Value::Null => match actual {
            None | Some(Value::Null) => Vec::new(),
            Some(other) => vec![format!("{path}: {other} is not null")],
        },
        scalar @ (Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
            if actual.is_some_and(|actual| same_scalar(scalar, actual)) {
                Vec::new()
            } else {
                vec![format!("{path}: expected {scalar}, got {}", shown(actual))]
            }
        }
    }
}

/// A value as a mismatch shows it, cut to a readable length.
fn shown(actual: Option<&Value>) -> String {
    actual.map_or_else(|| "<missing>".to_owned(), short)
}

fn short(value: &Value) -> String {
    let shown = value.to_string();
    match shown.char_indices().nth(240) {
        Some((cut, _)) => format!("{}...", shown.get(..cut).unwrap_or_default()),
        None => shown,
    }
}

fn same_scalar(expected: &Value, actual: &Value) -> bool {
    if let (Value::Number(want), Value::Number(got)) = (expected, actual) {
        return want.as_f64() == got.as_f64();
    }
    expected == actual
}

fn absent(actual: Option<&Value>) -> bool {
    matches!(actual, None | Some(Value::Null))
}

fn object_mismatches(
    expected: &Map<String, Value>,
    actual: Option<&Value>,
    path: &str,
) -> Vec<String> {
    if let Some(Value::Array(items)) = expected.get("$unordered") {
        return unordered_mismatches(items, actual, path);
    }
    if expected.get("$absent") == Some(&Value::Bool(true)) {
        return if absent(actual) {
            Vec::new()
        } else {
            vec![format!("{path}: {} is present", shown(actual))]
        };
    }
    if expected.get("$present") == Some(&Value::Bool(true)) {
        return if absent(actual) {
            vec![format!("{path}: absent")]
        } else {
            Vec::new()
        };
    }
    if let Some(Value::String(needle)) = expected.get("$lacks") {
        return match actual {
            Some(Value::String(text)) if !text.contains(needle.as_str()) => Vec::new(),
            other => vec![format!("{path}: {} contains {needle:?}", shown(other))],
        };
    }
    if let Some(Value::String(needle)) = expected.get("$contains") {
        return match actual {
            Some(Value::String(text)) if text.contains(needle.as_str()) => Vec::new(),
            other => vec![format!(
                "{path}: {} does not contain {needle:?}",
                shown(other)
            )],
        };
    }
    let Some(Value::Object(actual)) = actual else {
        return vec![format!("{path}: expected an object, got {}", shown(actual))];
    };
    expected
        .iter()
        .flat_map(|(key, value)| mismatches(value, actual.get(key), &format!("{path}.{key}")))
        .collect()
}

fn list_mismatches(expected: &[Value], actual: Option<&Value>, path: &str) -> Vec<String> {
    let Some(Value::Array(actual)) = actual else {
        return vec![format!("{path}: expected a list, got {}", shown(actual))];
    };
    if expected.len() != actual.len() {
        return vec![format!(
            "{path}: expected {} items, got {}: {}",
            expected.len(),
            actual.len(),
            short(&Value::Array(actual.clone()))
        )];
    }
    expected
        .iter()
        .zip(actual)
        .enumerate()
        .flat_map(|(position, (want, got))| {
            mismatches(want, Some(got), &format!("{path}[{position}]"))
        })
        .collect()
}

fn unordered_mismatches(expected: &[Value], actual: Option<&Value>, path: &str) -> Vec<String> {
    let Some(Value::Array(actual)) = actual else {
        return vec![format!("{path}: expected a list, got {}", shown(actual))];
    };
    unordered_items(expected, actual, path)
}

fn unordered_items(expected: &[Value], actual: &[Value], path: &str) -> Vec<String> {
    if expected.len() != actual.len() {
        return vec![format!(
            "{path}: expected {} items in any order, got {}",
            expected.len(),
            short(&Value::Array(actual.to_vec()))
        )];
    }
    let mut used = vec![false; actual.len()];
    if assign(expected, actual, &mut used) {
        Vec::new()
    } else {
        vec![format!(
            "{path}: no order of {} matches {}",
            short(&Value::Array(actual.to_vec())),
            short(&Value::Array(expected.to_vec()))
        )]
    }
}

fn assign(expected: &[Value], actual: &[Value], used: &mut [bool]) -> bool {
    let Some((first, rest)) = expected.split_first() else {
        return true;
    };
    for (index, item) in actual.iter().enumerate() {
        if used[index] || !mismatches(first, Some(item), "").is_empty() {
            continue;
        }
        used[index] = true;
        if assign(rest, actual, used) {
            return true;
        }
        used[index] = false;
    }
    false
}

/// A room's framework events: in order, a `$unordered` group matching as
/// many consecutive events in any order, a bare string naming the type.
pub(crate) fn framework_mismatches(
    expected: &[Value],
    actual: &[Value],
    path: &str,
) -> Vec<String> {
    let mut found = Vec::new();
    let mut cursor = 0;
    for (position, item) in expected.iter().enumerate() {
        let here = format!("{path}[{position}]");
        if let Some(Value::Array(group)) = item.get("$unordered") {
            let group: Vec<Value> = group.iter().map(as_event).collect();
            let end = (cursor + group.len()).min(actual.len());
            found.extend(unordered_items(&group, &actual[cursor..end], &here));
            cursor += group.len();
            continue;
        }
        match actual.get(cursor) {
            Some(event) => found.extend(mismatches(&as_event(item), Some(event), &here)),
            None => found.push(format!("{here}: expected {item}, got nothing more")),
        }
        cursor += 1;
    }
    if cursor < actual.len() {
        found.push(format!(
            "{path}: unexpected events {}",
            short(&Value::Array(actual[cursor..].to_vec()))
        ));
    }
    found
}

fn as_event(item: &Value) -> Value {
    match item {
        Value::String(name) => serde_json::json!({ "event": name }),
        other @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => other.clone(),
    }
}
