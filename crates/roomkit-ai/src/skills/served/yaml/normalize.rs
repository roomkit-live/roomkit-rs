//! Safe YAML construction precedes JSON conversion, including discarded values.

use super::Node;
use serde_json::{Map, Value};

/// Match `SafeLoader` constructors before dropping overwritten map/set values.
/// Collection tags consumed as merge or ordered-pair syntax are not constructed.
pub(super) fn validate(node: &Node) -> Result<(), String> {
    match node {
        Node::Scalar(_, _, false) | Node::NonFinite(_) => Ok(()),
        Node::Sequence(nodes, tag) if matches!(tag.as_str(), "" | "seq") => {
            nodes.iter().try_for_each(validate)
        }
        Node::Sequence(nodes, tag) if matches!(tag.as_str(), "omap" | "pairs") => {
            for node in nodes {
                let Node::Mapping(pairs, _) = node else {
                    return invalid();
                };
                let [(key, value)] = pairs.as_slice() else {
                    return invalid();
                };
                validate(key)?;
                validate(value)?;
            }
            Ok(())
        }
        Node::Mapping(pairs, tag) if matches!(tag.as_str(), "" | "map" | "set") => {
            for (key, value) in merged_pairs(pairs.clone())? {
                if !matches!(key, Node::Scalar(_, _, false) | Node::NonFinite(_)) {
                    return invalid();
                }
                validate(&value)?;
            }
            Ok(())
        }
        Node::Scalar(_, _, true) | Node::ValueTag(_) | Node::Sequence(..) | Node::Mapping(..) => {
            invalid()
        }
    }
}

fn invalid<T>() -> Result<T, String> {
    Err("the frontmatter is not YAML".to_owned())
}

pub(super) fn value(node: Node) -> Result<Value, String> {
    match node {
        Node::Scalar(value, _, false) => Ok(value),
        Node::Scalar(_, _, true) | Node::ValueTag(_) => {
            Err("the frontmatter is not YAML".to_owned())
        }
        Node::NonFinite(_) => Err("the frontmatter holds a value JSON cannot hold".to_owned()),
        Node::Sequence(values, tag) => match tag.as_str() {
            "" | "seq" => values
                .into_iter()
                .map(value)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            "omap" | "pairs" => values
                .into_iter()
                .map(|node| match node {
                    Node::Mapping(mut pairs, _) if pairs.len() == 1 => {
                        let (key, value) = pairs.pop().ok_or("the frontmatter is not YAML")?;
                        Ok(Value::Array(vec![self::value(key)?, self::value(value)?]))
                    }
                    Node::Scalar(..)
                    | Node::ValueTag(_)
                    | Node::NonFinite(_)
                    | Node::Sequence(..)
                    | Node::Mapping(..) => Err("the frontmatter is not YAML".to_owned()),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            _ => Err("the frontmatter is not YAML".to_owned()),
        },
        Node::Mapping(pairs, tag) if tag == "set" => normalize_set(pairs),
        Node::Mapping(pairs, tag) if matches!(tag.as_str(), "" | "map") => normalize_map(pairs),
        Node::Mapping(_, _) => Err("the frontmatter is not YAML".to_owned()),
    }
}

fn normalize_map(pairs: Vec<(Node, Node)>) -> Result<Value, String> {
    let pairs = merged_pairs(pairs)?;
    let mut unique: Vec<(Node, Node)> = Vec::new();
    for (key, value) in pairs {
        if let Some((_, kept)) = unique
            .iter_mut()
            .find(|(previous, _)| same_key(previous, &key))
        {
            *kept = value;
        } else {
            unique.push((key, value));
        }
    }
    let mut result = Map::new();
    for (key, value) in unique {
        let (Node::Scalar(_, Some(key), _) | Node::NonFinite(key)) = key else {
            return Err("the frontmatter holds a value JSON cannot hold".to_owned());
        };
        result.insert(key, self::value(value)?);
    }
    Ok(Value::Object(result))
}

/// Python constructs the dictionary before JSON renders its keys. In
/// particular true, 1, and 1.0 overwrite one another while retaining the
/// first key's representation; the string "1" is a different Python key.
fn same_key(a: &Node, b: &Node) -> bool {
    match (a, b) {
        (Node::NonFinite(a), Node::NonFinite(b)) => a == b,
        (Node::Scalar(Value::String(a), None, _), Node::Scalar(Value::String(b), None, _)) => {
            super::scalar::same_instant(a, b).unwrap_or_else(|| a == b)
        }
        (Node::Scalar(a, a_key, _), Node::Scalar(b, b_key, _))
            if a_key.is_some() == b_key.is_some() =>
        {
            match (a, b) {
                (Value::Bool(a), Value::Number(b)) | (Value::Number(b), Value::Bool(a)) => {
                    number_key(b) == Some(i128::from(*a))
                }
                (Value::Number(a), Value::Number(b)) => match (number_key(a), number_key(b)) {
                    (Some(a), Some(b)) => a == b,
                    _ => a == b,
                },
                _ => a == b,
            }
        }
        _ => false,
    }
}
fn number_key(number: &serde_json::Number) -> Option<i128> {
    if let Some(n) = number.as_i64() {
        return Some(i128::from(n));
    }
    if let Some(n) = number.as_u64() {
        return Some(i128::from(n));
    }
    let n = number.as_f64()?;
    (n.fract() == 0.0)
        .then(|| format!("{n:.0}").parse().ok())
        .flatten()
}

fn merged_pairs(pairs: Vec<(Node, Node)>) -> Result<Vec<(Node, Node)>, String> {
    let mut merged = Vec::new();
    let mut own = Vec::new();
    for (key, value) in pairs {
        if !matches!(key, Node::Scalar(_, _, true)) {
            let key = match key {
                Node::ValueTag(text) => {
                    Node::Scalar(Value::String(text.clone()), Some(text), false)
                }
                key @ (Node::Scalar(..)
                | Node::NonFinite(_)
                | Node::Sequence(..)
                | Node::Mapping(..)) => key,
            };
            own.push((key, value));
            continue;
        }
        match value {
            Node::Mapping(pairs, _) => {
                merged.extend(merged_pairs(pairs)?);
            }
            Node::Sequence(values, _) => {
                for value in values.into_iter().rev() {
                    let Node::Mapping(pairs, _) = value else {
                        return Err("the frontmatter is not YAML".to_owned());
                    };
                    merged.extend(merged_pairs(pairs)?);
                }
            }
            Node::Scalar(..) | Node::ValueTag(_) | Node::NonFinite(_) => {
                return Err("the frontmatter is not YAML".to_owned());
            }
        }
    }
    merged.extend(own);
    Ok(merged)
}

/// Sets are normalized only after YAML equality has selected their members.
fn normalize_set(pairs: Vec<(Node, Node)>) -> Result<Value, String> {
    let mut unique = Vec::new();
    for (key, _value) in merged_pairs(pairs)? {
        if !matches!(key, Node::Scalar(..) | Node::NonFinite(_)) {
            return Err("the frontmatter is not YAML".to_owned());
        }
        if !unique.iter().any(|previous| same_key(previous, &key)) {
            unique.push(key);
        }
    }
    let mut values = unique
        .into_iter()
        .map(value)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by_cached_key(crate::skills::texts::dumps_unescaped);
    Ok(Value::Array(values))
}
