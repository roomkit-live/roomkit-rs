//! Safe frontmatter events, with the reference's character/depth/alias bounds.

mod normalize;
mod scalar;

use super::SkillEntry;
use crate::SkillError;
use saphyr_parser::{Event, Parser};
use serde_json::{Map, Value};

#[derive(Clone, Debug)]
enum Node {
    Scalar(Value, Option<String>, bool),
    NonFinite(String),
    ValueTag(String),
    Sequence(Vec<Node>, String),
    Mapping(Vec<(Node, Node)>, String),
}

struct Frame {
    values: Vec<Node>,
    mapping: bool,
    tag: String,
}

fn failure(message: impl Into<String>) -> SkillError {
    SkillError::Verification {
        message: message.into(),
    }
}

/// Verify every normalized frontmatter field against the served entry (RFC §24.7).
/// The result is the body's stripped text; no unverified instruction is returned.
///
/// # Errors
/// Returns `SkillError::Verification` for unreadable YAML, an existing reference
/// bound, or any added, removed or differing field, including its scalar type.
pub fn verify_frontmatter(entry: &SkillEntry, skill_md: &str) -> Result<String, SkillError> {
    let (head, body) =
        split(skill_md).map_err(|message| failure(format!("{}: {message}", entry.uri())))?;
    let written = parse(head).map_err(|message| failure(format!("{}: {message}", entry.uri())))?;
    let mut differing: Vec<_> = written
        .keys()
        .chain(entry.frontmatter().keys())
        .filter(
            |key| match (written.get(*key), entry.frontmatter().get(*key)) {
                (Some(a), Some(b)) => !same_json(a, b),
                _ => true,
            },
        )
        .cloned()
        .collect();
    differing.sort();
    differing.dedup();
    if !differing.is_empty() {
        return Err(failure(format!(
            "{}: its frontmatter differs from the entry's on {}",
            entry.uri(),
            differing.join(", ")
        )));
    }
    Ok(body.to_owned())
}

fn same_json(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| same_json(value, other)))
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_json(a, b))
        }
        (Value::Number(a), Value::Number(b)) => a.is_f64() == b.is_f64() && a == b,
        _ => left == right,
    }
}

fn split(content: &str) -> Result<(&str, &str), String> {
    let content = content.trim_start_matches('\u{feff}');
    let rest = content
        .strip_prefix("---")
        .ok_or("SKILL.md must start with '---' frontmatter delimiter")?;
    let (head, body) = rest
        .split_once("\n---")
        .ok_or("SKILL.md missing closing '---' frontmatter delimiter")?;
    Ok((head.trim(), body.trim()))
}

fn parse(text: &str) -> Result<Map<String, Value>, String> {
    bounds(text)?;
    let mut parser = Parser::new_from_str(text);
    let mut stack: Vec<Frame> = Vec::new();
    let mut roots = Vec::new();
    let mut documents = 0;
    while let Some(event) = parser.next_event() {
        let (event, _) = event.map_err(|_| "the frontmatter is not YAML")?;
        match event {
            Event::Alias(_) => return Err("the frontmatter holds a YAML alias".to_owned()),
            Event::DocumentStart(_) => {
                documents += 1;
                if documents > 1 {
                    return Err("the frontmatter is not YAML".to_owned());
                }
            }
            Event::Scalar(value, style, _, tag) => {
                let node = scalar::read(&value, style, tag.as_deref())?;
                push(&mut stack, &mut roots, node);
            }
            Event::SequenceStart(_, ref tag) | Event::MappingStart(_, ref tag) => {
                let mapping = matches!(event, Event::MappingStart(..));
                let tag = scalar::collection_tag(tag.as_deref());
                stack.push(Frame {
                    values: Vec::new(),
                    mapping,
                    tag,
                });
                if stack.len() > 64 {
                    return Err("the frontmatter nests too deep".to_owned());
                }
            }
            Event::SequenceEnd | Event::MappingEnd => {
                let frame = stack.pop().ok_or("the frontmatter is not YAML")?;
                let node = if frame.mapping {
                    mapping(frame)?
                } else {
                    Node::Sequence(frame.values, frame.tag)
                };
                push(&mut stack, &mut roots, node);
            }
            Event::Nothing | Event::StreamStart | Event::StreamEnd | Event::DocumentEnd => {}
        }
    }
    let node = roots.pop().ok_or("the frontmatter is not a mapping")?;
    if !roots.is_empty() || !stack.is_empty() {
        return Err("the frontmatter is not YAML".to_owned());
    }
    normalize::validate(&node)?;
    match normalize::value(node)? {
        Value::Object(object) => Ok(object),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Err("the frontmatter is not a mapping".to_owned())
        }
    }
}

fn push(stack: &mut [Frame], roots: &mut Vec<Node>, node: Node) {
    if let Some(frame) = stack.last_mut() {
        frame.values.push(node);
    } else {
        roots.push(node);
    }
}

fn mapping(frame: Frame) -> Result<Node, String> {
    let mut values = frame.values.into_iter();
    let mut pairs = Vec::new();
    while let Some(key) = values.next() {
        pairs.push((key, values.next().ok_or("the frontmatter is not YAML")?));
    }
    Ok(Node::Mapping(pairs, frame.tag))
}

fn bounds(text: &str) -> Result<(), String> {
    if text.chars().count() > 65536 {
        return Err("the frontmatter is past 65536 characters".to_owned());
    }
    let mut depth = 0usize;
    for c in text.chars() {
        match c {
            '[' | '{' => {
                depth += 1;
                if depth > 64 {
                    return Err("the frontmatter nests too deep".to_owned());
                }
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}
