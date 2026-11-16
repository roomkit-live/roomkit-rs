//! Validate a server's JSON manifest before any registry can load it (RFC §24.7).

use std::collections::HashSet;

use roomkit::model::Metadata;
use serde_json::Value;

use super::entry::segments_ok;
use super::json_tree::copy_map;
use super::{SkillEntry, SkillResource, SkillResources};
use crate::SkillError;
use crate::skills::frontmatter::FRONTMATTER_MAX;
use crate::skills::texts::{fits_map, quoted_name};
use crate::skills::validation::is_kebab_case;

/// Parse one JSON entry, retaining its extra frontmatter as an owned copy.
///
/// Enforces URI, metadata and file-list shape (RFC §24.7). The operational
/// file-count/byte limits remain observable through [`SkillEntry::limit_exceeded`].
/// JSON is traversed iteratively within 65,536 rendered characters, without an
/// interpreter-specific recursion limit.
///
/// # Errors
/// Returns [`SkillError::EntryValidation`] for an invalid entry, frontmatter
/// exceeding the JSON character bound, or an inconsistent resource list.
pub fn parse_entry(raw: &Value) -> Result<SkillEntry, SkillError> {
    let raw = raw
        .as_object()
        .ok_or_else(|| invalid("the entry is not an object"))?;
    let uri = raw
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("the entry has no URI"))?;
    let root = skill_root(uri)?;
    let frontmatter = raw
        .get("frontmatter")
        .and_then(Value::as_object)
        .ok_or_else(|| at(uri, "the entry has no frontmatter"))?;
    check_frontmatter(uri, root, frontmatter)?;
    if !fits_map(frontmatter, FRONTMATTER_MAX) {
        return Err(at(
            uri,
            format!("the frontmatter is past {FRONTMATTER_MAX} characters"),
        ));
    }
    let resources = resources(uri, root, raw.get("resources").unwrap_or(&Value::Null))?;
    Ok(SkillEntry::new(uri, copy_map(frontmatter), resources))
}

fn invalid(message: impl Into<String>) -> SkillError {
    SkillError::EntryValidation {
        message: message.into(),
    }
}

fn at(uri: &str, reason: impl std::fmt::Display) -> SkillError {
    invalid(format!("{uri}: {reason}"))
}

fn skill_root(uri: &str) -> Result<&str, SkillError> {
    let bad_uri = || at(uri, "not the URI of a SKILL.md");
    let (scheme, tail) = uri.split_once("://").ok_or_else(bad_uri)?;
    let path = tail.strip_suffix("/SKILL.md").ok_or_else(bad_uri)?;
    if !valid_scheme(scheme) || tail.contains('\n') {
        return Err(bad_uri());
    }
    if !segments_ok(path) {
        return Err(at(uri, "the skill's path is not a plain path"));
    }
    uri.strip_suffix("/SKILL.md").ok_or_else(bad_uri)
}

fn valid_scheme(scheme: &str) -> bool {
    let mut characters = scheme.chars();
    characters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'))
}

fn check_frontmatter(uri: &str, root: &str, fields: &Metadata) -> Result<(), SkillError> {
    let name = fields
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| name.len() <= 64 && is_kebab_case(name))
        .ok_or_else(|| at(uri, "the frontmatter name is not a skill name"))?;
    if root.rsplit('/').next() != Some(name) {
        return Err(at(
            uri,
            format!(
                "the frontmatter name {} is not the URI's",
                quoted_name(name)
            ),
        ));
    }
    if !fields
        .get("description")
        .and_then(Value::as_str)
        .is_some_and(|value| (1..=1024).contains(&value.chars().count()))
    {
        return Err(at(
            uri,
            "the frontmatter description is missing or too long",
        ));
    }
    check_gating(uri, fields)
}

fn check_gating(uri: &str, fields: &Metadata) -> Result<(), SkillError> {
    for key in ["allowed_tools", "requires"] {
        let nested = match fields.get(key) {
            Some(Value::Object(_)) => true,
            Some(Value::Array(items)) => {
                items.iter().any(|item| item.is_object() || item.is_array())
            }
            _ => false,
        };
        if nested {
            return Err(at(
                uri,
                format!("the frontmatter's {key} is neither a text nor a list of texts"),
            ));
        }
    }
    Ok(())
}

fn resources(uri: &str, root: &str, raw: &Value) -> Result<SkillResources, SkillError> {
    if raw.as_str() == Some("dynamic") {
        return Ok(SkillResources::Dynamic);
    }
    let files = raw
        .as_array()
        .ok_or_else(|| at(uri, "resources is neither a list nor 'dynamic'"))?;
    let mut seen = HashSet::new();
    let mut resources = Vec::new();
    let prefix = format!("{root}/");
    for item in files {
        let file = resource(uri, item)?;
        if !file.uri().strip_prefix(&prefix).is_some_and(segments_ok) {
            return Err(at(uri, format!("{} is outside the skill", file.uri())));
        }
        if !seen.insert(file.uri().to_owned()) {
            return Err(at(uri, format!("{} is listed twice", file.uri())));
        }
        resources.push(file);
    }
    if !seen.contains(uri) {
        return Err(at(uri, "resources does not list the skill's own SKILL.md"));
    }
    Ok(SkillResources::Listed(resources))
}

fn resource(uri: &str, raw: &Value) -> Result<SkillResource, SkillError> {
    let fields = raw
        .as_object()
        .ok_or_else(|| at(uri, "a listed file is not an object"))?;
    let file_uri = fields
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| at(uri, "a listed file has no URI"))?;
    let digest = fields
        .get("digest")
        .and_then(Value::as_str)
        .filter(|value| valid_digest(value))
        .ok_or_else(|| at(uri, format!("{file_uri} has no sha256 digest")))?;
    let size = fields
        .get("size")
        .and_then(Value::as_u64)
        .ok_or_else(|| at(uri, format!("{file_uri} has no size")))?;
    Ok(SkillResource::new(file_uri, digest, size))
}

fn valid_digest(value: &str) -> bool {
    // Python's regexp uses match with $, which also accepts one final LF.
    let value = value.strip_suffix('\n').unwrap_or(value);
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}
