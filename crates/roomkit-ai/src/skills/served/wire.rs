//! Raw extension replies, consumed by the registry without a transport dependency.

use super::{McpSkillServer, McpSkillServerError, SkillEntry, parse_entry};
use crate::SkillError;
use crate::skills::texts::quoted_name;
use base64::Engine;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use roomkit::model::Metadata;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

pub(super) fn discovery(message: String, source: McpSkillServerError) -> SkillError {
    SkillError::ServerDiscovery {
        message,
        source: Arc::from(source),
    }
}

pub(super) async fn list(
    server: &dyn McpSkillServer,
    label: &str,
) -> Result<Vec<Value>, SkillError> {
    let mut entries = Vec::new();
    let mut uris = HashSet::new();
    let mut cursors = HashSet::new();
    let mut params = Metadata::new();
    for _ in 0..1000 {
        let page = server
            .request("skills/list", Some(&params))
            .await
            .map_err(|error| {
                discovery(format!("MCP server {label} could not list skills"), error)
            })?;
        let skills = page
            .get("skills")
            .and_then(Value::as_array)
            .ok_or_else(|| SkillError::ServerUnavailable {
                message: format!("MCP server {label} answered skills/list without a list"),
            })?;
        for entry in skills {
            if let Some(uri) = entry.get("uri").and_then(Value::as_str)
                && !uris.insert(uri.to_owned())
            {
                continue;
            }
            entries.push(entry.clone());
        }
        let Some(cursor) = page
            .get("nextCursor")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(entries);
        };
        if skills.is_empty() || !cursors.insert(cursor.to_owned()) {
            break;
        }
        params = Metadata::from_iter([("cursor".to_owned(), json!(cursor))]);
    }
    tracing::warn!(target:"roomkit::skills",label,"server pages skills/list without end; keeping entries read");
    Ok(entries)
}

pub(super) async fn get(
    server: &dyn McpSkillServer,
    name: &str,
    uri: &str,
) -> Result<SkillEntry, SkillError> {
    let params = Metadata::from_iter([("uri".to_owned(), json!(uri))]);
    let result = server
        .request("skills/get", Some(&params))
        .await
        .map_err(|source| {
            discovery(
                format!("Skill {} not found on its server", quoted_name(name)),
                source,
            )
        })?;
    let entry = parse_entry(result.get("skill").unwrap_or(&Value::Null)).map_err(|_| {
        SkillError::EntryValidation {
            message: format!(
                "Skill {}: its server's entry is malformed",
                quoted_name(name)
            ),
        }
    })?;
    if entry.uri() != uri {
        return Err(SkillError::EntryValidation {
            message: format!(
                "Skill {}: its server's entry is malformed",
                quoted_name(name)
            ),
        });
    }
    Ok(entry)
}

pub(super) async fn read(server: &dyn McpSkillServer, uri: &str) -> Result<Vec<u8>, SkillError> {
    let data =
        server
            .read_resource(uri)
            .await
            .map_err(|source| SkillError::VerificationCaused {
                message: format!("{uri}: the server did not serve it"),
                source: Arc::from(source),
            })?;
    let contents = data.get("contents").and_then(Value::as_array);
    for item in contents.into_iter().flatten() {
        if item.get("uri").and_then(Value::as_str) != Some(uri) {
            continue;
        }
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            return Ok(text.as_bytes().to_vec());
        }
        if let Some(blob) = item.get("blob").and_then(Value::as_str) {
            return decode(blob).ok_or_else(|| SkillError::Verification {
                message: format!("{uri}: the blob does not decode"),
            });
        }
    }
    Err(SkillError::Verification {
        message: format!("{uri}: the server returned no content for it"),
    })
}

/// Python validates the alphabet/padding but ignores unused bits of a final byte.
fn decode(text: &str) -> Option<Vec<u8>> {
    let engine = GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireCanonical),
    );
    engine.decode(text).ok()
}
