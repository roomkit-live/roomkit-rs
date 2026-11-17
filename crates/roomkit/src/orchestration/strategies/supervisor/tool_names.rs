//! The name of each worker's `delegate_to_<id>` tool (RFC §19.7.3):
//! Python's `orchestration/strategies/supervisor/_worker_tool_names.py`.
//!
//! A worker's tool is named after its channel id, which need not be a tool
//! name a provider accepts (`agent:<uuid>`). Every built-in provider
//! accepts `[A-Za-z0-9_-]` up to 128 characters, so a worker whose
//! `delegate_to_<id>` is such a name keeps it, and any other's id has each
//! other character replaced by `_`. A replaced name that another worker's
//! name already is, or one longer than 128 characters, ends with `_` and
//! the first 8 hex digits of the id's SHA-256 instead, cut to fit the
//! bound.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use sha2::{Digest, Sha256};

const PREFIX: &str = "delegate_to_";
const MAX_NAME: usize = 128;
const DIGEST_HEX: usize = 8;

/// Whether every built-in provider accepts `name` as a tool name.
fn any_provider_name(name: &str) -> bool {
    (1..=MAX_NAME).contains(&name.len()) && name.chars().all(name_char)
}

/// A character every built-in provider accepts in a tool name.
fn name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_' || character == '-'
}

/// Each worker's tool name, in the workers' order (Python's
/// `worker_tool_names`).
pub(super) fn worker_tool_names(worker_ids: &[&str]) -> Vec<String> {
    let kept: Vec<Option<String>> = worker_ids
        .iter()
        .map(|id| Some(format!("{PREFIX}{id}")).filter(|name| any_provider_name(name)))
        .collect();
    let taken: HashSet<&str> = kept.iter().flatten().map(String::as_str).collect();
    let replaced: Vec<Option<String>> = worker_ids
        .iter()
        .zip(&kept)
        .map(|(id, kept)| kept.is_none().then(|| replace(id)))
        .collect();
    let mut uses: HashMap<&str, usize> = HashMap::new();
    for name in replaced.iter().flatten() {
        *uses.entry(name.as_str()).or_default() += 1;
    }
    worker_ids
        .iter()
        .zip(kept.iter().zip(&replaced))
        .map(|(id, names)| match names {
            (Some(kept), _) => kept.clone(),
            (None, Some(name))
                if taken.contains(name.as_str())
                    || uses.get(name.as_str()).is_some_and(|&count| count > 1)
                    || name.len() > MAX_NAME =>
            {
                with_digest(id, name)
            }
            (None, replaced) => replaced.clone().unwrap_or_default(),
        })
        .collect()
}

/// `delegate_to_<id>` with each character a provider refuses replaced by `_`.
fn replace(id: &str) -> String {
    let replaced = id.chars().map(|c| if name_char(c) { c } else { '_' });
    PREFIX.chars().chain(replaced).collect()
}

/// `name` cut to fit, ending with `_` and the start of `id`'s digest.
fn with_digest(id: &str, name: &str) -> String {
    let digest = Sha256::digest(id.as_bytes())
        .iter()
        .take(DIGEST_HEX / 2)
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    // A replaced name is ASCII: its length is its characters'.
    let kept = name.get(..MAX_NAME - DIGEST_HEX - 1).unwrap_or(name);
    format!("{kept}_{digest}")
}

#[cfg(test)]
mod tests {
    use super::worker_tool_names;

    // Python: tests/orchestration/test_strategy_supervisor.py
    // TestSupervisorInstall::test_injects_delegation_tools (RMK-521).
    #[test]
    fn each_worker_s_tool_is_named_as_every_provider_accepts() {
        let host = "agent:6f1c2a9e-1111-2222-3333-444455556666";
        let long = "a".repeat(120);
        let cases: [(Vec<&str>, Vec<String>); 5] = [
            (
                vec!["w1", "w2"],
                vec!["delegate_to_w1".into(), "delegate_to_w2".into()],
            ),
            (
                vec![host, "researcher"],
                vec![
                    "delegate_to_agent_6f1c2a9e-1111-2222-3333-444455556666".into(),
                    "delegate_to_researcher".into(),
                ],
            ),
            (
                vec!["agent_x", "agent:x"],
                vec![
                    "delegate_to_agent_x".into(),
                    "delegate_to_agent_x_c9d679f9".into(),
                ],
            ),
            (
                vec!["agent:x", "agent.x"],
                vec![
                    "delegate_to_agent_x_c9d679f9".into(),
                    "delegate_to_agent_x_53d74197".into(),
                ],
            ),
            (
                vec![long.as_str()],
                vec![format!("delegate_to_{}_2f3d3354", "a".repeat(107))],
            ),
        ];
        for (ids, names) in cases {
            let named = worker_tool_names(&ids);
            assert_eq!(named, names, "{ids:?}");
            assert!(named.iter().all(|name| name.len() <= 128), "{ids:?}");
        }
    }
}
