//! A discussion's rules that read names and people (RFC §19.7.5): Python's
//! `orchestration/strategies/discussion/_names.py` and `_people.py`, here
//! with the Unicode tables and the register of authors they read.

pub mod names;
pub mod people;

use serde_json::Value;

use crate::model::Metadata;

/// The room metadata key a discussion's configuration is stored under (rule
/// 16).
pub const CONFIG_KEY: &str = "_discussion";

/// The name the discussion a room's `metadata` stores addresses `channel_id`
/// by: its handle, else the channel id itself (rule 4). Python's
/// `agent_handle`.
#[must_use]
pub fn agent_handle<'a>(metadata: &'a Metadata, channel_id: &'a str) -> &'a str {
    metadata
        .get(CONFIG_KEY)
        .and_then(|config| config.get("handles"))
        .and_then(|handles| handles.get(channel_id))
        .and_then(Value::as_str)
        .unwrap_or(channel_id)
}
