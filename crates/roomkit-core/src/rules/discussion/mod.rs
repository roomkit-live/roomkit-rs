//! A discussion's rules that read names and people (RFC §19.7.5): Python's
//! `orchestration/strategies/discussion/_names.py` and `_people.py`, here
//! with the Unicode tables and the register of authors they read.

pub mod config;
pub mod names;
pub mod people;

use serde_json::Value;

use self::config::DiscussionConfig;
use crate::model::{ChannelId, Metadata};

/// The room metadata key a discussion's configuration is stored under (rule
/// 16).
pub const CONFIG_KEY: &str = "_discussion";

/// The room metadata key a discussion's speak queue is stored under (rule
/// 16).
pub const STATE_KEY: &str = "_speak_queue";

/// The metadata key a discussion's turn trigger carries: the turn's mark,
/// which only the process that gave the turn reads.
pub const TURN_KEY: &str = "_discussion_turn";

/// Whether the room's `metadata` stores a discussion, which then takes the
/// room's turns (RFC §19.7.5, rule 16): one that does not read as a
/// configuration is none.
#[must_use]
pub fn holds(metadata: &Metadata) -> bool {
    DiscussionConfig::stored(metadata).is_some()
}

/// Whether the discussion the room's `metadata` stores is over (rule 15).
#[must_use]
pub fn is_over(metadata: &Metadata) -> bool {
    holds(metadata)
        && metadata
            .get(STATE_KEY)
            .and_then(|state| state.get("over"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

/// Whether `addressed_to` names an agent of the discussion the room's
/// `metadata` stores (rule 7).
#[must_use]
pub fn addresses_an_agent(metadata: &Metadata, addressed_to: &[ChannelId]) -> bool {
    DiscussionConfig::stored(metadata).is_some_and(|config| {
        addressed_to
            .iter()
            .any(|agent| config.agents.contains(agent))
    })
}

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
