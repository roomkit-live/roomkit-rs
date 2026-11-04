//! A discussion's rules that read names and people (RFC §19.7.5): Python's
//! `orchestration/strategies/discussion/_names.py` and `_people.py`, here
//! with the Unicode tables and the register of authors they read.

pub mod config;
pub mod names;
pub mod people;

use serde_json::Value;

use self::config::DiscussionConfig;
use crate::model::{ChannelId, Metadata, RoomContext, RoomEvent};
use crate::rules::permissions;

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

/// Whether `agent` can take a turn on `event` in the room of `context`: the
/// room still binds the event's source and that binding can write, and the
/// agent reads the event (RFC §19.7.5). A discussion asks before it takes an
/// entry off its queue, so that a turn it cannot give costs no turn.
#[must_use]
pub fn takes_turn(event: &RoomEvent, context: &RoomContext, agent: &ChannelId) -> bool {
    permissions::turn_event(event, context).is_some_and(|event| {
        permissions::plan_turn(&event, &context.bindings, agent, u32::MAX).is_some()
    })
}

/// The name the discussion a room's `metadata` stores addresses `channel_id`
/// by: its handle, else the channel id itself (rule 4). A handle counts only
/// for a channel the discussion holds as an agent and only if it can name one,
/// so what a store holds never labels a person as an agent. Python's
/// `agent_handle`.
#[must_use]
pub fn agent_handle<'a>(metadata: &'a Metadata, channel_id: &'a str) -> &'a str {
    let Some(config) = metadata.get(CONFIG_KEY) else {
        return channel_id;
    };
    let an_agent = config
        .get("agents")
        .and_then(Value::as_array)
        .is_some_and(|agents| {
            agents
                .iter()
                .any(|agent| agent.as_str() == Some(channel_id))
        });
    if !an_agent {
        return channel_id;
    }
    config
        .get("handles")
        .and_then(|handles| handles.get(channel_id))
        .and_then(Value::as_str)
        .filter(|handle| names::is_handle(handle))
        .unwrap_or(channel_id)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn stored(config: Value) -> Metadata {
        Metadata::from_iter([(CONFIG_KEY.to_owned(), config)])
    }

    // Python: tests/orchestration/test_discussion_handles.py::test_a_stored_handle_counts_for_an_agent_and_if_it_can_name_one
    #[test]
    fn test_a_stored_handle_counts_for_an_agent_and_if_it_can_name_one() {
        for (config, channel, handle) in [
            (
                json!({"agents": ["b"], "handles": {"b": "ops"}}),
                "b",
                "ops",
            ),
            // A channel that is no agent is no agent's handle: a person stays one.
            (
                json!({"agents": ["b"], "handles": {"sms": "b"}}),
                "sms",
                "sms",
            ),
            (json!({"handles": {"b": "ops"}}), "b", "b"),
            // What cannot name an agent: a line break would leave the transcript's label.
            (
                json!({"agents": ["b"], "handles": {"b": "two\nlines"}}),
                "b",
                "b",
            ),
            (json!({"agents": ["b"], "handles": {"b": 3}}), "b", "b"),
        ] {
            assert_eq!(
                agent_handle(&stored(config.clone()), channel),
                handle,
                "{config}"
            );
        }
    }
}
