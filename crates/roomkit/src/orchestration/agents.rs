//! A strategy's agents as it reads them at its install (RFC §10.6.2):
//! Python's `orchestration/_agents.py`. A loop's producer and reviewers are
//! each an agent the host gives the strategy, or the channel id of one the
//! kit holds.

use roomkit_core::model::{ChannelCategory, ChannelId};

use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::orchestration::StrategyAgent;
use crate::store::ConversationStore;

/// A strategy's agent as it reads it: its channel id, and the role its
/// identity gives it, Python's `HeldAgent`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeldAgent {
    pub(crate) channel_id: ChannelId,
    role: Option<String>,
}

impl HeldAgent {
    /// How a strategy names the agent to another: its role, else its
    /// channel id (Python's `getattr(agent, "role", None) or
    /// agent.channel_id`).
    pub(crate) fn name(&self) -> &str {
        self.role
            .as_deref()
            .filter(|role| !role.is_empty())
            .unwrap_or_else(|| self.channel_id.as_str())
    }
}

/// `agent` as given, or the agent `kit` holds under the id it is named by:
/// Python's `held(kit, agent)`.
///
/// # Errors
///
/// [`RoomKitError::ChannelNotRegistered`] when the kit holds no channel
/// under the id, [`RoomKitError::Value`] when it names no intelligence
/// channel, and [`RoomKitError::Type`] when it names an intelligence
/// channel that is no agent: give the strategy the channel itself.
pub(crate) fn held<S: ConversationStore>(
    kit: &RoomKit<S>,
    agent: &StrategyAgent,
) -> Result<HeldAgent, RoomKitError> {
    let channel_id = agent.channel_id().clone();
    if let Some(live) = agent.live() {
        let role = live
            .channel
            .declaration()
            .identity
            .and_then(|identity| identity.role);
        return Ok(HeldAgent { channel_id, role });
    }
    let declaration = kit
        .inner
        .channel(&channel_id)
        .map(|channel| channel.declaration())
        .ok_or_else(|| RoomKitError::ChannelNotRegistered {
            channel_id: channel_id.clone(),
            message: format!(
                "Channel '{channel_id}' is not registered: a strategy names by its id an agent \
                 the kit holds, live or by its declaration"
            ),
        })?;
    if declaration.category != ChannelCategory::Intelligence {
        return Err(RoomKitError::Value(format!(
            "Channel '{channel_id}' is not an intelligence channel"
        )));
    }
    let Some(identity) = declaration.identity else {
        return Err(RoomKitError::Type(format!(
            "Channel '{channel_id}' is not an Agent nor a declared one: a strategy takes it by \
             its id only then"
        )));
    };
    Ok(HeldAgent {
        channel_id,
        role: identity.role,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use roomkit_core::model::ChannelId;

    use super::HeldAgent;

    /// An agent `name` with no role, as a strategy holds it.
    pub(crate) fn held_agent(name: &str) -> HeldAgent {
        HeldAgent {
            channel_id: ChannelId::new(name).unwrap_or_else(|error| panic!("{error}")),
            role: None,
        }
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestReviewSequential::test_reviewer_uses_role_for_name
    // Python: tests/orchestration/test_loop_strategy.py::TestReviewSequential::test_reviewer_without_role_uses_channel_id
    #[test]
    fn an_agent_is_named_by_its_role_else_its_channel_id() {
        let mut agent = held_agent("r1");
        assert_eq!(agent.name(), "r1");
        agent.role = Some("QA Lead".to_owned());
        assert_eq!(agent.name(), "QA Lead");
        agent.role = Some(String::new());
        assert_eq!(agent.name(), "r1");
    }
}
