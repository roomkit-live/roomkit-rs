//! What a SYNC hook decides (RFC §9.4).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, ModelError, Observation, RoomEvent, Task};

/// What a SYNC hook returns (RFC §9.4).
///
/// `P` is the payload type of the hook's trigger: a [`RoomEvent`] for
/// `BEFORE_BROADCAST`, a string for a TTS trigger. Serialized with the RFC's
/// `action` and `event` fields; a `modify` without an event does not
/// deserialize (gap G-010), and the event of an `allow` or a `block` is
/// ignored.
///
/// ```
/// use roomkit_core::model::HookResult;
///
/// let result: HookResult = HookResult::block("SIN detected");
/// assert!(result.is_block());
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    bound(
        serialize = "P: Serialize + Clone",
        deserialize = "P: Deserialize<'de>"
    ),
    into = "RawHookResult<P>",
    try_from = "RawHookResult<P>"
)]
pub struct HookResult<P = RoomEvent> {
    /// Allow, block, or modify with a replacement payload.
    pub decision: HookDecision<P>,
    /// Why the hook blocked or modified.
    pub reason: Option<String>,
    /// Events to inject when blocking.
    pub injected_events: Vec<InjectedEvent>,
    /// Tasks to create, always collected.
    pub tasks: Vec<Task>,
    /// Observations to record, always collected.
    pub observations: Vec<Observation>,
    /// Additional hook data.
    pub metadata: Metadata,
}

/// A SYNC hook's decision on its payload (RFC §9.4).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum HookDecision<P> {
    /// Let the payload through unchanged.
    Allow,
    /// Stop the pipeline: no further hook runs.
    Block,
    /// Replace the payload, whatever the replacement's value.
    Modify(P),
}

impl<P> HookResult<P> {
    /// Let the payload through.
    pub fn allow() -> Self {
        Self::with_decision(HookDecision::Allow)
    }

    /// Stop the pipeline, saying why.
    pub fn block(reason: impl Into<String>) -> Self {
        Self::with_decision(HookDecision::Block).with_reason(reason)
    }

    /// Replace the payload with `payload`.
    pub fn modify(payload: P) -> Self {
        Self::with_decision(HookDecision::Modify(payload))
    }

    /// The same result with `reason`.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// The same result, also injecting `event`.
    #[must_use]
    pub fn inject(mut self, event: InjectedEvent) -> Self {
        self.injected_events.push(event);
        self
    }

    /// The same result, also creating `task`.
    #[must_use]
    pub fn with_task(mut self, task: Task) -> Self {
        self.tasks.push(task);
        self
    }

    /// The same result, also recording `observation`.
    #[must_use]
    pub fn with_observation(mut self, observation: Observation) -> Self {
        self.observations.push(observation);
        self
    }

    /// Whether the hook blocked.
    pub fn is_block(&self) -> bool {
        matches!(self.decision, HookDecision::Block)
    }

    fn with_decision(decision: HookDecision<P>) -> Self {
        Self {
            decision,
            reason: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            metadata: Metadata::new(),
        }
    }
}

/// An event a blocking hook injects (RFC §9.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InjectedEvent {
    /// The event to inject.
    pub event: RoomEvent,
    /// The channels to deliver it to; `None` stores it only.
    #[serde(default)]
    pub target_channel_ids: Option<Vec<ChannelId>>,
}

impl InjectedEvent {
    /// `event`, delivered to `targets`.
    pub fn to_channels(event: RoomEvent, targets: Vec<ChannelId>) -> Self {
        Self {
            event,
            target_channel_ids: Some(targets),
        }
    }

    /// `event`, stored and delivered to no channel.
    pub fn stored_only(event: RoomEvent) -> Self {
        Self {
            event,
            target_channel_ids: None,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HookAction {
    Allow,
    Block,
    Modify,
}

#[derive(Serialize, Deserialize)]
struct RawHookResult<P> {
    action: HookAction,
    event: Option<P>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    injected_events: Vec<InjectedEvent>,
    #[serde(default)]
    tasks: Vec<Task>,
    #[serde(default)]
    observations: Vec<Observation>,
    #[serde(default)]
    metadata: Metadata,
}

impl<P> From<HookResult<P>> for RawHookResult<P> {
    fn from(result: HookResult<P>) -> Self {
        let (action, event) = match result.decision {
            HookDecision::Allow => (HookAction::Allow, None),
            HookDecision::Block => (HookAction::Block, None),
            HookDecision::Modify(payload) => (HookAction::Modify, Some(payload)),
        };
        Self {
            action,
            event,
            reason: result.reason,
            injected_events: result.injected_events,
            tasks: result.tasks,
            observations: result.observations,
            metadata: result.metadata,
        }
    }
}

impl<P> TryFrom<RawHookResult<P>> for HookResult<P> {
    type Error = ModelError;

    fn try_from(raw: RawHookResult<P>) -> Result<Self, Self::Error> {
        let decision = match (raw.action, raw.event) {
            (HookAction::Allow, _) => HookDecision::Allow,
            (HookAction::Block, _) => HookDecision::Block,
            (HookAction::Modify, Some(payload)) => HookDecision::Modify(payload),
            (HookAction::Modify, None) => return Err(ModelError::ModifyWithoutPayload),
        };
        Ok(Self {
            decision,
            reason: raw.reason,
            injected_events: raw.injected_events,
            tasks: raw.tasks,
            observations: raw.observations,
            metadata: raw.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_block_serializes_its_action_and_reason() {
        let result: HookResult<String> = HookResult::block("SIN detected");
        let json = serde_json::to_value(result).unwrap();
        assert_eq!(
            (json["action"].clone(), json["reason"].clone()),
            (json!("block"), json!("SIN detected"))
        );
    }

    #[test]
    fn a_modify_carries_its_payload_as_the_event() {
        let json = serde_json::to_value(HookResult::modify("redacted".to_owned())).unwrap();
        assert_eq!(json["event"], json!("redacted"));
    }

    #[test]
    fn a_modify_to_an_empty_payload_keeps_the_empty_payload() {
        let parsed: HookResult<String> =
            serde_json::from_value(json!({ "action": "modify", "event": "" })).unwrap();
        assert_eq!(parsed.decision, HookDecision::Modify(String::new()));
    }

    #[test]
    fn a_modify_without_an_event_does_not_deserialize() {
        let parsed = serde_json::from_value::<HookResult<String>>(json!({ "action": "modify" }));
        assert!(parsed.is_err());
    }

    #[test]
    fn an_unknown_action_does_not_deserialize() {
        let parsed = serde_json::from_value::<HookResult<String>>(json!({ "action": "maybe" }));
        assert!(parsed.is_err());
    }

    #[test]
    fn an_allow_round_trips_through_json() {
        let result: HookResult<String> = HookResult::allow().with_reason("checked");
        let json = serde_json::to_value(result.clone()).unwrap();
        assert_eq!(
            serde_json::from_value::<HookResult<String>>(json).unwrap(),
            result
        );
    }
}
