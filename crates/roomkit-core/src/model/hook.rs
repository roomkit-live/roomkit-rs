//! What a SYNC hook decides (RFC §9.4).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, ModelError, Observation, RoomEvent, Task};

/// The hook metadata key [`HookResult::allow_unverified`] sets (RFC
/// §11.7.3), Python's `VERIFICATION_DECISION_KEY`; its value `pass` lets an
/// unverified sender's message through.
pub(crate) const VERIFICATION_DECISION_KEY: &str = "roomkit:verification";

/// The value of [`VERIFICATION_DECISION_KEY`] that passes a message.
pub(crate) const VERIFICATION_PASS: &str = "pass";

/// What a SYNC hook does with its payload (RFC §9.4): the Python
/// implementation's `Literal["allow", "block", "modify"]` of
/// `HookResult.action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HookAction {
    /// Let the payload through unchanged.
    Allow,
    /// Stop the pipeline: no further hook runs.
    Block,
    /// Replace the payload with `event`, whatever the replacement's value.
    Modify,
}

/// What a SYNC hook returns (RFC §9.4), the Python implementation's
/// `HookResult`.
///
/// `P` stands for Python's `event: Any`: the payload type of the hook's
/// trigger, a [`RoomEvent`] for `BEFORE_BROADCAST`. A `modify` carries its
/// replacement in `event`, and a `block` its `reason`, which may be blank:
/// one without is refused when deserialized, and a handler that returns
/// one built by struct literal is read as a hook that raised, as a Python
/// hook building it would have raised.
///
/// ```
/// use roomkit_core::model::hook::BlockOptions;
/// use roomkit_core::model::{HookAction, HookResult};
///
/// let result: HookResult = HookResult::block("SIN detected", BlockOptions::default());
/// assert_eq!(result.action, HookAction::Block);
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    bound(serialize = "P: Serialize", deserialize = "P: Deserialize<'de>"),
    try_from = "HookResultWire<P>"
)]
pub struct HookResult<P = RoomEvent> {
    /// Allow, block, or modify with a replacement payload.
    pub action: HookAction,
    /// The replacement payload, for a `modify`.
    pub event: Option<P>,
    /// Why the hook blocked: required for a `block`.
    pub reason: Option<String>,
    /// Events to inject, whatever the action; committed on
    /// `BEFORE_BROADCAST` only.
    pub injected_events: Vec<InjectedEvent>,
    /// Tasks to create, always collected.
    pub tasks: Vec<Task>,
    /// Observations to record, always collected.
    pub observations: Vec<Observation>,
    /// Additional hook data.
    pub metadata: Metadata,
}

/// What [`HookResult::allow`] takes beside nothing: Python's keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AllowOptions {
    /// Events to inject; `None` injects none.
    pub injected: Option<Vec<InjectedEvent>>,
}

/// What [`HookResult::block`] takes beside its reason: Python's keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BlockOptions {
    /// Events to inject; `None` injects none.
    pub injected: Option<Vec<InjectedEvent>>,
    /// Tasks to create; `None` creates none.
    pub tasks: Option<Vec<Task>>,
    /// Observations to record; `None` records none.
    pub observations: Option<Vec<Observation>>,
}

/// What [`HookResult::modify`] takes beside its replacement: Python's
/// keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModifyOptions {
    /// Events to inject; `None` injects none.
    pub injected: Option<Vec<InjectedEvent>>,
}

impl<P> HookResult<P> {
    /// A result of `action` with no event, reason or side effect, Python's
    /// `HookResult(action=…)`: a caller sets the other fields by struct
    /// update.
    pub fn new(action: HookAction) -> Self {
        Self {
            action,
            event: None,
            reason: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            metadata: Metadata::new(),
        }
    }

    /// Let the payload through.
    pub fn allow(options: AllowOptions) -> Self {
        Self {
            injected_events: options.injected.unwrap_or_default(),
            ..Self::new(HookAction::Allow)
        }
    }

    /// On `ON_VERIFICATION_REQUIRED`: let an unverified sender's message
    /// through (RFC §11.7.3), Python's `HookResult.allow_unverified()`. It
    /// goes on labelled with the sender's verification; without this, the
    /// message is held from the room's agents. A plain [`allow`](Self::allow)
    /// does not pass it: a hook that only watches never lets a message
    /// through by accident.
    pub fn allow_unverified() -> Self {
        let mut metadata = Metadata::new();
        metadata.insert(
            VERIFICATION_DECISION_KEY.to_owned(),
            VERIFICATION_PASS.into(),
        );
        Self {
            metadata,
            ..Self::new(HookAction::Allow)
        }
    }

    /// Stop the pipeline, saying why; `reason` may be blank.
    pub fn block(reason: impl Into<String>, options: BlockOptions) -> Self {
        Self {
            reason: Some(reason.into()),
            injected_events: options.injected.unwrap_or_default(),
            tasks: options.tasks.unwrap_or_default(),
            observations: options.observations.unwrap_or_default(),
            ..Self::new(HookAction::Block)
        }
    }

    /// Replace the payload with `event`.
    pub fn modify(event: P, options: ModifyOptions) -> Self {
        Self {
            event: Some(event),
            injected_events: options.injected.unwrap_or_default(),
            ..Self::new(HookAction::Modify)
        }
    }

    /// What the Python implementation's validator raises for this result:
    /// a `modify` without its event, a `block` without its reason. Outside
    /// the public API: deserialization refuses either; a hook returning a
    /// `modify` without its event follows the unusable-result policy of
    /// RFC §9.3 instead of being read as a constructor failure.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::ModifyWithoutPayload`] or
    /// [`ModelError::BlockWithoutReason`].
    #[doc(hidden)]
    pub fn check(&self) -> Result<(), ModelError> {
        match self.action {
            HookAction::Modify if self.event.is_none() => Err(ModelError::ModifyWithoutPayload),
            HookAction::Block if self.reason.is_none() => Err(ModelError::BlockWithoutReason),
            HookAction::Allow | HookAction::Block | HookAction::Modify => Ok(()),
        }
    }
}

/// An event a hook injects (RFC §9.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InjectedEvent {
    /// The event to inject.
    pub event: RoomEvent,
    /// The channels to deliver it to; `None` stores it only.
    #[serde(default)]
    pub target_channel_ids: Option<Vec<ChannelId>>,
}

impl InjectedEvent {
    /// `event`, stored and delivered to no channel: a caller names the
    /// channels by struct update, `target_channel_ids: Some(…)`.
    pub fn new(event: RoomEvent) -> Self {
        Self {
            event,
            target_channel_ids: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "P: Deserialize<'de>"))]
struct HookResultWire<P> {
    action: HookAction,
    #[serde(default = "Option::default")]
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

impl<P> TryFrom<HookResultWire<P>> for HookResult<P> {
    type Error = ModelError;

    fn try_from(wire: HookResultWire<P>) -> Result<Self, Self::Error> {
        let result = Self {
            action: wire.action,
            event: wire.event,
            reason: wire.reason,
            injected_events: wire.injected_events,
            tasks: wire.tasks,
            observations: wire.observations,
            metadata: wire.metadata,
        };
        result.check()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_block_serializes_its_action_and_reason() {
        let result: HookResult<String> = HookResult::block("SIN detected", BlockOptions::default());
        let json = serde_json::to_value(result).unwrap();
        assert_eq!(
            (json["action"].clone(), json["reason"].clone()),
            (json!("block"), json!("SIN detected"))
        );
    }

    #[test]
    fn a_modify_carries_its_payload_as_the_event() {
        let json = serde_json::to_value(HookResult::modify(
            "redacted".to_owned(),
            ModifyOptions::default(),
        ))
        .unwrap();
        assert_eq!(json["event"], json!("redacted"));
    }

    #[test]
    fn a_modify_to_an_empty_payload_keeps_the_empty_payload() {
        let parsed: HookResult<String> =
            serde_json::from_value(json!({ "action": "modify", "event": "" })).unwrap();
        assert_eq!(parsed.event, Some(String::new()));
    }

    #[test]
    fn a_modify_without_an_event_does_not_deserialize() {
        let parsed = serde_json::from_value::<HookResult<String>>(json!({ "action": "modify" }));
        assert!(parsed.is_err());
    }

    /// Python's validator: `action='block' requires 'reason' to be set`; a
    /// blank reason is one.
    #[test]
    fn a_block_without_a_reason_does_not_deserialize() {
        let without = serde_json::from_value::<HookResult<String>>(json!({ "action": "block" }));
        let blank: HookResult<String> =
            serde_json::from_value(json!({ "action": "block", "reason": "" })).unwrap();
        assert_eq!(
            (without.map_err(|error| error.to_string()), blank.reason),
            (
                Err("a block hook result carries no reason".to_owned()),
                Some(String::new())
            )
        );
    }

    #[test]
    fn an_unknown_action_does_not_deserialize() {
        let parsed = serde_json::from_value::<HookResult<String>>(json!({ "action": "maybe" }));
        assert!(parsed.is_err());
    }

    #[test]
    fn an_allow_round_trips_through_json() {
        let result: HookResult<String> = HookResult {
            reason: Some("checked".to_owned()),
            ..HookResult::allow(AllowOptions::default())
        };
        let json = serde_json::to_value(result.clone()).unwrap();
        assert_eq!(
            serde_json::from_value::<HookResult<String>>(json).unwrap(),
            result
        );
    }
}
