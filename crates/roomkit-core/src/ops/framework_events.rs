//! The framework events the operations emit (RFC §8.2).
//!
//! Framework events are global notifications for subscribers; they are never
//! stored in a room's timeline. This lists the rows the implemented steps
//! produce (gap G-004); each carries the data the RFC's table gives it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{
    Access, ChannelId, ChannelType, EventId, EventType, RoomId, RoomStatus, Visibility,
};
use crate::rules::hook_registry::HookTrigger;

/// A framework event (RFC §8.2), tagged by its RFC name under `event`.
///
/// The enumeration and each of its variants are `#[non_exhaustive]`: a row
/// RFC §8.2 adds, or a member it adds to a row, breaks no subscriber. A
/// subscriber matches a variant with `..`:
/// `FrameworkEvent::DeliveryFailed { channel_id, error, .. }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[non_exhaustive]
pub enum FrameworkEvent {
    /// A channel was registered with the kit.
    #[non_exhaustive]
    ChannelRegistered {
        /// The channel.
        channel_id: ChannelId,
        /// Its type.
        channel_type: ChannelType,
    },
    /// A channel was unregistered from the kit.
    #[non_exhaustive]
    ChannelUnregistered {
        /// The channel.
        channel_id: ChannelId,
        /// Its type.
        channel_type: ChannelType,
    },
    /// A room was created.
    #[non_exhaustive]
    RoomCreated {
        /// The room.
        room_id: RoomId,
    },
    /// A room's inactivity timer paused it. A pause the integrator asks for
    /// emits nothing (RFC §26.1).
    #[non_exhaustive]
    RoomPaused {
        /// The room.
        room_id: RoomId,
        /// What paused it.
        reason: TransitionReason,
    },
    /// A room was closed.
    #[non_exhaustive]
    RoomClosed {
        /// The room.
        room_id: RoomId,
        /// `timer` when its close timer closed it, `None` when the
        /// integrator did.
        reason: Option<TransitionReason>,
    },
    /// A room was archived.
    #[non_exhaustive]
    RoomArchived {
        /// The room.
        room_id: RoomId,
    },
    /// A room whose status refuses new events turned one away (RFC §5.1).
    #[non_exhaustive]
    RoomRefusedEvent {
        /// The room.
        room_id: RoomId,
        /// The refused event's id, which names nothing in the store.
        event_id: EventId,
        /// The status that refused it.
        status: RoomStatus,
        /// The path that met the gate.
        operation: RefusedOperation,
        /// The refused event's type.
        event_type: EventType,
    },
    /// A channel was attached to a room.
    #[non_exhaustive]
    RoomChannelAttached {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
        /// The binding's access.
        access: Access,
        /// The binding's visibility.
        visibility: Visibility,
    },
    /// A channel was detached from a room.
    #[non_exhaustive]
    RoomChannelDetached {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// `ON_SESSION_STARTED` fired: the inbound pipeline created a room for a
    /// message on a channel other than `VOICE` and `REALTIME_VOICE` (RFC §10.1
    /// step 2). A voice session's start is the voice step's.
    #[non_exhaustive]
    SessionStarted {
        /// The room.
        room_id: RoomId,
        /// The channel the message came in on.
        channel_id: ChannelId,
        /// Its type.
        channel_type: ChannelType,
    },
    /// An event that entered through the inbound pipeline finished its
    /// delivery set and its `AFTER_BROADCAST` hooks.
    #[non_exhaustive]
    EventProcessed {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
    },
    /// An event was blocked by a hook or by a source that cannot write.
    #[non_exhaustive]
    EventBlocked {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// The event's source.
        channel_id: ChannelId,
        /// Why.
        reason: Option<String>,
        /// What blocked it.
        blocked_by: String,
    },
    /// A transport channel's `deliver()` succeeded, for an event that
    /// entered through the inbound pipeline.
    #[non_exhaustive]
    DeliverySucceeded {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// A target's `on_event()` or `deliver()` failed, for an event that
    /// entered through the inbound pipeline.
    #[non_exhaustive]
    DeliveryFailed {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// The channel.
        channel_id: ChannelId,
        /// What failed.
        error: String,
    },
    /// A transport channel's circuit breaker opened (RFC §13.1): its sends
    /// are skipped until it lets a probe through.
    #[non_exhaustive]
    CircuitBreakerOpened {
        /// The channel.
        channel_id: ChannelId,
    },
    /// A transport channel's circuit breaker closed again: a send
    /// succeeded once it had opened (RFC §13.1).
    #[non_exhaustive]
    CircuitBreakerClosed {
        /// The channel.
        channel_id: ChannelId,
    },
    /// At least one target of an event failed; the event stays delivered.
    #[non_exhaustive]
    BroadcastPartialFailure {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// How many targets failed.
        failed: usize,
        /// How many targets there were.
        total: usize,
        /// What failed, per channel.
        errors: BTreeMap<ChannelId, String>,
    },
    /// A channel was not asked because of the chain-depth limit (RFC §8.3).
    #[non_exhaustive]
    ChainDepthExceeded {
        /// The room.
        room_id: RoomId,
        /// The record stored in place of the response.
        event_id: EventId,
        /// The channel not asked.
        channel_id: ChannelId,
        /// The depths.
        data: ChainDepthData,
    },
    /// A SYNC `BEFORE_BROADCAST` hook failed at an event's commit gate: it
    /// raised, timed out or returned an unusable result.
    #[non_exhaustive]
    HookError {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// The hook's name.
        hook: String,
        /// What failed.
        error: String,
    },
    /// A call's pre-commit phase passed its `process_timeout` (RFC §13.6):
    /// nothing of its event was written. It names the message's channel
    /// when the timeout passed before the room lock, the event when it
    /// passed under it (gap G-068).
    #[non_exhaustive]
    ProcessTimeout {
        /// The room; `None` when the timeout passed before the message had
        /// one, while it was routed (gap G-068).
        room_id: Option<RoomId>,
        /// The message's channel, when the timeout passed before the room
        /// lock.
        channel_id: Option<ChannelId>,
        /// The event, when the timeout passed under the room lock.
        event_id: Option<EventId>,
        /// The timeout, in seconds.
        timeout: f64,
    },
    /// A hook ran past its timeout.
    #[non_exhaustive]
    HookTimeout {
        /// The room.
        room_id: RoomId,
        /// The hook's name.
        hook_name: String,
        /// The trigger it ran on.
        trigger: HookTrigger,
        /// The timeout, in seconds.
        timeout: f64,
    },
}

impl FrameworkEvent {
    /// A [`ChannelRegistered`](Self::ChannelRegistered) event, `channel_registered` (RFC §8.2).
    pub fn channel_registered(channel_id: ChannelId, channel_type: ChannelType) -> Self {
        Self::ChannelRegistered {
            channel_id,
            channel_type,
        }
    }

    /// A [`ChannelUnregistered`](Self::ChannelUnregistered) event, `channel_unregistered` (RFC §8.2).
    pub fn channel_unregistered(channel_id: ChannelId, channel_type: ChannelType) -> Self {
        Self::ChannelUnregistered {
            channel_id,
            channel_type,
        }
    }

    /// A [`RoomCreated`](Self::RoomCreated) event, `room_created` (RFC §8.2).
    pub fn room_created(room_id: RoomId) -> Self {
        Self::RoomCreated { room_id }
    }

    /// A [`RoomPaused`](Self::RoomPaused) event, `room_paused` (RFC §8.2).
    pub fn room_paused(room_id: RoomId, reason: TransitionReason) -> Self {
        Self::RoomPaused { room_id, reason }
    }

    /// A [`RoomClosed`](Self::RoomClosed) event, `room_closed` (RFC §8.2).
    pub fn room_closed(room_id: RoomId, reason: Option<TransitionReason>) -> Self {
        Self::RoomClosed { room_id, reason }
    }

    /// A [`RoomArchived`](Self::RoomArchived) event, `room_archived` (RFC §8.2).
    pub fn room_archived(room_id: RoomId) -> Self {
        Self::RoomArchived { room_id }
    }

    /// A [`RoomRefusedEvent`](Self::RoomRefusedEvent) event, `room_refused_event` (RFC §8.2).
    pub fn room_refused_event(
        room_id: RoomId,
        event_id: EventId,
        status: RoomStatus,
        operation: RefusedOperation,
        event_type: EventType,
    ) -> Self {
        Self::RoomRefusedEvent {
            room_id,
            event_id,
            status,
            operation,
            event_type,
        }
    }

    /// A [`RoomChannelAttached`](Self::RoomChannelAttached) event, `room_channel_attached` (RFC §8.2).
    pub fn room_channel_attached(
        room_id: RoomId,
        channel_id: ChannelId,
        access: Access,
        visibility: Visibility,
    ) -> Self {
        Self::RoomChannelAttached {
            room_id,
            channel_id,
            access,
            visibility,
        }
    }

    /// A [`RoomChannelDetached`](Self::RoomChannelDetached) event, `room_channel_detached` (RFC §8.2).
    pub fn room_channel_detached(room_id: RoomId, channel_id: ChannelId) -> Self {
        Self::RoomChannelDetached {
            room_id,
            channel_id,
        }
    }

    /// A [`SessionStarted`](Self::SessionStarted) event, `session_started` (RFC §8.2).
    pub fn session_started(
        room_id: RoomId,
        channel_id: ChannelId,
        channel_type: ChannelType,
    ) -> Self {
        Self::SessionStarted {
            room_id,
            channel_id,
            channel_type,
        }
    }

    /// A [`EventProcessed`](Self::EventProcessed) event, `event_processed` (RFC §8.2).
    pub fn event_processed(room_id: RoomId, event_id: EventId) -> Self {
        Self::EventProcessed { room_id, event_id }
    }

    /// A [`EventBlocked`](Self::EventBlocked) event, `event_blocked` (RFC §8.2).
    pub fn event_blocked(
        room_id: RoomId,
        event_id: EventId,
        channel_id: ChannelId,
        reason: Option<String>,
        blocked_by: String,
    ) -> Self {
        Self::EventBlocked {
            room_id,
            event_id,
            channel_id,
            reason,
            blocked_by,
        }
    }

    /// A [`DeliverySucceeded`](Self::DeliverySucceeded) event, `delivery_succeeded` (RFC §8.2).
    pub fn delivery_succeeded(room_id: RoomId, event_id: EventId, channel_id: ChannelId) -> Self {
        Self::DeliverySucceeded {
            room_id,
            event_id,
            channel_id,
        }
    }

    /// A [`DeliveryFailed`](Self::DeliveryFailed) event, `delivery_failed` (RFC §8.2).
    pub fn delivery_failed(
        room_id: RoomId,
        event_id: EventId,
        channel_id: ChannelId,
        error: String,
    ) -> Self {
        Self::DeliveryFailed {
            room_id,
            event_id,
            channel_id,
            error,
        }
    }

    /// A [`CircuitBreakerOpened`](Self::CircuitBreakerOpened) event,
    /// `circuit_breaker_opened` (RFC §13.1).
    pub fn circuit_breaker_opened(channel_id: ChannelId) -> Self {
        Self::CircuitBreakerOpened { channel_id }
    }

    /// A [`CircuitBreakerClosed`](Self::CircuitBreakerClosed) event,
    /// `circuit_breaker_closed` (RFC §13.1).
    pub fn circuit_breaker_closed(channel_id: ChannelId) -> Self {
        Self::CircuitBreakerClosed { channel_id }
    }

    /// A [`BroadcastPartialFailure`](Self::BroadcastPartialFailure) event, `broadcast_partial_failure` (RFC §8.2).
    pub fn broadcast_partial_failure(
        room_id: RoomId,
        event_id: EventId,
        failed: usize,
        total: usize,
        errors: BTreeMap<ChannelId, String>,
    ) -> Self {
        Self::BroadcastPartialFailure {
            room_id,
            event_id,
            failed,
            total,
            errors,
        }
    }

    /// A [`ChainDepthExceeded`](Self::ChainDepthExceeded) event, `chain_depth_exceeded` (RFC §8.2).
    pub fn chain_depth_exceeded(
        room_id: RoomId,
        event_id: EventId,
        channel_id: ChannelId,
        data: ChainDepthData,
    ) -> Self {
        Self::ChainDepthExceeded {
            room_id,
            event_id,
            channel_id,
            data,
        }
    }

    /// A [`HookError`](Self::HookError) event, `hook_error` (RFC §8.2).
    pub fn hook_error(room_id: RoomId, event_id: EventId, hook: String, error: String) -> Self {
        Self::HookError {
            room_id,
            event_id,
            hook,
            error,
        }
    }

    /// A [`ProcessTimeout`](Self::ProcessTimeout) event, `process_timeout` (RFC §8.2).
    pub fn process_timeout(
        room_id: Option<RoomId>,
        channel_id: Option<ChannelId>,
        event_id: Option<EventId>,
        timeout: f64,
    ) -> Self {
        Self::ProcessTimeout {
            room_id,
            channel_id,
            event_id,
            timeout,
        }
    }

    /// A [`HookTimeout`](Self::HookTimeout) event, `hook_timeout` (RFC §8.2).
    pub fn hook_timeout(
        room_id: RoomId,
        hook_name: String,
        trigger: HookTrigger,
        timeout: f64,
    ) -> Self {
        Self::HookTimeout {
            room_id,
            hook_name,
            trigger,
            timeout,
        }
    }
}

/// What made a room transition on its own (RFC §8.2 `room_paused`,
/// `room_closed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TransitionReason {
    /// The room's timer fell due (RFC §5.1).
    Timer,
}

/// The path a refused event took (RFC §8.2, `room_refused_event`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RefusedOperation {
    /// An inbound message or a direct injection.
    Inbound,
    /// The framework's own re-injection of a response, or the pass of an
    /// event a hook injected (gap G-043).
    Reentry,
}

/// The depths of a `chain_depth_exceeded` event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainDepthData {
    /// The depth the response would have had.
    pub chain_depth: u32,
    /// The limit.
    pub max_chain_depth: u32,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_framework_event_is_tagged_with_its_rfc_name() {
        let event = FrameworkEvent::EventProcessed {
            room_id: RoomId::new("room_1").unwrap(),
            event_id: EventId::new("evt_1").unwrap(),
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({ "event": "event_processed", "room_id": "room_1", "event_id": "evt_1" })
        );
    }

    #[test]
    fn a_timer_s_close_carries_its_reason() {
        let event = FrameworkEvent::RoomClosed {
            room_id: RoomId::new("room_1").unwrap(),
            reason: Some(TransitionReason::Timer),
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({ "event": "room_closed", "room_id": "room_1", "reason": "timer" })
        );
    }

    /// RFC §8.2: `channel_id` before the room lock, or `event_id` under it.
    #[test]
    fn a_process_timeout_before_the_lock_names_the_channel() {
        let event = FrameworkEvent::ProcessTimeout {
            room_id: Some(RoomId::new("room_1").unwrap()),
            channel_id: Some(ChannelId::new("sms").unwrap()),
            event_id: None,
            timeout: 2.5,
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "event": "process_timeout",
                "room_id": "room_1",
                "channel_id": "sms",
                "event_id": null,
                "timeout": 2.5,
            })
        );
    }

    #[test]
    fn chain_depth_exceeded_carries_its_depths_under_data() {
        let event = FrameworkEvent::ChainDepthExceeded {
            room_id: RoomId::new("room_1").unwrap(),
            event_id: EventId::new("evt_9").unwrap(),
            channel_id: ChannelId::new("writer").unwrap(),
            data: ChainDepthData {
                chain_depth: 5,
                max_chain_depth: 5,
            },
        };
        assert_eq!(
            serde_json::to_value(event).unwrap()["data"],
            json!({ "chain_depth": 5, "max_chain_depth": 5 })
        );
    }
}
