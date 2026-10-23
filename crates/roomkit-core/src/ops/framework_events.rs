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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum FrameworkEvent {
    /// A channel was registered with the kit.
    ChannelRegistered {
        /// The channel.
        channel_id: ChannelId,
        /// Its type.
        channel_type: ChannelType,
    },
    /// A channel was unregistered from the kit.
    ChannelUnregistered {
        /// The channel.
        channel_id: ChannelId,
        /// Its type.
        channel_type: ChannelType,
    },
    /// A room was created.
    RoomCreated {
        /// The room.
        room_id: RoomId,
    },
    /// A room's inactivity timer paused it. A pause the integrator asks for
    /// emits nothing (RFC §26.1).
    RoomPaused {
        /// The room.
        room_id: RoomId,
        /// What paused it.
        reason: TransitionReason,
    },
    /// A room was closed.
    RoomClosed {
        /// The room.
        room_id: RoomId,
        /// `timer` when its close timer closed it, `None` when the
        /// integrator did.
        reason: Option<TransitionReason>,
    },
    /// A room was archived.
    RoomArchived {
        /// The room.
        room_id: RoomId,
    },
    /// A room whose status refuses new events turned one away (RFC §5.1).
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
    RoomChannelDetached {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// An event that entered through the inbound pipeline finished its
    /// delivery set and its `AFTER_BROADCAST` hooks.
    EventProcessed {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
    },
    /// An event was blocked by a hook or by a source that cannot write.
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
    /// At least one target of an event failed; the event stays delivered.
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
    /// A hook ran past its timeout.
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

/// What made a room transition on its own (RFC §8.2 `room_paused`,
/// `room_closed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    /// The room's timer fell due (RFC §5.1).
    Timer,
}

/// The path a refused event took (RFC §8.2, `room_refused_event`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
