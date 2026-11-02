//! Framework events (RFC §8.2) as the Python implementation hands them to
//! its handlers, and the handlers [`RoomKit::on`](crate::RoomKit::on)
//! registers.
//!
//! The core reports each event as a typed notice; the kit turns it into
//! Python's `FrameworkEvent`, its ids at the top and the rest in `data`
//! (`_emit_framework_event`, `helpers.py`), and runs the handlers
//! registered for its type, in registration order. The call that emitted an
//! event waits for its handlers before it goes on, as Python's awaits them:
//! a call's events reach them in its order, and a handler may call the kit,
//! whose events it then waits for in turn; two calls' handlers run side by
//! side, as Python's do.

use std::sync::{Arc, PoisonError, RwLock};

use roomkit_core::model::{ChannelId, EventId, Metadata, RoomId, Timestamp};
use roomkit_core::ops::{Environment, FrameworkEvent as Notice, SystemEnvironment};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::channel::BoxFuture;
use crate::tasks::{Kind, Tasks};
use crate::unwind::{message, unwinding};

/// A framework event (RFC §8.2), Python's `FrameworkEvent(*, type,
/// room_id=None, channel_id=None, event_id=None, timestamp=<factory>,
/// data=<factory>)`: a notification for the kit's handlers, never stored
/// in a room's timeline.
///
/// `type` is the RFC's name of the row (`room_created`, `event_blocked`,
/// `delivery_failed`, ...); the room, the channel and the event it is
/// about are at the top when it has them, and every other member the RFC's
/// row names is in `data`, as Python places it. `room_created` reads
/// `{type: "room_created", room_id: "r1", data: {room_id: "r1"}}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameworkEvent {
    /// The row of RFC §8.2's table.
    pub r#type: String,
    /// The room it is about, when there is one.
    pub room_id: Option<RoomId>,
    /// The channel it is about, when there is one.
    pub channel_id: Option<ChannelId>,
    /// The event it is about, when there is one.
    pub event_id: Option<EventId>,
    /// When it was emitted.
    pub timestamp: Timestamp,
    /// Every other member of its row.
    pub data: Metadata,
}

impl FrameworkEvent {
    /// An event of type `r#type`, about no room, channel or event, emitted
    /// now by the system clock, its data empty, as Python's constructor
    /// defaults them.
    pub fn new(r#type: impl Into<String>) -> Self {
        Self {
            r#type: r#type.into(),
            room_id: None,
            channel_id: None,
            event_id: None,
            timestamp: SystemEnvironment.now(),
            data: Metadata::new(),
        }
    }

    /// The event a core notice reports, emitted at `timestamp`: its ids at
    /// the top and its other members in `data`, but where Python's
    /// emission places them otherwise.
    pub(crate) fn of(notice: Notice, timestamp: Timestamp) -> Self {
        let mut event = placed(&notice, timestamp);
        match notice {
            // The channel is in its data too (`channel_ops.py`).
            Notice::ChannelRegistered {
                channel_id,
                channel_type,
                ..
            }
            | Notice::ChannelUnregistered {
                channel_id,
                channel_type,
                ..
            } => {
                event.data =
                    data(json!({ "channel_id": channel_id, "channel_type": channel_type }));
            }
            // A room's lifecycle names the room in its data
            // (`room_lifecycle.py`); a timer's transition its reason alone.
            Notice::RoomCreated { room_id, .. }
            | Notice::RoomArchived { room_id, .. }
            | Notice::RoomClosed {
                room_id,
                reason: None,
                ..
            } => event.data = data(json!({ "room_id": room_id })),
            // The channel is in its data, not at the top (`inbound.py`).
            Notice::SessionStarted {
                channel_id,
                channel_type,
                ..
            } => {
                event.channel_id = None;
                event.data =
                    data(json!({ "channel_id": channel_id, "channel_type": channel_type }));
            }
            // The depths are its data, flat (`lane_execution.py`).
            Notice::ChainDepthExceeded { data: depths, .. } => {
                event.data = data(json!({
                    "chain_depth": depths.chain_depth,
                    "max_chain_depth": depths.max_chain_depth,
                }));
            }
            // The channel and the breaker's state are its data too
            // (`event_router.py`).
            Notice::CircuitBreakerOpened { channel_id, .. } => {
                event.data = data(json!({ "channel_id": channel_id, "state": "open" }));
            }
            Notice::CircuitBreakerClosed { channel_id, .. } => {
                event.data = data(json!({ "channel_id": channel_id, "state": "closed" }));
            }
            // The rest Python places as their serialized form does.
            Notice::RoomPaused { .. }
            | Notice::RoomClosed { .. }
            | Notice::RoomRefusedEvent { .. }
            | Notice::RoomChannelAttached { .. }
            | Notice::RoomChannelDetached { .. }
            | Notice::EventProcessed { .. }
            | Notice::EventBlocked { .. }
            | Notice::DeliverySucceeded { .. }
            | Notice::DeliveryFailed { .. }
            | Notice::BroadcastPartialFailure { .. }
            | Notice::HookError { .. }
            | Notice::ProcessTimeout { .. }
            | Notice::HookTimeout { .. }
            | _ => {}
        }
        event
    }
}

/// `notice` placed by its serialized form: its tag the type, its ids at the
/// top, its other members its data.
fn placed(notice: &Notice, timestamp: Timestamp) -> FrameworkEvent {
    let mut fields = match serde_json::to_value(notice) {
        Ok(Value::Object(fields)) => fields,
        _ => Metadata::new(),
    };
    let r#type = match fields.shift_remove("event") {
        Some(Value::String(name)) => name,
        _ => "unknown".to_owned(),
    };
    let mut id = |name: &str| match fields.shift_remove(name) {
        Some(Value::String(id)) => Some(id),
        _ => None,
    };
    let room_id = id("room_id").and_then(|id| RoomId::new(id).ok());
    let channel_id = id("channel_id").and_then(|id| ChannelId::new(id).ok());
    let event_id = id("event_id").and_then(|id| EventId::new(id).ok());
    FrameworkEvent {
        r#type,
        room_id,
        channel_id,
        event_id,
        timestamp,
        data: fields,
    }
}

/// The members of `value`, an object.
fn data(value: Value) -> Metadata {
    if let Value::Object(members) = value {
        members
    } else {
        Metadata::new()
    }
}

/// A handler [`RoomKit::on`](crate::RoomKit::on) registered.
pub(crate) type Handler = Arc<dyn Fn(FrameworkEvent) -> BoxFuture<'static, ()> + Send + Sync>;

/// The handlers registered for framework events, by type.
#[derive(Default)]
pub(crate) struct Handlers {
    registered: RwLock<Vec<(String, Handler)>>,
}

impl std::fmt::Debug for Handlers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Handlers").finish_non_exhaustive()
    }
}

/// The end of one event's handlers, which a call waits for before it
/// returns.
#[derive(Debug)]
pub(crate) struct Handled(watch::Receiver<bool>);

impl Handled {
    /// Waits until the event's handlers have ended, or were cut by the
    /// kit's close.
    pub(crate) async fn wait(mut self) {
        drop(self.0.wait_for(|ended| *ended).await);
    }
}

impl Handlers {
    /// Registers `handler` for the events of `event_type`, after the
    /// handlers registered before.
    pub(crate) fn add(&self, event_type: String, handler: Handler) {
        self.registered
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push((event_type, handler));
    }

    /// Runs the handlers registered for `event`'s type on a task of the
    /// kit's: the end to wait for,
    /// `None` when no handler is registered for it or no task could run
    /// them (no runtime, or the kit shut down). A handler that panics is
    /// logged, and the next one runs.
    pub(crate) fn emit(&self, tasks: &Arc<Tasks>, event: FrameworkEvent) -> Option<Handled> {
        tracing::debug!(target: "roomkit::events", r#type = %event.r#type, "framework event");
        let handlers: Vec<Handler> = self
            .registered
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(event_type, _)| *event_type == event.r#type)
            .map(|(_, handler)| Arc::clone(handler))
            .collect();
        if handlers.is_empty() {
            return None;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::debug!(
                target: "roomkit::events",
                r#type = %event.r#type,
                "no runtime runs the handlers of a framework event emitted outside one"
            );
            return None;
        }
        let (ended, end) = watch::channel(false);
        let running = async move {
            for handler in handlers {
                if let Err(panic) = unwinding(handler(event.clone())).await {
                    tracing::error!(
                        target: "roomkit::events",
                        r#type = %event.r#type,
                        panic = %message(panic.as_ref()),
                        "a framework event handler failed"
                    );
                }
            }
            ended.send_replace(true);
        };
        tasks.spawn_alone(Kind::Observer, running)?;
        Some(Handled(end))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{Access, ChannelType, EventType, RoomStatus, Visibility};
    use roomkit_core::ops::{ChainDepthData, RefusedOperation, TransitionReason};
    use roomkit_core::rules::hook_registry::HookTrigger;

    use super::*;

    fn at() -> Timestamp {
        "2026-10-31T12:00:00Z".parse().unwrap()
    }

    fn room() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    fn channel() -> ChannelId {
        ChannelId::new("sms").unwrap()
    }

    fn event() -> EventId {
        EventId::new("e1").unwrap()
    }

    /// What each event reads, as Python's `_emit_framework_event` places
    /// it: its ids at the top, the rest of its row in `data`.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one case per row of RFC §8.2's table"
    )]
    fn each_event_is_placed_as_python_places_it() {
        let read = |notice: Notice| {
            let event = FrameworkEvent::of(notice, at());
            (
                event.r#type,
                event.room_id.map(|id| id.as_str().to_owned()),
                event.channel_id.map(|id| id.as_str().to_owned()),
                event.event_id.map(|id| id.as_str().to_owned()),
                Value::Object(event.data),
            )
        };
        let s = |value: &str| Some(value.to_owned());
        let cases = [
            (
                Notice::channel_registered(channel(), ChannelType::SMS),
                (
                    "channel_registered",
                    None,
                    s("sms"),
                    None,
                    json!({"channel_id": "sms", "channel_type": "sms"}),
                ),
            ),
            (
                Notice::room_created(room()),
                (
                    "room_created",
                    s("r1"),
                    None,
                    None,
                    json!({"room_id": "r1"}),
                ),
            ),
            (
                Notice::room_paused(room(), TransitionReason::Timer),
                (
                    "room_paused",
                    s("r1"),
                    None,
                    None,
                    json!({"reason": "timer"}),
                ),
            ),
            (
                Notice::room_closed(room(), None),
                ("room_closed", s("r1"), None, None, json!({"room_id": "r1"})),
            ),
            (
                Notice::room_closed(room(), Some(TransitionReason::Timer)),
                (
                    "room_closed",
                    s("r1"),
                    None,
                    None,
                    json!({"reason": "timer"}),
                ),
            ),
            (
                Notice::room_refused_event(
                    room(),
                    event(),
                    RoomStatus::Closed,
                    RefusedOperation::Inbound,
                    EventType::Message,
                ),
                (
                    "room_refused_event",
                    s("r1"),
                    None,
                    s("e1"),
                    json!({"status": "closed", "operation": "inbound", "event_type": "message"}),
                ),
            ),
            (
                Notice::room_channel_attached(
                    room(),
                    channel(),
                    Access::ReadWrite,
                    Visibility::All,
                ),
                (
                    "room_channel_attached",
                    s("r1"),
                    s("sms"),
                    None,
                    json!({"access": "read_write", "visibility": "all"}),
                ),
            ),
            (
                Notice::chain_depth_exceeded(
                    room(),
                    event(),
                    channel(),
                    ChainDepthData {
                        chain_depth: 6,
                        max_chain_depth: 5,
                    },
                ),
                (
                    "chain_depth_exceeded",
                    s("r1"),
                    s("sms"),
                    s("e1"),
                    json!({"chain_depth": 6, "max_chain_depth": 5}),
                ),
            ),
            (
                Notice::hook_timeout(room(), "audit".to_owned(), HookTrigger::AfterBroadcast, 0.5),
                (
                    "hook_timeout",
                    s("r1"),
                    None,
                    None,
                    json!({"hook_name": "audit", "trigger": "after_broadcast", "timeout": 0.5}),
                ),
            ),
        ];
        for (notice, (r#type, room_id, channel_id, event_id, data)) in cases {
            assert_eq!(
                read(notice),
                (r#type.to_owned(), room_id, channel_id, event_id, data)
            );
        }
    }
}
