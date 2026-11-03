//! What a turn publishes to its room's subscribers: Python's `TurnPublisher`
//! and `publish_frame`. A queue and a task that drains it, so that a backend
//! that is slow or stuck never stops the turn, each event published in order,
//! and only while the turn's answer reaches the whole room (RFC §8.4): its
//! subscribers are not bound channels, so no narrower scope can be tested
//! against them.
//!
//! The task runs only while the queue holds something: a turn that published
//! leaves nothing parked for the kit's close to wait on, whoever keeps its
//! host.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelId, RoomId};

use super::{TurnHost, TurnKit};
use crate::realtime::EphemeralEvent;

/// What a turn queued to publish, and whether a task is draining it.
#[derive(Debug, Default)]
pub(super) struct Publisher {
    state: Mutex<Queued>,
}

#[derive(Debug, Default)]
struct Queued {
    events: VecDeque<EphemeralEvent>,
    draining: bool,
}

impl Publisher {
    /// Queues `event`; whether a task is to start draining the queue.
    fn add(&self, event: EphemeralEvent) -> bool {
        let mut queued = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        queued.events.push_back(event);
        !std::mem::replace(&mut queued.draining, true)
    }

    /// The next event; none ends the drain, the next one queued starting
    /// another.
    fn next(&self) -> Option<EphemeralEvent> {
        let mut queued = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let event = queued.events.pop_front();
        queued.draining = event.is_some();
        event
    }

    /// Nothing drains any more, what was queued dropped: the kit is shut down.
    fn stop(&self) {
        let mut queued = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        queued.events.clear();
        queued.draining = false;
    }
}

/// Reopens the drain when its task ended early, a backend that panicked: the
/// next event queued starts another.
struct Draining(Arc<Publisher>);

impl Drop for Draining {
    fn drop(&mut self) {
        let mut queued = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        queued.draining = false;
    }
}

impl TurnHost {
    /// Queues `event` for the room's subscribers, published once the
    /// channel's binding, as the room holds it then, lets its answers reach
    /// every channel (Python's `binding_reaches_room`). For roomkit-ai's tool
    /// loop, whose caller reads the turn's own scope.
    #[doc(hidden)]
    pub fn publish_ephemeral(&self, event: EphemeralEvent) {
        let publisher = &self.shared.publisher;
        if !publisher.add(event) {
            return;
        }
        let drain = drain(
            Arc::clone(publisher),
            Arc::clone(&self.shared.kit),
            (self.shared.room_id.clone(), self.shared.channel_id.clone()),
        );
        if !self.shared.kit.spawn_publisher(Box::pin(drain)) {
            tracing::debug!(target: "roomkit::turns", "the kit is shut down: a turn publishes nothing");
            publisher.stop();
        }
    }
}

/// Publishes what `publisher` holds, in order, until it holds nothing.
async fn drain(
    publisher: Arc<Publisher>,
    kit: Arc<dyn TurnKit>,
    (room, channel): (RoomId, ChannelId),
) {
    let _ended = Draining(Arc::clone(&publisher));
    while let Some(event) = publisher.next() {
        if !kit.reaches_room(&room, &channel).await {
            tracing::debug!(target: "roomkit::turns", room_id = %room, "its answer does not reach the room: an ephemeral is not published");
            continue;
        }
        if let Some(realtime) = kit.realtime() {
            realtime.publish_to_room(&room, event).await;
        }
    }
}
