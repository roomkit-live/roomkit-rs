//! What a turn publishes to its room's subscribers: Python's `TurnPublisher`
//! and `publish_frame`. A queue and a task of its own, so that a backend that
//! is slow or stuck never stops the turn, each event published in order, and
//! only while the turn's answer reaches the whole room (RFC §8.4): its
//! subscribers are not bound channels, so no narrower scope can be tested
//! against them.

use std::sync::{Arc, PoisonError};

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::TurnHost;
use crate::realtime::EphemeralEvent;

impl TurnHost {
    /// Queues `event` for the room's subscribers, published once the
    /// channel's binding, as the room holds it then, lets its answers reach
    /// every channel (Python's `binding_reaches_room`). For roomkit-ai's tool
    /// loop, whose caller reads the turn's own scope.
    #[doc(hidden)]
    pub fn publish_ephemeral(&self, event: EphemeralEvent) {
        let mut publisher = self
            .shared
            .publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let queue = publisher.get_or_insert_with(|| self.start_publisher());
        if queue.send(event).is_err() {
            tracing::debug!(target: "roomkit::turns", "a turn's publisher ended: an ephemeral is dropped");
        }
    }

    /// The queue of the task that publishes the turn's ephemerals, which ends
    /// once the turn is dropped, what it was queued published first.
    fn start_publisher(&self) -> UnboundedSender<EphemeralEvent> {
        let (queue, mut queued) = unbounded_channel::<EphemeralEvent>();
        let kit = Arc::clone(&self.shared.kit);
        let (room, channel) = (self.shared.room_id.clone(), self.shared.channel_id.clone());
        let task = async move {
            while let Some(event) = queued.recv().await {
                if !kit.reaches_room(&room, &channel).await {
                    tracing::debug!(target: "roomkit::turns", room_id = %room, "its answer does not reach the room: an ephemeral is not published");
                    continue;
                }
                if let Some(realtime) = kit.realtime() {
                    realtime.publish_to_room(&room, event).await;
                }
            }
        };
        if !self.shared.kit.spawn_publisher(Box::pin(task)) {
            tracing::debug!(target: "roomkit::turns", "the kit is shut down: a turn publishes nothing");
        }
        queue
    }
}
