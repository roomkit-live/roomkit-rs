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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use roomkit_core::model::{ChannelId, RoomId, Timestamp};

use super::{TurnHost, TurnKit};
use crate::realtime::{EphemeralEvent, EphemeralEventType};

/// What a turn queued to publish, and whether a task is draining it.
#[derive(Debug, Default)]
pub(crate) struct Publisher {
    state: Mutex<Queued>,
    /// Whether the turn's reader commits it as a delegated task's trace, once
    /// the kit's executor runs the turn; none outside a turn.
    traced: OnceLock<Arc<AtomicBool>>,
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

    /// Whether `event` may be published: an answer's text only by a turn read
    /// as a delegated task's trace, the one stream no hook judges either, and
    /// while that turn runs: what it queued by then still goes (RFC §6.4,
    /// §23.3 step 5).
    fn may_publish(&self, event: &EphemeralEvent) -> bool {
        event.r#type != EphemeralEventType::TextDelta
            || self
                .traced
                .get()
                .is_some_and(|traced| traced.load(Ordering::Acquire))
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
struct Draining {
    publisher: Arc<Publisher>,
    finished: bool,
}

impl Draining {
    fn next(&mut self) -> Option<EphemeralEvent> {
        let next = self.publisher.next();
        self.finished = next.is_none();
        next
    }
}

impl Drop for Draining {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut queued = self
            .publisher
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        queued.draining = false;
    }
}

impl TurnHost {
    /// Reads from `traced` whether the turn's reader commits it as a
    /// delegated task's trace, which its owner says before its first read.
    pub(crate) fn reads_trace_from(&self, traced: Arc<AtomicBool>) {
        drop(self.shared.publisher.traced.set(traced));
    }

    /// Queues `event` for the room's subscribers, published once the
    /// channel's binding, as the room holds it then, lets its answers reach
    /// every channel (Python's `binding_reaches_room`). For roomkit-ai's tool
    /// loop, whose caller reads the turn's own scope.
    #[doc(hidden)]
    pub fn publish_ephemeral(&self, event: EphemeralEvent) {
        let publisher = &self.shared.publisher;
        if self.shared.link.uses_worker_link() {
            // Only the owner knows whether its reader is tracing this turn.
            // An ended turn can never carry answer text.
            if self.answer_unscoped
                && (event.r#type != EphemeralEventType::TextDelta || self.shared.link.is_open())
            {
                self.shared
                    .link
                    .publish_remote(crate::turns::wire::Frame::Ephemeral {
                        channel: format!("room:{}", self.shared.room_id),
                        event: serde_json::Value::Object(event.to_dict()),
                    });
            }
            return;
        }
        if !publisher.may_publish(&event) {
            tracing::debug!(target: "roomkit::turns", room_id = %self.shared.room_id, "no trace running: an answer's text is not published");
            return;
        }
        publisher.enqueue(
            &self.shared.kit,
            &self.shared.room_id,
            &self.shared.channel_id,
            self.shared.context.room.created_at,
            event,
        );
    }
}

impl Publisher {
    pub(crate) fn enqueue(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        room: &RoomId,
        channel: &ChannelId,
        mark: Timestamp,
        event: EphemeralEvent,
    ) {
        self.schedule(kit, event, || {
            drain(
                Arc::clone(self),
                Arc::clone(kit),
                (room.clone(), channel.clone()),
                mark,
            )
        });
    }

    pub(crate) fn enqueue_outside(
        self: &Arc<Self>,
        wiring: &crate::turns::TurnWiring,
        event: EphemeralEvent,
    ) {
        self.schedule(&wiring.kit, event, || {
            let wiring = wiring.clone();
            let mut draining = Draining {
                publisher: Arc::clone(self),
                finished: false,
            };
            async move {
                while let Some(event) = draining.next() {
                    wiring.publish_outside(event).await;
                }
            }
        });
    }

    fn schedule<F: Future<Output = ()> + Send + 'static>(
        &self,
        kit: &Arc<dyn TurnKit>,
        event: EphemeralEvent,
        drain: impl FnOnce() -> F,
    ) {
        if !self.add(event) {
            return;
        }
        if !kit.spawn_publisher(Box::pin(drain())) {
            tracing::debug!(target: "roomkit::turns", "the kit is shut down: a turn publishes nothing");
            self.stop();
        }
    }
}

/// Publishes what `publisher` holds, in order, until it holds nothing.
async fn drain(
    publisher: Arc<Publisher>,
    kit: Arc<dyn TurnKit>,
    (room, channel): (RoomId, ChannelId),
    mark: Timestamp,
) {
    let mut draining = Draining {
        publisher: Arc::clone(&publisher),
        finished: false,
    };
    while let Some(mut event) = draining.next() {
        // Of the turn's own room, whichever room holds its id now (RFC
        // §8.4, §17.2).
        event.room_created_at = Some(mark);
        if !kit.reaches_room(&room, &channel).await {
            tracing::debug!(target: "roomkit::turns", room_id = %room, "its answer does not reach the room: an ephemeral is not published");
            continue;
        }
        if let Some(realtime) = kit.realtime()
            && let Err(error) = realtime.publish_to_room(&room, event).await
        {
            tracing::error!(target: "roomkit::turns", room_id = %room, %error, "publishing a turn notification failed");
        }
    }
}

#[cfg(test)]
mod tests;
