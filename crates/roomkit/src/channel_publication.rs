//! Registration wiring for an adapter's notifications outside a turn.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use crate::RoomKit;
use crate::realtime::EphemeralEvent;
use crate::store::ConversationStore;

type Publish =
    Arc<dyn Fn(EphemeralEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Internal cross-crate bridge; registration holds its kit weakly.
#[derive(Clone, Default)]
pub struct ChannelPublication {
    publisher: Arc<Mutex<Option<Publish>>>,
}

impl std::fmt::Debug for ChannelPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelPublication").finish_non_exhaustive()
    }
}

impl ChannelPublication {
    /// Publishes from the current turn, preserving even an unreachable owner.
    pub async fn publish(&self, event: EphemeralEvent) {
        if let Some(host) = crate::turn::turn_in_scope() {
            host.publish_from_channel(event);
        } else {
            self.publish_outside(event).await;
        }
    }

    /// Publishes a known idle session's notification without borrowing a turn.
    pub async fn publish_outside(&self, event: EphemeralEvent) {
        // Only a turn's trace owner may publish its answer's text.
        if event.r#type == crate::realtime::EphemeralEventType::TextDelta {
            return;
        }
        let publisher = self
            .publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(publish) = publisher {
            publish(event).await;
        }
    }

    pub(crate) fn wire<S: ConversationStore>(&self, kit: &RoomKit<S>) {
        let kit = kit.downgrade();
        let publish: Publish = Arc::new(move |event| {
            let kit = kit.upgrade();
            Box::pin(async move {
                let Some(kit) = kit.filter(|kit| kit.inner.open().is_ok()) else {
                    return;
                };
                let room = event.room_id.clone();
                kit.inner.realtime.publish_to_room(&room, event).await;
            })
        });
        *self
            .publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(publish);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RoomKitOptions;
    use crate::model::RoomId;
    use crate::realtime::{EphemeralEventType, InMemoryRealtime, RealtimeBackend};

    // Python: tests/test_turn_ephemerals_scope.py::test_an_answers_text_published_outside_a_turn_is_dropped.
    #[tokio::test]
    async fn an_answers_text_published_outside_a_turn_is_dropped() {
        let realtime = Arc::new(InMemoryRealtime::default());
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<InMemoryRealtime>::clone(&realtime)),
            ..Default::default()
        });
        let publication = ChannelPublication::default();
        publication.wire(&kit);
        let room = RoomId::new("room").unwrap();
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        realtime
            .subscribe_to_room(
                &room,
                Arc::new(move |event| {
                    let sent = sent.clone();
                    Box::pin(async move {
                        sent.send(event.r#type).unwrap();
                    })
                }),
            )
            .await;
        publication
            .publish(EphemeralEvent::new(
                room.clone(),
                EphemeralEventType::TextDelta,
                "agent",
            ))
            .await;
        publication
            .publish(EphemeralEvent::new(
                room,
                EphemeralEventType::Custom,
                "agent",
            ))
            .await;
        // One subscription preserves publication order; CUSTOM is the barrier.
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), received.recv())
                .await
                .unwrap(),
            Some(EphemeralEventType::Custom)
        );
        kit.close().await.unwrap();
    }
}
