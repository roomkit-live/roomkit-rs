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
    /// The last publication outside a turn, which the next one follows.
    last: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
    #[expect(
        clippy::unused_async,
        reason = "the adapters await it, as they awaited the publication it hands over"
    )]
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
        let Some(publish) = publisher else {
            return;
        };
        // Handed over in order and not waited for: marking it reads the
        // store (RFC §8.4, §17.2), which the session's reader does not wait
        // for, as it waited for no read before.
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = last.take();
        *last = Some(tokio::spawn(async move {
            if let Some(previous) = previous {
                drop(previous.await);
            }
            publish(event).await;
        }));
    }

    pub(crate) fn wire<S: ConversationStore>(&self, kit: &RoomKit<S>) {
        let kit = kit.downgrade();
        let publish: Publish = Arc::new(move |mut event| {
            let kit = kit.upgrade();
            Box::pin(async move {
                let Some(kit) = kit.filter(|kit| kit.inner.open().is_ok()) else {
                    return;
                };
                let room = event.room_id.clone();
                if event.room_created_at.is_none() {
                    // Of the room that holds the id now: a subscription of a
                    // room gone, created again under its id, does not read it
                    // (RFC §8.4, §17.2).
                    event.room_created_at = kit
                        .store()
                        .get_room(&room)
                        .await
                        .ok()
                        .flatten()
                        .map(|held| held.created_at);
                }
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

    // Python: tests/test_room_org_scoping.py::TestScopedEphemerals::test_a_channel_s_publication_outside_a_turn_reaches_no_room_gone.
    // A channel's publication outside a turn is of the room the store holds:
    // the subscription of a room deleted and created again by another
    // process does not read it (RFC §8.4, §17.2).
    #[tokio::test]
    async fn a_channel_s_publication_outside_a_turn_reaches_no_room_gone() {
        use crate::store::InMemoryStore;
        use crate::{CreateRoomOptions, SubscribeRoomOptions};
        let store = Arc::new(InMemoryStore::new());
        let realtime: Arc<dyn RealtimeBackend> = Arc::new(InMemoryRealtime::default());
        let kits: Vec<RoomKit> = (0..2)
            .map(|_| {
                RoomKit::new(RoomKitOptions {
                    store: Arc::clone(&store),
                    realtime: Some(Arc::clone(&realtime)),
                    ..Default::default()
                })
            })
            .collect();
        let (acme, globex) = (&kits[0], &kits[1]);
        let room = RoomId::new("shared-id").unwrap();
        let scoped = |organization: &str| CreateRoomOptions {
            room_id: Some(room.clone()),
            organization_id: Some(organization.to_owned()),
            ..CreateRoomOptions::default()
        };
        let (read, mut acme_read) = tokio::sync::mpsc::unbounded_channel();
        acme.create_room(scoped("acme")).await.unwrap();
        let options = SubscribeRoomOptions {
            organization_id: Some("acme".to_owned()),
        };
        acme.subscribe_room(
            &room,
            move |event: EphemeralEvent| {
                let read = read.clone();
                async move {
                    read.send(event.user_id).unwrap();
                }
            },
            options,
        )
        .await
        .unwrap();
        store.delete_room(&room).await.unwrap();
        globex.create_room(scoped("globex")).await.unwrap();
        let (reached, mut reaching) = tokio::sync::mpsc::unbounded_channel();
        realtime
            .subscribe_to_room(
                &room,
                Arc::new(move |_| {
                    let reached = reached.clone();
                    Box::pin(async move {
                        reached.send(()).unwrap();
                    })
                }),
            )
            .await;
        let publication = ChannelPublication::default();
        publication.wire(globex);
        publication
            .publish(EphemeralEvent::new(
                room.clone(),
                EphemeralEventType::TypingStart,
                "globex-agent",
            ))
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), reaching.recv())
            .await
            .unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(acme_read.try_recv().is_err());
        for kit in &kits {
            kit.close().await.unwrap();
        }
    }
}
