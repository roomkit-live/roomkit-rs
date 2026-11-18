//! Registration wiring for an adapter's notifications outside a turn.

use crate::realtime::EphemeralEvent;
use crate::turns::TurnWiring;
#[cfg(test)]
use crate::{RoomKit, store::ConversationStore};
use std::sync::{Arc, Mutex, PoisonError};

/// Internal cross-crate bridge; registration holds its kit weakly.
#[derive(Clone, Default)]
pub struct ChannelPublication {
    publisher: Arc<Mutex<Option<TurnWiring>>>,
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
        } else if let Some(wiring) = self.wiring() {
            wiring.publish_to_room(event).await;
        }
    }

    /// Publishes a known idle session's notification without borrowing a turn.
    pub async fn publish_outside(&self, event: EphemeralEvent) {
        if let Some(wiring) = self.wiring() {
            wiring.publish_outside(event).await;
        }
    }

    fn wiring(&self) -> Option<TurnWiring> {
        self.publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn wire<S: ConversationStore>(&self, kit: &RoomKit<S>) {
        self.wire_to(kit.turn_executor().wiring());
    }

    pub(crate) fn wire_to(&self, wiring: TurnWiring) {
        *self
            .publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(wiring);
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

    // Python: tests/test_room_org_scoping.py::TestScopedEphemerals::test_a_room_gone_s_channel_publication_reaches_none_of_the_room_created_again.
    // A channel's publication outside a turn, made after its channel call in
    // acme's room while another process deleted the room and created it
    // again for globex, is acme's room's: globex's subscription does not
    // read it (RFC §8.4, §17.2).
    #[tokio::test]
    async fn a_room_gone_s_channel_publication_reaches_none_of_the_room_created_again() {
        use crate::model::{ChannelId, ChannelType};
        use crate::store::InMemoryStore;
        use crate::testing::{RecordingTransport, RecordingTransportOptions};
        use crate::{
            AttachChannelOptions, ChannelCallOptions, CreateRoomOptions, SubscribeRoomOptions,
        };
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
        let (room, agent) = (
            RoomId::new("shared-id").unwrap(),
            ChannelId::new("agent").unwrap(),
        );
        let scoped = |organization: &str| CreateRoomOptions {
            room_id: Some(room.clone()),
            organization_id: Some(organization.to_owned()),
            ..CreateRoomOptions::default()
        };
        acme.register_channel(RecordingTransport::new(
            agent.clone(),
            ChannelType::AI,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        acme.create_room(scoped("acme")).await.unwrap();
        acme.attach_channel(&room, &agent, AttachChannelOptions::default())
            .await
            .unwrap();
        // The call leaves acme's room as the room of the kit's last call.
        drop(
            acme.channel_call(&agent, &room, "nope", ChannelCallOptions::default())
                .await,
        );
        store.delete_room(&room).await.unwrap();
        globex.create_room(scoped("globex")).await.unwrap();
        let (read, mut globex_read) = tokio::sync::mpsc::unbounded_channel();
        let options = SubscribeRoomOptions {
            organization_id: Some("globex".to_owned()),
        };
        globex
            .subscribe_room(
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
        publication.wire(acme);
        publication
            .publish(EphemeralEvent::new(
                room.clone(),
                EphemeralEventType::TypingStart,
                "acme-agent",
            ))
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), reaching.recv())
            .await
            .unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(globex_read.try_recv().is_err());
        for kit in &kits {
            kit.close().await.unwrap();
        }
    }
}
