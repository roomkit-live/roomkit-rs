//! The kit's executor reads its store and publishes to the kit it belongs to.
use super::{RoomKitError, TurnFuture, WiringEnv};
use crate::kit::Inner;
use crate::model::{ChannelId, Metadata, RoomContext, RoomEvent, RoomId, Timestamp};
use crate::realtime::RealtimeBackend;
use crate::turn::TurnKit;
use crate::{ConversationStore, FrameworkEvent};
use std::sync::{Arc, Weak};

pub(crate) struct KitWiringEnv<S>(pub(crate) Weak<Inner<S>>);

impl<S: ConversationStore> WiringEnv for KitWiringEnv<S> {
    fn context<'a>(
        &'a self,
        room: &'a RoomId,
        _carrying: Option<&'a RoomContext>,
    ) -> TurnFuture<'a, Result<RoomContext, RoomKitError>> {
        Box::pin(async move {
            let kit = self.0.upgrade().ok_or(RoomKitError::KitClosed)?;
            crate::driver::load_context(&*kit.store, room, kit.config().recent_events())
                .await?
                .ok_or_else(|| RoomKitError::RoomNotFound(room.clone()))
        })
    }
    fn tool_rows<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        limit: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async move {
            let inner = self.0.upgrade().ok_or(RoomKitError::KitClosed)?;
            Ok(inner
                .store
                .list_events(
                    room,
                    crate::store::ListEventsOptions {
                        limit,
                        event_filter: Some(crate::history::tool_rows(channel)),
                        newest_first: true,
                        ..Default::default()
                    },
                )
                .await?)
        })
    }
    fn history_rows<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        after: Option<u64>,
        limit: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async move {
            let inner = self.0.upgrade().ok_or(RoomKitError::KitClosed)?;
            Ok(inner
                .store
                .list_events(
                    room,
                    crate::store::ListEventsOptions {
                        limit,
                        event_filter: Some(crate::history::history_rows(channel)),
                        after_index: after,
                        ..Default::default()
                    },
                )
                .await?)
        })
    }
    fn message_before<'a>(
        &'a self,
        room: &'a RoomId,
        index: u64,
    ) -> TurnFuture<'a, Result<Option<u64>, RoomKitError>> {
        Box::pin(async move {
            let inner = self.0.upgrade().ok_or(RoomKitError::KitClosed)?;
            Ok(crate::history::message_before(&*inner.store, room, index).await?)
        })
    }
    fn realtime(&self) -> Option<Arc<dyn RealtimeBackend>> {
        self.0
            .upgrade()
            .filter(|kit| kit.open().is_ok())
            .map(|kit| Arc::clone(&kit.realtime))
    }
    fn emit(&self, event: FrameworkEvent) -> TurnFuture<'_, ()> {
        Box::pin(async move {
            if let Some(kit) = self.0.upgrade() {
                let floor = crate::context::ExecutionContext::current().turn_depth();
                if let Some(done) = kit.events.emit_at(&kit.tasks, event, floor) {
                    done.wait().await;
                }
            }
        })
    }
    fn can_publish_direct<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        created_at: Timestamp,
    ) -> TurnFuture<'a, bool> {
        Box::pin(async move {
            let Some(inner) = self.0.upgrade() else {
                return false;
            };
            let Ok(Some(binding)) = inner.store.get_binding(room, channel).await else {
                return false;
            };
            binding.can_write()
                && binding.visibility == crate::model::Visibility::All
                && inner
                    .store
                    .get_room(room)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|current| current.created_at == created_at)
        })
    }
    fn room_mark<'a>(&'a self, room: &'a RoomId) -> TurnFuture<'a, Option<Timestamp>> {
        Box::pin(async move {
            let kit = self.0.upgrade()?;
            match kit.turns.room_mark(room) {
                Some(mark) => Some(mark),
                None => kit
                    .store
                    .get_room(room)
                    .await
                    .ok()
                    .flatten()
                    .map(|room| room.created_at),
            }
        })
    }
    fn room_tasks<'a>(
        &'a self,
        room: &'a RoomId,
    ) -> TurnFuture<'a, Result<Vec<Metadata>, RoomKitError>> {
        Box::pin(async move { Ok(self.0.room_tasks(room.clone()).await) })
    }
}
