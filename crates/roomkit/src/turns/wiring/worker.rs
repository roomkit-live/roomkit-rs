//! A standalone executor reads only what its room's owner made available.
use super::{RoomKitError, TurnFuture, TurnRequest, WiringEnv};
use crate::model::{ChannelId, EventType, RoomContext, RoomEvent, RoomId};
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Mutex;

const ROOMS_KEPT: usize = 256;

#[derive(Default)]
pub(crate) struct WorkerWiringEnv {
    rooms: Mutex<VecDeque<(RoomContext, BTreeSet<ChannelId>)>>,
}

fn with_rows(
    room: &RoomId,
    channel: &ChannelId,
    select: impl FnOnce(&[RoomEvent]) -> Vec<RoomEvent>,
) -> Vec<RoomEvent> {
    crate::turn::turn_in_scope()
        .filter(|host| host.room_id() == room && host.channel() == channel)
        .map_or_else(Vec::new, |host| select(host.channel_rows()))
}

impl WiringEnv for WorkerWiringEnv {
    fn turn_started(&self, request: &TurnRequest) {
        let mut rooms = super::super::lock(&self.rooms);
        let mut members = rooms
            .iter()
            .position(|(context, _)| context.room.id == request.room_id)
            .and_then(|i| rooms.remove(i))
            .map(|(_, channels)| channels)
            .unwrap_or_default();
        members.insert(request.channel_id.clone());
        rooms.push_back((request.context.clone(), members));
        if rooms.len() > ROOMS_KEPT {
            rooms.pop_front();
        }
    }
    fn room_left(&self, room: &RoomId, channel: &ChannelId) {
        let mut rooms = super::super::lock(&self.rooms);
        rooms.retain_mut(|(context, members)| {
            if context.room.id == *room {
                members.remove(channel);
                return !members.is_empty();
            }
            true
        });
    }
    fn context<'a>(
        &'a self,
        room: &'a RoomId,
        _: Option<&'a RoomContext>,
    ) -> TurnFuture<'a, Result<RoomContext, RoomKitError>> {
        Box::pin(async move {
            if let Some(host) = crate::turn::turn_in_scope().filter(|host| host.room_id() == room) {
                if host.open() {
                    return host.remote_hook_room().await;
                }
                return Ok(host.initial_context().clone());
            }
            super::super::lock(&self.rooms)
                .iter()
                .find(|(ctx, _)| ctx.room.id == *room)
                .map(|(ctx, _)| ctx.clone())
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
            Ok(with_rows(room, channel, |rows| {
                let mut selected: Vec<_> = rows
                    .iter()
                    .filter(|e| {
                        matches!(e.r#type, EventType::ToolCallStart | EventType::ToolCallEnd)
                    })
                    .collect();
                let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                // Python's rows[-limit:] reads every row when limit is zero.
                if limit != 0 {
                    selected.drain(..selected.len().saturating_sub(limit));
                }
                selected.into_iter().cloned().collect()
            }))
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
            Ok(with_rows(room, channel, |rows| {
                rows.iter()
                    .filter(|e| after.is_none_or(|i| e.index > i))
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                    .cloned()
                    .collect()
            }))
        })
    }
    fn message_before<'a>(
        &'a self,
        _: &'a RoomId,
        _: u64,
    ) -> TurnFuture<'a, Result<Option<u64>, RoomKitError>> {
        Box::pin(async { Ok(None) })
    }
}
