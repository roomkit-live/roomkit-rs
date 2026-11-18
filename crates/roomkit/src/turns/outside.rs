//! Owner messages after a turn ended (RFC §10.6.3 and §10.6.5).
//! Calls require the last turn itself. A publication can belong to older
//! work of the executor that currently holds that room and channel.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomId, Timestamp};
use serde_json::Value;

use super::Turns;
use super::context::TurnContext;
use super::owner;
use super::records::AnswerScope;
use super::wire::{Frame, OwnerMessage, OwnerMessageEnvelope, RoomCallResult};
use crate::realtime::{EphemeralEvent, EphemeralEventType};
use crate::turn::TurnKit;

impl Turns {
    pub(crate) async fn receive_owner_message(
        &self,
        kit: Arc<dyn TurnKit>,
        envelope: OwnerMessageEnvelope,
    ) -> Option<RoomCallResult> {
        match envelope.message {
            OwnerMessage::RoomCall(call) => {
                let (channel, room) = (call.channel_id.clone(), call.room_id.clone());
                let last = || owner::last_turn(&self.records(), &channel, &room);
                let answer = owner::answer_room_call(&kit, self.client.as_deref(), last, call);
                Some(TurnContext::default().scope(answer).await)
            }
            OwnerMessage::RoomPublication(super::wire::RoomPublication {
                channel_id,
                room_id,
                executor_id,
                turn_id,
                frame,
            }) => {
                let scope = {
                    let records = self.records();
                    records
                        .get(&channel_id, &room_id)
                        .filter(|held| held.executor_id.as_deref() == Some(executor_id.as_str()))
                        .map(|held| (held.scope(&turn_id), records.room_mark(&room_id)))
                };
                if let Some((scope, mark)) = scope {
                    if let Some(client) = &self.client {
                        if let Some(events) = &client.events {
                            let held = super::client::publications::Scope {
                                room: room_id,
                                channel: channel_id,
                                answer: scope,
                                mark,
                            };
                            TurnContext::default()
                                .at_least(scope.depth)
                                .scope(held.deliver(events.as_ref(), (*frame).into(), false))
                                .await;
                        }
                        return None;
                    }
                    let publish =
                        publish(&*kit, &room_id, &channel_id, scope, mark, (*frame).into());
                    TurnContext::default()
                        .at_least(scope.depth)
                        .scope(publish)
                        .await;
                } else {
                    tracing::warn!(target: "roomkit::turns", %room_id, %channel_id, %turn_id, %executor_id, "dropping publication from an executor that does not hold the room");
                }
                None
            }
        }
    }
}

async fn publish(
    kit: &dyn TurnKit,
    room: &RoomId,
    channel: &ChannelId,
    scope: AnswerScope,
    mark: Option<Timestamp>,
    frame: Frame,
) {
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "PublicationFrame already validated both allowed frame kinds"
    )]
    match frame {
        Frame::FrameworkEvent {
            event_type,
            room_id,
            channel_id,
            event_id,
            data,
        } => {
            owner::publish_for_room(
                kit,
                (room, channel, scope.depth),
                event_type,
                (room_id.as_deref(), channel_id.as_deref(), event_id),
                data,
            );
        }
        Frame::Ephemeral {
            channel: topic,
            event,
        } => {
            let Some(mut event) = ephemeral(room, channel, scope.unscoped, false, &topic, event)
            else {
                return;
            };
            if !kit.reaches_room(room, channel).await {
                return;
            }
            event.room_created_at = mark;
            if let Some(backend) = kit.realtime()
                && let Err(error) = backend.publish(&topic, event).await
            {
                tracing::warn!(target: "roomkit::turns", %room, %channel, %error, "failed to publish a turn event");
            }
        }
        _ => unreachable!("PublicationFrame admits only events"),
    }
}

/// Admit an ephemeral from either owner entry point to its own room and channel.
pub(super) fn ephemeral(
    room: &RoomId,
    channel: &ChannelId,
    unscoped: bool,
    traced: bool,
    topic: &str,
    event: Value,
) -> Option<EphemeralEvent> {
    if !unscoped || topic != format!("room:{room}") {
        return None;
    }
    let Value::Object(fields) = event else {
        return None;
    };
    let Ok(event) = EphemeralEvent::from_dict(fields) else {
        tracing::warn!(target: "roomkit::turns", %room, %channel, "dropping an invalid ephemeral publication");
        return None;
    };
    ((traced || event.r#type != EphemeralEventType::TextDelta)
        && &event.room_id == room
        && event
            .channel_id
            .as_ref()
            .is_none_or(|named| named == channel))
    .then_some(event)
}
