//! Delivery of the two kinds of publication allowed from a turn, in arrival order.
use super::OwnerEvents;
use crate::RoomKitError;
use crate::turn::TurnKit;
use crate::turns::context::TurnContext;
use crate::turns::owner::{Facts, emitted_by_its_turn};
use crate::turns::records::AnswerScope;
use crate::turns::wire::Frame;
use roomkit_core::model::{ChannelId, RoomId, Timestamp};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(in crate::turns) struct Scope {
    pub(in crate::turns) room: RoomId,
    pub(in crate::turns) channel: ChannelId,
    pub(in crate::turns) answer: AnswerScope,
    pub(in crate::turns) mark: Option<Timestamp>,
}
impl Scope {
    async fn publish(
        &self,
        events: &dyn OwnerEvents,
        frame: Frame,
        traced: bool,
    ) -> Result<(), RoomKitError> {
        match frame {
            Frame::FrameworkEvent {
                event_type,
                room_id,
                channel_id,
                event_id,
                data,
            } if emitted_by_its_turn(
                &self.room,
                &self.channel,
                &event_type,
                room_id.as_deref(),
                channel_id.as_deref(),
            ) =>
            {
                events
                    .emit(
                        &event_type,
                        Some(self.room.as_str()),
                        Some(self.channel.as_str()),
                        event_id.as_deref(),
                        data,
                    )
                    .await?;
            }
            Frame::Ephemeral {
                channel: topic,
                event,
            } => {
                if let Some(mut event) = crate::turns::outside::ephemeral(
                    &self.room,
                    &self.channel,
                    self.answer.unscoped,
                    traced,
                    &topic,
                    event,
                ) && events.reaches_room(&self.room, &self.channel).await?
                {
                    event.room_created_at = self.mark;
                    events.publish(&topic, event).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(in crate::turns) async fn deliver(
        &self,
        events: &dyn OwnerEvents,
        frame: Frame,
        traced: bool,
    ) {
        if let Err(error) = self.publish(events, frame, traced).await {
            tracing::warn!(%error,room_id=%self.room,channel_id=%self.channel,"failed to publish a turn event");
        }
    }
}

type Queued = (Frame, bool);
#[derive(Default)]
pub(in crate::turns) struct Publications(Option<mpsc::UnboundedSender<Queued>>);
impl Publications {
    pub(in crate::turns) fn queue(
        &mut self,
        kit: &dyn TurnKit,
        facts: &Facts,
        context: TurnContext,
        frame: Frame,
        traced: bool,
    ) {
        let Some((client, _)) = &facts.client else {
            return;
        };
        let Some(events) = &client.events else {
            return;
        };
        let sending = self.0.get_or_insert_with(|| {
            let events = Arc::clone(events);
            let (send, mut receive) = mpsc::unbounded_channel();
            let scope = Scope {
                room: facts.room_id.clone(),
                channel: facts.channel_id.clone(),
                mark: Some(facts.room.created_at),
                answer: AnswerScope {
                    depth: facts.response_depth,
                    unscoped: facts.answer_unscoped,
                },
            };
            kit.spawn_publisher(Box::pin(context.at_least(scope.answer.depth).scope(
                async move {
                    while let Some((frame, traced)) = receive.recv().await {
                        scope.deliver(events.as_ref(), frame, traced).await;
                    }
                },
            )));
            send
        });
        drop(sending.send((frame, traced)));
    }
}
