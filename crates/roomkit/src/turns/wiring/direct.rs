//! A direct prompt has a trigger, but no owner link to enforce its audience.
use super::{TurnWiring, WiringEnv};
use crate::model::{ChannelBinding, ChannelId, Room, RoomEvent, RoomId, Timestamp, Visibility};
use crate::realtime::EphemeralEvent;

tokio::task_local! {
    static PROMPT: Option<DirectPrompt>;
}

/// The audience of a directly called channel, captured by its prompt and child work.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct DirectPrompt {
    room: RoomId,
    channel: ChannelId,
    created_at: Timestamp,
    public: bool,
}

impl DirectPrompt {
    /// Scope a channel invocation without taking the place of an admitted turn.
    pub fn of(
        room: &Room,
        event: &RoomEvent,
        channel: &ChannelId,
        binding: &ChannelBinding,
    ) -> Self {
        Self {
            room: room.id.clone(),
            channel: channel.clone(),
            created_at: room.created_at,
            public: room.id == event.room_id
                && room.id == binding.room_id
                && channel == &binding.channel_id
                && event
                    .response_visibility
                    .as_ref()
                    .is_none_or(|scope| *scope == Visibility::All),
        }
    }

    /// Scope this prompt during its preparation and reads.
    pub fn run<F: Future>(self, work: F) -> impl Future<Output = F::Output> {
        PROMPT.scope(crate::turn::turn_in_scope().is_none().then_some(self), work)
    }

    pub(crate) fn current() -> Option<Self> {
        PROMPT.try_with(Clone::clone).ok().flatten()
    }

    pub(crate) fn keep<F: Future>(work: F) -> impl Future<Output = F::Output> {
        PROMPT.scope(Self::current(), work)
    }

    pub(super) async fn authorize(
        &self,
        env: &dyn WiringEnv,
        topic: &str,
        event: &mut EphemeralEvent,
    ) -> bool {
        if !self.public
            || topic.strip_prefix("room:") != Some(self.room.as_str())
            || event.room_id != self.room
            || event
                .channel_id
                .as_ref()
                .is_some_and(|id| *id != self.channel)
            || event
                .room_created_at
                .is_some_and(|mark| mark != self.created_at)
            || !env
                .can_publish_direct(&self.room, &self.channel, self.created_at)
                .await
        {
            return false;
        }
        event.room_created_at = Some(self.created_at);
        true
    }

    pub(crate) async fn publish(&self, wiring: &TurnWiring, event: EphemeralEvent) {
        if let Err(error) = wiring
            .realtime()
            .outside_room(&self.room, event, Some(self))
            .await
        {
            tracing::debug!(room = %self.room, %error, "could not publish a direct prompt notification");
        }
    }
}
