//! Regenerated answers are separate queued turns (RFC §19.7.5 rule 7).

use crate::context::Section;
use roomkit_core::model::{ChannelId, ErrorRecord, RoomContext, RoomEvent};
use roomkit_core::ops::InboundResult;
use roomkit_core::rules::discussion;

use super::SpeakQueueChange;
use super::queue::Ask;
use super::room::DiscussionRoom;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> DiscussionRoom<S> {
    pub(super) async fn regenerate(
        &self,
        event: RoomEvent,
        context: RoomContext,
        floor: u32,
    ) -> Result<InboundResult, RoomKitError> {
        let kit = self.kit()?;
        // Selection released its lock: an ended discussion must be read
        // from storage, not from this process's cached view (rule 15).
        if kit
            .store()
            .get_room(&event.room_id)
            .await?
            .is_some_and(|room| discussion::is_over(&room.metadata))
        {
            return Ok(InboundResult {
                blocked: true,
                reason: Some("discussion_over".to_owned()),
                ..InboundResult::default()
            });
        }
        let agents = self.answerers(&event, &context);
        if agents.is_empty() {
            return Ok(InboundResult::default());
        }
        let error = self.queue_answerers(&event, agents, floor).await?;
        Ok(InboundResult {
            event: Some(event),
            error,
            ..InboundResult::default()
        })
    }

    async fn queue_answerers(
        &self,
        event: &RoomEvent,
        agents: Vec<ChannelId>,
        floor: u32,
    ) -> Result<Option<ErrorRecord>, RoomKitError> {
        let kit = self.kit()?;
        let ask = Ask {
            event_id: event.id.to_string(),
            depth: event.chain_depth.max(floor),
            asker: String::new(),
            index: event.index,
            person: true,
            named: false,
        };
        let section = Section::enter(&kit.inner.locks, &event.room_id).await?;
        let queued = section
            .scope(self.edit(&kit, |state, live| {
                if state.over || live.closed || !live.stored {
                    return Vec::new();
                }
                let config = self.config();
                agents
                    .into_iter()
                    .filter(|agent| {
                        config.agents.contains(agent)
                            && state.queue_regeneration(agent, ask.clone())
                    })
                    .collect::<Vec<_>>()
            }))
            .await;
        let release_error = section.release().await.err().map(|error| {
            if let RoomKitError::Lock(record) = error {
                return record;
            }
            ErrorRecord::new(error.class_name(), error.to_string())
        });
        let queued = queued?;
        if !queued.is_empty() {
            self.fire(SpeakQueueChange::Queued, queued, Some(event.id.to_string()));
            self.rouse();
        }
        Ok(release_error)
    }

    fn answerers(&self, event: &RoomEvent, context: &RoomContext) -> Vec<ChannelId> {
        let mut agents = Vec::new();
        for answer in &context.recent_events {
            if answer.responds_to.as_ref() == Some(&event.id)
                && answer.blocked_by.as_deref() != Some("event_chain_depth_limit")
                && !agents.contains(&answer.source.channel_id)
            {
                agents.push(answer.source.channel_id.clone());
            }
        }
        if !agents.is_empty() {
            return agents;
        }
        let config = self.config();
        match &event.addressed_to {
            Some(addressed) => addressed
                .iter()
                .filter(|agent| config.agents.contains(agent))
                .cloned()
                .collect(),
            None if config.addressed_only => Vec::new(),
            None => self.everyone(),
        }
    }
}
