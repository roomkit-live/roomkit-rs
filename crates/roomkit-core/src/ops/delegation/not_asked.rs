//! The agents a delegated turn does not ask (RFC §8.3): an answer that
//! would reach the chain's depth limit is not asked for, as a room's
//! delivery set leaves it out, and the child room keeps the agent's record,
//! stored `BLOCKED`, and its observation, `chain_depth_exceeded` emitted
//! there. Python's router leaves such an agent out of the delivery plan and
//! commits its record (`_commit_blocked_events`).

use std::sync::Arc;

use super::DelegatedTurn;
use super::nested::Nested;
use crate::model::{ChannelBinding, RoomEvent};
use crate::ops::events::CommitRecord;
use crate::ops::protocol::{Effect, Notice, StoreRequest};
use crate::ops::{ChainDepthData, Environment, FrameworkEvent};
use crate::rules::chain_depth;

impl DelegatedTurn {
    /// Whether `binding`'s answer to `message` would reach the chain's
    /// depth limit: it is then not asked, its record queued for its commit.
    pub(super) fn not_asked(
        &mut self,
        binding: &ChannelBinding,
        message: &RoomEvent,
        environment: &dyn Environment,
    ) -> bool {
        if !chain_depth::reaches_limit(message.chain_depth, self.config.max_chain_depth) {
            return false;
        }
        let record = chain_depth::not_asked_record(
            environment.new_event_id(),
            message,
            binding,
            environment.now(),
        );
        self.not_asked.push_back((binding.clone(), record));
        true
    }

    /// Commits the records of the agents not asked, one at a time; once
    /// one is stored, its observation is stored and `chain_depth_exceeded`
    /// emitted.
    pub(super) fn advance_not_asked(
        &mut self,
        environment: &dyn Environment,
        effects: &mut Vec<Effect>,
    ) {
        loop {
            if let Some((commit, _)) = self.recording.as_mut() {
                let Some(stored) =
                    commit.poll(environment, &mut self.next_id, effects, &mut self.notices)
                else {
                    return;
                };
                let Some((_, agent)) = self.recording.take() else {
                    return;
                };
                match stored {
                    Ok(record) => self.not_asked_stored(&record, &agent, environment, effects),
                    Err(error) => {
                        tracing::warn!(target: "roomkit::tasks", room_id = %self.room_id, %error, "the record of an agent not asked was not stored");
                    }
                }
                continue;
            }
            let Some((agent, record)) = self.not_asked.pop_front() else {
                return;
            };
            let commit =
                CommitRecord::new(Arc::clone(&self.config), self.room_id.clone(), record, None);
            self.recording = Some((Nested::new(commit), agent));
        }
    }

    /// `agent`'s record is stored: its observation is stored, and
    /// `chain_depth_exceeded` emitted (RFC §8.2, §8.3).
    fn not_asked_stored(
        &mut self,
        record: &RoomEvent,
        agent: &ChannelBinding,
        environment: &dyn Environment,
        effects: &mut Vec<Effect>,
    ) {
        let Some(description) = self.committed.clone() else {
            return;
        };
        let max_chain_depth = self.config.max_chain_depth;
        let observation = chain_depth::not_asked_observation(
            environment.new_observation_id(),
            &description,
            agent,
            max_chain_depth,
            environment.now(),
        );
        let write = self.issue(StoreRequest::PersistSideEffects {
            room_id: self.room_id.clone(),
            tasks: Vec::new(),
            observations: vec![observation],
        });
        self.persisting.push(write.id);
        effects.push(write);
        let exceeded = FrameworkEvent::chain_depth_exceeded(
            record.room_id.clone(),
            record.id.clone(),
            agent.channel_id.clone(),
            ChainDepthData {
                chain_depth: record.chain_depth,
                max_chain_depth,
            },
        );
        self.notices.push(Notice::FrameworkEvent(exceeded));
    }
}
