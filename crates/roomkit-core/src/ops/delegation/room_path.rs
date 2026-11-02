//! The answers of a delegated turn committed as a room's, in a child room
//! a transport is shared into (RFC §23.3 step 5): Python's
//! `_deliver_answer`, its answer the one the room kept of the agent and its
//! stream's failure the agent's whose turn it ended (`_stream_failed_by`).

use std::sync::Arc;

use super::collected::{Buffered, Streamed};
use super::nested::Nested;
use super::{DelegatedAnswer, DelegatedTurn, DelegationError};
use crate::model::{ChannelBinding, ChannelCategory, EventType, Metadata};
use crate::ops::inbound::{Inbound, InboundResult, TurnResponses};
use crate::ops::{Effect, Environment};

impl DelegatedTurn {
    /// The answers committed as a room's responses, the streams read by
    /// the room's reader (RFC §23.3 step 5).
    pub(super) fn take_the_room_path(&mut self, buffered: Vec<Buffered>, streamed: Vec<Streamed>) {
        let (Some(event), Some(context)) = (self.committed.clone(), self.asked_context.as_deref())
        else {
            return;
        };
        self.streamers = streamed
            .iter()
            .map(|(_, _, binding)| binding.channel_id.clone())
            .collect();
        let responses = TurnResponses {
            event,
            context: context.clone(),
            buffered: buffered
                .into_iter()
                .map(|(_, binding, rows)| (binding, rows))
                .collect(),
            streamed: streamed
                .into_iter()
                .map(|(turn, _, binding)| (turn, binding))
                .collect(),
        };
        let inbound = Inbound::answering(Arc::clone(&self.config), responses);
        self.room_path = Some(Nested::new(inbound));
    }

    /// Advances the room's path; once it is over, each agent's rows are
    /// those the room kept of its answers, and a stream's failure is the
    /// failure of the agent whose turn it ended (RFC §23.3 step 6).
    pub(super) fn advance_room_path(
        &mut self,
        environment: &dyn Environment,
        effects: &mut Vec<Effect>,
    ) {
        let Some(path) = self.room_path.as_mut() else {
            return;
        };
        let over = path.poll(environment, &mut self.next_id, effects, &mut self.notices);
        let (settled, delivered) = path.take_entries();
        self.settled.extend(settled);
        self.delivered.extend(delivered);
        let Some(over) = over else {
            return;
        };
        self.room_path = None;
        match over {
            Ok(result) => self.kept_by_the_room(result),
            Err(error) => self.failed = Some(DelegationError::Room(error)),
        }
    }

    /// Each agent's rows, the answers the room kept of it, in the order
    /// stored; the cascade's error, a stream's failure, the agent's whose
    /// turn it ended, or logged when that cannot be told.
    fn kept_by_the_room(&mut self, result: InboundResult) {
        for answer in &mut self.answers {
            answer.rows = result
                .response_events
                .iter()
                .filter(|row| row.source.channel_id == answer.channel_id)
                .cloned()
                .collect();
        }
        let Some(error) = result.error else {
            return;
        };
        match self.stream_failed_by() {
            Some(answer) if answer.error.is_none() => answer.error = Some(error),
            Some(_) => {}
            None => {
                tracing::warn!(target: "roomkit::tasks", room_id = %self.room_id, error = %error.message, "a stream of the delegated room failed");
            }
        }
    }

    /// The answer whose turn the room's stream failure ended, among the
    /// agents that streamed: the only one's, else the task's agent's when
    /// it streamed and its turn names no end but an error's; `None` when
    /// it cannot be told (Python's `_stream_failed_by`).
    fn stream_failed_by(&mut self) -> Option<&mut DelegatedAnswer> {
        let failed_by = if let [only] = self.streamers.as_slice() {
            only.clone()
        } else {
            let agent = self
                .agent_id
                .clone()
                .filter(|agent| self.streamers.contains(agent))?;
            let answer = self
                .answers
                .iter()
                .find(|answer| answer.channel_id == agent)?;
            if !matches!(recorded_end(answer), None | Some("error")) {
                return None;
            }
            agent
        };
        self.answers
            .iter_mut()
            .find(|answer| answer.channel_id == failed_by)
    }
}

/// Whether the child room holds a transport, which only sharing puts there
/// (Python's `_shares_a_transport`).
pub(super) fn shares_a_transport(bindings: &[ChannelBinding]) -> bool {
    bindings
        .iter()
        .any(|binding| binding.category == ChannelCategory::Transport)
}

/// How an answer's turn ended: as its record names it, else as its last
/// message does (RFC §6.4).
fn recorded_end(answer: &DelegatedAnswer) -> Option<&str> {
    fn named(record: &Metadata) -> Option<&str> {
        record.get(LOOP_END_REASON).and_then(|end| end.as_str())
    }
    named(&answer.record).or_else(|| {
        answer
            .rows
            .iter()
            .rev()
            .filter(|row| row.r#type == EventType::Message)
            .find_map(|row| named(&row.metadata))
    })
}

/// The key a turn's record names how its loop ended under (RFC §6.4).
const LOOP_END_REASON: &str = "loop_end_reason";
