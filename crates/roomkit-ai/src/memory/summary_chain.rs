//! The summary a summarizing memory carries from one turn to the next
//! (RFC §20.3): what it last summarized of a room, so that a later read
//! starts from it rather than from the window alone. Python's
//! `roomkit.memory._summary_chain`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use roomkit::model::{ChannelId, EventId, RoomContext, RoomEvent, RoomId, Timestamp};
use sha2::{Digest as _, Sha256};
use tokio::time::Instant;

use super::MemoryResult;
use super::summary::{is_summary, summary_message};
use super::token_estimator::extract_event_text;

/// The summary of a room's turns, for the room created at `room`: the
/// turns it `covers`, each by its id and the fingerprint of what it said
/// when summarized, and the labels its lines named participants by
/// (`speakers`).
#[derive(Clone, Debug)]
pub(super) struct SummaryChain {
    summary: String,
    room: Timestamp,
    covers: Vec<(EventId, String)>,
    speakers: Vec<String>,
}

impl SummaryChain {
    /// The chain for `summary` of the `summarized` turns, each
    /// fingerprinted as the room's history in `context` holds it; `None`
    /// when one is not there, a turn the memory could never check again.
    pub(super) fn of(
        summary: String,
        summarized: &[RoomEvent],
        speakers: Vec<String>,
        context: &RoomContext,
    ) -> Option<Self> {
        let held: HashMap<&EventId, &RoomEvent> = context
            .recent_events
            .iter()
            .map(|event| (&event.id, event))
            .collect();
        let covers = summarized
            .iter()
            .map(|event| {
                let in_history = held.get(&event.id)?;
                Some((event.id.clone(), fingerprint(in_history)))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            summary,
            room: context.room.created_at,
            covers,
            speakers,
        })
    }

    /// Whether the summary still stands for the room of `context`: the
    /// room it was built for, not one created again under its id, and each
    /// turn it covers still in the room's history as the channel reads it,
    /// saying what it said. A turn deleted, edited or whose content is
    /// withheld since (RFC §10.3), or gone out of the history, where the
    /// memory can no longer tell, ends it.
    pub(super) fn holds(&self, context: &RoomContext) -> bool {
        if context.room.created_at != self.room {
            return false;
        }
        let said: HashMap<&EventId, String> = context
            .recent_events
            .iter()
            .map(|event| (&event.id, fingerprint(event)))
            .collect();
        self.covers
            .iter()
            .all(|(id, print)| said.get(id).is_some_and(|now| now == print))
    }

    /// Whether the window's `events` go on from the summary: the turn right
    /// after the last one it covers in the room's history, `current` aside,
    /// is among them, or there is none. A turn between the two would be
    /// neither summarized nor read. Python's `SummaryChain.reaches`.
    pub(super) fn reaches(
        &self,
        events: &[RoomEvent],
        context: &RoomContext,
        current: &RoomEvent,
    ) -> bool {
        let history: Vec<&RoomEvent> = context
            .recent_events
            .iter()
            .filter(|event| event.id != current.id)
            .collect();
        let after = history
            .iter()
            .rposition(|event| self.covers.iter().any(|(id, _)| *id == event.id))
            .map_or(0, |at| at + 1);
        history
            .get(after)
            .is_none_or(|following| events.iter().any(|event| event.id == following.id))
    }

    /// This chain, built on `prior`: the turns and speakers both cover.
    pub(super) fn after(mut self, prior: Option<&Self>) -> Self {
        if let Some(prior) = prior {
            let mut covers = prior.covers.clone();
            covers.append(&mut self.covers);
            self.covers = covers;
            self.speakers.extend(prior.speakers.iter().cloned());
            self.speakers.sort();
            self.speakers.dedup();
        }
        self
    }

    /// `result` read after the summary: its events past the ones the
    /// summary covers, and the summary's message in place of any summary
    /// `result` held, after its other messages.
    pub(super) fn carried(&self, result: MemoryResult) -> MemoryResult {
        let mut messages: Vec<_> = result
            .messages
            .into_iter()
            .filter(|message| !is_summary(message))
            .collect();
        messages.push(summary_message(&self.summary));
        let last = result
            .events
            .iter()
            .rposition(|event| self.covers.iter().any(|(id, _)| *id == event.id));
        let events = match last {
            Some(at) => result.events.into_iter().skip(at + 1).collect(),
            None => result.events,
        };
        let mut speakers = result.speakers;
        speakers.extend(self.speakers.iter().cloned());
        speakers.sort();
        speakers.dedup();
        MemoryResult {
            messages,
            events,
            speakers,
            ..result
        }
    }
}

/// What `event` says as the channel reads it, digested: its text, which is
/// empty for a deleted event whose content is withheld (RFC §10.3).
fn fingerprint(event: &RoomEvent) -> String {
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(extract_event_text(event).as_bytes()) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

type ChainKey = (RoomId, Option<ChannelId>);

/// The latest [`SummaryChain`] of each room an agent reads, kept in the
/// process like a cache: one a room has not used for `ttl` is forgotten,
/// and at most `limit` rooms are kept, the least recently used dropped
/// first. A forgotten chain is rebuilt from the window.
#[derive(Debug)]
pub(super) struct SummaryChains {
    ttl: Duration,
    limit: usize,
    chains: Mutex<Vec<(ChainKey, Instant, SummaryChain)>>,
}

impl SummaryChains {
    pub(super) fn new(ttl: Duration, limit: usize) -> Self {
        Self {
            ttl,
            limit,
            chains: Mutex::default(),
        }
    }

    /// The chain of `room_id` for `channel_id`'s agent, its use at `now`
    /// recorded; `None` when there is none or it expired.
    pub(super) fn get(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        now: Instant,
    ) -> Option<SummaryChain> {
        let mut chains = self.lock();
        let at = chains
            .iter()
            .position(|(key, _, _)| key.0 == *room_id && key.1.as_ref() == channel_id)?;
        let (key, used, chain) = chains.remove(at);
        if now.duration_since(used) >= self.ttl {
            return None;
        }
        chains.push((key, now, chain.clone()));
        Some(chain)
    }

    /// Make `chain` the one `room_id` is read after.
    pub(super) fn put(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        chain: SummaryChain,
        now: Instant,
    ) {
        let mut chains = self.lock();
        chains.retain(|(key, _, _)| !(key.0 == *room_id && key.1.as_ref() == channel_id));
        chains.push(((room_id.clone(), channel_id.cloned()), now, chain));
        while chains.len() > self.limit {
            chains.remove(0);
        }
    }

    /// Forget the chain of `room_id` for `channel_id`'s agent.
    pub(super) fn drop_chain(&self, room_id: &RoomId, channel_id: Option<&ChannelId>) {
        self.lock()
            .retain(|(key, _, _)| !(key.0 == *room_id && key.1.as_ref() == channel_id));
    }

    /// Forget every chain of `room_id`.
    pub(super) fn forget(&self, room_id: &RoomId) {
        self.lock().retain(|(key, _, _)| key.0 != *room_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(ChainKey, Instant, SummaryChain)>> {
        self.chains.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
