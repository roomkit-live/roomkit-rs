//! Who takes a person's unaddressed message, decided by the lease holder
//! (RFC §19.7.5 rule 18): Python's `orchestration/strategies/discussion/
//! _dispatching.py`.
//!
//! Routing leaves such a message waiting in the stored queue, with the
//! candidates it reaches and the place in the queue it came in at
//! ([`DiscussionRoom::leave_for_dispatch`]). The turn driver, holding the
//! lease, decides the first message waiting before it gives another turn:
//! the policy decides off the room's lock, within the discussion's bound,
//! and the agents it picks are put at the front of the queue at the
//! message's place, as rule 8 puts `everyone`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ChannelId, EventStatus, EventType, RoomContext, RoomEvent};
use roomkit_core::rules::discussion::people::{labelled, sees};
use roomkit_core::rules::transcript::turn_labels;
use roomkit_core::rules::verification::release::withdrawn;
use roomkit_core::rules::withheld_history;

use super::SpeakQueueChange;
use super::dispatch::{
    DispatchCandidate, DispatchDecision, DispatchDecisionEvent, DispatchPolicy, DispatchTurn,
};
use super::queue::Pending;
use super::room::DiscussionRoom;
use crate::driver;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::ConversationStore;
use crate::unwind;

/// The reason of a decision the policy did not take: the message asks what
/// it asks with no policy, the agents that asked its author, else every
/// candidate.
const FALLBACK: &str = "fallback";

impl<S: ConversationStore> DiscussionRoom<S> {
    /// Decides `pending` and queues the agents picked. A message gone from
    /// the room, or whose candidates all listen now, asks no agent; a
    /// decision that fails in any way asks every candidate. The decision is
    /// told once applied, never when another process decided first.
    pub(super) async fn decide(&self, kit: &RoomKit<S>, pending: Pending) {
        let listening = self.state().queue.listening.clone();
        let candidates: Vec<ChannelId> = pending
            .candidates
            .iter()
            .filter(|agent| self.config().agents.contains(agent) && !listening.contains(agent))
            .cloned()
            .collect();
        let (report, agents) = match self.decision(kit, &pending, &candidates).await {
            Ok(report) => {
                let agents = report
                    .as_ref()
                    .map(|report| report.decision.agents.clone())
                    .unwrap_or_default();
                (report, agents)
            }
            Err(error) => {
                tracing::error!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    event_id = %pending.event_id,
                    %error,
                    "the dispatch decision failed; it goes undecided"
                );
                let undecided = pending
                    .undecided()
                    .into_iter()
                    .filter(|agent| candidates.contains(agent))
                    .collect();
                (None, undecided)
            }
        };
        match self.apply(kit, &pending, agents).await {
            Ok(true) => {
                if let Some(report) = report {
                    self.tell_decision(report);
                }
            }
            Ok(false) => {}
            Err(error) => tracing::error!(
                target: "roomkit::orchestration::discussion",
                room_id = %self.room_id,
                event_id = %pending.event_id,
                %error,
                "the dispatch decision was not stored"
            ),
        }
    }

    /// The decision on `pending` as it will be told, or `None` when there is
    /// nothing to decide.
    async fn decision(
        &self,
        kit: &RoomKit<S>,
        pending: &Pending,
        candidates: &[ChannelId],
    ) -> Result<Option<DispatchDecisionEvent>, RoomKitError> {
        if candidates.is_empty() {
            return Ok(None);
        }
        let Ok(id) = pending.event_id.parse() else {
            return Ok(None);
        };
        let event = kit.store().get_event(&id).await?;
        // A message deleted while it waited is not handed to the policy.
        let Some(event) = event.filter(|event| event.room_id == self.room_id && !withdrawn(event))
        else {
            return Ok(None);
        };
        let reading = kit.inner.config().recent_events().read_by_the_caller();
        let context = driver::load_context(kit.store(), &self.room_id, reading).await?;
        let Some(context) = context else {
            return Ok(None);
        };
        let asked: Vec<ChannelId> = pending
            .asked
            .iter()
            .filter(|agent| candidates.contains(agent))
            .cloned()
            .collect();
        let undecided: Vec<ChannelId> = pending
            .undecided()
            .into_iter()
            .filter(|agent| candidates.contains(agent))
            .collect();
        let turn = self.turn(kit, &event, &context, candidates, asked);
        let (decision, duration_ms) = self.bounded(turn, undecided).await;
        Ok(Some(DispatchDecisionEvent {
            room_id: self.room_id.clone(),
            event,
            candidates: candidates.to_vec(),
            decision,
            duration_ms,
        }))
    }

    /// The policy's decision within the discussion's bound, cut down to the
    /// candidates, and how long it took: a policy that fails, panics or does
    /// not decide in time, or none in this process, leaves the message
    /// `undecided`: what it asks with no policy.
    async fn bounded(
        &self,
        turn: DispatchTurn,
        undecided: Vec<ChannelId>,
    ) -> (DispatchDecision, u64) {
        let fallback = DispatchDecision {
            agents: undecided,
            reason: FALLBACK.to_owned(),
            judgments: BTreeMap::new(),
        };
        let Some((policy, bound)) = self.dispatch_policy() else {
            tracing::warn!(
                target: "roomkit::orchestration::discussion",
                room_id = %self.room_id,
                "no dispatch policy in this process"
            );
            return (fallback, 0);
        };
        let candidates: Vec<ChannelId> = turn
            .candidates
            .iter()
            .map(|candidate| candidate.channel_id.clone())
            .collect();
        let event_id = turn.event.id.clone();
        let started = tokio::time::Instant::now();
        let deciding = unwind::unwinding(decided(policy, turn));
        let decided = tokio::time::timeout(bound, deciding).await;
        let took = millis(started.elapsed());
        match decided {
            Ok(Ok(Ok(decision))) => (within(decision, &candidates, self), took),
            Err(_) => {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    %event_id,
                    "the dispatch policy took over its bound; it goes undecided"
                );
                (fallback, millis(bound))
            }
            Ok(Ok(Err(error))) => {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    %event_id,
                    %error,
                    "the dispatch policy failed; it goes undecided"
                );
                (fallback, took)
            }
            Ok(Err(panic)) => {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    %event_id,
                    message = %unwind::message(&*panic),
                    "the dispatch policy failed; it goes undecided"
                );
                (fallback, took)
            }
        }
    }

    /// What the policy judges: the room's messages before `event` a
    /// candidate may read (a policy may hand them to a classifier outside),
    /// who said each, and the candidates' identity.
    fn turn(
        &self,
        kit: &RoomKit<S>,
        event: &RoomEvent,
        context: &RoomContext,
        candidates: &[ChannelId],
        asked: Vec<ChannelId>,
    ) -> DispatchTurn {
        // What a delete removed stays withheld (RFC §7.5 rule 8, §10.3).
        let recent: Vec<RoomEvent> = withheld_history(&context.recent_events)
            .into_iter()
            .filter(|earlier| {
                earlier.index < event.index
                    && earlier.r#type == EventType::Message
                    && earlier.status != EventStatus::Blocked
                    && candidates
                        .iter()
                        .any(|candidate| sees(candidate, earlier, context))
            })
            .collect();
        let said: Vec<&RoomEvent> = recent.iter().chain(std::iter::once(event)).collect();
        let labels = turn_labels(said.iter().copied(), context);
        let mut speakers = BTreeMap::new();
        let config = self.config();
        for (said, label) in said.iter().zip(labels) {
            let author = &said.source.channel_id;
            // A person named like one of an agent's own labels reads as a
            // participant, as the transcript of Python's turn_labels has it.
            let label = if config.agents.contains(author) {
                Some(config.label(author))
            } else {
                label.filter(|label| !label.is_empty()).map(labelled)
            };
            if let Some(label) = label.filter(|label| !label.is_empty()) {
                speakers.insert(said.id.clone(), label);
            }
        }
        DispatchTurn {
            room_id: self.room_id.clone(),
            event: event.clone(),
            recent,
            speakers,
            candidates: candidates
                .iter()
                .map(|agent| self.candidate(kit, agent))
                .collect(),
            asked,
        }
    }

    /// `agent` as a policy reads it: its identity and its handle.
    fn candidate(&self, kit: &RoomKit<S>, agent: &ChannelId) -> DispatchCandidate {
        let identity = kit
            .inner
            .channel(agent)
            .and_then(|channel| channel.declaration().identity)
            .unwrap_or_default();
        DispatchCandidate {
            channel_id: agent.clone(),
            name: identity.name,
            role: identity.role,
            description: identity.description,
            handle: Some(self.config().handle(agent).to_owned()),
        }
    }

    /// Puts `agents` at the front at the message's place; whether the
    /// decision was applied: not when another process decided the message
    /// meanwhile, the discussion ended, or this process stopped (the message
    /// then waits for the next holder).
    async fn apply(
        &self,
        kit: &RoomKit<S>,
        pending: &Pending,
        agents: Vec<ChannelId>,
    ) -> Result<bool, RoomKitError> {
        let applied = self
            .editing(kit, |state, live| {
                let waiting = state
                    .dispatching
                    .iter()
                    .any(|waiting| waiting.event_id == pending.event_id);
                if live.closed || !waiting {
                    return false;
                }
                state
                    .dispatching
                    .retain(|waiting| waiting.event_id != pending.event_id);
                if state.over {
                    return false;
                }
                state.queue_picked(pending, &agents);
                true
            })
            .await?;
        if applied && !agents.is_empty() {
            self.fire(
                SpeakQueueChange::Queued,
                agents,
                Some(pending.event_id.clone()),
            );
        }
        Ok(applied)
    }
}

/// `policy`'s decision on `turn`, owned for the bound around it.
async fn decided(
    policy: Arc<dyn DispatchPolicy>,
    turn: DispatchTurn,
) -> Result<DispatchDecision, super::dispatch::DispatchError> {
    policy.decide(turn).await
}

/// `decision` with only candidates, each once: a policy is no way around
/// visibility or the listening state.
fn within<S: ConversationStore>(
    mut decision: DispatchDecision,
    candidates: &[ChannelId],
    room: &DiscussionRoom<S>,
) -> DispatchDecision {
    let mut picked: Vec<ChannelId> = Vec::new();
    let mut left_out: Vec<ChannelId> = Vec::new();
    for agent in decision.agents {
        if !candidates.contains(&agent) {
            left_out.push(agent);
        } else if !picked.contains(&agent) {
            picked.push(agent);
        }
    }
    if !left_out.is_empty() {
        tracing::warn!(
            target: "roomkit::orchestration::discussion",
            room_id = %room.room_id,
            left_out = ?left_out,
            "a dispatch decision named agents that are not candidates"
        );
    }
    decision.agents = picked;
    decision
}

/// `elapsed` in whole milliseconds, a half rounded up (Python's `round()`
/// rounds it to even: they differ under a millisecond only).
fn millis(elapsed: Duration) -> u64 {
    let micros = elapsed.as_micros();
    u64::try_from((micros + 500) / 1000).unwrap_or(u64::MAX)
}
