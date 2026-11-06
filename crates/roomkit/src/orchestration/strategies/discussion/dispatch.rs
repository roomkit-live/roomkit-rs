//! Who takes a person's unaddressed message in a discussion (RFC §19.7.5
//! rule 18): Python's `orchestration/strategies/discussion/dispatch`
//! (`base.py`, `mock.py`).
//!
//! A person's message that names no agent goes, by rule 8, to the agents
//! that asked that person, else to `everyone`, one turn each. A
//! [`DispatchPolicy`] decides instead, once for the room, which agents take
//! it, in which order, or that none does.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, EventId, RoomEvent, RoomId};
use roomkit_core::rules::hook_registry::HookTrigger;

use crate::hooks::TurnPayload;

/// What a policy raises: Python's exception. A policy that fails does not
/// silence the room: the message asks what it asks with no policy.
pub type DispatchError = Box<dyn Error + Send + Sync>;

/// A policy's decision on a turn, as [`DispatchPolicy::decide`] gives it.
pub type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DispatchDecision, DispatchError>> + Send + 'a>>;

/// Decides which agents of a discussion take a person's unaddressed message:
/// Python's `DispatchPolicy`.
///
/// A policy that fails, or does not decide within the discussion's
/// `dispatch_timeout`, does not silence the room: the message asks what it
/// asks with no policy (the agents that asked its author, else every
/// candidate), and the decision reported carries the reason `fallback`. A
/// message that names an agent never reaches a policy: a name is the
/// person's word.
pub trait DispatchPolicy: Send + Sync {
    /// The decision on `turn`: agents among its candidates, in order.
    fn decide(&self, turn: DispatchTurn) -> DispatchFuture<'_>;
}

/// An agent a policy may pick, with the identity the host gave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchCandidate {
    /// The agent's channel id.
    pub channel_id: ChannelId,
    /// Its display name.
    pub name: Option<String>,
    /// Its role.
    pub role: Option<String>,
    /// What the agent does, as a turn's notes tell the others (rule 14).
    pub description: Option<String>,
    /// The name the room addresses it by (rule 4), which the conversation a
    /// policy reads uses for it; `None`: its channel id.
    pub handle: Option<String>,
}

impl DispatchCandidate {
    /// The candidate `channel_id`, with no identity: Python's defaults.
    #[must_use]
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            name: None,
            role: None,
            description: None,
            handle: None,
        }
    }

    /// The agent as a model reads it named: `@` and its handle.
    #[must_use]
    pub fn label(&self) -> String {
        format!(
            "@{}",
            self.handle.as_deref().unwrap_or(self.channel_id.as_str())
        )
    }
}

/// What a policy judges: the message, the conversation before it, the
/// candidates.
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchTurn {
    /// The room.
    pub room_id: RoomId,
    /// The person's message.
    pub event: RoomEvent,
    /// The conversation before it, oldest first: the room's messages a
    /// candidate may read, without tool records.
    pub recent: Vec<RoomEvent>,
    /// Who said `event` and each of `recent`, by event id: a person by the
    /// label the transcript gives them (RFC §6.4), an agent as `@handle`.
    pub speakers: BTreeMap<EventId, String>,
    /// The agents the policy may pick: those that asked the message's
    /// author, then `everyone` in its order; only those the message reaches,
    /// less those that only listen.
    pub candidates: Vec<DispatchCandidate>,
    /// The candidates that asked the message's author a question and wait
    /// for the answer (RFC §19.7.5 rule 10): the message may answer them, or
    /// not.
    pub asked: Vec<ChannelId>,
}

impl DispatchTurn {
    /// The turn on `event` in `room_id`, with nothing else yet: Python's
    /// defaults.
    #[must_use]
    pub fn new(room_id: RoomId, event: RoomEvent) -> Self {
        Self {
            room_id,
            event,
            recent: Vec::new(),
            speakers: BTreeMap::new(),
            candidates: Vec::new(),
            asked: Vec::new(),
        }
    }
}

/// Which agents take a message, in the order they answer, and why.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DispatchDecision {
    /// Their channel ids; empty, no agent takes the message.
    pub agents: Vec<ChannelId>,
    /// Why, in a few words, for logs and `ON_DISPATCH_DECISION`.
    pub reason: String,
    /// What the policy weighed, by name: what makes a decision measurable.
    pub judgments: BTreeMap<String, f64>,
}

/// Carried to `ON_DISPATCH_DECISION` hooks: one decision on one message.
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchDecisionEvent {
    /// The room.
    pub room_id: RoomId,
    /// The person's message decided on.
    pub event: RoomEvent,
    /// The candidates the policy chose among.
    pub candidates: Vec<ChannelId>,
    /// As applied: its agents are candidates, each once.
    pub decision: DispatchDecision,
    /// How long the policy took: the discussion's bound when it did not
    /// decide in time.
    pub duration_ms: u64,
}

impl DispatchDecisionEvent {
    /// The decision on `event`, taken in no time yet: Python's default
    /// `duration_ms=0`.
    #[must_use]
    pub fn new(
        room_id: RoomId,
        event: RoomEvent,
        candidates: Vec<ChannelId>,
        decision: DispatchDecision,
    ) -> Self {
        Self {
            room_id,
            event,
            candidates,
            decision,
            duration_ms: 0,
        }
    }
}

impl TurnPayload for DispatchDecisionEvent {
    const TRIGGER: HookTrigger = HookTrigger::OnDispatchDecision;
}

/// What [`MockDispatchPolicy::new`] takes: Python's
/// `MockDispatchPolicy(decisions=(), *, delay=0.0, error=None)`.
#[derive(Clone, Debug, Default)]
pub struct MockDispatchPolicyOptions {
    /// The decisions, in order, the last one repeating; none: each message
    /// asks what it asks with no policy. Python's channel-id shortcut is a
    /// decision with the reason `scripted`.
    pub decisions: Vec<DispatchDecision>,
    /// How long each decision is held back.
    pub delay: Duration,
    /// The message of the error each decision raises instead.
    pub error: Option<String>,
}

/// A scripted dispatch policy for tests: Python's `MockDispatchPolicy`.
///
/// Each call takes the next decision, the last one repeating. With no
/// script, the message asks what it asks with no policy: the candidates that
/// asked its author, else every candidate. Records every turn.
#[derive(Debug, Default)]
pub struct MockDispatchPolicy {
    decisions: Vec<DispatchDecision>,
    delay: Duration,
    error: Option<String>,
    turns: Mutex<Vec<DispatchTurn>>,
}

impl MockDispatchPolicy {
    /// The policy deciding `options.decisions` in order, the last one
    /// repeating.
    #[must_use]
    pub fn new(options: MockDispatchPolicyOptions) -> Self {
        let MockDispatchPolicyOptions {
            decisions,
            delay,
            error,
        } = options;
        Self {
            decisions,
            delay,
            error,
            turns: Mutex::new(Vec::new()),
        }
    }

    /// The turns it was handed, in order.
    #[must_use]
    pub fn turns(&self) -> Vec<DispatchTurn> {
        self.turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl DispatchPolicy for MockDispatchPolicy {
    fn decide(&self, turn: DispatchTurn) -> DispatchFuture<'_> {
        let asked = {
            let mut turns = self.turns.lock().unwrap_or_else(PoisonError::into_inner);
            turns.push(turn.clone());
            turns.len()
        };
        Box::pin(async move {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if let Some(error) = &self.error {
                return Err(error.clone().into());
            }
            let at = asked.min(self.decisions.len()).checked_sub(1);
            let Some(decision) = at.and_then(|at| self.decisions.get(at)) else {
                let undecided = if turn.asked.is_empty() {
                    turn.candidates
                        .into_iter()
                        .map(|candidate| candidate.channel_id)
                        .collect()
                } else {
                    turn.asked
                };
                return Ok(DispatchDecision {
                    agents: undecided,
                    reason: "as with no policy".to_owned(),
                    judgments: BTreeMap::new(),
                });
            };
            Ok(decision.clone())
        })
    }
}
