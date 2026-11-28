//! Admission before memory or generation; the policy never runs on instructions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{ChannelBinding, ChannelOutput, EventType, RoomContext, RoomEvent};
use roomkit::{ChannelError, current_turn_host};
use roomkit_core::rules::{transcript::speaking_people, visible_events};

use super::{AiChannel, AiProvider};
use crate::AiError;
use crate::speaking::{
    SharedRoomEvent, SpeakDecision, SpeakDecisionEvent, SpeakMode, SpeakPolicy, SpeakTurn,
    Speakers, Thinker, Thought,
};

const OFFER_NOTE: &str = "Nobody asked you on this turn. Offer, in one short sentence, what you could add, without giving it.";

pub(super) struct Speech {
    pub(super) policy: Option<Arc<dyn SpeakPolicy>>,
    timeout: Duration,
    pub(super) thinker: Option<Arc<dyn Thinker>>,
    pub(super) wait: Duration,
    pub(super) minds: Mutex<HashMap<roomkit::model::RoomId, Arc<super::mind::Mind>>>,
}

impl Speech {
    pub(super) fn new(options: &super::AiChannelOptions) -> Result<Self, AiError> {
        let (timeout, wait) = (options.speak_timeout, options.think_wait);
        let (policy, thinker) = (&options.speak_policy, &options.thinker);
        if timeout.is_zero() {
            return Err(AiError::SpeakTimeout);
        }
        if thinker.is_some() && policy.is_none() {
            return Err(AiError::ThinkerNeedsPolicy);
        }
        Ok(Self {
            policy: policy.clone(),
            timeout,
            thinker: thinker.clone(),
            wait,
            minds: Mutex::default(),
        })
    }
}

impl<P: AiProvider> AiChannel<P> {
    pub(super) async fn admitted(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if !self.answers(&event) {
            if !roomkit::discussion_turn(&event) {
                self.ingest(&event, &context).await;
            }
            return Ok(ChannelOutput::empty());
        }
        if current_turn_host().is_some_and(|host| host.is_strategy_turn()) {
            if !roomkit::discussion_turn(&event) {
                self.ingest(&event, &context).await;
            }
            return self
                .answer_in_context(event, binding, context, Vec::new())
                .await;
        }
        let decision = self
            .speak_decision(
                &event,
                &context,
                self.thought_of(&context.room.id),
                false,
                false,
            )
            .await;
        let listened = decision
            .as_ref()
            .is_some_and(|d| d.mode == SpeakMode::Silent);
        if listened && !roomkit::discussion_turn(&event) {
            self.ingest(&event, &context).await;
        }
        let decision = match decision {
            Some(decision) if decision.mode == SpeakMode::Silent => Some(
                self.think_while_listening(&event, &binding, &context, decision)
                    .await?,
            ),
            decision => decision,
        };
        if decision
            .as_ref()
            .is_some_and(|d| d.mode == SpeakMode::Silent)
        {
            return Ok(ChannelOutput::empty());
        }
        let mut added = if decision.is_some() {
            self.thought_notes(&context.room.id)
        } else {
            Vec::new()
        };
        added.extend(notes(decision.as_ref()));
        if !listened && !roomkit::discussion_turn(&event) {
            self.ingest(&event, &context).await;
        }
        self.answer_in_context(event, binding, context, added).await
    }

    pub(super) async fn speak_decision(
        &self,
        event: &RoomEvent,
        context: &RoomContext,
        thought: Option<Thought>,
        asked_again: bool,
        late: bool,
    ) -> Option<SpeakDecision> {
        let policy = self.speech.policy.as_ref()?;
        if event.r#type == EventType::Instruction || event.source.channel_id == self.channel_id {
            return None;
        }
        let turn = speak_turn(event, context, &self.channel_id, thought.clone());
        let started = tokio::time::Instant::now();
        let decision = match tokio::time::timeout(self.speech.timeout, policy.decide(&turn)).await {
            Ok(Ok(decision)) => decision,
            failure => {
                tracing::warn!(target: "roomkit::ai", event_id = %event.id, ?failure,
                    "Speak policy failed or timed out; speaking");
                SpeakDecision {
                    reason: "fallback".into(),
                    ..SpeakMode::Speak.into()
                }
            }
        };
        let report = SpeakDecisionEvent {
            duration_ms: (started.elapsed().as_secs_f64() * 1000.0).round_ties_even(),
            asked_again,
            thought,
            late,
            ..SpeakDecisionEvent::new(
                context.room.id.to_string(),
                self.channel_id.to_string(),
                turn.event,
                decision,
            )
        };
        let callbacks = self.channel_callbacks(&context.room.id, current_turn_host().as_ref());
        Some(hooked(report, callbacks).await)
    }
}

pub(crate) async fn hooked(
    report: SpeakDecisionEvent,
    callbacks: Option<roomkit::turns::ChannelTurnHooks>,
) -> SpeakDecision {
    let Some(hooks) = callbacks else {
        return report.decision;
    };
    // Only the decision is read off the hooks' payload: the event decided
    // on, the room and the channel stay the channel's, which the ASYNC hooks
    // then observe with the decision that stands (RFC §6.4). Python's
    // `dataclasses.replace(event, decision=...)`.
    let asked = report.clone();
    hooks
        .decide_and_observe(report, |chain| {
            let decision = match chain.blocked {
                Some((hook, _)) => SpeakDecision {
                    reason: if chain.failed_closed {
                        format!("hook_error:{hook}")
                    } else {
                        format!("blocked by {hook}")
                    },
                    ..SpeakMode::Silent.into()
                },
                None => chain.payload.decision,
            };
            SpeakDecisionEvent { decision, ..asked }
        })
        .await
        .decision
}

fn speak_turn(
    event: &RoomEvent,
    context: &RoomContext,
    channel: &roomkit::model::ChannelId,
    thought: Option<Thought>,
) -> SpeakTurn {
    let recent: Vec<_> = visible_events(context, channel)
        .into_iter()
        .filter(|e| e.id != event.id && e.r#type == EventType::Message)
        .collect();
    let events: Vec<_> = recent.iter().chain([event]).collect();
    let (labels, people) = speaking_people(context, channel, &events);
    let speakers = Speakers::new();
    for (event, label) in events.into_iter().zip(labels) {
        if event.source.channel_id != *channel
            && let Some(label) = label
        {
            speakers.insert(event.id.to_string(), label);
        }
    }
    SpeakTurn {
        recent: recent.into_iter().map(SharedRoomEvent::new).collect(),
        people,
        channel_id: channel.to_string(),
        speakers,
        thought,
        ..SpeakTurn::new(event.clone())
    }
}

pub(super) fn notes(decision: Option<&SpeakDecision>) -> Vec<String> {
    let Some(decision) = decision else {
        return Vec::new();
    };
    let mut notes = decision.notes.clone();
    if decision.mode == SpeakMode::Offer {
        notes.push(OFFER_NOTE.into());
    }
    notes
}
