//! The kit's hooks on the loop's own triggers (RFC §6.4, §9.2), fired
//! through the turn's host where the Python implementation's channel fires
//! them:
//!
//! - `BEFORE_AI_GENERATION` once, before the turn's first generation: a
//!   block runs none, and what the hooks leave of the context is the
//!   turn's: its messages, system prompt, sampling and provider data, and
//!   its toolset, a tool they added or redefined declared as they left it,
//!   one they took out withdrawn for the turn;
//! - `AFTER_TOOL_ROUND` after each round that ran calls, before the next is
//!   built: its tools withdrawn, its messages read after the results;
//! - `ON_AI_RESPONSE` once the loop reached its end, observed, then
//!   `ai_response`.

use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit::model::{LoopEndReason, Metadata, RoomEvent};
use roomkit_core::rules::transcript::{Message, Role, without_mark_copies};
use serde_json::Value;

use super::{Loop, Spent};
use crate::loop_events::{
    AiGenerationEvent, AiResponseEvent, DeclaredTool, FromToolOptions, ToolRoundEvent,
    response_transcript,
};
use crate::provider::{
    AiContext, AiMessage, AiProvider, AiToolCall, AiToolResultPart, ai_messages,
};

/// What `BEFORE_AI_GENERATION`'s hooks and `AFTER_TOOL_ROUND`'s left of the
/// turn's context beside its prompt and sampling, which the loop holds
/// itself (RFC §6.4).
#[derive(Clone, Debug, Default)]
pub(crate) struct Adopted {
    /// The messages the hooks gave, or a compaction made (RFC §6.4),
    /// standing for the memory's and for the conversation's first
    /// messages, as many as the second member says.
    pub(crate) messages: Option<(Vec<AiMessage>, usize)>,
    /// The provider data every round carries.
    pub(crate) metadata: Metadata,
    /// The tools withdrawn for the rest of the turn.
    pub(crate) withdrawn: BTreeSet<String>,
    /// Where the turn's input is among the messages a round is asked on,
    /// its notes included, as the hooks left it: what a compaction keeps
    /// whole (RFC §6.4).
    pub(crate) turn_input: Option<usize>,
}

/// The calls a round ran, as `AFTER_TOOL_ROUND` hands them: none when the
/// round ran none.
#[derive(Debug, Default)]
pub(super) struct Ran {
    pub(super) calls: Vec<AiToolCall>,
    pub(super) results: Vec<AiToolResultPart>,
}

impl<P: AiProvider> Loop<P> {
    /// Fires `BEFORE_AI_GENERATION` on the turn answering `trigger`, whose
    /// conversation starts with `messages`, and emits
    /// `before_ai_generation` (RFC §8.2); whether the turn generates. What
    /// the hooks leave is the turn's context. Outside a kit nothing fires.
    pub(crate) async fn before_generation(
        &mut self,
        trigger: &RoomEvent,
        messages: &[Message],
    ) -> bool {
        let Some(turn) = self.turn.clone() else {
            return true;
        };
        let built = AiContext {
            tools: self.declared(),
            ..self.round_context(messages, self.system_prompt.clone())
        };
        let event = AiGenerationEvent {
            room_id: Some(self.context.room_id.clone()),
            provider_name: Some(self.provider.name().to_owned()),
            trigger: Some(trigger.clone()),
            ..AiGenerationEvent::new(built.clone(), self.context.channel_id.clone())
        };
        let chain = turn.run_turn_hooks(event).await;
        let blocked_by = chain.blocked.as_ref().map(|(name, reason)| {
            tracing::info!(target: "roomkit::ai", hook = %name, ?reason, "the generation was blocked by a hook");
            name.clone()
        });
        let mut data = Metadata::new();
        data.insert("allowed".to_owned(), Value::Bool(blocked_by.is_none()));
        data.insert(
            "blocked_by".to_owned(),
            blocked_by.clone().map_or(Value::Null, Value::from),
        );
        turn.emit_turn_event("before_ai_generation", data);
        if blocked_by.is_some() {
            return false;
        }
        // Counted as the provider reads them: a round's results are one message.
        let conversation = ai_messages(&self.shown_messages(messages)).len();
        self.adopt(&built, chain.payload.ai_context, conversation);
        true
    }

    /// Makes `left`, what the hooks left of the context `built` of a
    /// conversation of `conversation` messages, the turn's: its toolset
    /// too, a tool they added or redefined declared as they left it, one
    /// they took out withdrawn (Python's `_adopt_hook_toolset`).
    fn adopt(&mut self, built: &AiContext, left: AiContext, conversation: usize) {
        if left.messages != built.messages {
            self.adopted.messages = Some((left.messages, conversation));
        }
        self.system_prompt = left.system_prompt;
        self.temperature = left.temperature;
        self.max_tokens = left.max_tokens;
        self.adopted.metadata = left.metadata;
        if left.tools != built.tools {
            self.toolbox = Arc::new(self.toolbox.as_left(&left.tools));
        }
        let kept: BTreeSet<&str> = left.tools.iter().map(|tool| tool.name.as_str()).collect();
        self.adopted.withdrawn.extend(
            built
                .tools
                .iter()
                .filter(|tool| !kept.contains(tool.name.as_str()))
                .map(|tool| tool.name.clone()),
        );
    }

    /// Fires `AFTER_TOOL_ROUND` on the round `round_index`, whose calls
    /// `ran`, and applies what its hooks left, a block's included: their
    /// withdrawals for the rest of the turn; returns their messages, the
    /// user's, each copy of a runtime mark replaced (RFC §6.4). A round
    /// that ran no call fires nothing.
    pub(super) async fn after_tool_round(&mut self, round_index: u32, ran: Ran) -> Vec<Message> {
        let Some(turn) = self.turn.clone() else {
            return Vec::new();
        };
        if ran.calls.is_empty() {
            return Vec::new();
        }
        let event = ToolRoundEvent {
            tools: self.declared().into_iter().map(|tool| tool.name).collect(),
            ..ToolRoundEvent::new(
                self.context.channel_id.clone(),
                Some(self.context.room_id.clone()),
                round_index,
                ran.calls,
                ran.results,
            )
        };
        let left = turn.run_turn_hooks(event).await.payload;
        self.adopted.withdrawn.extend(left.withdrawn);
        left.messages
            .iter()
            .map(|text| Message::new(Role::User, without_mark_copies(text)))
            .collect()
    }

    /// Reports the turn that ended for `reason` after `rounds` rounds of
    /// calls, having `spent` what it did, to `ON_AI_RESPONSE`'s hooks,
    /// then emits `ai_response` once they ended (RFC §8.2, §9.2). Outside a
    /// kit nothing fires.
    pub(super) fn report(&self, reason: LoopEndReason, rounds: u32, spent: &Spent) {
        let Some(turn) = self.turn.clone() else {
            return;
        };
        let (segments, content) = response_transcript(spent.segments.iter().cloned());
        let mut usage = Metadata::new();
        usage.insert("input_tokens".to_owned(), Value::from(0));
        usage.insert("output_tokens".to_owned(), Value::from(0));
        usage.extend(spent.usage.clone());
        let latency_ms = u64::try_from(spent.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let event = AiResponseEvent {
            room_id: Some(self.context.room_id.clone()),
            tool_calls_count: spent.calls,
            usage,
            round_count: rounds,
            loop_end_reason: Some(reason),
            latency_ms,
            streaming: true,
            segments,
            declared_tools: spent.declared.clone(),
            ..AiResponseEvent::new(self.context.channel_id.clone(), content)
        };
        let mut data = Metadata::new();
        data.insert("tool_calls_count".to_owned(), Value::from(spent.calls));
        data.insert("latency_ms".to_owned(), Value::from(latency_ms));
        data.insert("streaming".to_owned(), Value::Bool(true));
        let host = turn.clone();
        turn.observe_turn(&event, move || host.emit_turn_event("ai_response", data));
    }

    /// The tools the turn declares, by their own schemas: what the tool
    /// policy permits, less what was withdrawn.
    pub(super) fn declared(&self) -> Vec<crate::tools::AiTool> {
        self.toolbox
            .declared(&self.policy)
            .into_iter()
            .filter(|tool| !self.adopted.withdrawn.contains(&tool.name))
            .collect()
    }

    /// The declaration the turn reports (RFC §6.4): its first round's,
    /// which every later round's is part of, a withdrawal only taking
    /// tools away.
    pub(super) fn declared_tools(&self) -> Vec<DeclaredTool> {
        self.declared()
            .iter()
            .map(|tool| DeclaredTool::from_tool(tool, FromToolOptions::default()))
            .collect()
    }
}
