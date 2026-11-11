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
use crate::loop_events::{AiGenerationEvent, AiResponseEvent, ToolRoundEvent, response_transcript};
use crate::provider::{AiContext, AiMessage, AiToolCall, AiToolResultPart};
use crate::tool_eviction::REREAD_TOOL;
use crate::tools::AiTool;

/// What `BEFORE_AI_GENERATION`'s hooks and `AFTER_TOOL_ROUND`'s left of the
/// turn's context beside its prompt and sampling, which the loop holds
/// itself (RFC §6.4).
#[derive(Clone, Default)]
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
    /// The message a discussion answers, when it reads before later messages.
    pub(crate) answered_input: Option<usize>,
}

/// The calls a round ran, as `AFTER_TOOL_ROUND` hands them: none when the
/// round ran none.
#[derive(Debug, Default)]
pub(super) struct Ran {
    pub(super) calls: Vec<AiToolCall>,
    pub(super) results: Vec<AiToolResultPart>,
}

impl Loop {
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
            tools: self.hook_tools(),
            ..self.round_context(messages, self.system_prompt.clone())
        };
        let event = AiGenerationEvent {
            room_id: Some(self.context.room_id.clone()),
            provider_name: Some(self.provider.dyn_name().to_owned()),
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
        let conversation = self.shown(messages).len();
        self.adopt(&built, chain.payload.ai_context, conversation);
        true
    }

    /// Makes `left`, what the hooks left of the context `built` of a
    /// conversation of `conversation` messages, the turn's: its toolset
    /// too, a tool they added or redefined declared as they left it, one
    /// they took out withdrawn (Python's `_adopt_hook_toolset`).
    fn adopt(&mut self, built: &AiContext, left: AiContext, conversation: usize) {
        if left.messages != built.messages {
            self.adopted.answered_input = self
                .adopted
                .answered_input
                .and_then(|at| retained_input(at, &built.messages, &left.messages));
            self.adopted.messages = Some((left.messages, conversation));
        }
        self.system_prompt = left.system_prompt;
        self.temperature = left.temperature;
        self.max_tokens = left.max_tokens;
        self.thinking_budget = left.thinking_budget;
        self.enable_thinking = left.enable_thinking;
        self.reasoning_effort = left.reasoning_effort;
        // A schema outside the subset is its provider's to refuse, before any
        // request, as for a context built by hand (HITL 85eb4aa0, 17bfa822).
        self.response_schema = left.response_schema;
        self.adopted.metadata = left.metadata;
        self.record.adopt(left.response_metadata);
        if left.tools != built.tools {
            let adopted = self.toolbox.as_left(&left.tools);
            // A tool it adds is declared at every round, never hidden (RFC §6.4).
            let known = self.toolbox.names();
            self.search
                .never_hide(adopted.names().into_iter().filter(|n| !known.contains(n)));
            self.toolbox = Arc::new(adopted);
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
            tools: self
                .hook_tools()
                .into_iter()
                .map(|tool| tool.name)
                .collect(),
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
            loop_end_reason: Some(reason.to_string()),
            latency_ms,
            streaming: true,
            segments,
            declared_tools: self.reported_tools(),
            thinking: spent.thinking.join("\n\n"),
            ..AiResponseEvent::new(self.context.channel_id.clone(), content)
        };
        let mut data = Metadata::new();
        data.insert("tool_calls_count".to_owned(), Value::from(spent.calls));
        data.insert("latency_ms".to_owned(), Value::from(latency_ms));
        data.insert("streaming".to_owned(), Value::Bool(true));
        let host = turn.clone();
        turn.observe_turn(&event, move || host.emit_turn_event("ai_response", data));
    }

    /// The turn's toolset as its hooks read it, by the tools' own schemas:
    /// what the tool policy permits, less what was withdrawn, Tool Search's
    /// whole catalogue included (RFC §6.4).
    pub(super) fn declared(&self) -> Vec<AiTool> {
        self.toolbox
            .declared(&self.policy)
            .into_iter()
            .filter(|tool| !self.adopted.withdrawn.contains(&tool.name))
            .collect()
    }

    /// The turn's toolset as its generation and round hooks read it: what
    /// [`declared`](Self::declared) gives, `read_stored_result` last once the
    /// room stored a result when the turn began (RFC §6.4: the rounds declare
    /// it from the first either way). Python's `_hook_toolset`.
    pub(super) fn hook_tools(&self) -> Vec<AiTool> {
        let mut tools = self.declared();
        if self.reread_shown {
            tools.extend(self.reread_kept());
        }
        tools
    }

    /// `read_stored_result`, unless the turn withdrew it.
    fn reread_kept(&self) -> Option<AiTool> {
        self.toolbox
            .reread()
            .filter(|tool| !self.adopted.withdrawn.contains(&tool.name))
            .cloned()
    }

    /// `tools`, a round's declaration, with `read_stored_result` last when
    /// they declare a tool or the room stores a result: declared from the
    /// first round of such a turn, so that the declaration does not change
    /// when a result is stored (RFC §6.4, §21.5). Python's
    /// `with_reread_tool`.
    fn with_reread(&self, mut tools: Vec<AiTool>) -> Vec<AiTool> {
        if tools.is_empty() && !self.eviction.has_evicted(self.context.room_id.as_str()) {
            return tools;
        }
        tools.extend(self.reread_kept());
        tools
    }

    /// What the first round declares: the toolset, collapsed when Tool Search
    /// hides its catalogue, then `read_stored_result`.
    pub(super) fn first_declaration(&self) -> Vec<AiTool> {
        self.with_reread(self.search.collapsed(self.declared()))
    }

    /// What this round's request shows, each tool's schema with the intent
    /// property when the agent asks for one, the round's declaration joining
    /// what the turn reports (RFC §6.4).
    pub(super) fn round_tools(&self) -> Vec<AiTool> {
        let round = self.with_reread(self.search.round_declaration(self.declared()));
        // What Tool Search lists is the turn's offer: the toolset, then the
        // re-read of stored results once the round declares it (Python's
        // `offered_tools`).
        if let Some(reread) = round.iter().find(|tool| tool.name == REREAD_TOOL) {
            self.search.offer(reread.clone());
        }
        let round = self.held_declaration(round);
        self.record_tools(&round);
        self.shown_tools(round)
    }
}

/// Keep the existing boundary when its message is unchanged. Only relocate a
/// moved input when there is exactly one match: repeated questions are not
/// interchangeable, even when their rendered words are identical.
fn retained_input(at: usize, built: &[AiMessage], left: &[AiMessage]) -> Option<usize> {
    let input = built.get(at)?;
    if left.get(at) == Some(input) {
        return Some(at);
    }
    let mut matches = left
        .iter()
        .enumerate()
        .filter(|(_, message)| *message == input);
    let (at, _) = matches.next()?;
    matches.next().is_none().then_some(at)
}
