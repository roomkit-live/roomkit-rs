//! The payloads of an AI channel's loop triggers (RFC §6.4, §9.2), in the
//! Python implementation's shape: [`AiGenerationEvent`], which
//! `BEFORE_AI_GENERATION` hands, [`ToolRoundEvent`], `AFTER_TOOL_ROUND`'s,
//! and [`AiResponseEvent`], `ON_AI_RESPONSE`'s, with the [`DeclaredTool`]s
//! a response reports and [`response_transcript`].
//!
//! A hook on these triggers registers on the kit as any hook does
//! ([`roomkit::RoomKit::hook`]); the channel's turn fires it on the turn's
//! executor. A hook gets an owned payload: what Python's hook edits in place
//! a Rust hook hands back in `HookResult::modify(event)`.
//!
//! ```
//! use roomkit::model::hook::ModifyOptions;
//! use roomkit::model::{HookResult, RoomContext};
//! use roomkit::{HookOptions, HookTrigger, RoomKit};
//! use roomkit_ai::{AiGenerationEvent, ToolRoundEvent};
//!
//! let kit = RoomKit::new(Default::default());
//! kit.hook(
//!     HookTrigger::BeforeAiGeneration,
//!     |mut event: AiGenerationEvent, _context: RoomContext| async move {
//!         event.ai_context.system_prompt = Some("You answer in French.".into());
//!         HookResult::modify(event, ModifyOptions::default())
//!     },
//!     HookOptions::default(),
//! )?;
//! kit.hook(
//!     HookTrigger::AfterToolRound,
//!     |mut event: ToolRoundEvent, _context: RoomContext| async move {
//!         event.withdraw(["delete_account"]);
//!         HookResult::modify(event, ModifyOptions::default())
//!     },
//!     HookOptions::default(),
//! )?;
//! # Ok::<(), roomkit::RoomKitError>(())
//! ```

use std::collections::BTreeSet;

use roomkit::model::{ChannelId, LoopEndReason, Metadata, RoomEvent, RoomId, Timestamp};
use roomkit::{HookTrigger, TurnPayload};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::provider::{AiContext, AiToolCall, AiToolResultPart};
use crate::tools::AiTool;

/// What separates two segments of a turn in
/// [`AiResponseEvent::response_content`]: a paragraph break, only ever
/// between two segments (Python's `RESPONSE_SEGMENT_SEPARATOR`).
const RESPONSE_SEGMENT_SEPARATOR: &str = "\n\n";

/// The turn's text as `ON_AI_RESPONSE` reports it, Python's
/// `response_transcript(segments)`: the segments kept, the empty stretches
/// (a round in which the model said nothing) dropped, and their join, a
/// blank line between two.
///
/// ```
/// let (segments, content) = roomkit_ai::response_transcript(["", "Let me look.", "", "Done."]);
/// assert_eq!(segments, ["Let me look.", "Done."]);
/// assert_eq!(content, "Let me look.\n\nDone.");
/// ```
pub fn response_transcript<I, S>(segments: I) -> (Vec<String>, String)
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let kept: Vec<String> = segments
        .into_iter()
        .map(Into::into)
        .filter(|segment| !segment.is_empty())
        .collect();
    let content = kept.join(RESPONSE_SEGMENT_SEPARATOR);
    (kept, content)
}

/// What a model call on a turn's context is for (RFC §6.4): Python's
/// `GenerationPurpose`, `answer` or `thought`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationPurpose {
    /// The agent's turn.
    #[default]
    Answer,
    /// Its thinker reading the event's context while the agent stays
    /// silent.
    Thought,
}

/// `BEFORE_AI_GENERATION`'s payload (RFC §6.4, What the generation hooks
/// see): Python's `AIGenerationEvent(ai_context, channel_id, room_id=None,
/// provider_name=None, timestamp=<factory>, trigger=None,
/// purpose='answer')`. Fired once per turn, before its first generation;
/// what the hooks leave of `ai_context` is the turn's context, and a block
/// runs no generation.
#[derive(Clone, Debug, PartialEq)]
pub struct AiGenerationEvent {
    /// The context the turn starts from: its messages, its system prompt,
    /// and its `tools`, the turn's toolset as the tool policy leaves it.
    pub ai_context: AiContext,
    /// The AI channel about to generate.
    pub channel_id: ChannelId,
    /// The room the turn runs in.
    pub room_id: Option<RoomId>,
    /// The provider the turn generates with.
    pub provider_name: Option<String>,
    /// When the generation was asked.
    pub timestamp: Timestamp,
    /// The event the turn answers (RFC §8.5).
    pub trigger: Option<RoomEvent>,
    /// What the generation is for.
    pub purpose: GenerationPurpose,
}

impl AiGenerationEvent {
    /// The generation of `channel_id` on `ai_context`, now by the system
    /// clock, with Python's defaults for the rest.
    pub fn new(ai_context: AiContext, channel_id: ChannelId) -> Self {
        Self {
            ai_context,
            channel_id,
            room_id: None,
            provider_name: None,
            timestamp: SystemEnvironment.now(),
            trigger: None,
            purpose: GenerationPurpose::Answer,
        }
    }
}

impl TurnPayload for AiGenerationEvent {
    const TRIGGER: HookTrigger = HookTrigger::BeforeAiGeneration;
}

/// `AFTER_TOOL_ROUND`'s payload (RFC §6.4, Between two rounds): Python's
/// `ToolRoundEvent(channel_id, room_id, round_index, calls, results,
/// answered=[], tools=[], withdrawn=set(), messages=[])`. Fired once the
/// channel ran a round's calls, before the next round is built: a hook reads
/// the round whole, and acts on the rest of the turn through
/// [`withdraw`](Self::withdraw) and [`add_message`](Self::add_message).
#[derive(Clone, Debug, PartialEq)]
pub struct ToolRoundEvent {
    /// The AI channel whose loop ran the round.
    pub channel_id: ChannelId,
    /// The room the turn runs in.
    pub room_id: Option<RoomId>,
    /// The round's index in the turn, from 0, its generations counted.
    pub round_index: u32,
    /// The calls the channel ran, in the order the model made them.
    pub calls: Vec<AiToolCall>,
    /// The channel's result of each call, in the same order, as the model
    /// reads it.
    pub results: Vec<AiToolResultPart>,
    /// The results of the round's calls its provider served itself.
    pub answered: Vec<AiToolResultPart>,
    /// The names of the turn's toolset the next round is built from, as
    /// `BEFORE_AI_GENERATION` sees it, less what was withdrawn.
    pub tools: Vec<String>,
    /// The names withdrawn for the rest of the turn.
    pub withdrawn: BTreeSet<String>,
    /// The texts the next round reads, as the user's, after this round's
    /// results.
    pub messages: Vec<String>,
}

impl ToolRoundEvent {
    /// The round `round_index` of `channel_id` in `room_id`, its `calls`
    /// and their `results`, with Python's defaults for the rest.
    pub fn new(
        channel_id: ChannelId,
        room_id: Option<RoomId>,
        round_index: u32,
        calls: Vec<AiToolCall>,
        results: Vec<AiToolResultPart>,
    ) -> Self {
        Self {
            channel_id,
            room_id,
            round_index,
            calls,
            results,
            answered: Vec::new(),
            tools: Vec::new(),
            withdrawn: BTreeSet::new(),
            messages: Vec::new(),
        }
    }

    /// Takes `names` out of the rest of the turn, with every guarantee of a
    /// withdrawal by `BEFORE_AI_GENERATION`: never declared again, a call
    /// naming one refused. Python's `withdraw(*names)`.
    pub fn withdraw<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.withdrawn.extend(names.into_iter().map(Into::into));
    }

    /// Has the next round read `text`, as the user's, after the results:
    /// Python's `add_message(text)`.
    pub fn add_message(&mut self, text: impl Into<String>) {
        self.messages.push(text.into());
    }
}

impl TurnPayload for ToolRoundEvent {
    const TRIGGER: HookTrigger = HookTrigger::AfterToolRound;
}

/// Why a tool was in the toolset the provider received (RFC §6.4): Python's
/// `ToolDeclarationOrigin`. A turn without Tool Search declares every tool
/// `always`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDeclarationOrigin {
    /// A tool Tool Search never hid.
    #[default]
    Always,
    /// Admitted by the channel's pinned tools.
    Pinned,
    /// Called or found by the room in an earlier turn.
    Sticky,
    /// Revealed by `find_tools` in this turn.
    Revealed,
}

/// One tool as the provider received it, and why it was there: Python's
/// `DeclaredTool(name, description, parameters, origin='always')`.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredTool {
    /// The tool's name.
    pub name: String,
    /// Its description, as declared.
    pub description: String,
    /// Its parameters' schema, as declared, not the provider's wire form.
    pub parameters: Value,
    /// Why it was declared.
    pub origin: ToolDeclarationOrigin,
}

impl DeclaredTool {
    /// The tool `name`, described by `description`, taking `parameters`,
    /// declared `always`: Python's `DeclaredTool(name, description,
    /// parameters, origin='always')`.
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
            origin: ToolDeclarationOrigin::Always,
        }
    }

    /// The record of `tool`, declared for the reason `options` says:
    /// Python's `DeclaredTool.from_tool(tool, origin='always')`.
    pub fn from_tool(tool: &AiTool, options: FromToolOptions) -> Self {
        Self {
            origin: options.origin,
            ..Self::new(
                tool.name.clone(),
                tool.description.clone(),
                tool.parameters.clone(),
            )
        }
    }
}

/// What [`DeclaredTool::from_tool`] takes beside the tool: Python's
/// `from_tool(tool, origin='always')`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FromToolOptions {
    /// Python `origin='always'`: why the tool was declared.
    pub origin: ToolDeclarationOrigin,
}

/// `ON_AI_RESPONSE`'s payload (RFC §6.4, §9.2): Python's
/// `AIResponseEvent(channel_id, response_content, room_id=None,
/// tool_calls_count=0, usage={}, thinking='', round_count=0,
/// loop_end_reason=None, latency_ms=0, streaming=False,
/// timestamp=<factory>, segments=[], usage_metadata={},
/// declared_tools=[])`. Fired once a turn's loop reached its end; a turn
/// that raised before a round, or whose stream was dropped first, fires
/// none.
#[derive(Clone, Debug, PartialEq)]
pub struct AiResponseEvent {
    /// The AI channel whose turn it was.
    pub channel_id: ChannelId,
    /// Everything the model said in the turn: its [`segments`](Self::segments)
    /// joined by a blank line.
    pub response_content: String,
    /// The room the turn ran in.
    pub room_id: Option<RoomId>,
    /// The calls the turn ran.
    pub tool_calls_count: u32,
    /// What its generations used, summed: the input and output tokens,
    /// zero when unreported, and every other counter reported.
    pub usage: Metadata,
    /// The model's reasoning text, when its provider hands one.
    pub thinking: String,
    /// The rounds of calls the turn ran.
    pub round_count: u32,
    /// The rule that ended the turn.
    pub loop_end_reason: Option<LoopEndReason>,
    /// How long the turn took, in milliseconds.
    pub latency_ms: u64,
    /// Whether the response was streamed.
    pub streaming: bool,
    /// When the turn completed.
    pub timestamp: Timestamp,
    /// The turn's text, one entry per stretch between calls, the empty
    /// stretches dropped.
    pub segments: Vec<String>,
    /// The provenance of the provider's usage observations.
    pub usage_metadata: Metadata,
    /// The tools the provider received this turn, over every round, in
    /// first declaration order, one entry per name.
    pub declared_tools: Vec<DeclaredTool>,
}

impl AiResponseEvent {
    /// The response of `channel_id` saying `response_content`, now by the
    /// system clock, with Python's defaults for the rest.
    pub fn new(channel_id: ChannelId, response_content: impl Into<String>) -> Self {
        Self {
            channel_id,
            response_content: response_content.into(),
            room_id: None,
            tool_calls_count: 0,
            usage: Metadata::new(),
            thinking: String::new(),
            round_count: 0,
            loop_end_reason: None,
            latency_ms: 0,
            streaming: false,
            timestamp: SystemEnvironment.now(),
            segments: Vec::new(),
            usage_metadata: Metadata::new(),
            declared_tools: Vec::new(),
        }
    }
}

impl TurnPayload for AiResponseEvent {
    const TRIGGER: HookTrigger = HookTrigger::OnAiResponse;
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_ai_response_transcript.py::test_the_transcript_keeps_the_separator_between_segments_only
    #[test]
    fn the_transcript_keeps_the_separator_between_segments_only() {
        let (segments, content) = response_transcript(["", "Let me look.", "", "Done.", ""]);
        assert_eq!(segments, ["Let me look.", "Done."]);
        assert_eq!(content, "Let me look.\n\nDone.");
        assert_eq!(
            response_transcript(Vec::<String>::new()),
            (Vec::new(), String::new())
        );
    }

    // Python: tests/test_before_ai_generation.py::test_an_event_built_positionally_keeps_its_released_meaning
    #[test]
    fn an_event_built_with_its_required_members_keeps_python_s_defaults() {
        let channel = ChannelId::new("ai1").unwrap();
        let event = AiGenerationEvent::new(AiContext::default(), channel);
        assert_eq!(
            (
                event.room_id,
                event.provider_name,
                event.trigger,
                event.purpose
            ),
            (None, None, None, GenerationPurpose::Answer)
        );
    }
}
