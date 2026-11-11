//! The tool loop of an AI turn (RFC §6.4 "One loop, streamed or not",
//! Appendix A.9): generate, run the round's calls, hand back their results,
//! generate again, up to the round bound, every provider alike.
//!
//! The loop is a stream of items, pulled by its reader (step 4, decision
//! 6): the text as it comes, each call's start, each call's end in call
//! order, then the loop's end. A round's calls start only once the reader
//! has read past their starts.
//!
//! - this module: the items and the loop's rounds;
//! - `generation`: one generation read from its provider, and its calls;
//! - `resilience`: a generation its provider refused, replayed once
//!   compacted, tried again, then handed to the provider the channel's
//!   router names (RFC §6.4);
//! - `compaction`: a context the provider refused as too long, compacted
//!   for its replay (RFC §6.4 "A turn compacted");
//! - `calls`: one call through its gate (RFC §21.1) and its tool, reported
//!   to the kit's hooks (RFC §9.3);
//! - `repeat_guard`: a call or an answer that repeats within the turn (RFC
//!   §6.4).
//!
//! The loop reads the directives a host steers into it (RFC §21.3,
//! `crate::steering`) at its stops: at its start, after each round of
//! calls, and once the model has answered, when a message steered in
//! meanwhile asks the model again in the same turn. A `Cancel` also stops
//! a generation at its next event, and the round's calls announced but not
//! yet run.

mod between_rounds;
mod calls;
mod compaction;
mod deferred;
pub(crate) use deferred::DeferredTools;
mod footprint;
mod generation;
mod hooks;
mod intents;
mod repeat_guard;
mod resilience;
mod rules;
mod spans;
mod text_window;
mod thinking;
mod tool_events;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    LoopEndMarker, LoopEndReason, Metadata, ToolCall, ToolCallOutcome, UsageMarker, add_usage,
    reports_counters,
};
use roomkit::{ToolCallReport, TurnHost, TurnRecord};
use roomkit_core::rules::transcript::{Message, Role};
use serde_json::{Map, Value};
use spans::TurnSpan;
pub(crate) use spans::TurnTelemetry;
use tokio::time::Instant;

use self::generation::{Called, Failed, Generation};
pub(crate) use self::hooks::Adopted;
use self::hooks::Ran;
use self::repeat_guard::FORCE_STOP_NUDGE;
pub(crate) use self::repeat_guard::RepeatGuard;
pub(crate) use self::resilience::Resilience;
use self::rules::{Limits, final_round_reason, unanswered};
pub(crate) use self::text_window::TextPublication;
use self::text_window::{RoundWindows, TextWindow};
pub(crate) use self::thinking::ThinkingPublication;
use self::thinking::ThinkingWindow;
use self::tool_events::{Ended, Running, ToolEvents};
use crate::author_images::refuses_content;
use crate::memory::{MemoryProvider, after_memory};
use crate::provider::{
    AiContentPart, AiContext, AiMessage, AiToolResultPart, DynAiProvider, ProviderError,
    ProviderErrorOptions, ResponseSchemaError, ai_messages, ai_messages_with,
};
use crate::steering::{Loops, Steering};
use crate::stream::{Items, Yielder, items, join_all};
use crate::tool_eviction::ToolEviction;
use crate::tool_search::TurnSearch;
use crate::tool_usage::{ToolUsageMemory, TurnMedia, carries_turn_images};
use crate::tools::ToolContext;
use crate::tools::ToolPolicy;
use crate::tools::ToolResult;
use crate::tools::toolbox::Toolbox;
use crate::turn_budget::TurnBudget;

/// One item of a turn's loop, in the order it happens.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TurnItem {
    /// Text the model wrote, as it came.
    Text(String),
    /// A call of the round starts: its start row.
    ToolStart(ToolCall),
    /// A call ended: its end row, in the round's call order.
    ToolEnd(ToolEnd),
    /// The loop ended: why, after how many rounds, what they used (RFC
    /// §6.4 "The turn's record").
    End(LoopEndMarker),
    /// The loop asks the model again after it answered: the text before is
    /// a segment of its own.
    SegmentBreak,
    /// A piece of the model's reasoning, as it came (RFC §6.7).
    Thinking(String),
    /// What a generation used, told after it (RFC §6.4).
    Usage(UsageMarker),
    /// The application's instruction the turn read, handed first, which the
    /// kit keeps as the agent's own row for its later turns (RFC §10.1.1).
    InstructionRead(String),
}

/// How a call ended.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ToolEnd {
    /// The call, with the arguments it ran with, or those the gate had when
    /// it stopped it (RFC §9.3).
    pub(crate) call: ToolCall,
    /// How it ended.
    pub(crate) outcome: ToolCallOutcome,
    /// What the model reads of it: a text, or its parts.
    pub(crate) result: ToolResult,
    /// How long it took, its gate included.
    pub(crate) duration: Duration,
    /// The structured copy a surface renders of its result, which the model
    /// never reads: what the kit's hooks left of it (RFC §9.3).
    pub(crate) structured_content: Option<Value>,
    pub(crate) references: Vec<String>,
}

/// The items of a turn's loop, or the provider's error that ended it.
pub(crate) type TurnItems<'a> = Items<'a, Result<TurnItem, ProviderError>>;

/// How an agent's loop runs: its bounds.
#[derive(Clone, Debug)]
pub(crate) struct Bounds {
    /// The most rounds of calls a turn runs (Appendix A.9, 50 by default).
    pub(crate) max_rounds: u32,
    /// How long the loop may run, read between rounds (Appendix A.9, 300
    /// seconds by default); `None` sets no deadline.
    pub(crate) loop_timeout: Option<Duration>,
    /// After how many rounds of calls the loop logs that it still runs (25
    /// by default).
    pub(crate) warn_after: u32,
    /// How many times a round with no call to run is tried again (1 by
    /// default).
    pub(crate) max_empty_retries: u32,
    /// The longest a call's tool may take (RFC §21.6, 30 seconds by
    /// default); `None` bounds nothing.
    pub(crate) tool_timeout: Option<Duration>,
    /// A bound per tool name, above `tool_timeout` and a tool's own.
    pub(crate) tool_timeouts: BTreeMap<String, Option<Duration>>,
}

impl Bounds {
    /// The bound of a call to `name` (RFC §21.6), Python's
    /// `ToolTimeouts.for_call(name, waits=...)`: the tool's own bound when
    /// `tool_timeouts` names it, else none for a tool that `waits` on a
    /// person by design, else the channel's.
    pub(crate) fn for_call(&self, name: &str, waits: bool) -> Option<Duration> {
        if let Some(own) = self.tool_timeouts.get(name) {
            return *own;
        }
        if waits { None } else { self.tool_timeout }
    }
}

/// One turn's loop: what it calls and how, owned, so that the turn's
/// stream outlives the turn's `on_event()`, which hands it to the kit.
pub(crate) struct Loop {
    /// The turn's one record, which a generation hook may replace (RFC §6.7).
    pub(crate) record: TurnRecord,
    /// The provider the turn runs on: the one its channel's router picked
    /// when the turn started, then the one a retry handed a generation to,
    /// which every later round stays on; the channel's own without a router
    /// (RFC §6.4).
    pub(crate) provider: Arc<dyn DynAiProvider>,
    /// What the router was asked at the turn's start, which a retry asks
    /// again; `None` for a channel serving one provider.
    pub(crate) route_request: Option<crate::router::RouteRequest>,
    pub(crate) toolbox: Arc<Toolbox>,
    pub(crate) policy: ToolPolicy,
    pub(crate) bounds: Bounds,
    /// What the turn may spend (RFC §6.4); `None` sets no budget.
    pub(crate) budget: Option<TurnBudget>,
    /// How its generations recover from their provider's refusals.
    pub(crate) resilience: Resilience,
    pub(crate) system_prompt: Option<String>,
    /// How freely the model samples, handed to the provider.
    pub(crate) temperature: f64,
    /// The most tokens a generation writes, handed to the provider.
    pub(crate) max_tokens: Option<u32>,
    /// The turn's reasoning effort, handed to the provider.
    pub(crate) thinking_budget: Option<i64>,
    pub(crate) enable_thinking: Option<bool>,
    /// How a round's reasoning reaches the room's subscribers.
    pub(crate) thinking_publication: ThinkingPublication,
    pub(crate) text_publication: TextPublication,
    pub(crate) reasoning_effort: Option<String>,
    /// The JSON Schema the turn's answer satisfies (RFC §6.7), handed to the
    /// provider: its text is held until checked, and a loop that stops short
    /// of its answer fails `truncated`.
    pub(crate) response_schema: Option<Map<String, Value>>,
    /// The kit's turn, whose hooks each call runs; `None` outside the kit.
    pub(crate) turn: Option<TurnHost>,
    pub(crate) context: ToolContext,
    /// Whether the model writes each call's intent (RFC §6.4).
    pub(crate) intents: bool,
    /// Whether enabled intents are required in the model-facing schemas.
    pub(crate) intents_required: bool,
    /// The tools that ask for an intent; `None` asks every tool.
    pub(crate) intents_only: Option<HashSet<String>>,
    /// The messages the memory built (a summary), read before the
    /// conversation at every round (RFC §20.2).
    pub(crate) remembered: Vec<AiMessage>,
    /// The channel's memory, which compacts the turn's history between two
    /// of its tool rounds (RFC §20.1).
    pub(crate) memory: Arc<dyn MemoryProvider>,
    /// The channel's running loops, which this one joins as it starts
    /// (RFC §21.3).
    pub(crate) loops: Arc<Loops>,
    /// What the turn's hooks left of its context (RFC §6.4).
    pub(crate) adopted: Adopted,
    pub(crate) deferred: DeferredTools,
    /// The author images the turn's requests send, which a refusal of its
    /// first request marks refused in the room (RFC §6.4).
    pub(crate) sent_images: Option<crate::author_images::SentImages>,
    /// The images the room's tool calls returned, which a call records and
    /// a refusal of the turn's first request forgets (RFC §6.4).
    pub(crate) tool_usage: Arc<ToolUsageMemory>,
    /// The results of parts the turn's calls answered and the images its
    /// input replays, which each request carries.
    pub(crate) media: TurnMedia,
    /// Where the turn reports its spans and metrics.
    pub(crate) telemetry: TurnTelemetry,
    /// The turn's `llm.generate` span, once it runs.
    pub(crate) turn_span: Option<TurnSpan>,
    /// The turn's Tool Search, which reads its reveals (RFC §6.4).
    pub(crate) search: Arc<TurnSearch>,
    /// What each of the turn's rounds said, by its message's index: its
    /// reasoning, its text and its calls in the order they came, replayed
    /// within the turn, never stored (RFC §6.4).
    pub(crate) said: HashMap<usize, Vec<AiContentPart>>,
    /// What the turn's calls and answers repeated (RFC §6.4).
    pub(crate) repeat_guard: RepeatGuard,
    /// The channel's stored results, which bound what the model reads of
    /// each call (RFC §21.5).
    pub(crate) eviction: Arc<ToolEviction>,
    /// Whether the room stored a result when the turn began: its generation
    /// hooks then see `read_stored_result`, which every round declares
    /// either way (RFC §6.4).
    pub(crate) reread_shown: bool,
    /// The application's instruction the turn read, which its stream hands
    /// the kit first, to keep for its later turns (RFC §10.1.1); none for a
    /// turn no instruction opened, or one that stands alone.
    pub(crate) instruction: Option<String>,
}

impl Loop {
    /// Round `round`'s generation, its reasoning observed and kept in
    /// `spent`.
    async fn read_round(
        &mut self,
        messages: &[Message],
        asked: (Vec<crate::tools::AiTool>, Option<String>),
        (steering, out, prefix, round, limits): (
            &Steering,
            &Yielder<Result<TurnItem, ProviderError>>,
            &str,
            u32,
            &Limits,
        ),
    ) -> Result<Generation, Failed> {
        let mut windows = self.round_windows(round);
        let reading = (steering, out, prefix, &mut windows, limits);
        let read = self.generate(messages, asked, reading).await;
        read.map(|generation| Generation {
            thinking: windows.thinking.text(),
            ..generation
        })
    }

    /// The windows of round `round`'s reasoning and answer text, for the
    /// turn's observers.
    fn round_windows(&self, round: u32) -> RoundWindows {
        let ids = (&self.context.room_id, &self.context.channel_id);
        RoundWindows {
            thinking: ThinkingWindow::new(
                self.turn.as_ref(),
                ids,
                round,
                self.thinking_publication,
            ),
            text: TextWindow::new(self.turn.as_ref(), ids, round, self.text_publication),
        }
    }

    /// The loop's items on `conversation`, as its reader pulls them. Its span
    /// opens at the first pull: a stream nobody read, muted or cut, opened
    /// none, as Python's generator never started.
    pub(crate) fn run(mut self, conversation: Vec<Message>) -> TurnItems<'static> {
        items(move |out| async move {
            let (room_id, channel_id) = (&self.context.room_id, &self.context.channel_id);
            let name = self.provider.dyn_type_name();
            self.turn_span = self.telemetry.start_turn(room_id, channel_id, name);
            self.rounds(conversation, out).await;
        })
    }

    /// Runs rounds until the model answers, the round bound or the deadline
    /// passes, a host cancels it, or the provider fails; a round with no
    /// call to run may be tried again, in a round's place (Appendix A.9).
    /// The loop joins the channel's running loops as it starts, and reads
    /// the directives steered into it at its stops (RFC §21.3).
    async fn rounds(
        mut self,
        mut messages: Vec<Message>,
        out: Yielder<Result<TurnItem, ProviderError>>,
    ) {
        let turn_id = self.turn.as_ref().map(|turn| turn.turn_id().to_owned());
        let steering = self
            .loops
            .start(Some(self.context.room_id.clone()), turn_id);
        let mut system_prompt = self.system_prompt.clone();
        let mut spent = Spent::starting();
        let mut had_tool_round = false;
        let mut tool_rounds = 0;
        let mut prefix = String::new();
        let mut limits = Limits::starting(&self.bounds, self.budget.clone());
        let mut nudged = false;
        self.open_declaration(&messages);
        if steering.read(&mut messages, &mut system_prompt) {
            let ending = (&limits, &steering, &out);
            return self.end(LoopEndReason::Cancelled, 0, spent, ending).await;
        }
        self.hand_instruction(&out).await;
        for round in 0..=self.bounds.max_rounds {
            if steering.cancelled() {
                let ending = (&limits, &steering, &out);
                return self
                    .end(LoopEndReason::Cancelled, tool_rounds, spent, ending)
                    .await;
            }
            nudged = nudged || self.tell_it_to_stop(round, &mut messages);
            // Declared as the provider is about to be asked, nothing reported
            // of a round a cancel kept from running (RFC §6.4).
            let asked = (self.round_tools(), system_prompt.clone());
            let reading = (&steering, &out, prefix.as_str(), round, &limits);
            let read = self.read_round(&messages, asked, reading).await;
            let read = read.and_then(|read| self.counted(read, &mut spent, &mut limits));
            let mut generation = match read {
                Ok(generation) => generation,
                Err(failure) => {
                    self.refused(round == 0 && !failure.received, &failure.error, &messages);
                    let ending = (&limits, &steering, &out);
                    let failed = (had_tool_round, tool_rounds, spent);
                    return self.failed(failed, failure, ending).await;
                }
            };
            out.give(Ok(TurnItem::Usage(self.usage_of(&generation))))
                .await;
            self.take_intents(&mut generation.calls);
            // What the room saw (RFC §6.4, The turn's record).
            spent.segments.push(generation.delivered.clone());
            let ending = (&limits, &steering, &out);
            if let Some(stop) = self.stop_after_round(&generation, &steering) {
                return self.end(stop, tool_rounds, spent, ending).await;
            }
            if generation.calls.is_empty() {
                let tried = (had_tool_round, &generation);
                if try_again(&mut limits, tried, &mut messages, &out).await {
                    continue;
                }
                let finish_reason = generation.done.finish_reason.as_deref();
                let limit = limits.passed();
                let reason =
                    final_round_reason(had_tool_round, &generation.text, finish_reason, limit);
                let asked = (&steering, &mut messages, &mut system_prompt);
                if reason == LoopEndReason::Completed
                    && round < self.bounds.max_rounds
                    && self.steered_after_answer(generation, asked)
                {
                    out.give(Ok(TurnItem::SegmentBreak)).await;
                    continue;
                }
                let ending = (&limits, &steering, &out);
                return self.end(reason, tool_rounds, spent, ending).await;
            }
            if round == self.bounds.max_rounds {
                break;
            }
            if let Some(limit) = limits.reached(tool_rounds) {
                let ending = (&limits, &steering, &out);
                return self.end(limit, tool_rounds, spent, ending).await;
            }
            had_tool_round = true;
            limits.warn_if_due(tool_rounds);
            // A later round that opens by replaying this one's words is
            // handed to the room from where it goes on.
            if !generation.text.is_empty() {
                prefix.clone_from(&generation.text);
            }
            self.said
                .extend(generation.said().map(|said| (messages.len(), said)));
            let usage = generation.done.usage.clone();
            let (read, ran) = self.run_calls(generation, (&steering, round), &out).await;
            self.keep_round_results(messages.len(), &ran.results);
            messages.extend(read);
            // A round counts once its calls ran (Python's `_run_announced_calls`).
            self.count_calls(&mut spent, ran.calls.len());
            tool_rounds += u32::from(!ran.calls.is_empty());
            self.between_rounds(round, ran, &mut messages, &usage).await;
            if steering.read(&mut messages, &mut system_prompt) {
                let ending = (&limits, &steering, &out);
                return self
                    .end(LoopEndReason::Cancelled, tool_rounds, spent, ending)
                    .await;
            }
        }
        // The round bound passed, a try in the last round's place included.
        tracing::warn!(target: "roomkit::ai", max_rounds = self.bounds.max_rounds, "the tool loop reached its round bound");
        let ending = (&limits, &steering, &out);
        self.end(LoopEndReason::MaxRounds, tool_rounds, spent, ending)
            .await;
    }

    /// Hands the kit the instruction the turn read, before anything else
    /// of the turn, to keep for its later turns (RFC §10.1.1).
    async fn hand_instruction(&mut self, out: &Yielder<Result<TurnItem, ProviderError>>) {
        if let Some(instruction) = self.instruction.take() {
            out.give(Ok(TurnItem::InstructionRead(instruction))).await;
        }
    }

    /// Whether a message steered in while the model wrote `generation`, its
    /// answer, asks it again (RFC §21.3): the answer then joins the
    /// conversation as it came, its reasoning and what its provider keeps for
    /// its replay with it (RFC §6.4), then the message.
    fn steered_after_answer(
        &mut self,
        generation: Generation,
        (steering, messages, system_prompt): (&Steering, &mut Vec<Message>, &mut Option<String>),
    ) -> bool {
        let said = generation.said();
        let mut answer = Message::new(Role::Assistant, generation.text);
        answer.metadata = replayed(generation.done.metadata);
        let at = messages.len();
        let again = steering.read_after_answer(messages, system_prompt, answer);
        if again {
            self.said.extend(said.map(|said| (at, said)));
        }
        again
    }

    /// Why the loop stops after `generation`, if it does (Python's
    /// `_round_stop_reason`): a stop that came after the model's last event
    /// counts as one that came during it (RFC §21.3), and the generation after
    /// the guard's force stop was the last, whatever it said, none of its calls
    /// run (RFC §6.4).
    fn stop_after_round(
        &self,
        generation: &Generation,
        steering: &Steering,
    ) -> Option<LoopEndReason> {
        if generation.cancelled || steering.cancelled() {
            return Some(LoopEndReason::Cancelled);
        }
        self.repeat_guard
            .force_stopped()
            .then_some(LoopEndReason::ForceStopped)
    }

    /// Whether the guard force-stopped the turn before round `round`, so that
    /// its generation is told, its tools still declared (RFC §6.4).
    fn tell_it_to_stop(&self, round: u32, messages: &mut Vec<Message>) -> bool {
        if !self.repeat_guard.force_stopped() {
            return false;
        }
        tracing::warn!(target: "roomkit::ai", round, "the guard force-stopped a model repeating a call");
        messages.push(Message::new(Role::User, FORCE_STOP_NUDGE));
        true
    }

    /// Ends the loop for `reason` after `rounds` rounds of calls, with what
    /// its generations used, under its `limits`: it reads the directives
    /// `steering` holds no more (RFC §21.3), then tells `out`.
    async fn end(&self, reason: LoopEndReason, rounds: u32, spent: Spent, ending: Ending<'_>) {
        let (limits, steering, out) = ending;
        if let Some(error) = unanswered(self.response_schema.is_some(), reason) {
            return self.fail_unanswered(error, steering, out).await;
        }
        steering.close();
        if let Some(span) = &self.turn_span {
            span.ended(reason);
        }
        let end = LoopEndMarker {
            usage: spent.usage.clone(),
            last_usage: Some(spent.last.clone()),
            usage_complete: Some(spent.complete),
            ..self.end_of(reason, rounds, limits.timeout)
        };
        out.give(Ok(TurnItem::End(end))).await;
        // Once the reader read past the end, as Python's turn reports it
        // when its stream is read to its close.
        self.report(reason, rounds, &spent);
    }

    /// Fails a turn under a response schema whose loop stopped short of its
    /// answer with `error` (RFC §6.7): the turn delivers no answer and
    /// reports none, its span ending in error.
    async fn fail_unanswered(
        &self,
        error: ProviderError,
        steering: &Steering,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) {
        steering.close();
        if let Some(span) = &self.turn_span {
            span.failed(&error);
        }
        out.give(Err(error)).await;
    }

    /// The provider failed with `error` on a request on `messages`, the
    /// turn's `first` before anything streamed: the only one that can say
    /// its content was refused, the author images it sent then refused in
    /// the room, and the tool images it replayed replayed no more (RFC
    /// §6.4, Python's `_forget_sent_images`).
    fn refused(&self, first: bool, error: &ProviderError, messages: &[Message]) {
        if !first {
            return;
        }
        if refuses_content(error)
            && carries_turn_images(&self.round_context(messages, None).messages)
        {
            self.tool_usage.forget_images(&self.context.room_id);
        }
        if let Some(sent) = &self.sent_images {
            sent.refused_by(error);
        }
    }

    /// Counts the `calls` of a round into what the turn `spent` and on its
    /// span.
    fn count_calls(&self, spent: &mut Spent, calls: usize) {
        let calls = u32::try_from(calls).unwrap_or(u32::MAX);
        spent.calls += calls;
        if let Some(span) = &self.turn_span {
            span.calls_ran(calls);
        }
    }

    /// Counts what `generation` used into what the turn `spent` and
    /// against its `limits`. A usage its turn's price cannot read fails the
    /// generation, after it handed its text, as Python's
    /// `ModelPricing.cost_for` raises in the loop: the failure of the
    /// provider that served it.
    fn counted(
        &self,
        generation: Generation,
        spent: &mut Spent,
        limits: &mut Limits,
    ) -> Result<Generation, Failed> {
        spent.count(&generation);
        if let Some(span) = &self.turn_span {
            for report in &generation.reports {
                span.generation_used(report);
            }
        }
        let priced = generation.reports.iter();
        let Err(error) = priced.into_iter().try_for_each(|used| limits.count(used)) else {
            // A round that failed reports no reasoning, as Python keeps a
            // round's once its generation returned.
            spent.add_thinking(generation.thinking.clone());
            return Ok(generation);
        };
        // The provider that served it: the turn's, which a retry the router
        // handed to another provider moved to that one (RFC §6.4).
        let options = ProviderErrorOptions {
            provider: self.provider.dyn_name().to_owned(),
            ..ProviderErrorOptions::default()
        };
        Err(Failed {
            error: ProviderError::new(error.to_string(), options).with_source(error),
            streamed: true,
            received: true,
            delivered: generation.delivered,
            reports: Vec::new(),
        })
    }

    /// The provider failed with `failure`: after a round of calls, the loop
    /// ends `error`, its rounds kept (RFC §6.4 "A turn cut short"), and the
    /// turn is reported, the text the failed round handed the room its last
    /// segment, what its done events reported counted; then the error.
    /// `failed` is whether a round ran, the rounds of calls and what the
    /// turn spent. A response schema's check refusing the answer is the
    /// answer failing, no interruption; a turn under a schema interrupted
    /// has no answer to deliver, and fails `truncated` (RFC Appendix A.9).
    async fn failed(
        &self,
        (after_round, rounds, mut spent): (bool, u32, Spent),
        failure: Failed,
        ending: Ending<'_>,
    ) {
        let (limits, steering, out) = ending;
        steering.close();
        let Failed {
            error,
            delivered,
            reports,
            ..
        } = failure;
        let interrupted = after_round && ResponseSchemaError::of(&error).is_none();
        let constrained = self.response_schema.is_some();
        let unanswered = unanswered(constrained && interrupted, LoopEndReason::Error);
        let interrupted = interrupted && unanswered.is_none();
        let error = unanswered.unwrap_or(error);
        spent.segments.push(delivered);
        for report in &reports {
            add_usage(&mut spent.usage, report);
        }
        if let Some(span) = &self.turn_span {
            for report in &reports {
                span.generation_used(report);
            }
            span.failed(&error);
        }
        if interrupted {
            let end = LoopEndMarker {
                usage: spent.usage.clone(),
                usage_complete: Some(false),
                ..self.end_of(LoopEndReason::Error, rounds, limits.timeout)
            };
            out.give(Ok(TurnItem::End(end))).await;
            self.report(LoopEndReason::Error, rounds, &spent);
        }
        out.give(Err(error)).await;
    }

    /// What a round's generation is asked on `messages` under
    /// `system_prompt`, its tools aside: the memory's messages then the
    /// conversation, or what the turn's hooks gave in their place, then
    /// the rest (RFC §6.4, §20.2).
    fn round_context(&self, messages: &[Message], system_prompt: Option<String>) -> AiContext {
        let shown = self.shown(messages);
        // What the hooks or a compaction left holds the replayed images.
        let asked = if let Some((given, replaced)) = &self.adopted.messages {
            given
                .iter()
                .cloned()
                .chain(shown.into_iter().skip(*replaced))
                .collect()
        } else {
            let conversation = shown.len();
            let asked = after_memory(&self.remembered, shown);
            self.media.with_turn_images(asked, conversation)
        };
        AiContext {
            messages: asked,
            system_prompt,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            thinking_budget: self.thinking_budget,
            enable_thinking: self.enable_thinking,
            reasoning_effort: self.reasoning_effort.clone(),
            tools: Vec::new(),
            response_schema: self.response_schema.clone(),
            metadata: self.adopted.metadata.clone(),
            response_metadata: self.record.current(),
        }
    }

    /// Places the images the turn replays at its input, the last of
    /// `messages`, the conversation its first round reads (RFC §6.4).
    pub(crate) fn place_turn_images(&mut self, messages: &[Message]) {
        let shown = ai_messages(&self.shown_messages(messages));
        self.media.place(&shown);
    }

    /// `messages` as a round shows them to the provider: each call's
    /// intent as the model wrote it, a round's results one message, a
    /// result of parts as its call answered it (RFC §6.4).
    pub(crate) fn shown(&self, messages: &[Message]) -> Vec<AiMessage> {
        ai_messages_with(
            &self.shown_messages(messages),
            self.media.results(),
            &self.said,
        )
    }

    /// The end of the loop for `reason` after `rounds` rounds of calls, the
    /// round bound and the deadline it ran under, and the model and
    /// provider of its last generation: the turn's provider, which a retry
    /// the router handed to another moved to that one (RFC §6.4); no usage
    /// yet.
    fn end_of(
        &self,
        reason: LoopEndReason,
        rounds: u32,
        timeout: Option<Duration>,
    ) -> LoopEndMarker {
        let (model, provider) = self.served_by();
        LoopEndMarker {
            rounds,
            max_rounds: Some(self.bounds.max_rounds),
            timeout,
            budget_tokens: self.budget.as_ref().and_then(|budget| budget.tokens),
            budget_usd: self.budget.as_ref().and_then(|budget| budget.usd),
            model: Some(model.to_owned()),
            provider: Some(provider.to_owned()),
            ..LoopEndMarker::new(reason)
        }
    }

    /// The model and the provider of the turn's latest generation: the
    /// provider the turn is on (RFC §6.4).
    fn served_by(&self) -> (&str, &str) {
        (self.provider.dyn_model_name(), self.provider.dyn_name())
    }

    /// What `generation` tells of itself once it returned.
    fn usage_of(&self, generation: &Generation) -> UsageMarker {
        let (model, provider) = self.served_by();
        UsageMarker {
            usage: generation.done.usage.clone(),
            model: Some(model.to_owned()),
            provider: Some(provider.to_owned()),
        }
    }

    /// Runs round `round`'s calls, each starting once the reader read every
    /// start, concurrently, their ends handed over in call order; returns
    /// what the next generation reads of the round: its message with its
    /// calls, then each result. A `Cancel` that came while the calls were
    /// announced stops them all: none runs, each ends cancelled (RFC
    /// §21.3). The room's subscribers read the calls' start before they
    /// run and their end once the reader took their end rows, or, when the
    /// round is cut, their own end for the end rows the reader asked past
    /// and an aborted end for the others (RFC §6.4, §8.4).
    async fn run_calls(
        &self,
        generation: Generation,
        (steering, round): (&Steering, u32),
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> (Vec<Message>, Ran) {
        for called in &generation.calls {
            out.give(Ok(TurnItem::ToolStart(called.call.clone()))).await;
        }
        let unrun = steering.cancelled();
        // A round a `Cancel` keeps from running publishes nothing of its
        // calls, as Python's ends before it executes them.
        let ids = (&self.context.room_id, &self.context.channel_id);
        let answers_room = self.thinking_publication.answers_room && !unrun;
        let events = ToolEvents::new(self.turn.as_ref(), ids, round, answers_room);
        let made = generation.calls.iter().map(|called| &called.call);
        let mut running = Running::start(events, made);
        let started = Instant::now();
        let ends = if unrun {
            generation
                .calls
                .iter()
                .map(|called| self.unrun(called.call.clone()))
                .collect()
        } else {
            let calls = &generation.calls;
            let serving = calls
                .iter()
                .enumerate()
                .map(|(position, called)| self.serve(called, place_under_its_id(calls, position)));
            join_all(serving.collect()).await
        };
        if running.published() {
            let ended = ends.iter().map(|end| Ended {
                call: end.call.clone(),
                result: end.result.as_text(),
                failed: end.outcome != ToolCallOutcome::Served,
            });
            let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            running.calls_ended(ended.collect(), duration_ms);
        }
        let mut ran = Ran::default();
        if !unrun {
            ran.calls = generation.calls.iter().map(Called::replayed).collect();
        }
        let made: Vec<ToolCall> = generation
            .calls
            .into_iter()
            .map(|called| called.call)
            .collect();
        let mut assistant = Message::assistant_calls(generation.text, made);
        assistant.metadata = replayed(generation.done.metadata);
        let mut read = vec![assistant];
        // Before the next round's calls, which may read what these left.
        for end in &ends {
            self.toolbox.settled(&end.call, end.outcome, &self.context);
        }
        for end in ends {
            let mut result = Message::tool_result(end.call.id.clone(), end.result.as_text());
            // The model reads every outcome but served as a failure (RFC §9.3,
            // Python's `ToolOutcome.as_part`).
            result.is_error = end.outcome != ToolCallOutcome::Served;
            if !unrun {
                ran.results.push(AiToolResultPart {
                    tool_call_id: end.call.id.clone(),
                    name: end.call.name.clone(),
                    result: end.result.clone(),
                    is_error: result.is_error,
                    references: end.references.clone(),
                });
            }
            read.push(result);
            out.give(Ok(TurnItem::ToolEnd(end))).await;
            running.took_end();
        }
        running.handed();
        self.search.next_round();
        (read, ran)
    }
}

/// The place of the `position`-th of `calls` among those under its id, `0`
/// for the earliest: two calls under one id are two calls (RFC §6.4).
fn place_under_its_id(calls: &[Called], position: usize) -> usize {
    let Some(called) = calls.get(position) else {
        return 0;
    };
    calls
        .iter()
        .take(position)
        .filter(|earlier| earlier.call.id == called.call.id)
        .count()
}

/// What a loop's end reads: its limits, its steering, its reader.
type Ending<'a> = (
    &'a Limits,
    &'a Steering,
    &'a Yielder<Result<TurnItem, ProviderError>>,
);

impl Loop {
    /// The end of an announced call a `Cancel` kept from running (RFC
    /// §21.3): failed, `cancelled`, reported to the kit's observers once as
    /// a call the turn cut (RFC §9.3).
    fn unrun(&self, call: ToolCall) -> ToolEnd {
        if let Some(turn) = &self.turn {
            let tool = serde_json::to_string(&call.name).unwrap_or_default();
            let read = format!(
                "{{\"error\": \"Tool call cancelled\", \"tool\": {tool}, \"hint\": \"The turn ended before its result.\"}}"
            );
            let report = ToolCallReport::new(call.clone())
                .ended(ToolCallOutcome::Cancelled, Value::String(read))
                .with_error_detail("cancelled".to_owned());
            turn.report_tool_call(&report);
        }
        ToolEnd {
            call,
            outcome: ToolCallOutcome::Cancelled,
            result: ToolResult::Text("cancelled".to_owned()),
            duration: Duration::ZERO,
            structured_content: None,
            references: Vec::new(),
        }
    }
}

/// Whether the round `generation` read, which made no call to run after a
/// round of calls or none (`had_tool_round`), is tried again under `limits`
/// (Appendix A.9): its text then ends its segment (RFC §6.4), and
/// `messages` take what the next generation reads after it.
async fn try_again(
    limits: &mut Limits,
    (had_tool_round, generation): (bool, &Generation),
    messages: &mut Vec<Message>,
    out: &Yielder<Result<TurnItem, ProviderError>>,
) -> bool {
    let finish_reason = generation.done.finish_reason.as_deref();
    let Some(said) = limits.try_again(had_tool_round, &generation.text, finish_reason) else {
        return false;
    };
    out.give(Ok(TurnItem::SegmentBreak)).await;
    messages.extend(said);
    true
}

/// What a turn's generations used: their counters summed, the last one's,
/// and whether every one reported its input and output tokens; and what
/// `ON_AI_RESPONSE` reports of the turn (RFC §6.4): each generation's text,
/// the calls it ran, its declaration, and when it started.
struct Spent {
    usage: Metadata,
    last: Metadata,
    complete: bool,
    segments: Vec<String>,
    calls: u32,
    started: Instant,
    /// Each round's reasoning, those that reasoned.
    thinking: Vec<String>,
}

impl Spent {
    /// Nothing spent yet, and nothing declared.
    fn starting() -> Self {
        Self {
            usage: Metadata::new(),
            last: Metadata::new(),
            complete: true,
            segments: Vec::new(),
            calls: 0,
            started: Instant::now(),
            thinking: Vec::new(),
        }
    }

    /// Keeps a round's reasoning, when it reasoned.
    fn add_thinking(&mut self, thinking: String) {
        if !thinking.is_empty() {
            self.thinking.push(thinking);
        }
    }

    /// Counts one generation's usage counters.
    fn count(&mut self, generation: &Generation) {
        let last = &generation.done.usage;
        for report in &generation.reports {
            add_usage(&mut self.usage, report);
        }
        self.complete &= reports_counters(last);
        self.last.clone_from(last);
    }
}

/// What a generation's end keeps for its round's replay: its provider's own
/// data, without what the response said of itself, the model that answered
/// and a refusal (Python's `answered_by`), which Python's round message
/// never carries.
fn replayed(mut metadata: Metadata) -> Metadata {
    for key in ["model", "refusal"] {
        metadata.shift_remove(key);
    }
    metadata
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn bounds(default: Option<u64>, own: &[(&str, Option<u64>)]) -> Bounds {
        Bounds {
            max_rounds: 50,
            loop_timeout: None,
            warn_after: 25,
            max_empty_retries: 1,
            tool_timeout: default.map(Duration::from_secs),
            tool_timeouts: own
                .iter()
                .map(|(name, bound)| ((*name).to_owned(), bound.map(Duration::from_secs)))
                .collect(),
        }
    }

    // Python: tests/test_tool_timeout.py::TestToolTimeouts::test_a_call_takes_the_default
    #[test]
    fn a_call_takes_the_default() {
        assert_eq!(
            bounds(Some(10), &[]).for_call("lookup", false),
            Some(Duration::from_secs(10))
        );
    }

    // Python: tests/test_tool_timeout.py::TestToolTimeouts::test_a_tool_bound_overrides_the_default
    #[test]
    fn a_tool_bound_overrides_the_default() {
        let bounds = bounds(Some(10), &[("report", Some(120)), ("export", None)]);
        assert_eq!(
            (
                bounds.for_call("report", false),
                bounds.for_call("export", false)
            ),
            (Some(Duration::from_secs(120)), None)
        );
    }

    // Python: tests/test_tool_timeout.py::TestToolTimeouts::test_a_tool_that_waits_keeps_its_own_bound_unless_named
    #[test]
    fn a_tool_that_waits_keeps_its_own_bound_unless_named() {
        assert_eq!(
            (
                bounds(Some(10), &[]).for_call("ask_user", true),
                bounds(Some(10), &[("ask_user", Some(30))]).for_call("ask_user", true)
            ),
            (None, Some(Duration::from_secs(30)))
        );
    }
}
