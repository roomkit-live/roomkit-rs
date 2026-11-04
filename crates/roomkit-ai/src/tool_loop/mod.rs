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
//!   compacted, tried again, then handed to the fallback provider (RFC
//!   §6.4);
//! - `compaction`: a context the provider refused as too long, compacted
//!   for its replay (RFC §6.4 "A turn compacted");
//! - `calls`: one call through its gate (RFC §21.1) and its tool, reported
//!   to the kit's hooks (RFC §9.3).
//!
//! The loop reads the directives a host steers into it (RFC §21.3,
//! `crate::steering`) at its stops: at its start, after each round of
//! calls, and once the model has answered, when a message steered in
//! meanwhile asks the model again in the same turn. A `Cancel` also stops
//! a generation at its next event, and the round's calls announced but not
//! yet run.

mod calls;
mod compaction;
mod footprint;
mod generation;
mod hooks;
mod intents;
mod resilience;
mod rules;
mod spans;
mod thinking;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    LoopEndMarker, LoopEndReason, Metadata, ToolCall, ToolCallOutcome, UsageMarker, add_usage,
    reports_counters,
};
use roomkit::{ToolCallReport, TurnHost, TurnRecord};
use roomkit_core::rules::transcript::{Message, Role};
use serde_json::Value;
use spans::TurnSpan;
pub(crate) use spans::TurnTelemetry;
use tokio::time::Instant;

use self::generation::{Failed, Generation};
pub(crate) use self::hooks::Adopted;
use self::hooks::Ran;
pub(crate) use self::resilience::Resilience;
use self::rules::{Limits, final_round_reason};
pub(crate) use self::thinking::ThinkingPublication;
use self::thinking::ThinkingWindow;
use crate::author_images::refuses_content;
use crate::loop_events::DeclaredTool;
use crate::memory::after_memory;
use crate::provider::{
    AiContentPart, AiContext, AiMessage, AiProvider, AiToolCall, AiToolResultPart, ProviderError,
    ProviderErrorOptions, ai_messages, ai_messages_with,
};
use crate::steering::{Loops, Steering};
use crate::stream::{Items, Yielder, items, join_all};
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
pub(crate) struct Loop<P> {
    /// The turn's one record, which a generation hook may replace (RFC §6.7).
    pub(crate) record: TurnRecord,
    pub(crate) provider: Arc<P>,
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
    pub(crate) reasoning_effort: Option<String>,
    /// The kit's turn, whose hooks each call runs; `None` outside the kit.
    pub(crate) turn: Option<TurnHost>,
    pub(crate) context: ToolContext,
    /// Whether the model writes each call's intent (RFC §6.4).
    pub(crate) intents: bool,
    /// Whether enabled intents are required in the model-facing schemas.
    pub(crate) intents_required: bool,
    /// The messages the memory built (a summary), read before the
    /// conversation at every round (RFC §20.2).
    pub(crate) remembered: Vec<AiMessage>,
    /// The channel's running loops, which this one joins as it starts
    /// (RFC §21.3).
    pub(crate) loops: Arc<Loops>,
    /// What the turn's hooks left of its context (RFC §6.4).
    pub(crate) adopted: Adopted,
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
}

impl<P: AiProvider> Loop<P> {
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
        let mut window = self.thinking_window(round);
        let reading = (steering, out, prefix, &mut window, limits);
        let read = self.generate(messages, asked, reading).await;
        read.map(|generation| Generation {
            thinking: window.text(),
            ..generation
        })
    }

    /// The window of round `round`'s reasoning, for the turn's observers.
    fn thinking_window(&self, round: u32) -> ThinkingWindow {
        ThinkingWindow::new(
            self.turn.as_ref(),
            (&self.context.room_id, &self.context.channel_id),
            round,
            self.thinking_publication,
        )
    }

    /// The loop's items on `conversation`, as its reader pulls them.
    pub(crate) fn run(mut self, conversation: Vec<Message>) -> TurnItems<'static> {
        items(move |out| {
            let (room_id, channel_id) = (&self.context.room_id, &self.context.channel_id);
            self.turn_span = self.telemetry.start_turn::<P>(room_id, channel_id);
            self.rounds(conversation, out)
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
        self.find_turn_input(&messages, system_prompt.clone());
        if steering.read(&mut messages, &mut system_prompt) {
            let ending = (&limits, &steering, &out);
            return self.end(LoopEndReason::Cancelled, 0, spent, ending).await;
        }
        for round in 0..=self.bounds.max_rounds {
            if steering.cancelled() {
                let ending = (&limits, &steering, &out);
                return self
                    .end(LoopEndReason::Cancelled, tool_rounds, spent, ending)
                    .await;
            }
            // Declared as the provider is about to be asked, nothing reported
            // of a round a cancel kept from running (RFC §6.4).
            let asked = (self.round_tools(&mut spent), system_prompt.clone());
            let reading = (&steering, &out, prefix.as_str(), round, &limits);
            let read = self.read_round(&messages, asked, reading).await;
            let read = read.and_then(|read| self.counted(read, &mut spent, &mut limits));
            let mut generation = match read {
                Ok(generation) => generation,
                Err(failure) => {
                    self.refused(round == 0 && !failure.streamed, &failure.error, &messages);
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
            // A stop that came after the model's last event counts as one
            // that came during it (RFC §21.3).
            if generation.cancelled || steering.cancelled() {
                return self
                    .end(LoopEndReason::Cancelled, tool_rounds, spent, ending)
                    .await;
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
                let said = generation.said();
                let answer = Message::new(Role::Assistant, generation.text);
                let at = messages.len();
                if reason == LoopEndReason::Completed
                    && round < self.bounds.max_rounds
                    && steering.read_after_answer(&mut messages, &mut system_prompt, answer)
                {
                    self.said.extend(said.map(|said| (at, said)));
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
            let (read, ran) = self.round_of_calls(generation, &steering, &out).await;
            self.media.keep_results(messages.len(), &ran.results);
            messages.extend(read);
            // A round counts once its calls ran (Python's `_run_announced_calls`).
            self.count_calls(&mut spent, ran.calls.len());
            tool_rounds += u32::from(!ran.calls.is_empty());
            messages.extend(self.after_tool_round(round, ran).await);
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

    /// Ends the loop for `reason` after `rounds` rounds of calls, with what
    /// its generations used, under its `limits`: it reads the directives
    /// `steering` holds no more (RFC §21.3), then tells `out`.
    async fn end(&self, reason: LoopEndReason, rounds: u32, spent: Spent, ending: Ending<'_>) {
        let (limits, steering, out) = ending;
        steering.close();
        if let Some(span) = &self.turn_span {
            span.ended(reason);
        }
        let end = LoopEndMarker {
            usage: spent.usage.clone(),
            last_usage: Some(spent.last.clone()),
            usage_complete: Some(spent.complete),
            ..self.end_of(reason, rounds, limits.timeout, spent.by_fallback)
        };
        out.give(Ok(TurnItem::End(end))).await;
        // Once the reader read past the end, as Python's turn reports it
        // when its stream is read to its close.
        self.report(reason, rounds, &spent);
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
    /// provider that served it, the fallback's when it did.
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
        let fallback = self.resilience.fallback_provider.as_ref();
        let provider = match fallback.filter(|_| generation.by_fallback) {
            Some(fallback) => fallback.dyn_name(),
            None => self.provider.name(),
        };
        let options = ProviderErrorOptions {
            provider: provider.to_owned(),
            ..ProviderErrorOptions::default()
        };
        Err(Failed {
            error: ProviderError::new(error.to_string(), options).with_source(error),
            streamed: true,
            delivered: generation.delivered,
            reports: Vec::new(),
        })
    }

    /// The provider failed with `failure`: after a round of calls, the loop
    /// ends `error`, its rounds kept (RFC §6.4 "A turn cut short"), and the
    /// turn is reported, the text the failed round handed the room its last
    /// segment, what its done events reported counted; then the error.
    /// `failed` is whether a round ran, the rounds of calls and what the
    /// turn spent.
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
        spent.segments.push(delivered);
        for report in &reports {
            add_usage(&mut spent.usage, report);
        }
        if let Some(span) = &self.turn_span {
            for report in &reports {
                span.generation_used(report);
            }
            span.failed(error.to_string());
        }
        if after_round {
            let end = LoopEndMarker {
                usage: spent.usage.clone(),
                usage_complete: Some(false),
                ..self.end_of(
                    LoopEndReason::Error,
                    rounds,
                    limits.timeout,
                    spent.by_fallback,
                )
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
    /// provider of its last generation, the fallback's when it served it
    /// (`by_fallback`); no usage yet.
    fn end_of(
        &self,
        reason: LoopEndReason,
        rounds: u32,
        timeout: Option<Duration>,
        by_fallback: bool,
    ) -> LoopEndMarker {
        let (model, provider) = self.served_by(by_fallback);
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

    /// The model and the provider of a generation, its fallback's when it
    /// served it.
    fn served_by(&self, by_fallback: bool) -> (&str, &str) {
        let fallback = self.resilience.fallback_provider.as_ref();
        match fallback.filter(|_| by_fallback) {
            Some(fallback) => (fallback.dyn_model_name(), fallback.dyn_name()),
            None => (self.provider.model_name(), self.provider.name()),
        }
    }

    /// What `generation` tells of itself once it returned.
    fn usage_of(&self, generation: &Generation) -> UsageMarker {
        let (model, provider) = self.served_by(generation.by_fallback);
        UsageMarker {
            usage: generation.done.usage.clone(),
            model: Some(model.to_owned()),
            provider: Some(provider.to_owned()),
        }
    }

    /// Runs a round's calls, each starting once the reader read every
    /// start, concurrently, their ends handed over in call order; returns
    /// what the next generation reads of the round: its message with its
    /// calls, then each result. A `Cancel` that came while the calls were
    /// announced stops them all: none runs, each ends cancelled (RFC
    /// §21.3).
    async fn round_of_calls(
        &self,
        generation: Generation,
        steering: &Steering,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> (Vec<Message>, Ran) {
        for called in &generation.calls {
            out.give(Ok(TurnItem::ToolStart(called.call.clone()))).await;
        }
        let unrun = steering.cancelled();
        let ends = if unrun {
            generation
                .calls
                .iter()
                .map(|called| self.unrun(called.call.clone()))
                .collect()
        } else {
            join_all(
                generation
                    .calls
                    .iter()
                    .map(|called| self.serve(called))
                    .collect(),
            )
            .await
        };
        let made: Vec<ToolCall> = generation
            .calls
            .into_iter()
            .map(|called| called.call)
            .collect();
        let mut ran = Ran::default();
        if !unrun {
            ran.calls = made.iter().map(ai_tool_call).collect();
        }
        let mut assistant = Message::assistant_calls(generation.text, made);
        assistant.metadata = generation.done.metadata;
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
                });
            }
            read.push(result);
            out.give(Ok(TurnItem::ToolEnd(end))).await;
        }
        self.search.next_round();
        (read, ran)
    }
}

/// What a loop's end reads: its limits, its steering, its reader.
type Ending<'a> = (
    &'a Limits,
    &'a Steering,
    &'a Yielder<Result<TurnItem, ProviderError>>,
);

impl<P: AiProvider> Loop<P> {
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

/// `call` as `AFTER_TOOL_ROUND` hands it: Python's `AIToolCall`.
fn ai_tool_call(call: &ToolCall) -> AiToolCall {
    AiToolCall {
        arguments: call.arguments.clone(),
        intent: call.intent.clone(),
        ..AiToolCall::new(call.id.clone(), call.name.clone())
    }
}

/// What a turn's generations used: their counters summed, the last one's,
/// and whether every one reported its input and output tokens; and what
/// `ON_AI_RESPONSE` reports of the turn (RFC §6.4): each generation's text,
/// the calls it ran, its declaration, and when it started.
struct Spent {
    usage: Metadata,
    last: Metadata,
    /// The fallback provider served the last generation (RFC §6.4).
    by_fallback: bool,
    complete: bool,
    segments: Vec<String>,
    calls: u32,
    declared: Vec<DeclaredTool>,
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
            by_fallback: false,
            complete: true,
            segments: Vec::new(),
            calls: 0,
            declared: Vec::new(),
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

    /// Adds what a round declared to the turn's declaration: one entry per
    /// name, in first-declaration order, a later round's reveal kept once a
    /// search dropped it (RFC §6.4).
    fn declare(&mut self, round: impl Iterator<Item = DeclaredTool>) {
        for tool in round {
            if !self.declared.iter().any(|known| known.name == tool.name) {
                self.declared.push(tool);
            }
        }
    }

    /// Counts one generation's usage counters, and which provider served
    /// it.
    fn count(&mut self, generation: &Generation) {
        let last = &generation.done.usage;
        self.by_fallback = generation.by_fallback;
        for report in &generation.reports {
            add_usage(&mut self.usage, report);
        }
        self.complete &= reports_counters(last);
        self.last.clone_from(last);
    }
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
