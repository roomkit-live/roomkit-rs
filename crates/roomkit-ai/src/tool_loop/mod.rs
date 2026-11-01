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
//! - `calls`: one call through its gate (RFC §21.1) and its tool, reported
//!   to the kit's hooks (RFC §9.3).

mod calls;
mod generation;
mod intents;

use std::sync::Arc;
use std::time::Duration;

use roomkit::TurnHost;
use roomkit::model::{LoopEndMarker, LoopEndReason, Metadata, ToolCall, ToolCallOutcome};
use roomkit_core::rules::transcript::Message;
use serde_json::Value;

use self::generation::Generation;
use crate::provider::{AiContext, AiProvider, ProviderError, ai_messages, ran_out_of_room};
use crate::stream::{Items, Yielder, items, join_all};
use crate::tools::ToolContext;
use crate::tools::ToolPolicy;
use crate::tools::toolbox::Toolbox;

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
}

/// How a call ended.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ToolEnd {
    /// The call, with the arguments it ran with, or those the gate had when
    /// it stopped it (RFC §9.3).
    pub(crate) call: ToolCall,
    /// How it ended.
    pub(crate) outcome: ToolCallOutcome,
    /// What the model reads of it.
    pub(crate) result: String,
    /// How long it took, its gate included.
    pub(crate) duration: Duration,
}

/// The items of a turn's loop, or the provider's error that ended it.
pub(crate) type TurnItems<'a> = Items<'a, Result<TurnItem, ProviderError>>;

/// How an agent's loop runs: its bounds and its system prompt.
#[derive(Clone, Debug)]
pub(crate) struct Bounds {
    /// The most rounds of calls a turn runs (Appendix A.9, 50 by default).
    pub(crate) max_rounds: u32,
    /// The longest a call's tool may take (RFC §21.6, 30 seconds by
    /// default); `None` bounds nothing.
    pub(crate) tool_timeout: Option<Duration>,
}

/// One turn's loop: what it calls and how, owned, so that the turn's
/// stream outlives the turn's `on_event()`, which hands it to the kit.
pub(crate) struct Loop<P> {
    pub(crate) provider: Arc<P>,
    pub(crate) toolbox: Arc<Toolbox>,
    pub(crate) policy: ToolPolicy,
    pub(crate) bounds: Bounds,
    pub(crate) system_prompt: Option<String>,
    /// How freely the model samples, handed to the provider.
    pub(crate) temperature: Option<f64>,
    /// The most tokens a generation writes, handed to the provider.
    pub(crate) max_tokens: Option<u32>,
    /// The kit's turn, whose hooks each call runs; `None` outside the kit.
    pub(crate) turn: Option<TurnHost>,
    pub(crate) context: ToolContext,
    /// Whether the model writes each call's intent (RFC §6.4).
    pub(crate) intents: bool,
    /// Whether enabled intents are required in the model-facing schemas.
    pub(crate) intents_required: bool,
}

impl<P: AiProvider> Loop<P> {
    /// The loop's items on `conversation`, as its reader pulls them.
    pub(crate) fn run(self, conversation: Vec<Message>) -> TurnItems<'static> {
        items(move |out| self.rounds(conversation, out))
    }

    /// Runs rounds until the model answers, the round bound passes, or the
    /// provider fails.
    async fn rounds(
        self,
        mut messages: Vec<Message>,
        out: Yielder<Result<TurnItem, ProviderError>>,
    ) {
        let declared = self.shown_tools(self.toolbox.declared(&self.policy));
        let mut usage = Metadata::new();
        let mut usage_complete = true;
        let mut had_tool_round = false;
        for round in 0..=self.bounds.max_rounds {
            let context = AiContext {
                messages: ai_messages(&self.shown_messages(&messages)),
                system_prompt: self.system_prompt.clone(),
                temperature: self.temperature,
                max_tokens: self.max_tokens,
                tools: declared.clone(),
                metadata: Metadata::new(),
            };
            let mut generation =
                match Generation::read(&*self.provider, &context, round, &out).await {
                    Ok(generation) => generation,
                    Err(error) => {
                        let end = LoopEndMarker {
                            usage,
                            usage_complete: Some(false),
                            ..self.end_of(LoopEndReason::Error, round)
                        };
                        return failed(round, end, error, &out).await;
                    }
                };
            self.take_intents(&mut generation.calls);
            add_usage(&mut usage, &generation.done.usage);
            usage_complete &= ["input_tokens", "output_tokens"].iter().all(|key| {
                generation
                    .done
                    .usage
                    .get(*key)
                    .and_then(Value::as_u64)
                    .is_some()
            });
            let end = |reason, usage| LoopEndMarker {
                usage,
                last_usage: Some(generation.done.usage.clone()),
                usage_complete: Some(usage_complete),
                ..self.end_of(reason, round)
            };
            if generation.calls.is_empty() {
                let reason = final_reason(&generation, had_tool_round);
                return out.give(Ok(TurnItem::End(end(reason, usage)))).await;
            }
            if round == self.bounds.max_rounds {
                tracing::warn!(target: "roomkit::ai", max_rounds = round, "the tool loop reached its round bound");
                let reason = LoopEndReason::MaxRounds;
                return out.give(Ok(TurnItem::End(end(reason, usage)))).await;
            }
            had_tool_round = true;
            messages.extend(self.round_of_calls(generation, &out).await);
        }
    }

    /// The end of the loop for `reason` after `rounds` rounds of calls, the
    /// round bound it ran under, its model and provider; no usage yet.
    fn end_of(&self, reason: LoopEndReason, rounds: u32) -> LoopEndMarker {
        LoopEndMarker {
            rounds,
            max_rounds: Some(self.bounds.max_rounds),
            model: Some(self.provider.model_name().to_owned()),
            provider: Some(self.provider.name().to_owned()),
            ..LoopEndMarker::new(reason)
        }
    }

    /// Runs a round's calls, each starting once the reader read every
    /// start, concurrently, their ends handed over in call order; returns
    /// what the next generation reads of the round: its message with its
    /// calls, then each result.
    async fn round_of_calls(
        &self,
        generation: Generation,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> Vec<Message> {
        for called in &generation.calls {
            out.give(Ok(TurnItem::ToolStart(called.call.clone()))).await;
        }
        let ends = join_all(
            generation
                .calls
                .iter()
                .map(|called| self.serve(called))
                .collect(),
        )
        .await;
        let made = generation
            .calls
            .into_iter()
            .map(|called| called.call)
            .collect();
        let mut assistant = Message::assistant_calls(generation.text, made);
        assistant.metadata = generation.done.metadata;
        let mut read = vec![assistant];
        // Before the next round's calls, which may read what these left.
        for end in &ends {
            self.toolbox.settled(&end.call, end.outcome, &self.context);
        }
        for end in ends {
            read.push(Message::tool_result(
                end.call.id.clone(),
                end.result.clone(),
            ));
            out.give(Ok(TurnItem::ToolEnd(end))).await;
        }
        read
    }
}

/// The provider failed in `round`: after a round, the loop ends `error`,
/// its rounds kept (RFC §6.4 "A turn cut short"); then the error.
async fn failed(
    round: u32,
    end: LoopEndMarker,
    error: ProviderError,
    out: &Yielder<Result<TurnItem, ProviderError>>,
) {
    if round > 0 {
        out.give(Ok(TurnItem::End(end))).await;
    }
    out.give(Err(error)).await;
}

/// Why a loop whose round made no call ends there (RFC §6.4): an answer, or
/// a turn that ran no call, completes; after a round of calls, no text reads
/// as the room running out, or as an empty answer.
fn final_reason(generation: &Generation, had_tool_round: bool) -> LoopEndReason {
    if !generation.text.trim().is_empty() || !had_tool_round {
        return LoopEndReason::Completed;
    }
    if ran_out_of_room(generation.done.finish_reason.as_deref()) {
        LoopEndReason::Truncated
    } else {
        LoopEndReason::EmptyResponse
    }
}

/// Adds `round`'s integer counters to `total`'s.
fn add_usage(total: &mut Metadata, round: &Metadata) {
    for (name, used) in round {
        let Some(used) = used.as_i64() else {
            continue;
        };
        let sum = total
            .get(name)
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .saturating_add(used);
        total.insert(name.clone(), Value::from(sum));
    }
}
