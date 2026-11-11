//! One call of a round: its gate, its tool, its hooks (RFC §9.3, §21.1,
//! §21.6).

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{Metadata, ToolCall, ToolCallOutcome};
use roomkit::{ToolCallReport, ToolGate};
use serde_json::Value;
use tokio::time::Instant;

use super::generation::{Called, Partial};
use super::{Loop, ToolEnd};
use crate::rows::end_row;
use crate::skills::ACTIVATE_SKILL;
use crate::texts;
use crate::tool_eviction::kept_whole;
use crate::tool_search::TOOL_SEARCH_INFRA_TOOL_NAMES;
use crate::tools::{ToolContext, ToolError, ToolResult, read_tool_result, repair, schema};

/// The kind a call past its bound reads as (RFC §21.6).
const TIMED_OUT: &str = "ToolTimeoutError";

/// What a call's gate decided.
enum Gated {
    /// It runs, as `call`; `asked` are the model's arguments, repaired,
    /// before any hook rewrote them.
    Runs { call: ToolCall, asked: Metadata },
    /// It was refused, and ends so.
    Refused(ToolCallReport),
    /// Model arguments rejected after retaining the tool schema.
    InvalidArguments(ToolCallReport),
}

impl Loop {
    /// Serves `called`, the `nth` of its round's calls under its id: its
    /// gate, then its tool, then the kit's hooks on a served result; tells
    /// the kit its end and reports it once to the kit's observers (RFC
    /// §9.3: every call fires `ON_TOOL_CALL` once), and ends it as the model
    /// reads it.
    pub(super) async fn serve(&self, called: &Called, nth: usize) -> ToolEnd {
        let started = Instant::now();
        let mut span = None;
        let (ended, asked, invalid_arguments) = match self.gate(called).await {
            Gated::Runs { call, asked } => {
                span = self
                    .turn_span
                    .as_ref()
                    .map(|turn| turn.start_call(&call.name, &call.id));
                // The arguments the guard counts it on, which its hooks may
                // rewrite once it ran.
                let ran_with = call.arguments.clone();
                (self.guarded(call).await, Some((asked, ran_with)), false)
            }
            Gated::Refused(refused) => (self.refused_again(called, refused), None, false),
            Gated::InvalidArguments(refused) => (self.refused_again(called, refused), None, true),
        };
        let (ended, parts) = self.shaped(ended);
        // A skill's activation the hooks left as it served it is read whole.
        let own = kept_whole(&ended.call.name).then(|| ended.result.clone());
        let ended = match &self.turn {
            Some(turn) => turn.on_tool_call(ended).await,
            None => ended,
        };
        if let Some(span) = span {
            span.end(raised_of(&ended));
        }
        let result = self.for_model(read_tool_result(&ended.result));
        let outcome = ended.outcome.unwrap_or(ToolCallOutcome::Failed);
        // What tells a repeat (Python's `outcome.answer`): the tool's own
        // text, or a hook's, not parts a model that takes no image reads as
        // text, which are told by the copy the model reads.
        let answer = match &result {
            ToolResult::Text(text) if parts.as_ref() != Some(&ended.result) => Some(text.clone()),
            ToolResult::Text(_) | ToolResult::Json(_) | ToolResult::Parts(_) => None,
        };
        // The call's own answer, neither replaced nor withheld by a hook.
        let served = outcome == ToolCallOutcome::Served && own.as_ref() == Some(&ended.result);
        let shown = self.model_copy(called, &ended.call, &result, served);
        let reported = ended.ended(outcome, result.clone().into_value());
        log_failure(&reported);
        let result = if let Some((asked, ran_with)) = asked {
            self.search
                .settle(&reported.call.id, &reported.call.name, outcome);
            self.remember(&reported.call.name, &asked, outcome, &result);
            self.forget_repeats_on_change(&reported.call.name, &ran_with, outcome);
            self.with_repeat_note(&reported.call.name, shown, answer.as_deref())
        } else {
            shown
        };
        let references = if outcome == ToolCallOutcome::Served || invalid_arguments {
            self.reference_shown()
        } else {
            Vec::new()
        };
        let end = ToolEnd {
            references,
            outcome,
            call: reported.call.clone(),
            result,
            duration: started.elapsed(),
            structured_content: reported.structured_content.clone(),
        };
        self.tell(called, nth, &reported, &end);
        end
    }

    /// Tells the kit how `called`, the `nth` of its round's calls under its
    /// id, ended, before its end is read: a cut of the turn closes it so
    /// (RFC §12.2 step 13s), as Python's `ToolCallStartMarker.ended`; then
    /// reports it to the kit's observers.
    fn tell(&self, called: &Called, nth: usize, reported: &ToolCallReport, end: &ToolEnd) {
        let Some(turn) = &self.turn else {
            return;
        };
        turn.call_ended(nth, end_row(end.clone()));
        if called.duplicate {
            turn.report_call_under_held_id(reported);
        } else {
            turn.report_tool_call(reported);
        }
    }

    /// `call`, past its gate, on its tool, unless the turn's guard refuses it
    /// as a repeat of the arguments it would run with (RFC §6.4).
    async fn guarded(&self, call: ToolCall) -> ToolCallReport {
        match self.repeat_refusal(&call.name, &call.arguments) {
            Some(words) => ended(call, ToolCallOutcome::Refused, words),
            None => self.execute(call).await,
        }
    }

    /// `refused`, a call its gate stopped, counted as an attempt on the model's
    /// own arguments: past the limit, the guard's words are the model's to read
    /// (Python's `_reject_call`). A call under a held id is no attempt: it never
    /// reached a gate (Python's `_refuse_id_in_flight`).
    fn refused_again(&self, called: &Called, refused: ToolCallReport) -> ToolCallReport {
        if called.duplicate {
            return refused;
        }
        match self.repeat_refusal(&called.call.name, &called.call.arguments) {
            Some(words) => refused.ended(ToolCallOutcome::Refused, Value::String(words)),
            None => refused,
        }
    }

    /// What the guard says of a call to `name` with `arguments`, Tool Search's
    /// own names, whose catalogue holds within the turn, the pure ones wherever
    /// the channel keeps them for itself, a search hiding anything or not
    /// (Python's `traits.pure`).
    fn repeat_refusal(&self, name: &str, arguments: &Metadata) -> Option<String> {
        let pure = TOOL_SEARCH_INFRA_TOOL_NAMES.contains(&name) && self.toolbox.is_reserved(name);
        self.repeat_guard.refusal(name, arguments, pure)
    }

    /// Starts the turn's repeat counts again once a call to `name` that ran
    /// with `ran_with` was served, when its tool changes state (RFC §6.4).
    /// Python's `_forget_repeats_on_change`.
    fn forget_repeats_on_change(&self, name: &str, ran_with: &Metadata, outcome: ToolCallOutcome) {
        let changes_state = self
            .toolbox
            .find(name)
            .is_some_and(|(tool, _)| tool.changes_state);
        if outcome == ToolCallOutcome::Served && changes_state {
            self.repeat_guard.forget_all_but(name, ran_with);
        }
    }

    /// `result`, what the model reads of a call past its gate, noted when its
    /// tool already gave that answer this turn: counted on `answer`, the
    /// tool's own, when it is a text, else on `result` itself, a stored
    /// copy's placeholder included; a result of parts a model that sees reads
    /// is not compared (Python's `_model_part`).
    fn with_repeat_note(&self, name: &str, result: ToolResult, answer: Option<&str>) -> ToolResult {
        match result {
            ToolResult::Text(text) => ToolResult::Text(self.repeat_guard.noted(name, text, answer)),
            ToolResult::Json(_) | ToolResult::Parts(_) => result,
        }
    }

    /// `ended`, its result of parts read as text for a model that takes no
    /// image (RFC §6.4), so that the hooks read the shape the model reads,
    /// and that text, when it was so read. Python's `_shape_for_model`,
    /// before `_apply_tool_call_hook`.
    fn shaped(&self, mut ended: ToolCallReport) -> (ToolCallReport, Option<Value>) {
        let ToolResult::Parts(parts) = read_tool_result(&ended.result) else {
            return (ended, None);
        };
        if self.provider.dyn_supports_vision() {
            return (ended, None);
        }
        let text = Value::String(ToolResult::Parts(parts).as_text());
        ended.result = text.clone();
        (ended, Some(text))
    }

    /// `result`, the model's copy of an outcome of `call` as `called`
    /// reached it: bounded, the observers reading it whole as the hooks did
    /// (RFC §21.5), save a call refused before any gate for coming under an
    /// id its round holds, which Python's `_refuse_id_in_flight` never bounds.
    fn model_copy(
        &self,
        called: &Called,
        call: &ToolCall,
        result: &ToolResult,
        served: bool,
    ) -> ToolResult {
        if called.duplicate {
            return result.clone();
        }
        self.bounded(call, result.clone(), served)
    }

    /// `result`, the model's copy of an outcome of `call`, bounded as every
    /// outcome is, after the hooks read it whole (RFC §21.5): stored and
    /// previewed when past the agent's threshold, save a skill's
    /// instructions its activation `served` itself, which the model reads
    /// whole (RFC §24.4). Python's `_bound_tool_result`.
    fn bounded(&self, call: &ToolCall, result: ToolResult, served: bool) -> ToolResult {
        if served && kept_whole(&call.name) {
            return result;
        }
        self.eviction
            .bounded(self.context.room_id.as_str(), result, &call.id)
    }

    /// `result` as the model reads it: parts as they are for a model that
    /// sees, else their text, an image an `[image]` mark, as a message's
    /// (RFC §6.4): an image a model that does not see cannot take would
    /// fail the request. Python's `_shape_for_model`.
    fn for_model(&self, result: ToolResult) -> ToolResult {
        match result {
            ToolResult::Parts(_) if !self.provider.dyn_supports_vision() => {
                ToolResult::Text(result.as_text())
            }
            ToolResult::Text(_) | ToolResult::Json(_) | ToolResult::Parts(_) => result,
        }
    }

    /// Records a call that ran with the model's `asked` arguments and
    /// ended `outcome` with `result`, in the room's tool memory: the images
    /// it returned ride later turns (RFC §6.4). A refusal, a call nothing
    /// served or one cancelled stands for no answer, and the channel's own
    /// tools are not the agent's work: neither is kept. Python's
    /// `_remember_call` and `kept_in_tool_memory`.
    fn remember(
        &self,
        name: &str,
        asked: &Metadata,
        outcome: ToolCallOutcome,
        result: &ToolResult,
    ) {
        let kept = matches!(
            outcome,
            ToolCallOutcome::Served | ToolCallOutcome::Failed | ToolCallOutcome::Blocked
        );
        if kept && self.toolbox.in_tool_memory(name) {
            self.tool_usage
                .record(&self.context.room_id, name, asked, result);
        }
    }

    /// The call `called` may run as, or its refusal: one under an id its round
    /// holds first (RFC §12.4), then in the order of RFC §21.1 "One refusal per
    /// cause": a call that never runs, one to a tool withdrawn for the turn (RFC
    /// §6.4), one the turn does not carry, one the policy denies, arguments that
    /// do not fit the tool's schema once repaired, then the kit's
    /// `BEFORE_TOOL_USE` hooks, whose arguments are checked again, unrepaired.
    async fn gate(&self, called: &Called) -> Gated {
        let model_call = &called.call;
        let refuse = |text: String| {
            Gated::Refused(ended(model_call.clone(), ToolCallOutcome::Refused, text))
        };
        // Before any gate, whatever is wrong with it otherwise: the first call
        // holds the id, and this one reports as its own (RFC §12.4).
        if called.duplicate {
            return refuse(texts::call_id_in_flight(&model_call.id));
        }
        if let Some(partial) = called.partial {
            return refuse(match partial {
                Partial::Cut => texts::cut(&model_call.name),
                Partial::Unreadable => texts::unreadable(&model_call.name),
            });
        }
        // A tool withdrawn for the turn is gone, the channel's own
        // included (RFC §6.4).
        if self.adopted.withdrawn.contains(&model_call.name) {
            return refuse(texts::not_available(&model_call.name));
        }
        let Some((tool, _)) = self.toolbox.find(&model_call.name) else {
            // While a catalogue is hidden, a name no tool carries may be a
            // tool the model misspelled: it can search (RFC §21.1).
            return refuse(if self.search.active() {
                texts::no_such_tool(&model_call.name)
            } else {
                texts::not_declared(&model_call.name)
            });
        };
        if !self.toolbox.permits(&self.policy, &model_call.name) {
            return refuse(texts::not_permitted(&model_call.name));
        }
        // A hidden tool the model named: a search applied at call time, every
        // guard after this one still standing (RFC §6.4).
        self.search.note_recovery(&model_call.name);
        let call = match self.repaired(model_call, &tool.parameters) {
            Ok(call) => call,
            Err(refused) => {
                // Its next attempt needs the schema on the wire (RFC §6.4).
                self.search.retain(&model_call.name);
                return Gated::InvalidArguments(*refused);
            }
        };
        let asked = call.arguments.clone();
        let Some(turn) = &self.turn else {
            return Gated::Runs { call, asked };
        };
        let arguments = match turn.before_tool_use(&call).await {
            ToolGate::Allowed(arguments) => arguments,
            ToolGate::Refused { text, error_detail } => {
                let refused = ended(call, ToolCallOutcome::Refused, text);
                return Gated::Refused(match error_detail {
                    Some(detail) => refused.with_error_detail(detail),
                    None => refused,
                });
            }
            // A gate this loop does not know refuses: it fails closed.
            _ => return refuse(texts::error_envelope("The call was not allowed.")),
        };
        let mut gated = call.clone();
        if arguments != call.arguments {
            gated.arguments = arguments;
            if let Some(why) = schema::mismatch(&tool.parameters, &gated.arguments) {
                let text = texts::invalid_rewritten_arguments(&call.name, &why);
                return Gated::Refused(ended(gated, ToolCallOutcome::Refused, text));
            }
        }
        Gated::Runs { call: gated, asked }
    }

    /// The model's `call` with its arguments repaired for the tool's
    /// `parameters` (RFC §21.1, Python's `repair_tool_arguments`), or its
    /// refusal: both hub forms at once, the model's arguments as they came,
    /// or repaired arguments that still do not fit the schema.
    fn repaired(
        &self,
        call: &ToolCall,
        parameters: &Value,
    ) -> Result<ToolCall, Box<ToolCallReport>> {
        let repair = repair::repair(parameters, &call.arguments);
        if let Some(why) = repair.error {
            tracing::warn!(target: "roomkit::ai", tool = %call.name, %why, "a tool call's arguments are ambiguous");
            let text = texts::invalid_arguments(&call.name, &why);
            return Err(Box::new(ended(
                call.clone(),
                ToolCallOutcome::Refused,
                text,
            )));
        }
        let model = self.provider.dyn_model_name();
        if !repair.folded.is_empty() {
            tracing::info!(target: "roomkit::ai", tool = %call.name, folded = ?repair.folded, model, "folded a call's hoisted arguments into its container");
        }
        if !repair.unquoted.is_empty() {
            tracing::info!(target: "roomkit::ai", tool = %call.name, unquoted = ?repair.unquoted, model, "read a call's quoted arguments as their declared types");
        }
        let mut repaired = call.clone();
        repaired.arguments = repair.arguments;
        if let Some(why) = schema::mismatch(parameters, &repaired.arguments) {
            tracing::warn!(target: "roomkit::ai", tool = %call.name, %why, "a tool call's arguments were refused");
            let text = texts::invalid_arguments(&call.name, &why);
            return Err(Box::new(ended(repaired, ToolCallOutcome::Refused, text)));
        }
        Ok(repaired)
    }

    /// What `call` is told of its turn: an activation also what its hint may
    /// name and reveal (RFC §24.4).
    fn call_context(&self, call: &ToolCall) -> ToolContext {
        let mut context = self.context.clone();
        if call.name == ACTIVATE_SKILL {
            context.search = Some(Arc::clone(&self.search));
            context.hintable = self
                .declared()
                .into_iter()
                .map(|tool| tool.name)
                .filter(|name| self.toolbox.hintable(name))
                .collect();
        }
        context
    }

    /// Runs `call` on the tool serving it, within the agent's bound per
    /// call (RFC §21.6), and reads its answer (RFC §9.3).
    async fn execute(&self, call: ToolCall) -> ToolCallReport {
        let Some((_, toolset)) = self.toolbox.find(&call.name) else {
            let text = texts::not_declared(&call.name);
            return ended(call, ToolCallOutcome::Refused, text);
        };
        let context = self.call_context(&call);
        // The tool gets its own call: its intent is the person's, as a
        // handler of the reference implementation never reads it (RFC §6.4).
        let mut handed = call.clone();
        handed.intent = None;
        let answer = toolset.call(handed, context);
        // A person's answer is bounded by its handler's timeout alone, unless
        // the tool has its own bound.
        let bound = self.bounds.for_call(&call.name, toolset.bounds_itself());
        let answered = match bound {
            Some(bound) => tokio::time::timeout(bound, answer)
                .await
                .map_err(|_elapsed| bound),
            None => Ok(answer.await),
        };
        match answered {
            Ok(Ok(output)) => served(call, output.with_parts_read()),
            Ok(Err(error)) => failure(call, error),
            Err(bound) => timed_out(call, bound),
        }
    }
}

/// What the tool raised, for its span, when the call ended `Failed`, or was
/// refused past its gate, as Python's `_judged_call` reads a raise.
fn raised_of(ended: &ToolCallReport) -> Option<String> {
    let raised = matches!(
        ended.outcome,
        Some(ToolCallOutcome::Failed | ToolCallOutcome::Refused)
    );
    raised.then(|| {
        ended
            .error_detail
            .clone()
            .unwrap_or_else(|| read_tool_result(&ended.result).as_text())
    })
}

/// `call`, served with `output`, a text, JSON or parts as its hooks read
/// them.
fn served(call: ToolCall, output: ToolResult) -> ToolCallReport {
    ToolCallReport::new(call).ended(ToolCallOutcome::Served, output.into_value())
}

/// A call whose tool answered `error`: a refusal's and a failure's words
/// as they are, a raised error by its kind, its message for the
/// observers (RFC §9.3).
fn failure(call: ToolCall, error: ToolError) -> ToolCallReport {
    match error {
        ToolError::Refused(words) => ended(call, ToolCallOutcome::Refused, words),
        ToolError::UnservedToolCall(message) => {
            tracing::debug!(target: "roomkit::ai", tool = %call.name, %message, "a tool declined a call");
            let text = texts::unserved(&call.name);
            ended(call, ToolCallOutcome::Unserved, text)
        }
        ToolError::Failed(words) => {
            ended(call, ToolCallOutcome::Failed, words.clone()).with_error_detail(words)
        }
        ToolError::Raised { kind, message } => {
            let text = texts::failed(&call.name, &kind);
            ended(call, ToolCallOutcome::Failed, text)
                .with_error_detail(format!("{kind}: {message}"))
        }
    }
}

/// A call whose tool did not answer within `bound`, cut: it fails as a
/// tool that raised (RFC §21.6).
fn timed_out(call: ToolCall, bound: Duration) -> ToolCallReport {
    let detail = format!(
        "{TIMED_OUT}: tool '{}' did not answer within {} s",
        call.name,
        bound.as_secs_f64()
    );
    let text = texts::failed(&call.name, TIMED_OUT);
    ended(call, ToolCallOutcome::Failed, text).with_error_detail(detail)
}

/// `call`, ended with `outcome`, the model reading `text`.
fn ended(call: ToolCall, outcome: ToolCallOutcome, text: String) -> ToolCallReport {
    ToolCallReport::new(call).ended(outcome, Value::String(text))
}

/// Logs a call that did not serve, with what failed (RFC §9.3: the
/// message goes to the log and the observers).
fn log_failure(report: &ToolCallReport) {
    if report.outcome == Some(ToolCallOutcome::Served) {
        return;
    }
    tracing::info!(
        target: "roomkit::ai",
        tool = %report.call.name,
        call = %report.call.id,
        outcome = ?report.outcome,
        detail = report.error_detail.as_deref().unwrap_or_default(),
        "a tool call did not serve"
    );
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use crate::tools::{ToolResult, read_tool_result};

    // Python: tests/test_tool_results.py::test_a_return_value_outside_the_contract_reaches_the_model_as_json
    /// A handler's JSON value reaches the model as Python's `as_tool_result`
    /// writes it, `json.dumps(value, ensure_ascii=False)`.
    #[test]
    fn a_return_value_outside_the_contract_reaches_the_model_as_json() {
        let cases = [
            (json!(["a", "b"]), r#"["a", "b"]"#),
            (json!([{"id": 1}]), r#"[{"id": 1}]"#),
            (
                json!({"a": 1, "b": [true, null]}),
                r#"{"a": 1, "b": [true, null]}"#,
            ),
            (json!(null), "null"),
            (json!(42), "42"),
            (json!({"city": "Montréal"}), r#"{"city": "Montréal"}"#),
        ];
        for (value, read) in cases {
            assert_eq!(read_tool_result(&value), ToolResult::Text(read.to_owned()));
        }
    }
}
