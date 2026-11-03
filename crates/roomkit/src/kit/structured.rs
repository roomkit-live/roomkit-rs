//! A delegation that requires a structured result (RFC §23.3): the worker
//! hands its work back by calling the result tool, served in the child room
//! alone, and is re-prompted when a turn ends without the call. Python's
//! `_run_with_structured_result`, `_owed_result`,
//! `_scan_for_submitted_result`, `capture_result` and
//! `orchestration.result`'s `SUBMIT_RESULT`.

use std::sync::Arc;

use roomkit_core::model::{
    ChannelCategory, ChannelId, EventContent, EventType, Metadata, RoomEvent, RoomId,
    ToolCallOutcome, ToolCallStatus,
};
use serde_json::{Value, json};

use super::RoomKit;
use super::background_tools::collision;
use super::tasks::{TaskEnd, failed, kit_failure, task_outcome};
use crate::error::RoomKitError;
use crate::store::{ConversationStore, ListEventsOptions};
use crate::tasks::json::{dumps_object, json as write_json};
use crate::tasks::{DelegatedTask, ResultTool};
use crate::turn::OwnerToolAnswer;
use crate::turns::served::OwnerToolEntry;

/// The result tool a delegation forces by default: Python's
/// `SUBMIT_RESULT_TOOL_NAME`.
const SUBMIT_RESULT: &str = "submit_result";

/// What a turn ending without the call is told: Python's
/// `SUBMIT_RESULT.reminder`.
const REMINDER: &str = "You did not submit a result. You MUST now call the `submit_result` tool \
with your final structured result. Do NOT reply with plain text or a question — call submit_result.";

/// How many of the child room's latest events the scan reads, where the
/// worker's last call lives: Python's `limit=100`.
const SCANNED: u64 = 100;

/// A cursor past any index, so the scan reads the room's tail: Python's
/// `_LATEST_TAIL_CURSOR`, the largest 32-bit integer.
const LATEST_TAIL_CURSOR: u64 = 2_147_483_647;

impl<S: ConversationStore> RoomKit<S> {
    /// The task's turns when it requires a structured result (RFC §23.3):
    /// the result tool served in the child room alone while they run, the
    /// worker re-prompted with its reminder up to `retries` times, then the
    /// tool's missing payload on its behalf. A worker whose turns never
    /// declare the owner's tools has its answer collected as text. Python's
    /// `_run_with_structured_result`.
    pub(super) async fn structured_turns(
        &self,
        task: &DelegatedTask,
        context: Option<&Metadata>,
        retries: u32,
        result_tool: Option<&ResultTool>,
    ) -> TaskEnd {
        if !self.takes_owner_tools(&task.agent_id) {
            return Box::pin(self.delegated_turn(task, context)).await;
        }
        let tool = result_tool.map_or(Forced::SubmitResult, Forced::Given);
        let served = ServedResult::serve(self, &task.agent_id, &task.child_room_id, tool);
        let _served = match served {
            Ok(served) => served,
            Err(error) => return failed(kit_failure(&error), context),
        };
        let mut message = task.description.clone();
        let mut last_output = String::new();
        let mut turns = Metadata::new();
        for _attempt in 0..=retries {
            let answers = match Box::pin(self.delegated_answers(task, message)).await {
                Ok(answers) => answers,
                Err(failure) => return failed(failure, context),
            };
            let outcome = task_outcome(&answers, &task.agent_id);
            turns = outcome.turns;
            let submitted = self
                .submitted_result(&task.child_room_id, &task.agent_id, tool)
                .await;
            // A turn cut short, or failed after it began, keeps a result it
            // submitted before, and fails the task without one.
            if let Some(failure) = outcome.failure
                && submitted.is_none()
            {
                return TaskEnd::finished(None, Some(failure), context, turns);
            }
            if let Some(payload) = submitted {
                return TaskEnd::finished(Some(payload), None, context, turns);
            }
            if let Some(text) = outcome.response.filter(|text| !text.is_empty()) {
                last_output = text;
            }
            message = String::from(tool.reminder());
        }
        let attempts = retries.saturating_add(1);
        tracing::warn!(target: "roomkit::tasks", task = %task.id, agent = %task.agent_id, attempts, tool = tool.name(), "a delegated agent never called its result tool");
        let missing = tool.missing(task.agent_id.as_str(), &last_output, attempts);
        TaskEnd::finished(Some(missing), None, context, turns)
    }

    /// Whether `agent` is an intelligence channel whose turns declare the
    /// tools the room's owner serves (RFC §10.6.5): Python's
    /// `_takes_owner_tools`.
    fn takes_owner_tools(&self, agent: &ChannelId) -> bool {
        self.inner.channel(agent).is_some_and(|channel| {
            let declaration = channel.declaration();
            declaration.category == ChannelCategory::Intelligence && declaration.takes_owner_tools
        })
    }

    /// The payload of the worker's last call of `tool` in its child room
    /// that ended served, normalized and written; `None` when there is none.
    /// A refused, failed or blocked call is no result, and another
    /// channel's call is not the worker's (RFC §23.3). Python's
    /// `_scan_for_submitted_result`.
    async fn submitted_result(
        &self,
        room_id: &RoomId,
        worker: &ChannelId,
        tool: Forced<'_>,
    ) -> Option<String> {
        let options = ListEventsOptions {
            before_index: Some(LATEST_TAIL_CURSOR),
            limit: SCANNED,
            ..ListEventsOptions::default()
        };
        let events = self.store().list_events(room_id, options).await.ok()?;
        events
            .iter()
            .rev()
            .find_map(|event| submitted_by(event, worker, tool))
    }
}

/// The payload `event` carries, written, when it is `worker`'s call of
/// `tool` that ended served; a row written before outcomes existed says it
/// by its status (Python's `_served`).
fn submitted_by(event: &RoomEvent, worker: &ChannelId, tool: Forced<'_>) -> Option<String> {
    if event.r#type != EventType::ToolCallEnd || &event.source.channel_id != worker {
        return None;
    }
    let EventContent::ToolCall(call) = &event.content else {
        return None;
    };
    let served = match call.outcome {
        Some(outcome) => outcome == ToolCallOutcome::Served,
        None => call.status == ToolCallStatus::Completed,
    };
    (tool.matches(&call.tool_name) && served).then(|| tool.payload(&call.arguments))
}

/// The tool a structured delegation forces: `submit_result`, Python's
/// `SUBMIT_RESULT`, or the one the caller gave.
#[derive(Clone, Copy)]
enum Forced<'tool> {
    SubmitResult,
    Given(&'tool ResultTool),
}

impl<'tool> Forced<'tool> {
    fn name(self) -> &'tool str {
        match self {
            Self::SubmitResult => SUBMIT_RESULT,
            Self::Given(tool) => tool.name(),
        }
    }

    fn definition(self) -> Value {
        match self {
            Self::SubmitResult => submit_result_definition(),
            Self::Given(tool) => tool.tool.clone(),
        }
    }

    fn reminder(self) -> &'tool str {
        match self {
            Self::SubmitResult => REMINDER,
            Self::Given(tool) => &tool.reminder,
        }
    }

    fn matches(self, tool_name: &str) -> bool {
        match self {
            Self::SubmitResult => matches_result_tool(tool_name),
            Self::Given(tool) => tool.matches(tool_name),
        }
    }

    /// A served call's `arguments` as the payload returned, written as
    /// Python's `json.dumps` writes it.
    fn payload(self, arguments: &Metadata) -> String {
        match self {
            Self::SubmitResult => dumps(&normalize(arguments)),
            Self::Given(tool) => written((tool.normalize)(arguments)),
        }
    }

    /// The payload returned for a worker that never called the tool.
    fn missing(self, role: &str, last_output: &str, attempts: u32) -> String {
        match self {
            Self::SubmitResult => dumps(&missing_payload(role, last_output, attempts)),
            Self::Given(tool) => written((tool.on_missing)(role, last_output, attempts)),
        }
    }
}

/// A payload a caller's function built, written in its keys' order.
fn written(payload: Metadata) -> String {
    let mut text = String::new();
    write_json(&mut text, &Value::Object(payload), true);
    text
}

/// The result tool served for one worker in one child room, withdrawn
/// when dropped: Python's `capture_result`.
struct ServedResult<'kit, S> {
    kit: &'kit RoomKit<S>,
    agent: ChannelId,
    room_id: RoomId,
    name: String,
}

impl<'kit, S: ConversationStore> ServedResult<'kit, S> {
    /// Serves the result tool to `agent`'s turns in `room_id` alone, its
    /// call answered `{"status": "received"}`, its payload read from the
    /// call's stored end once `ON_TOOL_CALL` judged it.
    fn serve(
        kit: &'kit RoomKit<S>,
        agent: &ChannelId,
        room_id: &RoomId,
        tool: Forced<'_>,
    ) -> Result<Self, RoomKitError> {
        let name = tool.name().to_owned();
        let own = kit
            .inner
            .channel(agent)
            .map(|channel| channel.own_tool_names())
            .unwrap_or_default();
        if own.contains(&name) {
            return Err(collision(&name, "one of the channel's own tools", agent));
        }
        let entry = OwnerToolEntry {
            definition: tool.definition(),
            serve: Arc::new(|_kit, _call| {
                Box::pin(async { OwnerToolAnswer::Served(Value::String(received())) })
            }),
            waits: false,
            owner: None,
        };
        kit.inner
            .owner_tools
            .register_in_room(agent, room_id, entry)
            .map_err(|clash| collision(&name, &clash, agent))?;
        Ok(Self {
            kit,
            agent: agent.clone(),
            room_id: room_id.clone(),
            name,
        })
    }
}

impl<S> Drop for ServedResult<'_, S> {
    fn drop(&mut self) {
        self.kit
            .inner
            .owner_tools
            .unregister_in_room(&self.agent, &self.room_id, &self.name);
    }
}

/// The result tool's answer to its call: Python's
/// `json.dumps({"status": "received"})`.
fn received() -> String {
    "{\"status\": \"received\"}".to_owned()
}

/// Whether `tool_name` is a call of the result tool: its name, or its name
/// as an MCP server exposes it (`mcp__<server>__<name>`). Python's
/// `ResultTool.matches`.
fn matches_result_tool(tool_name: &str) -> bool {
    tool_name == SUBMIT_RESULT || tool_name.ends_with(&format!("__{SUBMIT_RESULT}"))
}

/// A result call's arguments in the payload's shape, in Python's order:
/// Python's `normalize_result`.
fn normalize(arguments: &Metadata) -> Payload {
    let truthy = |key: &str| arguments.get(key).filter(|value| is_truthy(value));
    let text = |key: &str| Value::String(truthy(key).map_or_else(String::new, value_text));
    let shaped = |key: &str, keep: fn(&Value) -> bool, empty: Value| {
        arguments
            .get(key)
            .filter(|value| keep(value))
            .cloned()
            .unwrap_or(empty)
    };
    vec![
        (
            "status",
            truthy("status")
                .cloned()
                .unwrap_or_else(|| json!("completed")),
        ),
        ("summary", text("summary")),
        ("data", shaped("data", Value::is_object, json!({}))),
        (
            "deliverables",
            shaped("deliverables", Value::is_array, json!([])),
        ),
        ("reason", text("reason")),
    ]
}

/// What a delegation returns for a worker that never called the result
/// tool after `attempts` tries: Python's `orchestration_fail`, a failure of
/// the mechanism, not the worker's own.
fn missing_payload(role: &str, last_output: &str, attempts: u32) -> Payload {
    vec![
        ("status", json!("failed")),
        ("by", json!("orchestration")),
        (
            "reason",
            json!(format!("no_structured_result_after_{attempts}_attempts")),
        ),
        ("role", json!(role)),
        ("last_output", json!(last_output)),
    ]
}

/// A payload's members, in the order Python's dict holds them.
type Payload = Vec<(&'static str, Value)>;

/// `payload` as Python's `json.dumps` writes it.
fn dumps(payload: &Payload) -> String {
    dumps_object(payload.iter().map(|(name, value)| (*name, value)), true)
}

/// Whether Python reads `value` as true.
fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// A value as Python's `str` writes it: a string itself, unquoted.
fn value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        other @ (Value::Null | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
            other.to_string()
        }
    }
}

/// The result tool's definition: Python's `SUBMIT_RESULT_TOOL`.
fn submit_result_definition() -> Value {
    json!({
        "name": SUBMIT_RESULT,
        "description": "Submit your FINAL result for this task. You MUST call this exactly once, \
    when your work is done — it is the ONLY way to hand your work to the next step. Do NOT end your \
    turn with a plain message or a question back to the user; call submit_result with your \
    structured result.",
        "parameters": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["completed", "failed"],
                    "description": "completed if you did the task; failed only if you genuinely could not.",
                },
                "summary": {
                    "type": "string",
                    "description": "One or two sentences summarizing what you produced.",
                },
                "data": {
                    "type": "object",
                    "description": "Your structured result, for the next step to build on.",
                },
                "deliverables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "url": {"type": "string"},
                        },
                    },
                    "description": "Concrete artifacts you produced (e.g. a published report URL).",
                },
                "reason": {
                    "type": "string",
                    "description": "If status is failed, explain why.",
                },
            },
            "required": ["status", "summary"],
        },
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use roomkit_core::model::{ChannelType, EventId, EventSource, Timestamp, ToolCallContent};

    use super::*;

    // Python: tests/tasks/test_structured_result.py (normalize_result)
    #[test]
    fn a_result_call_is_normalized_as_python_s() {
        let mut arguments = Metadata::new();
        arguments.insert("summary".to_owned(), json!(12));
        arguments.insert("data".to_owned(), json!("not an object"));
        arguments.insert("deliverables".to_owned(), json!([{ "title": "t" }]));
        assert_eq!(
            dumps(&normalize(&arguments)),
            "{\"status\": \"completed\", \"summary\": \"12\", \"data\": {}, \"deliverables\": \
             [{\"title\": \"t\"}], \"reason\": \"\"}"
        );
    }

    // Python: tests/tasks/test_structured_result.py::TestResultHelpers::test_normalize_coerces_bad_types
    #[test]
    fn a_result_s_data_and_deliverables_of_another_type_are_emptied() {
        let mut arguments = Metadata::new();
        arguments.insert("summary".to_owned(), json!("s"));
        arguments.insert("data".to_owned(), json!("not-a-dict"));
        arguments.insert("deliverables".to_owned(), json!("nope"));
        assert_eq!(
            dumps(&normalize(&arguments)),
            "{\"status\": \"completed\", \"summary\": \"s\", \"data\": {}, \"deliverables\": [], \
             \"reason\": \"\"}"
        );
    }

    fn end_row(channel: &str, tool: &str, outcome: ToolCallOutcome) -> RoomEvent {
        let mut call = ToolCallContent::new(tool, "c1");
        call.arguments.insert("summary".to_owned(), json!("mine"));
        call.status = ToolCallStatus::Completed;
        call.outcome = Some(outcome);
        let source = EventSource::new(ChannelId::new(channel).unwrap(), ChannelType::AI);
        RoomEvent::at(
            EventId::new("e1").unwrap(),
            RoomId::new("r::task-1").unwrap(),
            EventType::ToolCallEnd,
            source,
            call,
            Timestamp::UNIX_EPOCH,
        )
    }

    // Python: tests/tasks/test_structured_result.py::TestOnlyTheWorkersServedResultCounts::test_the_scan_ignores_it
    #[test]
    fn the_scan_ignores_a_call_refused_failed_or_another_channel_s() {
        let worker = ChannelId::new("w1").unwrap();
        let rows = [
            end_row("w1", SUBMIT_RESULT, ToolCallOutcome::Refused),
            end_row("w1", SUBMIT_RESULT, ToolCallOutcome::Failed),
            end_row("other", SUBMIT_RESULT, ToolCallOutcome::Served),
        ];
        assert!(
            rows.iter()
                .all(|row| submitted_by(row, &worker, Forced::SubmitResult).is_none())
        );
    }

    // Python: tests/tasks/test_structured_result.py::TestOnlyTheWorkersServedResultCounts::test_the_scan_takes_the_workers_served_call
    // Python: tests/tasks/test_structured_result.py::TestResultHelpers::test_normalize_fills_defaults
    #[test]
    fn the_scan_takes_the_worker_s_served_call() {
        let worker = ChannelId::new("w1").unwrap();
        let row = end_row("w1", SUBMIT_RESULT, ToolCallOutcome::Served);
        assert_eq!(
            submitted_by(&row, &worker, Forced::SubmitResult),
            Some(
                "{\"status\": \"completed\", \"summary\": \"mine\", \"data\": {}, \
                 \"deliverables\": [], \"reason\": \"\"}"
                    .to_owned()
            )
        );
    }

    // Python: tests/tasks/test_structured_result.py::TestAnotherResultTool::test_a_prefixed_gateway_call_is_found_in_the_trace
    #[test]
    fn a_prefixed_call_of_a_given_tool_is_found_in_the_trace() {
        let given = ResultTool::new(
            json!({ "name": "submit_verdict" }),
            Arc::new(|arguments: &Metadata| arguments.clone()),
            Arc::new(|_: &str, _: &str, _: u32| Metadata::new()),
            "Call it.",
        );
        let worker = ChannelId::new("w1").unwrap();
        let row = end_row(
            "w1",
            "mcp__gateway__submit_verdict",
            ToolCallOutcome::Served,
        );
        assert_eq!(
            submitted_by(&row, &worker, Forced::Given(&given)),
            Some("{\"summary\": \"mine\"}".to_owned())
        );
    }

    // Python: tests/tasks/test_structured_result.py::test_captures_prefixed_gateway_call_from_trace
    // Python: tests/tasks/test_structured_result.py::TestResultHelpers::test_is_submit_result_matches_bare_and_prefixed
    #[test]
    fn a_prefixed_gateway_call_is_the_result_tool() {
        assert_eq!(
            [
                matches_result_tool("submit_result"),
                matches_result_tool("mcp__gateway__submit_result"),
                matches_result_tool("submit_results"),
                matches_result_tool("web_search"),
                matches_result_tool("submit_result_extra"),
            ],
            [true, true, false, false, false]
        );
    }
}
