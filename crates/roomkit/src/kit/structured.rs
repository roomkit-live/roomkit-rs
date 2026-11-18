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
use serde_json::Value;

use super::RoomKit;
use super::background_tools::collision;
use super::tasks::{TaskEnd, failed, kit_failure, task_outcome};
use crate::error::RoomKitError;
use crate::orchestration::SUBMIT_RESULT;
use crate::store::{ConversationStore, ListEventsOptions};
use crate::tasks::json::json as write_json;
use crate::tasks::{DelegatedTask, ResultTool};
use crate::turn::OwnerToolAnswer;
use crate::turns::served::OwnerToolEntry;

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
        let tool = result_tool.unwrap_or(&SUBMIT_RESULT);
        let served = ServedResult::serve(self, &task.agent_id, &task.child_room_id, tool);
        let _served = match served {
            Ok(served) => served,
            Err(error) => return failed(kit_failure(&error), context),
        };
        let mut last_output = String::new();
        let mut turns = Metadata::new();
        for attempt in 0..=retries {
            let message = if attempt == 0 {
                &task.description
            } else {
                &tool.reminder
            };
            let answers = match Box::pin(self.delegated_answers(task, message.to_owned())).await {
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
        }
        let attempts = retries.saturating_add(1);
        tracing::warn!(target: "roomkit::tasks", task = %task.id, agent = %task.agent_id, attempts, tool = tool.name(), "a delegated agent never called its result tool");
        let missing = written((tool.on_missing)(
            task.agent_id.as_str(),
            &last_output,
            attempts,
        ));
        TaskEnd::finished(Some(missing), None, context, turns)
    }

    /// Whether `agent` is an intelligence channel whose turns declare the
    /// tools the room's owner serves (RFC §10.6.5): Python's
    /// `_takes_owner_tools`.
    pub(crate) fn takes_owner_tools(&self, agent: &ChannelId) -> bool {
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
        tool: &ResultTool,
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
fn submitted_by(event: &RoomEvent, worker: &ChannelId, tool: &ResultTool) -> Option<String> {
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
    (tool.matches(&call.tool_name) && served).then(|| written((tool.normalize)(&call.arguments)))
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
        tool: &ResultTool,
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
            definition: tool.tool.clone(),
            serve: Arc::new(|_kit, _call| {
                Box::pin(async { OwnerToolAnswer::Served(Value::String(received())) })
            }),
            waits: false,
            ends_turn: true,
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

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
            written((SUBMIT_RESULT.normalize)(&arguments)),
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
            written((SUBMIT_RESULT.normalize)(&arguments)),
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
            end_row("w1", SUBMIT_RESULT.name(), ToolCallOutcome::Refused),
            end_row("w1", SUBMIT_RESULT.name(), ToolCallOutcome::Failed),
            end_row("other", SUBMIT_RESULT.name(), ToolCallOutcome::Served),
        ];
        assert!(
            rows.iter()
                .all(|row| submitted_by(row, &worker, &SUBMIT_RESULT).is_none())
        );
    }

    // Python: tests/tasks/test_structured_result.py::TestOnlyTheWorkersServedResultCounts::test_the_scan_takes_the_workers_served_call
    // Python: tests/tasks/test_structured_result.py::TestResultHelpers::test_normalize_fills_defaults
    #[test]
    fn the_scan_takes_the_worker_s_served_call() {
        let worker = ChannelId::new("w1").unwrap();
        let row = end_row("w1", SUBMIT_RESULT.name(), ToolCallOutcome::Served);
        assert_eq!(
            submitted_by(&row, &worker, &SUBMIT_RESULT),
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
            submitted_by(&row, &worker, &given),
            Some("{\"summary\": \"mine\"}".to_owned())
        );
    }

    // Python: tests/tasks/test_structured_result.py::test_captures_prefixed_gateway_call_from_trace
    // Python: tests/tasks/test_structured_result.py::TestResultHelpers::test_is_submit_result_matches_bare_and_prefixed
    #[test]
    fn a_prefixed_gateway_call_is_the_result_tool() {
        assert_eq!(
            [
                SUBMIT_RESULT.matches("submit_result"),
                SUBMIT_RESULT.matches("mcp__gateway__submit_result"),
                SUBMIT_RESULT.matches("submit_results"),
                SUBMIT_RESULT.matches("web_search"),
                SUBMIT_RESULT.matches("submit_result_extra"),
            ],
            [true, true, false, false, false]
        );
    }
}
