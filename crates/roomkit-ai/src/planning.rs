//! Room-scoped plans, served through the ordinary tool path (RFC §6.4,
//! §9.2, §21.1): Python's private `TaskPlanner`.

mod tasks;
#[cfg(test)]
mod tests;

use std::sync::{Mutex, PoisonError};

use roomkit::model::{Metadata, RoomId, ToolCall};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use serde_json::json;

use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};
use crate::{PlanTaskHandle, PlanUpdatedEvent};

pub(crate) const PLAN_TOOL: &str = "plan_tasks";
const MAX_ROOMS: usize = 100;
const MAX_TASKS: usize = 100;
const MAX_TITLE_LENGTH: usize = 500;

/// Least recently read or written room first. A channel's clones share it;
/// validation and the cache mutation complete before observers are notified.
#[derive(Debug, Default)]
pub(crate) struct TaskPlanner {
    plans: Mutex<Vec<(RoomId, Vec<PlanTaskHandle>)>>,
}

impl TaskPlanner {
    fn store(&self, room: &RoomId, tasks: Vec<PlanTaskHandle>) {
        let mut plans = self.plans.lock().unwrap_or_else(PoisonError::into_inner);
        plans.retain(|(key, _)| key != room);
        plans.push((room.clone(), tasks));
        if plans.len() > MAX_ROOMS {
            plans.remove(0);
        }
    }

    fn plan_for(&self, room: &RoomId) -> Option<Vec<PlanTaskHandle>> {
        let mut plans = self.plans.lock().unwrap_or_else(PoisonError::into_inner);
        let at = plans.iter().position(|(key, _)| key == room)?;
        let entry = plans.remove(at);
        let tasks = entry.1.clone();
        plans.push(entry);
        Some(tasks)
    }

    pub(crate) fn forget(&self, room: &RoomId) {
        self.plans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(key, _)| key != room);
    }

    /// A plan is input working state, never a system instruction. Reading an
    /// empty plan still refreshes its LRU entry; standalone turns read none.
    pub(crate) fn notes(&self, room: &RoomId, standalone: bool) -> Vec<String> {
        if standalone {
            return Vec::new();
        }
        let Some(tasks) = self.plan_for(room).filter(|tasks| !tasks.is_empty()) else {
            return Vec::new();
        };
        let mut lines = vec!["## Current Task Plan".to_owned()];
        lines.extend(tasks.into_iter().map(|task| tasks::line(&task.get())));
        vec![lines.join("\n")]
    }

    fn handle(&self, arguments: &Metadata, context: &ToolContext) -> ToolResult {
        let tasks = match tasks::validated(arguments.get("tasks")) {
            Ok(tasks) => tasks,
            Err(error) => return ToolResult::Text(crate::texts::error_envelope(&error)),
        };
        let tasks: Vec<_> = tasks.into_iter().map(PlanTaskHandle::new).collect();
        self.store(&context.room_id, tasks.clone());
        notify(context, &tasks);
        ToolResult::Text(tasks::summary(
            &tasks.iter().map(PlanTaskHandle::get).collect::<Vec<_>>(),
        ))
    }

    pub(crate) fn definition() -> AiTool {
        tasks::definition()
    }
}

/// Publishing hands the event to the turn's proxy, without waiting for the
/// physical backend. The hook sees stored state, even with no backend; its
/// observer work is tracked by the existing turn machinery (RFC §10.6.6).
fn notify(context: &ToolContext, tasks: &[PlanTaskHandle]) {
    let Some(turn) = context
        .turn()
        .filter(|_| !context.room_id.as_str().is_empty())
    else {
        return;
    };
    turn.publish_ephemeral(EphemeralEvent {
        channel_id: Some(context.channel_id.clone()),
        data: Metadata::from_iter([
            ("type".to_owned(), json!("plan_updated")),
            (
                "tasks".to_owned(),
                json!(tasks.iter().map(PlanTaskHandle::get).collect::<Vec<_>>()),
            ),
        ]),
        ..EphemeralEvent::new(
            context.room_id.clone(),
            EphemeralEventType::Custom,
            context.channel_id.to_string(),
        )
    });
    let event = PlanUpdatedEvent {
        tasks: tasks.to_vec(),
        ..PlanUpdatedEvent::new(context.room_id.to_string(), context.channel_id.to_string())
    };
    turn.observe_turn(&event, || {});
}

impl DynToolset for TaskPlanner {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move { Ok(self.handle(&call.arguments, &context)) })
    }
}
