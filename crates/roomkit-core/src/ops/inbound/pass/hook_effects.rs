//! What the hooks gave an event that has no delivery set to store it with,
//! stored at its decision: its tasks and its observations are written under
//! the room's lock, before the lock is released, and each task stored has
//! its `ON_TASK_CREATED` observers run then.
//!
//! - An event the hooks blocked has them written once its `BLOCKED` record
//!   is committed (RFC §10.1 steps 10 and 11), as Python's `_store_blocked`
//!   writes them; a blocked instruction, never stored, has them written so
//!   too (RFC §10.1.1 rule 4).
//! - An instruction the discussion holding the room takes, which gives its
//!   agents their turns and has no set (RFC §19.7.5 rule 7), has them
//!   written once the discussion is told, as Python's `_write_decision`
//!   writes them.
//!
//! A write the store fails is logged at ERROR, the others still written, and
//! never fails the pass (RFC §10.1 step 15).

use std::collections::HashMap;
use std::mem;

use super::{CommitPass, PassResult, State};
use crate::model::Task;
use crate::ops::Cx;
use crate::ops::inbound::{persist, report};
use crate::ops::protocol::{Outcome, StoreRequest};

impl CommitPass {
    /// Writes the hooks' tasks and observations of `result`, a record
    /// stored blocked, a blocked instruction or an instruction the
    /// discussion took, under the lock; with none, the lock is released at
    /// once.
    pub(super) fn store_hook_effects(&mut self, cx: &mut Cx<'_>, result: PassResult) -> State {
        if self.tasks.is_empty() && self.observations.is_empty() {
            return self.release(cx, Ok(result));
        }
        let tasks = mem::take(&mut self.tasks);
        let room_id = self.event.room_id.clone();
        let request = StoreRequest::PersistSideEffects {
            room_id: room_id.clone(),
            tasks: tasks.clone(),
            observations: mem::take(&mut self.observations),
        };
        State::StoringHookEffects {
            effect: cx.io.issue_locked(request, room_id),
            result,
            tasks,
        }
    }

    /// The event's side effects are written, but those the store failed,
    /// logged: each task stored has its `ON_TASK_CREATED` observers run, on
    /// the event it was made for and the room as the hooks left it, then the
    /// lock is released.
    pub(super) fn on_hook_effects_stored(
        &mut self,
        cx: &mut Cx<'_>,
        result: PassResult,
        tasks: Vec<Task>,
        outcome: Outcome,
    ) -> State {
        let stored = persist::stored_tasks(&self.event.room_id, tasks, outcome.into_persisted());
        if let PassResult::Blocked { event, context, .. } | PassResult::Stored { event, context } =
            &result
        {
            let made_for: HashMap<_, _> = stored
                .iter()
                .map(|task| (task.id.clone(), (**event).clone()))
                .collect();
            report::tasks_created(cx, &stored, &made_for, context);
        }
        self.release(cx, Ok(result))
    }
}
