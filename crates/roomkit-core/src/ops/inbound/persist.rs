//! Step 15 of the inbound pipeline (RFC §10.1 step 15): once the cascade's
//! passes and sets are over, the tasks and observations every pass and set
//! collected are stored, the room metadata updates the channels returned
//! are applied (gap G-049), and each stored task's `ON_TASK_CREATED`
//! observers run. Then the operation ends.

use super::{Machine, Stage, report};
use crate::model::{RoomUpdates, Task};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Outcome, Request};

impl Machine {
    /// Stores the side effects every pass and set collected, and applies the
    /// room metadata updates the channels returned (RFC §10.1 step 15, gap
    /// G-049), then ends. The update names its keys only, so it needs no
    /// lock: it never overwrites the counters of an event committed
    /// meanwhile.
    pub(super) fn persist_side_effects(&mut self, cx: &mut Cx<'_>) {
        let side_effects = self.issue_side_effects(cx);
        let metadata = self.issue_metadata_updates(cx);
        self.await_persisted(cx, side_effects, metadata);
    }

    /// Asks for the collected tasks and observations to be stored, if any.
    fn issue_side_effects(&mut self, cx: &mut Cx<'_>) -> Option<(EffectId, Vec<Task>)> {
        if self.tasks.is_empty() && self.observations.is_empty() {
            return None;
        }
        let room_id = self.room_id.clone()?;
        let tasks = std::mem::take(&mut self.tasks);
        let effect = cx.io.issue(Request::PersistSideEffects {
            room_id,
            tasks: tasks.clone(),
            observations: std::mem::take(&mut self.observations),
        });
        Some((effect, tasks))
    }

    /// Asks for the collected room metadata updates to be written, if any.
    fn issue_metadata_updates(&mut self, cx: &mut Cx<'_>) -> Option<EffectId> {
        if self.metadata_updates.is_empty() {
            return None;
        }
        let room_id = self.room_id.clone()?;
        Some(cx.io.issue(Request::UpdateRoom {
            room_id,
            updates: RoomUpdates {
                updated_at: Some(cx.env.now()),
                metadata: std::mem::take(&mut self.metadata_updates),
                ..RoomUpdates::default()
            },
        }))
    }

    /// Takes the answer of one of step 15's writes.
    pub(super) fn on_persisting(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match (side_effects, metadata) {
            (Some((expected, tasks)), metadata) if expected == effect => {
                self.on_persisted(cx, outcome, &tasks);
                self.await_persisted(cx, None, metadata);
            }
            (side_effects, Some(expected)) if expected == effect => {
                if let Err(error) = outcome.into_stored() {
                    self.note_error(Some(error));
                }
                self.await_persisted(cx, side_effects, None);
            }
            (side_effects, metadata) => {
                debug_assert!(false, "inbound received {effect:?} while persisting");
                self.stage = Stage::Persisting {
                    side_effects,
                    metadata,
                };
            }
        }
    }

    /// Waits for step 15's outstanding writes, or ends once none is left.
    fn await_persisted(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
    ) {
        if side_effects.is_none() && metadata.is_none() {
            let result = std::mem::take(&mut self.result);
            return self.end(cx, Ok(result));
        }
        self.stage = Stage::Persisting {
            side_effects,
            metadata,
        };
    }

    /// The side effects are stored: each task's `ON_TASK_CREATED` observers
    /// run (RFC §10.1 step 15).
    fn on_persisted(&mut self, cx: &mut Cx<'_>, outcome: Outcome, tasks: &[Task]) {
        match outcome.into_persisted() {
            Ok(()) => {
                if let Some(context) = &self.task_context {
                    report::tasks_created(cx, tasks, context);
                }
            }
            Err(error) => self.note_error(Some(error)),
        }
    }
}
