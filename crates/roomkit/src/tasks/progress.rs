//! A task's progress, posted from its child room (RFC §23.3): the kit's side
//! of Python's `tasks.status.post_task_progress`, which roomkit-ai's
//! `post_task_progress` calls with the room of the tool call it runs in.

use roomkit_core::model::RoomId;
use serde_json::{Value, json};

use crate::kit::RoomKit;
use crate::orchestration::lifecycle::DETAIL_LIMIT;
use crate::orchestration::{PostOptions, StatusLevel};
use crate::store::ConversationStore;
use crate::tasks::lines::{TASK_ACTION, truthy};
use crate::tasks::text::bounded_text;
use crate::{GetRoomOptions, RoomKitError};

impl<S: ConversationStore> RoomKit<S> {
    /// Posts `detail` as how far the task whose child room is
    /// `child_room_id` got, for the room that delegated it (RFC §23.3): an
    /// `info` entry under the task's worker, action `task`, with the task's
    /// metadata, its detail bounded; it does not end the task. Whether it
    /// posted: a room that is no task's child room, or that the store does
    /// not hold, posts nothing.
    ///
    /// For roomkit-ai's `post_task_progress`, which reads the room from its
    /// tool call.
    ///
    /// # Errors
    ///
    /// The store's failure to read the room, as Python's raises it.
    #[doc(hidden)]
    pub async fn post_task_progress_in(
        &self,
        child_room_id: &RoomId,
        detail: &str,
    ) -> Result<bool, RoomKitError> {
        let room = match self
            .get_room(child_room_id, GetRoomOptions::default())
            .await
        {
            Ok(room) => room,
            Err(RoomKitError::RoomNotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        let named = |key: &str| {
            room.metadata
                .get(key)
                .filter(|value| truthy(value))
                .map(python_str)
        };
        let (Some(parent_room_id), Some(task_id)) = (named("parent_room_id"), named("task_id"))
        else {
            return Ok(false);
        };
        let worker = named("task_agent_id").unwrap_or_else(|| "worker".to_owned());
        let mut metadata = roomkit_core::model::Metadata::new();
        metadata.insert("room_id".to_owned(), json!(parent_room_id));
        metadata.insert("task_id".to_owned(), json!(task_id));
        metadata.insert("child_room_id".to_owned(), json!(child_room_id));
        // Posted as Python's `post_agent_lifecycle` posts an `info` entry:
        // under the worker's id as the room names it, its detail bounded.
        let options = PostOptions {
            detail: bounded_text(detail, DETAIL_LIMIT),
            metadata: Some(metadata),
        };
        self.status_bus()
            .post(worker, TASK_ACTION, StatusLevel::Info, options);
        Ok(true)
    }
}

/// `value` as Python's `str()` reads it.
fn python_str(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}
