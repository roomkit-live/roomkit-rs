//! Long work started as a task (RFC §23.4): Python's
//! `BackgroundMixin.start_task` and `tasks.background.start_background_task`.
//!
//! The host starts, for an agent of the room, the task its
//! `work_in_background` call would: a delegation on the agent's worker
//! (the agent itself when its background was not set up), told how to work
//! and where its findings go, handed the room's last messages, its result
//! relayed or posted back to the agent.

use roomkit_core::model::{ChannelCategory, ChannelId, RoomId};

use super::RoomKit;
use super::tasks::DelegateOptions;
use crate::GetRoomOptions;
use crate::error::RoomKitError;
use crate::store::ConversationStore;
use crate::tasks::DelegatedTask;
use crate::tasks::brief::{BACKGROUND_CONVERSATION, worker_brief};

/// What [`RoomKit::start_task`] takes beside the room, the agent and the
/// task: Python's `start_task(room_id, agent_id, task, *, hand_back=None,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartTaskOptions {
    /// Python `hand_back=None`: `relay` or `post`; the agent's background
    /// setup's when `None`.
    pub hand_back: Option<String>,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2): the room is read with it before anything is started.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Starts `task` for `agent_id`, an agent of `room_id`, as its
    /// `work_in_background` call would (RFC §23.4): Python's
    /// `start_task(room_id, agent_id, task, **options)`. It runs on the
    /// agent's worker, handed the room's last messages, its result relayed
    /// or posted back to the agent; an agent with no background set up
    /// does the work itself and has its result relayed
    /// ([`setup_background`](RoomKit::setup_background)).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4), before the arguments are read;
    /// [`RoomKitError::RoomNotFound`] for a room of another organization
    /// than `organization_id`, nothing started; [`RoomKitError::Value`] for
    /// an `agent_id` that is no intelligence channel attached to the room,
    /// a blank `task`, or an unknown hand-back; the errors of
    /// [`delegate`](RoomKit::delegate).
    pub async fn start_task(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        task: impl Into<String>,
        options: StartTaskOptions,
    ) -> Result<DelegatedTask, RoomKitError> {
        self.inner.open()?;
        let StartTaskOptions {
            hand_back,
            organization_id,
        } = options;
        if organization_id.is_some() {
            self.get_room(room_id, GetRoomOptions { organization_id })
                .await?;
        }
        let task = task.into();
        if task.trim().is_empty() {
            return Err(RoomKitError::Value(
                "A background task needs a task".to_owned(),
            ));
        }
        self.check_agent_of(room_id, agent_id).await?;
        let setup = self.background_setup(agent_id);
        let mode = hand_back.unwrap_or(setup.hand_back);
        let worker = setup.worker.unwrap_or_else(|| agent_id.clone());
        // The agent as the room addresses it: its channel id, a discussion's
        // handle once discussions come (RFC §19.7.5 rule 4).
        let brief = worker_brief(&task, agent_id.as_str(), &mode);
        let options = DelegateOptions {
            brief: Some(brief),
            notify: Some(agent_id.clone()),
            conversation: BACKGROUND_CONVERSATION,
            hand_back: mode,
            ..DelegateOptions::default()
        };
        self.delegate(room_id, &worker, task, options).await
    }

    /// Refuses an `agent_id` that is no intelligence channel attached to
    /// `room_id`.
    async fn check_agent_of(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
    ) -> Result<(), RoomKitError> {
        let intelligence = self
            .get_channel(agent_id)
            .is_some_and(|channel| channel.category() == ChannelCategory::Intelligence);
        let bound = self.store().get_binding(room_id, agent_id).await?.is_some();
        if intelligence && bound {
            return Ok(());
        }
        Err(RoomKitError::Value(format!(
            "{:?} is not an agent of room {:?}",
            agent_id.as_str(),
            room_id.as_str()
        )))
    }
}
