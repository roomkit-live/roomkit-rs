//! The regeneration of a room's last answer (RFC §13.6): Python's
//! `RegenerateMixin`, `regenerate_response` and `regenerate_target`.

use roomkit_core::model::{EventId, RoomEvent, RoomId};
use roomkit_core::ops::{Inbound, RegenerationSelection, SelectRegeneration};
use roomkit_core::rules::chain_depth::Draw;
use roomkit_core::rules::regeneration;

use crate::InboundResult;
use crate::context::ExecutionContext;
use crate::driver;
use crate::error::RoomKitError;
use crate::kit::events::room_work;
use crate::kit::{GetRoomOptions, RoomKit};
use crate::store::{ConversationStore, StoreError};

/// The kit's call a regeneration's run is named by in the logs.
const REGENERATE: &str = "regenerate_response";

/// What [`RoomKit::regenerate_response`] takes beside the room: Python's
/// `regenerate_response(room_id, *, trigger_id=None, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegenerateResponseOptions {
    /// The message the caller read as the one to answer again
    /// ([`RoomKit::regenerate_target`]): a choice under the room's lock that
    /// is another message, or none, refuses the call `trigger_moved`, with
    /// nothing run nor written. `None` answers whatever the choice is.
    pub trigger_id: Option<EventId>,
    /// Scopes the call to one tenant (RFC §17.2): a room of another is
    /// refused before anything is planned.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::regenerate_target`] takes beside the room: Python's
/// `regenerate_target(room_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegenerateTargetOptions {
    /// Scopes the read to one tenant (RFC §17.2): a room of another is
    /// refused before anything of it is read.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The message [`regenerate_response`](Self::regenerate_response) would
    /// answer again: the newest message of the room's recent history a
    /// transport binding wrote and the room accepted (never one a hook
    /// blocked, nor one a delete withdrew), whose source binding can still
    /// write; `None` when there is none.
    ///
    /// A read outside the room's lock, so it can be stale by the time a
    /// regeneration acts on it: the regeneration chooses again under the
    /// lock. A host that must not answer a message landed in between hands
    /// this event's id back as
    /// [`RegenerateResponseOptions::trigger_id`].
    ///
    /// # Errors
    ///
    /// [`RoomKitError::RoomClosed`] when the room's status refuses new
    /// events (RFC §5.1), as a regeneration would be refused;
    /// [`RoomKitError::RoomNotFound`] for a room the store lacks, or of
    /// another organization than `options` names; the store's failure.
    pub async fn regenerate_target(
        &self,
        room_id: &RoomId,
        options: RegenerateTargetOptions,
    ) -> Result<Option<RoomEvent>, RoomKitError> {
        if options.organization_id.is_some() {
            let scope = GetRoomOptions {
                organization_id: options.organization_id,
            };
            self.get_room(room_id, scope).await?;
        }
        let recent_events = self.config().recent_events().read_by_the_caller();
        let context = driver::load_context(self.store(), room_id, recent_events)
            .await
            .map_err(|error| self.read_failed(error))?
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()))?;
        if !context.room.status.accepts_events() {
            return Err(RoomKitError::RoomClosed {
                room_id: room_id.clone(),
            });
        }
        Ok(regeneration::target(&context).cloned())
    }

    /// The room's agents answer its last message again, without a new
    /// inbound event: the message keeps its id, index and time, and its
    /// own hooks, delivery report and `event_processed` do not run again.
    ///
    /// Under the room's lock, its wait and its read of the room within
    /// `process_timeout`, the message is chosen as [`regenerate_target`](Self::regenerate_target)
    /// chooses it; it is handed to the room's intelligence channels alone
    /// (no transport receives it again), in the room's delivery lane behind
    /// every event committed, off the lock and unbounded (RFC §13.5,
    /// §13.6). The new answers re-enter as a first turn's do (RFC §10.1
    /// step 14): their hooks run, their source's right to write is checked,
    /// they are stored and delivered, and the result holds them in its
    /// `response_events`. The answers already given stay: removing them is
    /// the caller's, before the call.
    ///
    /// A discussion queues one turn of its own for each agent that answered
    /// the trigger, falling back to the agents it originally asked. Those
    /// turns run independently: the result names the trigger and has no
    /// `response_events` (RFC §19.7.5 rule 7). A stored discussion that is
    /// over refuses the call `discussion_over` after target selection.
    ///
    /// Returns `None` when the room holds no message to answer again. A
    /// room whose status refuses new events is refused first, blocked
    /// `room_closed` with a `room_refused_event` (RFC §5.1); then a call
    /// naming a [`trigger_id`](RegenerateResponseOptions::trigger_id) the
    /// choice is not, blocked `trigger_moved`; past `process_timeout`,
    /// blocked `process_timeout`. None of these runs an agent or writes.
    ///
    /// Made from the body of an `ASYNC` hook a cascade fires once an event's
    /// delivery set has run, the call draws one of that cascade's hook calls
    /// as it starts, and its answers draw on that cascade's budget; made
    /// from other work of the room, they draw on that work's, and outside
    /// it on a budget of their own (RFC §10.3). Made within a turn, the
    /// message is handed to the agents at least as deep as a call made
    /// there (RFC §8.3).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::RoomNotFound`] for a room the store lacks, or of
    /// another organization than `options` names;
    /// [`RoomKitError::ReentryLoopCap`] from an `ASYNC` hook's body past the
    /// budget that body runs on; and what fails the call before its choice
    /// is planned or queued in a discussion,
    /// the store's failure among them. Nothing is run or written then.
    pub async fn regenerate_response(
        &self,
        room_id: &RoomId,
        options: RegenerateResponseOptions,
    ) -> Result<Option<InboundResult>, RoomKitError> {
        let RegenerateResponseOptions {
            trigger_id,
            organization_id,
        } = options;
        self.inner.open()?;
        if organization_id.is_some() {
            self.get_room(room_id, GetRoomOptions { organization_id })
                .await?;
        }
        // Only a hook's body draws as it starts; any work of the room lends
        // its budget to the answers.
        let work = room_work(room_id).map(|(draw, _)| draw);
        if let Some(draw @ Draw::HookCall(_)) = &work
            && !draw.take()
        {
            return Err(RoomKitError::ReentryLoopCap(room_id.clone()));
        }
        match driver::regeneration_room(&self.inner, room_id).await {
            Ok(Some(room)) => self.follow_discussion(&room),
            Ok(None) => {}
            Err(RoomKitError::KitClosed) => {
                return Ok(Some(driver::cut_before_its_commit().into()));
            }
            Err(error) => return Err(error),
        }
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        if let Some(discussion) = self.inner.discussions.get(room_id) {
            return self
                .regenerate_in_discussion(room_id, trigger_id, floor, discussion)
                .await;
        }
        let mut regenerate = Inbound::regenerate(self.config(), room_id.clone(), trigger_id, floor);
        if let Some(draw) = work {
            regenerate = regenerate.drawing_on(draw);
        }
        let ran = driver::run_rerun(&self.inner, REGENERATE, room_id, regenerate, None).await;
        let result = ran.output??;
        // A call that found no message to answer names no event and is no
        // refusal.
        Ok((result.event.is_some() || result.blocked).then(|| result.into()))
    }

    async fn regenerate_in_discussion(
        &self,
        room_id: &RoomId,
        trigger_id: Option<EventId>,
        floor: u32,
        discussion: std::sync::Arc<dyn crate::kit::discussions::DiscussionSeam>,
    ) -> Result<Option<InboundResult>, RoomKitError> {
        let selection = SelectRegeneration::new(self.config(), room_id.clone(), trigger_id);
        let selected = match driver::run(&self.inner, REGENERATE, selection).await {
            Ok(selected) => selected?,
            Err(RoomKitError::KitClosed) => {
                return Ok(Some(driver::cut_before_its_commit().into()));
            }
            Err(error) => return Err(error),
        };
        match selected {
            RegenerationSelection::Nothing => Ok(None),
            RegenerationSelection::Refused(result) => Ok(Some((*result).into())),
            RegenerationSelection::Selected {
                trigger,
                context,
                release_error,
            } => {
                // Selection is over: close no longer cuts this call. Keep
                // the original discussion even if another call uninstalls it.
                let mut result = discussion
                    .queue_regeneration(*trigger, *context, floor)
                    .await?;
                result.error = release_error.or(result.error);
                Ok((result.event.is_some() || result.blocked).then(|| result.into()))
            }
        }
    }
}
