//! Direct injection (RFC §10.5) and records committed outside the pipeline,
//! and the way every call into a named room takes: adopted by the call
//! whose work in that room it runs within (RFC §13.5, §10.1 step 18), run
//! apart otherwise.

use roomkit_core::model::{DirectEvent, RoomEvent, RoomId};
use roomkit_core::ops::{CommitRecord, Entry, Inbound, InboundResult, InboundTarget};

use super::RoomKit;
use super::scope::Scope;
use crate::context::{self, Adopting};
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Injects `event` into the room `room_id` (RFC §10.5): it takes the
    /// inbound pipeline from the room lock on, without a channel's parse.
    /// Its source is the registered channel the event names, `INBOUND` for a
    /// transport, `OUTBOUND` for an intelligence channel; its hooks run, an
    /// off-lock check with its admission ticket included (RFC §9.5.1), its
    /// source's permissions apply, an edit or a delete is checked against
    /// its target, and it is committed and delivered, its responses
    /// re-entering as a message's do. Its idempotency key is checked per
    /// room and channel, as an inbound message's (RFC §13.4). A room that
    /// does not exist is not created, and a channel the room does not bind
    /// is not attached: its event is refused, nothing written (gap G-069).
    /// The room is read unscoped; an organization's
    /// [`send_event`](crate::OrganizationKit::send_event) reaches its own
    /// rooms only.
    ///
    /// Returns the committed event, a record stored `BLOCKED` included,
    /// once its delivery set is over; a replay returns the event its key
    /// committed first. An instruction is returned uncommitted, as it is
    /// never stored (RFC §10.1.1).
    ///
    /// Called from inside the room's own work under the room's lock or in
    /// its admission order, by a hook or a channel's parse, the event is
    /// adopted by that work's call instead of waiting for it: it takes no
    /// admission ticket, runs every hook under the lock, commits ahead of
    /// the event a locked hook is deciding on, and returns at its commit,
    /// its delivery following in the room's lane (RFC §13.5, §9.5.1
    /// Reentrance). Called from a turn or a delivery in the room, it is a
    /// new arrival, with its own ticket and `process_timeout`, drawing on
    /// the reentry budget of the cascade that delivers (gap G-070), and
    /// returns at its decision, its delivery following the caller's own set
    /// in the lane (RFC §10.1 step 18). Made within an
    /// agent's turn, the event is at least as deep as the turn's response,
    /// whatever `chain_depth` it was given (RFC §10.6, §8.3).
    ///
    /// Dropped before it returns, the call goes on, detached: once its
    /// event is committed, or while its commit is in flight, it delivers
    /// it; before, it expires as one past its `process_timeout`, nothing of
    /// its event written, every event it committed delivered (gap G-076).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Inbound`] with
    /// [`InboundError::Refused`](crate::InboundError::Refused)
    /// when the room refused the event before its commit, nothing written:
    /// `source_unbound` for a channel the room does not bind, `room_closed`,
    /// `process_timeout`, one of the refusals of an edit or a delete, an
    /// instruction's refusal; with another error when the event cannot be
    /// one (an unregistered channel, the visibility `internal`, `standalone`
    /// on a type other than an instruction) or the store, the lock or the
    /// admission ticket failed before its commit.
    pub async fn send_event(
        &self,
        room_id: &RoomId,
        event: DirectEvent,
    ) -> Result<RoomEvent, KitError> {
        self.unscoped().send_event(room_id, event).await
    }

    /// Commits `event` to the room `room_id` outside the pipeline (RFC
    /// §10.5 `commit_event`): a record no member is meant to receive, a
    /// trace, a display snapshot, a copy a branched conversation starts
    /// from. It is committed under the room lock with the room's next
    /// index, its thread parent rooted as every committed event's is (RFC
    /// §10.1 step 8), as the caller gives it otherwise, a blank
    /// idempotency key read as none, and nothing follows: no hook, no
    /// delivery, no framework event. It moves the room's counters,
    /// not its activity, so a paused room stays paused (gap G-071).
    ///
    /// The room is read unscoped; an organization's
    /// [`commit_event`](crate::OrganizationKit::commit_event) reaches its
    /// own rooms only. Called from code that holds the room's lock, a
    /// `BEFORE_BROADCAST` hook among others, it takes the lock again at once
    /// (RFC §13.5).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the record names another room,
    /// before anything is read; when the room does not exist; when its
    /// status refuses events (RFC §5.1); when the store refuses it, a key
    /// its channel holds in the room among others (`IdempotencyKeyTaken`:
    /// a record is never a replay, RFC §14.3); or when the store or the
    /// room lock fails.
    pub async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
    ) -> Result<RoomEvent, KitError> {
        self.unscoped().commit_event(room_id, event).await
    }

    /// Runs `entry`, a call into the room it names, at least as deep as the
    /// turn it is made within, if any (RFC §10.6): adopted by the call of
    /// the caller's execution context whose work in the room made it, if
    /// one did, apart otherwise (RFC §13.5). A call run apart whose context
    /// still holds the room's lock, or stands in its admission order, fails
    /// there, rather than wait for itself.
    pub(super) async fn enter(
        &self,
        mut entry: Entry,
        call: &'static str,
    ) -> Result<InboundResult, KitError> {
        let chain_depth = match &mut entry {
            Entry::Message { message, .. } => &mut message.chain_depth,
            Entry::Direct { event, .. } => &mut event.chain_depth,
            Entry::Post { post, .. } => &mut post.chain_depth,
        };
        context::as_deep_as_its_turn(chain_depth);
        self.inner.open()?;
        let room_id = entry.room_id().clone();
        let entry = match context::adopt(entry).await {
            Adopting::Settled(result) => return Ok((*result)?),
            // A run the kit's shutdown ended abandons the calls it adopted:
            // their callers are told the kit is shut down.
            Adopting::Abandoned => {
                self.inner.open()?;
                return Err(KitError::Abandoned { room_id, call });
            }
            Adopting::Apart(entry) => entry,
        };
        let room_id = entry.room_id().clone();
        let operation = match entry {
            Entry::Message {
                room_id,
                organization_id,
                message,
            } => Inbound::new(
                self.config(),
                InboundTarget::Room(room_id),
                organization_id,
                *message,
            ),
            Entry::Direct {
                room_id,
                organization_id,
                event,
            } => Inbound::inject(self.config(), room_id, organization_id, *event),
            Entry::Post {
                room_id,
                organization_id,
                post,
            } => Inbound::post(self.config(), room_id, organization_id, *post),
        };
        Ok(driver::run_inbound(&self.inner, call, Some(&room_id), operation).await??)
    }
}

impl<S: ConversationStore> Scope<'_, S> {
    /// Injects `event` into a room the scope reaches, adopted by the call
    /// whose work in the room made it, when one did (RFC §13.5).
    pub(super) async fn send_event(
        &self,
        room_id: &RoomId,
        event: DirectEvent,
    ) -> Result<RoomEvent, KitError> {
        let entry = Entry::Direct {
            room_id: room_id.clone(),
            organization_id: self.organization_id(),
            event: Box::new(event),
        };
        let result = self.kit.enter(entry, SEND_EVENT).await?;
        result.event.ok_or(KitError::NoEvent)
    }

    /// Commits the record to a room the scope reaches.
    pub(super) async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
    ) -> Result<RoomEvent, KitError> {
        let operation = CommitRecord::new(
            self.kit.config(),
            room_id.clone(),
            event,
            self.organization_id(),
        );
        Ok(driver::run(&self.kit.inner, "commit_event", operation).await??)
    }
}

/// The kit's call that injects an event, as an error names it.
const SEND_EVENT: &str = "send_event";
