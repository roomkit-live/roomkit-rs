//! Direct injection (RFC §10.5) and records committed outside the pipeline,
//! and the way every call into a named room takes: adopted by the call
//! whose work in that room it runs within (RFC §13.5, §10.1 step 18), run
//! apart otherwise.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, DirectEvent, EventContent, Metadata, RoomEvent, RoomId};
use roomkit_core::ops::{
    CommitRecord, Entry, Inbound, InboundError, InboundResult, InboundTarget, KIT_CLOSED,
};
use tokio::task::JoinHandle;

use super::RoomKit;
use super::options::{CommitEventOptions, SendEventOptions};
use crate::context::{self, Adopting, Completion, ExecutionContext};
use crate::delivery_keys::{Named, NamedGuard};
use crate::driver::{self, InboundOutput, Ran, Record, Stop};
use crate::error::RoomKitError;
use crate::inbound::DeliveryHandle;
use crate::store::ConversationStore;
use crate::tasks::Tasks;
use crate::telemetry::LateSpan;

impl<S: ConversationStore> RoomKit<S> {
    /// Injects an event of `content` from the registered channel
    /// `channel_id` into the room `room_id` (RFC §10.5), as `options` make
    /// it: it takes the inbound pipeline from the room lock on, without a
    /// channel's parse. Its source is the channel, `INBOUND` for a
    /// transport, `OUTBOUND` for an intelligence channel; its hooks run, an
    /// off-lock check with its admission ticket included (RFC §9.5.1), its
    /// source's permissions apply, an edit or a delete is checked against
    /// its target, and it is committed and delivered, its responses
    /// re-entering as a message's do. Its idempotency key is checked per
    /// room and channel, as an inbound message's (RFC §13.4). A room that
    /// does not exist is not created, and a channel the room does not bind
    /// is not attached: its event is refused, nothing written (gap G-069).
    /// A room of another organization than the one `options` name, or of
    /// none, is not found, as a room that does not exist, before its hooks,
    /// nothing written (RFC §17.2, gap G-077).
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
    /// As Python's `send_event` raises, nothing written:
    /// [`RoomKitError::RoomNotFound`] for a room that does not exist or that
    /// the organization does not reach; when the room refused the event
    /// before its commit, [`RoomKitError::ChannelNotFound`] for a channel the
    /// room does not bind, [`RoomKitError::RoomClosed`],
    /// [`RoomKitError::ProcessTimeout`], [`RoomKitError::ChangeRefused`] for
    /// the refusal of an edit or a delete; [`RoomKitError::ChannelNotRegistered`]
    /// for an unregistered channel; [`RoomKitError::Value`] when the event
    /// cannot be one (the visibility `internal`, `standalone` on a type other
    /// than an instruction, an instruction's refusal, a key in the
    /// framework's publication namespace, `roomkit:publication:`, RFC
    /// §22.1); or the store's or the lock's failure before its commit.
    pub async fn send_event(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        content: impl Into<EventContent>,
        options: SendEventOptions,
    ) -> Result<RoomEvent, RoomKitError> {
        let organization_id = options.organization_id.clone();
        let event = options.event(channel_id.clone(), content.into());
        self.inject(room_id, organization_id, event).await
    }

    /// Injects `event` into a room `organization_id` reaches, adopted by the
    /// call whose work in the room made it, when one did (RFC §13.5): what
    /// [`send_event`](Self::send_event) and a turn's host run.
    pub(crate) async fn inject(
        &self,
        room_id: &RoomId,
        organization_id: Option<String>,
        mut event: DirectEvent,
    ) -> Result<RoomEvent, RoomKitError> {
        crate::delivery::refuse_reserved(event.idempotency_key.as_deref())?;
        // A content a struct literal built past its model's check is refused
        // before anything is read, as Python's model refuses it when built,
        // and one kept is written as its constructor writes it.
        event.content.check()?;
        let span = self
            .inner
            .send_event_span(room_id, &event.channel_id, event.event_type);
        let entry = Entry::Direct {
            room_id: room_id.clone(),
            organization_id,
            event: Box::new(event),
        };
        let injected = span
            .within(Box::pin(self.enter(entry, SEND_EVENT, false)))
            .await;
        injected_under(&span, injected)
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
    /// A room of another organization than the one `options` name, or of
    /// none, is not found, nothing written in it (RFC §17.2). Called from
    /// code that holds the room's lock, a
    /// `BEFORE_BROADCAST` hook among others, it takes the lock again at once
    /// (RFC §13.5).
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started, or
    /// when it cut the call before its commit landed (RFC §12.10.4);
    /// [`RoomKitError::Value`] when the record names another room, or
    /// carries a key in the framework's publication namespace (RFC §22.1),
    /// before anything is read; [`RoomKitError::RoomNotFound`] when the room
    /// does not exist; [`RoomKitError::RoomClosed`] when its status refuses
    /// events (RFC §5.1); [`RoomKitError::IdempotencyKeyTaken`] for a key
    /// its channel holds in the room (a record is never a replay, RFC
    /// §14.3), as Python raises `IdempotencyKeyTakenError`; or the store's
    /// or the room lock's failure.
    pub async fn commit_event(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
        options: CommitEventOptions,
    ) -> Result<RoomEvent, RoomKitError> {
        crate::delivery::refuse_reserved(event.idempotency_key.as_deref())?;
        event.check()?;
        let taken = (
            event.source.channel_id.clone(),
            event.idempotency_key.clone(),
        );
        let operation = CommitRecord::new(
            self.config(),
            room_id.clone(),
            event,
            options.organization_id,
        );
        let committed = match driver::run_reading_back(&self.inner, "commit_event", operation).await
        {
            Ok(committed) => committed,
            // The kit's close cut it as its commit had landed: stored
            // (RFC §12.10.4).
            Err(Stop::Cut(Some(committed))) => return Ok(committed.event),
            Err(stop) => return Err(stop.into_error()),
        };
        committed.map_err(|error| match (RoomKitError::from(error), taken) {
            (refused, (channel_id, Some(key)))
                if refused.recorded_refusal() == Some("IdempotencyKeyTaken") =>
            {
                RoomKitError::IdempotencyKeyTaken {
                    room_id: room_id.clone(),
                    channel_id,
                    key,
                }
            }
            (error, _) => error,
        })
    }

    /// Runs `entry`, a call into the room it names, at least as deep as the
    /// turn it is made within, if any (RFC §10.6): adopted by the call of
    /// the caller's execution context whose work in the room made it, if
    /// one did, apart otherwise (RFC §13.5). A call run apart whose context
    /// still holds the room's lock, or stands in its admission order, fails
    /// there, rather than wait for itself.
    pub(super) async fn enter(
        &self,
        entry: Entry,
        call: &'static str,
        defer: bool,
    ) -> Result<Entered, RoomKitError> {
        self.enter_named(entry, call, defer, None).await
    }

    /// [`enter`](Self::enter), the call's delivery named by `named`, a
    /// key's name of it, until its delivery is over (RFC §10.1 "Explicit
    /// delivery cancellation"): its run's while it goes on, or, for a call
    /// the room's work adopted, its entry's in that run until its delivery
    /// set ran.
    pub(super) async fn enter_named(
        &self,
        mut entry: Entry,
        call: &'static str,
        defer: bool,
        named: Option<NamedGuard>,
    ) -> Result<Entered, RoomKitError> {
        let chain_depth = match &mut entry {
            Entry::Message { message, .. } => &mut message.chain_depth,
            Entry::Direct { event, .. } => &mut event.chain_depth,
            Entry::Row { event, .. } => &mut event.chain_depth,
            Entry::Post { post, .. } => &mut post.chain_depth,
            Entry::Greeting { chain_depth, .. } => chain_depth,
        };
        context::as_deep_as_its_turn(chain_depth);
        self.inner.open()?;
        self.inner.subscribed().await;
        self.reached_from_its_work(&entry).await?;
        let refused = refusal_of(&entry);
        if let Some(named) = named.as_ref().filter(|named| named.named().has_key())
            && ExecutionContext::current().works_in_room(entry.room_id())
        {
            self.locate_in_room(named.named(), entry.room_id()).await;
        }
        let adopted_named = named
            .as_ref()
            .filter(|named| named.named().has_key())
            .map(|named| Arc::clone(named.named()));
        let entry = match context::adopt(entry, adopted_named).await {
            // The run that adopted it names its delivery until its set ran.
            Adopting::Settled(result, completion) => {
                return Ok(Entered {
                    result: (*result).map_err(refused)?,
                    following: Following::Adopted(completion),
                });
            }
            // A run the kit's shutdown ended abandons the calls it adopted:
            // their callers are told the call was cancelled, as the call
            // that adopted it was.
            Adopting::Abandoned => {
                self.inner.open()?;
                return Err(RoomKitError::Cancelled);
            }
            Adopting::Apart(entry) => entry,
        };
        let room_id = entry.room_id().clone();
        let operation = self.operation(entry)?;
        let ran =
            driver::run_inbound(&self.inner, call, Some(&room_id), operation, defer, named).await;
        Entered::ran(ran, refused)
    }

    /// Locates `named`, the delivery of a call made from the work of
    /// `room_id`, which that work may adopt, in the room's organization,
    /// unless its call named its organization: the room is read before the
    /// call is handed over, as Python's adopted call reads it first
    /// (`_prepare_event_inner`). A room the read does not find, gone or not
    /// read, leaves it where it is.
    async fn locate_in_room(&self, named: &Named, room_id: &RoomId) {
        if named.is_located() {
            return;
        }
        match self.store().get_room(room_id).await {
            Ok(Some(room)) => named.locate(room.organization_id.as_deref()),
            Ok(None) => {}
            Err(error) => tracing::debug!(
                target: "roomkit::delivery",
                room_id = %room_id,
                %error,
                "reading the room of a call from its work failed; its delivery stays unlocated"
            ),
        }
    }

    /// Checks that the organization `entry` names reaches its room when the
    /// call is made from that room's own work, where it would draw on the
    /// work's reentry budget first thing: a room of another organization
    /// reads as not found and draws nothing, as the Python implementation
    /// reads the room first (RFC §17.2, §8.3).
    async fn reached_from_its_work(&self, entry: &Entry) -> Result<(), RoomKitError> {
        let organization_id = match entry {
            Entry::Message {
                organization_id, ..
            }
            | Entry::Direct {
                organization_id, ..
            }
            | Entry::Post {
                organization_id, ..
            } => organization_id.as_deref(),
            Entry::Row { .. } | Entry::Greeting { .. } => None,
        };
        let room_id = entry.room_id();
        if organization_id.is_none() || !ExecutionContext::current().works_in_room(room_id) {
            return Ok(());
        }
        self.reached(room_id, organization_id).await
    }

    /// The operation that runs `entry`, a call run apart.
    fn operation(&self, entry: Entry) -> Result<Inbound, RoomKitError> {
        Ok(match entry {
            Entry::Message {
                room_id,
                organization_id,
                message,
                answers,
            } => {
                let target = InboundTarget::Room(room_id);
                match answers {
                    Some(answers) => Inbound::answered(
                        self.config(),
                        target,
                        organization_id,
                        *message,
                        *answers,
                    ),
                    None => Inbound::new(self.config(), target, organization_id, *message),
                }
            }
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
            Entry::Greeting {
                room_id,
                agent_id,
                text,
                chain_depth,
            } => Inbound::greeting(self.config(), room_id, agent_id, text, chain_depth),
            // A stream's row goes to the run that reads the stream, never
            // through a kit's call: one here is a defect of this crate.
            Entry::Row { .. } => {
                return Err(RoomKitError::Internal(
                    "a stream's row was entered through a kit's call".to_owned(),
                ));
            }
        })
    }
}

/// What a call into a room comes to: its result, and what follows a call
/// answered at its decision.
#[derive(Debug)]
pub(super) struct Entered {
    pub(super) result: InboundResult,
    pub(super) following: Following,
}

/// What follows a call answered at its decision.
#[derive(Debug)]
pub(super) enum Following {
    /// Its run, going on, with its record.
    Run(JoinHandle<InboundOutput>, Arc<Record>),
    /// The end of the delivery set of the call the room's work adopted.
    Adopted(Completion),
    /// Nothing: the call ended.
    Nothing,
}

impl Entered {
    /// What a run apart came to, its error read by `refused`.
    pub(super) fn ran(
        ran: Ran,
        refused: impl FnOnce(InboundError) -> RoomKitError,
    ) -> Result<Self, RoomKitError> {
        let following = match ran.delivery {
            Some((run, record)) => Following::Run(run, record),
            None => Following::Nothing,
        };
        Ok(Self {
            result: ran.output?.map_err(refused)?,
            following,
        })
    }

    /// The result of a call that entered so, deferred when `defer`: with
    /// its completion handle, but for a call refused before the locked
    /// region, which has no deferred tail (RFC §10.1 step 18).
    pub(super) fn into_result(self, defer: bool, tasks: &Arc<Tasks>) -> crate::InboundResult {
        let Self { result, following } = self;
        if !defer || result.refused_before_the_lock {
            return result.into();
        }
        let decided = result.clone();
        let tasks = Arc::clone(tasks);
        // Refused at its deadline, the call delivers nothing of its own: its
        // handle waits for no entry it adopted, whose cascade goes on as its
        // own, as Python's (RFC §9.5.1 Reentrance, §13.6).
        let refused_at_its_deadline = result.blocked
            && result.event.is_none()
            && result.reason.as_deref() == Some("process_timeout");
        let handle = match following {
            _ if refused_at_its_deadline => DeliveryHandle::over(decided, tasks),
            Following::Run(run, record) => DeliveryHandle::of_run(decided, run, record, tasks),
            Following::Adopted(completion) => DeliveryHandle::adopted(decided, completion, tasks),
            Following::Nothing => DeliveryHandle::over(decided, tasks),
        };
        crate::InboundResult::of(result, Some(handle))
    }
}

/// How an entry's error reads to its caller: a direct entry's refusal before
/// its commit by Python's class for it, which names its room and source
/// ([`RoomKitError::of_entry`]); a message's, which a refusal never fails,
/// as [`From`] reads it.
fn refusal_of(entry: &Entry) -> impl FnOnce(InboundError) -> RoomKitError + use<> {
    let source = match entry {
        Entry::Direct { room_id, event, .. } => Some((room_id.clone(), event.channel_id.clone())),
        Entry::Post { room_id, post, .. } => Some((
            room_id.clone(),
            post.channel_id.clone().unwrap_or_else(ChannelId::host),
        )),
        Entry::Message { .. } | Entry::Row { .. } | Entry::Greeting { .. } => None,
    };
    move |error| match source {
        Some((room_id, channel_id)) => RoomKitError::of_entry(error, &room_id, &channel_id),
        None => error.into(),
    }
}

/// What a call under `span` that injects an event came to, and the span's
/// end.
pub(super) fn injected_under(
    span: &LateSpan,
    injected: Result<Entered, RoomKitError>,
) -> Result<RoomEvent, RoomKitError> {
    let result = injected.and_then(|entered| event_of(entered.result));
    match &result {
        Ok(_) => span.end(Metadata::new()),
        Err(error) => span.fail_error(error.class_name(), error.to_string(), Metadata::new()),
    }
    result
}

/// The event an injection committed: once the kit's close cut it before
/// its commit, nothing of it stored, [`RoomKitError::KitClosed`] (RFC
/// §12.10.4).
pub(super) fn event_of(result: InboundResult) -> Result<RoomEvent, RoomKitError> {
    match result.event {
        Some(event) => Ok(event),
        None if result.cancellation_reason.as_deref() == Some(KIT_CLOSED) => {
            Err(RoomKitError::KitClosed)
        }
        None => Err(no_event()),
    }
}

/// What a direct injection that ended with neither its event nor a refusal
/// fails with (RFC §10.5): a defect of this crate.
fn no_event() -> RoomKitError {
    RoomKitError::Internal(
        "a direct injection ended with neither its event nor a refusal".to_owned(),
    )
}

/// The kit's call that injects an event, as an error names it.
const SEND_EVENT: &str = "send_event";
