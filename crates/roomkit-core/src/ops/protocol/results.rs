//! What an inbound call comes to ([`InboundResult`]) and why it fails
//! ([`InboundError`]): the inbound operation's output, and what the protocol
//! hands the caller of an entry it adopted ([`Settled`](super::Settled)) or
//! of a call it decided ([`Decided`](super::Decided)).

use std::collections::BTreeMap;

use thiserror::Error;

use crate::model::{
    ChannelId, DeliveryResult, ErrorRecord, Metadata, ParticipantId, RoomEvent, RoomId,
};

/// What became of an inbound message (RFC §10.1 `InboundResult`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InboundResult {
    /// The committed event: delivered, or the record stored `BLOCKED` when a
    /// hook or the source's permissions refused it (gap G-026). `None` when
    /// nothing was committed, as when the room refused the message or step
    /// 8a refused an edit or a delete.
    ///
    /// An instruction, which is never committed, is the exception (RFC
    /// §10.1.1 rule 5): once its pass decided it, the result carries it
    /// uncommitted, `DELIVERED` or `BLOCKED` with its `blocked_by`, at the
    /// index its hooks were shown, which the next event takes (gap G-065).
    pub event: Option<RoomEvent>,
    /// Whether the call is a replay of an idempotency key its channel holds
    /// in the room (RFC §13.4): `event` is the event the key committed, and
    /// no new delivery plan was resolved.
    pub duplicate: bool,
    /// The channels the event addressed (`addressed_to`) that its committed
    /// delivery plan asks no turn of: unbound, unregistered, or not an
    /// intelligence channel of the room, in the order the event names them
    /// (RFC §10.1 step 18). `None` when no plan was resolved: a replay, a
    /// refused or blocked event.
    pub unavailable_targets: Option<Vec<ChannelId>>,
    /// Whether the message was blocked: refused by the room, step 8a, a hook
    /// or its source's permissions, an instruction refused before anything
    /// was read, or a call past its `process_timeout` (RFC §13.6).
    pub blocked: bool,
    /// Why it was blocked: `instruction_unaddressed` or
    /// `instruction_not_idempotent` (RFC §10.1.1); `room_closed`; one of step
    /// 8a's `target_event_not_found`, `identity_required_for_edit`,
    /// `not_authorized`, `not_original_author` (RFC §10.1 step 8a); a hook's
    /// reason; `source_muted`, `source_read_only`, `source_unbound`;
    /// `process_timeout` (RFC §13.6), with no event. A replay gives the
    /// record's `blocked_by`, the hook's reason not being stored.
    pub reason: Option<String>,
    /// The first error met once the message was decided: a turn's, a
    /// later commit's, or the room lock's release (gap G-025). What was
    /// decided stands (RFC §13.6).
    pub error: Option<ErrorRecord>,
    /// The merged response metadata of the turns the message asked for.
    pub response_metadata: Metadata,
    /// The responses this call's cascade committed and delivered, in their
    /// commit order: the answers to its event and to their answers, a
    /// stream's rows among them, never the event itself, a response stored
    /// `BLOCKED`, nor another call's (RFC §10.1 step 14).
    pub response_events: Vec<RoomEvent>,
    /// The deliveries of the event to its transport channels.
    pub delivery_results: BTreeMap<ChannelId, DeliveryResult>,
    /// Why the call's delivery was cancelled, the event it committed
    /// standing: [`KIT_CLOSED`] for a call the kit's close cut, its
    /// delivery dropped or cut running, or under way as the close started
    /// (RFC §10.1 step 18, §12.10.4), as the Python implementation's
    /// `cancellation_reason` reads; `None` for a delivery that ran.
    pub cancellation_reason: Option<String>,
    /// Whether the call was refused before the locked region (RFC §10.1
    /// step 18): past the reentry budget it draws on, an instruction that
    /// cannot be one, or past its `process_timeout` before the room's
    /// lock. Such a call has no deferred tail: a deferred one gets no
    /// completion handle.
    pub refused_before_the_lock: bool,
}

/// The cancellation reason of a call the kit's close cut (RFC §12.10.4):
/// its delivery dropped before it ran or cut running, or the call under
/// way as the close started; the code of a send it cut.
pub const KIT_CLOSED: &str = "kit_closed";

/// The cancellation reason of a delivery a caller cancelled while it ran
/// (RFC §10.1 step 18), the Python implementation's `cancel_delivery`'s:
/// never the kit's close's.
pub const LANE_CANCELLED: &str = "lane_cancelled";

/// Why an inbound message could not be processed. Nothing of the message was
/// committed, unless the error says otherwise.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum InboundError {
    /// No channel is registered under the message's channel id (RFC §10.1
    /// step 1).
    #[error("no channel {0} is registered")]
    UnknownChannel(ChannelId),
    /// The room does not exist, or disappeared while the call was
    /// processed. A room of another organization than the one the caller
    /// acts for, or of none, is not found either, whether a message, the
    /// router or a direct injection names it (RFC §17.2, §10.1 step 2, gap
    /// G-077). Nothing was written.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The message arrived on a host channel, which receives no external
    /// payload: the host speaks through `post` and `send_event` only (RFC
    /// §10.1 step 1, §10.5). Nothing was read or written.
    #[error("channel {0} is a host channel, which receives no external payload")]
    InboundOnHostChannel(ChannelId),
    /// `post` speaks from a channel of type `HOST` only: the channel it
    /// names is of another type, or the id `host` it speaks from by default
    /// is held by one (RFC §10.5). Nothing was written.
    #[error("channel {0} is not a host channel: post() speaks from a channel of type HOST")]
    NotAHostChannel(ChannelId),
    /// The host channel a post speaks from declares itself an intelligence
    /// channel: its first attach would bind it as one, which RFC §6.2 and
    /// §10.5 refuse. Nothing was read or written.
    #[error("channel {0} is a host channel, never bound as an intelligence channel")]
    HostAsIntelligence(ChannelId),
    /// The speaker a post names is no participant of the room its host
    /// channel reaches, by their id or their identity's (RFC §10.5, §5.5):
    /// `reached_elsewhere` says whether the id names a participant of the
    /// room reached through other channels only, or nobody of the room.
    /// Nothing was written.
    #[error(
        "post() speaks as a participant of room {room_id} the host channel {channel_id} reaches, \
         and {participant_id} {}",
        if *reached_elsewhere {
            "names a participant reached through other channels only"
        } else {
            "names nobody of the room"
        }
    )]
    ParticipantNotFound {
        /// The room.
        room_id: RoomId,
        /// The speaker the post names.
        participant_id: ParticipantId,
        /// The host channel the post speaks from.
        channel_id: ChannelId,
        /// Whether the id names a participant reached through other
        /// channels only.
        reached_elsewhere: bool,
    },
    /// The kit's router failed to route the message (RFC §10.4). Nothing was
    /// written.
    #[error("the router failed: {0:?}")]
    Routing(ErrorRecord),
    /// `standalone` belongs to instructions only (RFC §10.1.1 rule 7).
    #[error("only an instruction may stand alone")]
    StandaloneOutsideInstruction,
    /// Only the framework writes the visibility `internal`, on the records
    /// it writes itself (gap G-041): an entry's visibility, or where a
    /// message's answers may go (gap G-073), is never `internal`. Nothing was
    /// read or written.
    #[error("only the framework writes the visibility internal")]
    InternalVisibility,
    /// A directly injected event refused before its commit (RFC §10.5),
    /// with nothing written: `reason` says why, as an inbound result's
    /// `reason` would: `room_closed`, `process_timeout`, one of step 8a's
    /// refusals, `instruction_unaddressed` or `instruction_not_idempotent`.
    #[error("the event was refused before its commit: {reason}")]
    Refused {
        /// Why.
        reason: String,
    },
    /// The channel's `handle_inbound()` failed.
    #[error("channel {channel_id} could not parse the message: {error:?}")]
    ChannelFailed {
        /// The channel.
        channel_id: ChannelId,
        /// What failed.
        error: ErrorRecord,
    },
    /// The store failed before the message was committed.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken. A failed release is no error:
    /// what was decided under the lock stands (gap G-025).
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
    /// The room's admission ticket could not be taken or waited for (RFC
    /// §9.5.1). Nothing was written. A failed release is no error: what the
    /// call decided stands.
    #[error("the room's admission ticket failed: {0:?}")]
    Ticket(ErrorRecord),
}
