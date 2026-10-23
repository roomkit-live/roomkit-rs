//! What an inbound call comes to ([`InboundResult`]) and why it fails
//! ([`InboundError`]): the inbound operation's output, and what the protocol
//! hands the caller of an entry it adopted ([`Settled`](super::Settled)) or
//! of a call it decided ([`Decided`](super::Decided)).

use std::collections::BTreeMap;

use thiserror::Error;

use crate::model::{ChannelId, DeliveryResult, ErrorRecord, Metadata, RoomEvent, RoomId};

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
    /// The deliveries of the event to its transport channels.
    pub delivery_results: BTreeMap<ChannelId, DeliveryResult>,
}

/// Why an inbound message could not be processed. Nothing of the message was
/// committed, unless the error says otherwise.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum InboundError {
    /// No channel is registered under the message's channel id (RFC §10.1
    /// step 1).
    #[error("no channel {0} is registered")]
    UnknownChannel(ChannelId),
    /// The room disappeared while the message was processed.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The named room belongs to another organization than the one the
    /// caller acts for (RFC §17.2, gap G-051). Nothing was written.
    #[error("room {0} belongs to another organization")]
    OtherOrganization(RoomId),
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
