//! What an inbound message became, and why one could not be processed.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::model::{ChannelId, DeliveryResult, ErrorRecord, Metadata, RoomEvent, RoomId};

/// What became of an inbound message (RFC §10.1 `InboundResult`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InboundResult {
    /// The committed event; `None` when the message was blocked.
    pub event: Option<RoomEvent>,
    /// Whether the message was blocked: refused by the room, a hook or its
    /// source's permissions.
    pub blocked: bool,
    /// Why it was blocked: `room_closed`, a hook's reason, `source_muted`,
    /// `source_read_only`.
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
    /// The named room does not exist.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The message's channel is not bound to the room.
    #[error("channel {channel_id} is not bound to room {room_id}")]
    ChannelNotBound {
        /// The channel.
        channel_id: ChannelId,
        /// The room.
        room_id: RoomId,
    },
    /// `standalone` belongs to instructions only (RFC §10.1.1 rule 7).
    #[error("only an instruction may stand alone")]
    StandaloneOutsideInstruction,
    /// The message needs a part of the pipeline this step does not provide.
    #[error("{0} is not supported yet")]
    Unsupported(&'static str),
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
}
