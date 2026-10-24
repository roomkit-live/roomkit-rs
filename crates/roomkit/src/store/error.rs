//! Why a store refused or failed a request, and what a store that could
//! not be reached reports as its cause.

use std::error::Error;
use std::ops::Deref;

use roomkit_core::model::{ChannelType, ErrorRecord, EventId, IdentityId, ParticipantId, RoomId};
use thiserror::Error;

use crate::error;

/// What a store that could not be reached or failed reports as its cause:
/// a database driver's error, an I/O error, or a message (`"connection
/// refused".into()`).
///
/// It dereferences to the error it holds, which is
/// [`StoreError::Unavailable`]'s source, and equals only itself: a cause
/// cannot be compared.
pub struct StoreFailure(Box<dyn Error + Send + Sync>);

impl StoreFailure {
    /// The failure `cause` stands for: the error the store met, or a
    /// message.
    pub fn new(cause: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(cause.into())
    }
}

impl Deref for StoreFailure {
    type Target = dyn Error + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Shows the error it holds, as that error shows itself.
impl std::fmt::Debug for StoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, formatter)
    }
}

/// One failure equals itself only.
impl PartialEq for StoreFailure {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for StoreFailure {}

/// Why a store refused or failed a request.
///
/// Two errors are equal when they are the same refusal, field for field; an
/// [`Unavailable`](Self::Unavailable) error only to itself, since its cause
/// cannot be compared.
#[derive(Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// A room with the id exists already.
    #[error("room {0} exists already")]
    RoomExists(RoomId),
    /// No room has the id.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The room has a participant with the id already (RFC §5.5).
    #[error("room {room_id} has a participant {participant_id} already")]
    ParticipantExists {
        /// The room.
        room_id: RoomId,
        /// The participant.
        participant_id: ParticipantId,
    },
    /// The room has no participant with the id.
    #[error("room {room_id} has no participant {participant_id}")]
    ParticipantNotFound {
        /// The room.
        room_id: RoomId,
        /// The participant.
        participant_id: ParticipantId,
    },
    /// An identity with the id exists already.
    #[error("identity {0} exists already")]
    IdentityExists(IdentityId),
    /// No identity of the organization has the id.
    #[error("no identity {0} in the organization")]
    IdentityNotFound(IdentityId),
    /// Another identity of the organization uses the address on channels of
    /// the type already (RFC §14.1, gap G-060).
    #[error("the {channel_type:?} address {address} belongs to identity {identity_id} already")]
    AddressLinked {
        /// The channel type.
        channel_type: ChannelType,
        /// The address.
        address: String,
        /// The identity that uses it.
        identity_id: IdentityId,
    },
    /// No event has the id (RFC §14.1 `update_event`), or the target of
    /// an edit or a delete is gone by its commit.
    #[error("event {0} does not exist")]
    EventNotFound(EventId),
    /// The commit's index is not the room's next one (RFC §8.1): another
    /// writer committed first, or the index was given out already, a
    /// hard-deleted one included, which is never given again.
    #[error("index {index} is not room {room_id}'s next index")]
    IndexTaken {
        /// The room.
        room_id: RoomId,
        /// The index.
        index: u64,
    },
    /// The store could not be reached or failed: the error it met is the
    /// cause ([`std::error::Error::source`]), which the message leaves to
    /// a reader of the chain, [`to_record`](Self::to_record) among them.
    #[error("the store failed")]
    Unavailable(#[source] StoreFailure),
}

impl StoreError {
    /// A store that could not be reached or failed, with `cause`, the error
    /// it met, or a message.
    pub fn unavailable(cause: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::Unavailable(StoreFailure::new(cause))
    }

    /// The error as the core records it, its message followed by each of
    /// its causes': `the store failed: connection refused`.
    pub fn to_record(&self) -> ErrorRecord {
        let error_type = match self {
            Self::RoomExists(_) => "RoomExists",
            Self::RoomNotFound(_) => "RoomNotFound",
            Self::ParticipantExists { .. } => "ParticipantExists",
            Self::ParticipantNotFound { .. } => "ParticipantNotFound",
            Self::IdentityExists(_) => "IdentityExists",
            Self::IdentityNotFound(_) => "IdentityNotFound",
            Self::AddressLinked { .. } => "AddressLinked",
            Self::EventNotFound(_) => "EventNotFound",
            Self::IndexTaken { .. } => "IndexTaken",
            Self::Unavailable(_) => "StoreUnavailable",
        };
        ErrorRecord::new(error_type, error::with_causes(self))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use pretty_assertions::assert_eq;

    use super::*;

    /// The cause is the error's source, shown once: in the record the core
    /// keeps, which reads the whole chain, not in the error's own message.
    #[test]
    fn an_unavailable_store_keeps_the_error_it_met_as_its_cause() {
        let met = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
        let error = StoreError::unavailable(met);
        let cause = error
            .source()
            .and_then(|cause| cause.downcast_ref::<io::Error>())
            .map(io::Error::kind);
        assert_eq!(
            (cause, error.to_string(), error.to_record().message),
            (
                Some(io::ErrorKind::ConnectionRefused),
                "the store failed".to_owned(),
                "the store failed: connection refused".to_owned()
            )
        );
    }

    /// A cause of no size takes no address of its own.
    #[derive(Debug)]
    struct Refused;

    impl std::fmt::Display for Refused {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("refused")
        }
    }

    impl Error for Refused {}

    /// An unavailable store's cause cannot be compared: the error equals
    /// itself only, as a refusal equals the same refusal, whatever its cause
    /// is, one of no size included.
    #[test]
    fn an_unavailable_store_equals_itself_only() {
        let error = StoreError::unavailable(Refused);
        let itself = &error;
        let room = RoomId::new("room_1").unwrap();
        assert_eq!(
            (
                error == *itself,
                error == StoreError::unavailable(Refused),
                StoreError::unavailable("connection refused")
                    == StoreError::unavailable("connection refused"),
                StoreError::RoomNotFound(room.clone()) == StoreError::RoomNotFound(room),
            ),
            (true, false, false, true)
        );
    }
}
