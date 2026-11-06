//! Who receives the failure of a call's own stream, and so logs it (RFC
//! §15.2): the framework's line for it is then a DEBUG one.

use super::Call;

/// Who receives the failure of a call's own stream, and so logs it, the
/// framework's line then a DEBUG one (RFC §15.2; Python's `caller_logs` and
/// `streamed_too`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureReader {
    /// Nobody: a direct injection's or a host post's caller has the stored
    /// event, a greeting's and a discussion turn's have nothing.
    Nobody,
    /// The caller, on its result, unless a streaming target rendered it,
    /// whose line is kept (RMK-403): a message's, a regeneration's, a
    /// verification's release.
    Caller,
    /// The caller, rendered or not: a delegation, whose task logs every
    /// failure of its turn.
    Delegation,
}

impl FailureReader {
    /// Who receives the failure of `call`'s own stream.
    pub(super) fn of(call: &Call) -> Self {
        match call {
            Call::Message { .. } | Call::Rerun { .. } | Call::Regenerate { .. } => Self::Caller,
            Call::Responses { .. } => Self::Delegation,
            Call::Direct { .. } | Call::Post { .. } | Call::Turn { .. } | Call::Greeting { .. } => {
                Self::Nobody
            }
        }
    }

    /// Whether it receives the failure of a stream, `own` the call's, which
    /// a streaming target read when `rendered`: never a chained or adopted
    /// one's, which is not the call's (Python's `caller_logs_failure_of`).
    pub(super) fn receives(self, own: bool, rendered: bool) -> bool {
        own && match self {
            Self::Nobody => false,
            Self::Caller => !rendered,
            Self::Delegation => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelId, EventId, RoomId};

    fn room_id() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    #[test]
    fn a_message_s_and_a_rerun_s_caller_receives_a_failure_no_target_rendered() {
        let rerun = Call::Rerun {
            room_id: room_id(),
            event_id: EventId::new("evt_1").unwrap(),
            floor: 0,
        };
        let reader = FailureReader::of(&rerun);
        assert_eq!(
            (
                reader,
                reader.receives(true, false),
                reader.receives(true, true)
            ),
            (FailureReader::Caller, true, false)
        );
    }

    #[test]
    fn a_discussion_turn_s_and_a_greeting_s_caller_receives_none() {
        let turn = Call::Turn { room_id: room_id() };
        let greeting = Call::Greeting {
            room_id: room_id(),
            agent_id: ChannelId::new("ai1").unwrap(),
            text: "Hello".to_owned(),
            chain_depth: 0,
        };
        assert_eq!(
            [FailureReader::of(&turn), FailureReader::of(&greeting)],
            [FailureReader::Nobody, FailureReader::Nobody]
        );
        assert!(!FailureReader::Nobody.receives(true, false));
    }

    #[test]
    fn a_delegation_receives_its_own_streams_failures_rendered_or_not() {
        let delegation = FailureReader::Delegation;
        assert_eq!(
            (
                delegation.receives(true, false),
                delegation.receives(true, true)
            ),
            (true, true)
        );
    }

    /// Python's `caller_logs_failure_of`: a chained or adopted stream is
    /// not the caller's.
    #[test]
    fn no_caller_receives_a_stream_not_its_own() {
        let readers = [
            FailureReader::Caller,
            FailureReader::Delegation,
            FailureReader::Nobody,
        ];
        let received: Vec<bool> = readers
            .iter()
            .flat_map(|reader| [reader.receives(false, false), reader.receives(false, true)])
            .collect();
        assert_eq!(received, [false; 6]);
    }
}
