//! What watches the messages the backend carries: the host's tap
//! ([`crate::testing::TurnTap`]), and, in this crate's tests, the faults a
//! transport could make.

use std::fmt;
use std::sync::Arc;

/// The family a carried message belongs to (RFC §10.6.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Carried {
    /// A turn's request, owner to executor.
    Request,
    /// A frame, executor to owner, in its envelope.
    Frame,
    /// A control message or a call's result, owner to executor, in its
    /// envelope.
    Control,
    /// A channel control message, owner to executor, outside any turn.
    ChannelControl,
    /// The answer to a per-room channel verb.
    ChannelResult,
    /// A host verb made after its turn, executor to owner.
    RoomCall,
    /// The answer to a `room_call`, owner to executor.
    RoomCallResult,
}

impl Carried {
    /// The family's name, as a tap reports it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Frame => "frame",
            Self::Control => "control",
            Self::ChannelControl => "channel_control",
            Self::ChannelResult => "channel_result",
            Self::RoomCall => "room_call",
            Self::RoomCallResult => "room_call_result",
        }
    }
}

/// A tap: told each message as it is carried, serialized.
pub(crate) type Tap = Arc<dyn Fn(Carried, &str) + Send + Sync>;

/// What becomes of a message a fault meets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Fate {
    /// Refused with an error before numbering, unlike a silently unsent frame.
    #[cfg(test)]
    pub(crate) refused: Option<&'static str>,
    /// Never sent: it takes no number.
    pub(crate) unsent: bool,
    /// Numbered, then lost on its way.
    pub(crate) lost: bool,
    /// Delivered twice.
    pub(crate) duplicated: bool,
}

/// A transport's fault, in tests: what becomes of each message.
#[cfg(test)]
pub(crate) type Fault = Arc<dyn Fn(Carried, &str) -> Fate + Send + Sync>;

/// The watchers of the messages a backend carries.
#[derive(Clone, Default)]
pub(crate) struct Observers {
    tap: Option<Tap>,
    #[cfg(test)]
    fault: Option<Fault>,
}

impl fmt::Debug for Observers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Observers")
            .field("tapped", &self.tap.is_some())
            .finish_non_exhaustive()
    }
}

impl Observers {
    /// The same watchers, `tap` told every message.
    pub(crate) fn with_tap(mut self, tap: Tap) -> Self {
        self.tap = Some(tap);
        self
    }

    /// The same watchers, `fault` deciding each message's fate.
    #[cfg(test)]
    pub(crate) fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }

    /// Whether a tap watches: a request is then written whole for it.
    pub(crate) fn tapped(&self) -> bool {
        self.tap.is_some()
    }

    /// What becomes of `raw`, a `carried` message about to be queued; the
    /// tap is told one that is queued.
    pub(crate) fn carry(&self, carried: Carried, raw: &str) -> Fate {
        let fate = self.fate(carried, raw);
        #[cfg(test)]
        if fate.refused.is_some() {
            return fate;
        }
        if let Some(tap) = &self.tap
            && !fate.unsent
            && !fate.lost
        {
            tap(carried, raw);
        }
        fate
    }

    /// Tells the tap of `raw`, a message carried outside a pipe.
    pub(crate) fn tell(&self, carried: Carried, raw: &str) {
        if let Some(tap) = &self.tap {
            tap(carried, raw);
        }
    }

    #[cfg(test)]
    fn fate(&self, carried: Carried, raw: &str) -> Fate {
        self.fault
            .as_ref()
            .map_or_else(Fate::default, |fault| fault(carried, raw))
    }

    #[cfg(not(test))]
    #[expect(
        clippy::unused_self,
        reason = "the same call as the tests', whose faults decide"
    )]
    fn fate(&self, _carried: Carried, _raw: &str) -> Fate {
        Fate::default()
    }
}
