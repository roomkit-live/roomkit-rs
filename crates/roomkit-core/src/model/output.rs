//! What a channel returns for an event (RFC §6.1).

use serde::{Deserialize, Serialize};

use super::{Metadata, Observation, ProviderResult, RoomEvent, Task};

/// A channel's answer to `deliver()` or `on_event()` (RFC §6.1).
///
/// The RFC's response stream is not modeled until streamed responses are:
/// a response is a list of events.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelOutput {
    /// The channel answered the event; only then do its response events
    /// re-enter the room.
    pub responded: bool,
    /// The response events, subject to permissions.
    pub response_events: Vec<RoomEvent>,
    /// The turn's response-metadata record (RFC §6.7).
    pub response_metadata: Metadata,
    /// A transport delivery's provider answer.
    pub provider_result: Option<ProviderResult>,
    /// Tasks, always collected.
    pub tasks: Vec<Task>,
    /// Observations, always collected.
    pub observations: Vec<Observation>,
    /// Room metadata to update, written key by key once the cascade is over;
    /// a key under `roomkit.` is the implementation's and is refused (gap
    /// G-049).
    pub metadata_updates: Metadata,
    /// An error met while producing the output; the output is still used.
    pub error: Option<ErrorRecord>,
}

impl ChannelOutput {
    /// An output that answers the event with `events`.
    pub fn respond(events: Vec<RoomEvent>) -> Self {
        Self {
            responded: true,
            response_events: events,
            ..Self::default()
        }
    }
}

/// An error, as data: what crosses between the core and a driver in place of
/// an exception.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// The error's type name, or a provider's error code.
    pub error_type: String,
    /// The error's message.
    pub message: String,
}

impl ErrorRecord {
    /// An error of `error_type` saying `message`.
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_output_has_not_responded() {
        assert!(!ChannelOutput::default().responded);
    }

    #[test]
    fn a_responding_output_has_responded() {
        assert!(ChannelOutput::respond(Vec::new()).responded);
    }
}
