//! The standalone configuration of an agent's background tasks (RFC §23.4).

use crate::error::{RoomKitError, repr};

/// How an agent's background tasks run. Python's
/// `BackgroundSetup(worker=None, hand_back='relay')`.
///
/// The worker is the host's text, or the agent itself when absent. The
/// configuration is immutable after construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundSetup {
    worker: Option<String>,
    hand_back: String,
}

impl BackgroundSetup {
    /// Configure the worker and how its result reaches the agent.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] unless `hand_back` is `relay` or `post`.
    pub fn new(worker: Option<String>, hand_back: impl Into<String>) -> Result<Self, RoomKitError> {
        let hand_back = hand_back.into();
        validate_hand_back(&hand_back)?;
        Ok(Self { worker, hand_back })
    }

    /// The named worker, or `None` for the agent itself.
    pub fn worker(&self) -> Option<&str> {
        self.worker.as_deref()
    }

    /// Whether the result is relayed by the agent or posted directly.
    pub fn hand_back(&self) -> &str {
        &self.hand_back
    }
}

impl Default for BackgroundSetup {
    fn default() -> Self {
        Self {
            worker: None,
            hand_back: "relay".to_owned(),
        }
    }
}

pub(crate) fn validate_hand_back(mode: &str) -> Result<(), RoomKitError> {
    match mode {
        "relay" | "post" => Ok(()),
        _ => Err(RoomKitError::Value(format!(
            "hand_back must be one of ('relay', 'post'), not {}",
            repr(mode)
        ))),
    }
}
