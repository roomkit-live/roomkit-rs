//! The live outcome shared by every handle to a human-input request.

use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::model::PendingInputStatus;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct Outcome {
    pub(super) status: PendingInputStatus,
    pub(super) result: Option<String>,
    pub(super) reject_reason: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SharedOutcome(Arc<Mutex<Outcome>>);

impl SharedOutcome {
    pub(super) fn new(outcome: Outcome) -> Self {
        Self(Arc::new(Mutex::new(outcome)))
    }

    pub(super) fn read(&self) -> Outcome {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn set(&self, outcome: Outcome) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = outcome;
    }
}

impl PartialEq for SharedOutcome {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.read() == other.read()
    }
}

impl Serialize for SharedOutcome {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.read().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SharedOutcome {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Outcome::deserialize(deserializer).map(Self::new)
    }
}
