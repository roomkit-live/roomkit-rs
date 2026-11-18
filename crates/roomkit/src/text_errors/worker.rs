//! The original error of a pre-wrapped worker failure while its delegated
//! turn runs. The ordinary observers still receive the complete wrapper.

use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelId, ErrorRecord};

use crate::ChannelError;
use crate::turns::{RemoteError, to_record};

use super::TaskTurnFailedError;

/// One delegated turn and its requested worker. Nested or simultaneous turns
/// each own a separate capture.
#[derive(Clone, Debug)]
pub(crate) struct WorkerCause {
    worker: ChannelId,
    cause: Arc<Mutex<Option<ErrorRecord>>>,
}

impl WorkerCause {
    pub(crate) fn new(worker: ChannelId) -> Self {
        Self {
            worker,
            cause: Arc::default(),
        }
    }

    pub(crate) fn for_worker(&self, channel: &ChannelId) -> Option<Self> {
        (&self.worker == channel).then(|| self.clone())
    }

    pub(crate) fn capture(&self, failure: &ChannelError) {
        let cause = if let Some(wrapper) = failure.downcast_ref::<TaskTurnFailedError>() {
            Some(wrapper.cause_record())
        } else if let Some(RemoteError(wire)) = failure.downcast_ref::<RemoteError>()
            && wire.kind == "TaskTurnFailedError"
        {
            wire.causes.first().map(to_record)
        } else {
            None
        };
        if let Some(cause) = cause {
            self.cause
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .replace(cause);
        }
    }

    pub(crate) fn take(&self) -> Option<ErrorRecord> {
        self.cause
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}
