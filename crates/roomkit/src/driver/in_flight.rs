//! The commit a run has in flight as the kit's close cuts it (RFC
//! §12.10.4): the run reads it back by its id within the unwind the close
//! gives it, and a run the close aborts first, its unwind run out, reports
//! nothing stored, with the warning a failed read-back gives.

use std::sync::{Mutex, PoisonError};

use roomkit_core::model::EventId;

/// The commit that decides a run, once it started: what a run the kit's
/// close aborted before it read its commit back warns of.
#[derive(Debug, Default)]
pub(crate) struct InFlight(Mutex<Option<(&'static str, EventId)>>);

impl InFlight {
    /// The run of `call` started the commit of `event_id`, which decides it.
    pub(crate) fn commits(&self, call: &'static str, event_id: &EventId) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some((call, event_id.clone()));
    }

    /// The run read its commit back, or failed to, and said so: an abort
    /// from now on warns of nothing more.
    pub(crate) fn settled(&self) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
    }

    /// The kit's close aborted the run, its unwind run out before the run
    /// read its commit back: a commit in flight is reported as a read-back
    /// that failed (RFC §12.10.4).
    pub(crate) fn aborted(&self) {
        let noted = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((call, event_id)) = noted {
            read_back_failed(call, &event_id, "it did not end within the close's unwind");
        }
    }
}

/// Warns that the kit closed as `call` committed `event_id`, and that
/// reading it back failed, as `failure` says: the call reports nothing
/// stored (RFC §12.10.4).
pub(crate) fn read_back_failed(call: &'static str, event_id: &EventId, failure: &str) {
    tracing::warn!(
        target: "roomkit::driver",
        %event_id,
        call,
        failure,
        "the kit closed as a call committed its event, and reading it back failed: the call \
         reports nothing stored"
    );
}
