//! One turn's human-input requests on its executor (RFC §10.6.5), as the
//! Python implementation's `ExecutorInputs`.
//!
//! A request its channel's handler opens is sent to the owner as an
//! `input_request` before `ON_USER_INPUT_REQUIRED` fires; the owner's
//! `input_answer` settles it here while it is still open, and
//! `input_taken` says whether it did. A request settled here (an answer
//! the handler took, a hook's block, its bound, a release) is sent as an
//! `input_closed`, and the owner answers it no more. When the turn ends,
//! its requests still open are withdrawn, but a detached one, its
//! creator's to release, which the turn only lets go of; nothing more is
//! sent.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use roomkit_core::model::Metadata;

use super::link::FrameSender;
use crate::turns::lock;
use crate::turns::wire::{Frame, InputOutcome};

/// What settles a request with the outcome its owner recorded: the
/// human-input handler that opened it.
pub(crate) trait InputSettler: Send + Sync {
    /// Settles `pending_id` with an answer (`value`) or a rejection
    /// (`reason`): whether it was open.
    fn settle(&self, pending_id: &str, value: Option<String>, reason: Option<String>) -> bool;

    /// Withdraws `pending_id`: its turn ended while it was open. One its
    /// creator releases (a detached request) is only let go of.
    fn withdraw(&self, pending_id: &str);
}

/// The open human-input requests of one turn, and their way to its owner.
pub(crate) struct ExecutorInputs {
    sender: Arc<FrameSender>,
    state: Mutex<Open>,
}

struct Open {
    /// Whether the turn still runs: a request opened after it ended is the
    /// handler's alone.
    running: bool,
    requests: HashMap<String, Arc<dyn InputSettler>>,
}

impl std::fmt::Debug for ExecutorInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open = lock(&self.state);
        formatter
            .debug_struct("ExecutorInputs")
            .field("running", &open.running)
            .field("requests", &open.requests.len())
            .finish_non_exhaustive()
    }
}

impl ExecutorInputs {
    pub(crate) fn new(sender: Arc<FrameSender>) -> Self {
        Self {
            sender,
            state: Mutex::new(Open {
                running: true,
                requests: HashMap::new(),
            }),
        }
    }

    /// Whether the turn still runs.
    pub(crate) fn is_open(&self) -> bool {
        lock(&self.state).running
    }

    /// The turn ended: its requests still open are withdrawn, on both sides
    /// (RFC §10.6.5), but a detached one, which its creator releases; later
    /// ones are the handler's alone.
    pub(crate) fn close(&self) {
        let still_open = {
            let mut open = lock(&self.state);
            open.running = false;
            std::mem::take(&mut open.requests)
        };
        for (pending_id, settle) in still_open {
            settle.withdraw(&pending_id);
        }
    }

    /// The request `pending_id` of the tool `tool_name` asked with
    /// `arguments` is open: the owner learns it, and answers it through
    /// `settle`, an answer it held early included.
    pub(crate) fn opened(
        &self,
        pending_id: &str,
        tool_name: &str,
        arguments: &Metadata,
        settle: Arc<dyn InputSettler>,
    ) {
        {
            let mut open = lock(&self.state);
            if !open.running {
                return;
            }
            open.requests.insert(pending_id.to_owned(), settle);
        }
        let frame = Frame::InputRequest {
            request_id: pending_id.to_owned(),
            tool_name: tool_name.to_owned(),
            arguments: arguments.clone(),
        };
        if let Err(uncarried) = self.sender.send(&frame) {
            tracing::warn!(target: "roomkit::turns", request = pending_id, why = %uncarried.0, "a human-input request could not reach its room owner");
        }
    }

    /// The request `pending_id` settled here, `reason` its status, not by
    /// the owner's answer: the owner closes its record.
    pub(crate) fn closed(&self, pending_id: &str, reason: &str) {
        {
            let mut open = lock(&self.state);
            if !open.running || open.requests.remove(pending_id).is_none() {
                return;
            }
        }
        let frame = Frame::InputClosed {
            request_id: pending_id.to_owned(),
            reason: reason.to_owned(),
        };
        if self.sender.send(&frame).is_err() {
            tracing::warn!(target: "roomkit::turns", request = pending_id, "a human-input close could not reach the room owner");
        }
    }

    /// The owner's outcome for `request_id`: it settles the request if
    /// still open, and the owner learns whether it did.
    pub(crate) fn answer(
        &self,
        request_id: &str,
        outcome: InputOutcome,
        value: Option<String>,
        reason: Option<String>,
    ) {
        let settle = lock(&self.state).requests.remove(request_id);
        let taken = settle.is_some_and(|settle| match outcome {
            InputOutcome::Answered => {
                settle.settle(request_id, Some(value.unwrap_or_default()), None)
            }
            InputOutcome::Rejected => {
                settle.settle(request_id, None, Some(reason.unwrap_or_default()))
            }
        });
        let frame = Frame::InputTaken {
            request_id: request_id.to_owned(),
            taken,
        };
        drop(self.sender.send(&frame));
    }
}
