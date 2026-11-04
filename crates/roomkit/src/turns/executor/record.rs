//! What a turn's channel writes on its record reaches the owner (RFC §10.6.4):
//! before each item the executor sends, it sends the record when it changed.
//! Before a delta, it counts as changed only once one of its keys was written:
//! a stream of deltas that leaves it alone costs no copy of it (Python's
//! `turns/_executor_changes.py`).

use std::sync::{Mutex, PoisonError};

use roomkit_core::model::{Metadata, ResponseMetadata};

use super::link::FrameSender;
use crate::turns::pipe::Uncarried;
use crate::turns::wire::Frame;

/// The record as last sent, and the writes it had taken when copied.
#[derive(Default)]
struct Sent {
    live: Option<ResponseMetadata>,
    record: Metadata,
    writes: u64,
}

/// One turn's record, sent before the items it precedes.
#[derive(Default)]
pub(super) struct RecordChanges {
    sent: Mutex<Sent>,
}

impl RecordChanges {
    /// Watches `live`, the channel's output's record, as the `output` frame
    /// is about to carry it: a change is one from it.
    pub(super) fn watch(&self, live: &ResponseMetadata) {
        let mut sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
        sent.writes = live.writes();
        sent.record = live.to_metadata();
        sent.live = Some(live.clone());
    }

    /// Sends the record when it differs from the last one sent, read before
    /// an item that is a delta only once a key was written since.
    pub(super) fn report(&self, sender: &FrameSender, before_delta: bool) -> Result<(), Uncarried> {
        let mut sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(live) = sent.live.clone() else {
            return Ok(());
        };
        let writes = live.writes();
        if before_delta && writes == sent.writes {
            return Ok(());
        }
        sent.writes = writes;
        let record = live.to_metadata();
        if record == sent.record {
            return Ok(());
        }
        sent.record.clone_from(&record);
        drop(sent);
        sender.send(&Frame::Record { record }).map(drop)
    }
}
