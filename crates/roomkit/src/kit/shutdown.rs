//! The kit's one shutdown (RFC §12.10.4): the first `close()` starts it in
//! a task of its own, and every `close()`, concurrent or later, waits for
//! that one, as the Python implementation's `close` does (RMK-1015).
//!
//! Each caller spares the units of the kit's work it runs within
//! ([`Marks`]), whether it started the shutdown or joined one under way: a
//! caller from a hook's body, a tool's call, a delivery or a hook a
//! delivery fired is neither waited for nor cut. A caller that stops
//! waiting, its future dropped, stops only its own wait. A caller from the
//! shutdown's own work, a channel's close, returns at once. The shutdown's
//! end answers every caller, the first included: the channels whose close
//! failed, or a shutdown cut before it ended.

use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::ChannelId;
use tokio::sync::watch;

use crate::error::RoomKitError;
use crate::tasks::{Marks, Tasks, Unit};

/// Where the kit's shutdown is: not started, or started in its unit, its
/// end told by a watch.
#[derive(Debug, Default)]
pub(super) struct Shutdown {
    started: Mutex<Option<Started>>,
}

#[derive(Debug)]
struct Started {
    /// The unit the shutdown's own work runs in.
    unit: Unit,
    /// How the shutdown ended, once it did; closed with nothing for a
    /// shutdown that never ended, its task dropped with its runtime or
    /// panicked.
    ended: watch::Receiver<Option<Ended>>,
}

/// How the kit's shutdown ended: the channels whose close failed, each
/// with what its panic said, none when every channel closed.
pub(super) type Ended = Arc<[(ChannelId, String)]>;

/// What a `close()` does once it joined the shutdown.
#[derive(Debug)]
pub(super) enum Joined {
    /// It returns at once: it is the shutdown's own work, which waiting
    /// for the shutdown would wait for itself.
    Own,
    /// It waits for the shutdown's end.
    Waits(watch::Receiver<Option<Ended>>),
}

/// What the task of the shutdown it starts is given: the marks its own
/// work runs within, and where it tells its end.
#[derive(Debug)]
pub(super) struct Starting {
    pub(super) marks: Marks,
    pub(super) ended: watch::Sender<Option<Ended>>,
}

impl Shutdown {
    /// Joins the current code to the kit's shutdown, `tasks` sparing the
    /// units it runs within, and starts the shutdown with `start` when
    /// none began.
    pub(super) fn join(&self, tasks: &Tasks, start: impl FnOnce(Starting)) -> Joined {
        let marks = Marks::current();
        let mut started = self.started.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(started) = started.as_ref() {
            if marks.contains(started.unit) {
                return Joined::Own;
            }
            tasks.spare(&marks);
            return Joined::Waits(started.ended.clone());
        }
        tasks.spare(&marks);
        let unit = Unit::new();
        let (ended, told) = watch::channel(None);
        *started = Some(Started {
            unit,
            ended: told.clone(),
        });
        start(Starting {
            marks: Marks::default().with(unit),
            ended,
        });
        Joined::Waits(told)
    }
}

/// Waits for the shutdown `ended` tells the end of, and answers it.
///
/// # Errors
///
/// Returns [`RoomKitError::ExceptionGroup`] when a channel's close
/// panicked, and [`RoomKitError::ShutdownCancelled`] when the shutdown never
/// ended.
pub(super) async fn wait_for(
    mut ended: watch::Receiver<Option<Ended>>,
) -> Result<(), RoomKitError> {
    let failures = match ended.wait_for(Option::is_some).await {
        Ok(ended) => ended.clone().unwrap_or_default(),
        Err(_cut) => return Err(RoomKitError::ShutdownCancelled),
    };
    if failures.is_empty() {
        return Ok(());
    }
    Err(RoomKitError::ExceptionGroup {
        failures: failures.to_vec(),
    })
}
