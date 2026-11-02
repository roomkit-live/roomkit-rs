//! A room's lock the host's code holds outside every run of the driver: a
//! strategy's install (RFC §19.7), which calls the kit back while the
//! room waits for it, as the Python implementation's install runs under
//! its reentrant room lock (RFC §13.5).
//!
//! The section takes the lock, unless the code runs under a live lease on
//! it already (an install called from a hook under the lock), and runs its
//! code within a frame that holds a lease on the lock's tenure: a separate
//! operation that code calls, an attach or a metadata change, is granted
//! the lock at once, and its release gives nothing back; any other caller
//! waits for the section's end. The work the code started in the kit
//! (its calls' runs, their store writes) runs within the section's unit,
//! which [`Tasks::none_within`](crate::tasks::Tasks::none_within) waits
//! for: what reads the room after a cut must not read it before a write
//! the cut call had decided lands.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::RoomId;
use roomkit_core::ops::EffectId;

use super::{ExecutionContext, Frame, RunShared, Work};
use crate::locks::{RoomGuard, RoomMutexes};
use crate::tasks::{Marks, Unit};

/// A room's lock held by the host's code, and the frame its calls run in.
#[derive(Debug)]
pub(crate) struct Section {
    /// The frame whose lease the code's calls are granted the lock on;
    /// none when the code holds a lease on it already, from its caller.
    held: Option<Held>,
    /// The unit of the kit's work the code's calls run within.
    unit: Unit,
}

#[derive(Debug)]
struct Held {
    room_id: RoomId,
    run: Arc<RunShared>,
    frame: Frame,
    /// Dropped after the run let its tenure go: no lease outlives the lock.
    #[expect(dead_code, reason = "held for its drop, which releases the lock")]
    guard: RoomGuard<'static>,
}

impl Section {
    /// Takes `room_id`'s lock for the current code, waiting for it as any
    /// caller does, unless the code runs under a live lease on it.
    pub(crate) async fn enter(locks: &RoomMutexes, room_id: &RoomId) -> Self {
        let unit = Unit::new();
        if ExecutionContext::current().lease(room_id).is_some() {
            return Self { held: None, unit };
        }
        let guard = locks.lock_owned(room_id).await;
        let (run, _adoptions) = RunShared::new();
        run.locked(room_id.clone());
        let effect = EffectId::of_host_section();
        run.calling(effect);
        let work = Work {
            locked: Some(room_id.clone()),
            ..Work::default()
        };
        let frame = Frame::new(&run, effect, work);
        let held = Held {
            room_id: room_id.clone(),
            run,
            frame,
            guard,
        };
        Self {
            held: Some(held),
            unit,
        }
    }

    /// The unit of the kit's work the section's calls run within.
    pub(crate) fn unit(&self) -> Unit {
        self.unit
    }

    /// Runs `future` within the section: its calls into the room are
    /// granted the lock the section holds, and the kit's work they start
    /// runs within the section's unit.
    pub(crate) async fn scope<F: Future>(&self, future: F) -> F::Output {
        let marks = Marks::current().with(self.unit);
        let Some(held) = &self.held else {
            return marks.scoped(future).await;
        };
        let context = ExecutionContext::current().within(held.frame.clone());
        marks.scoped(context.scope(future)).await
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        // The lease dies with the tenure, before the lock is let go.
        self.run.released(&self.room_id);
        self.run.clear();
    }
}
