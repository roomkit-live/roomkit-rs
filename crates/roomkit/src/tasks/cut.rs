//! The cut of a held task's work (RFC §23.3, §12.2 step 13s): what a
//! cancel, a caller that stops waiting for an inline task, or the kit's
//! close stops a task's work with, as Python's cancel raises in the task's
//! coroutine.
//!
//! A cut work is dropped where it waits, unless a read of one of its
//! turns' streams runs: that read sees the cut as a stop from outside,
//! cuts its turn and stores what the turn said, the text it was handed
//! marked `cancelled` and its open calls ended `failed`, as Python's
//! reader does in its `finally`; the work is dropped once no read runs.
//! A read finds the cut of the task its code runs within by its marks:
//! the innermost unit a held task's work runs in, unless a run set apart
//! from that work stands between them (a deferred delivery, which Python
//! runs as a task of its own).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;

use super::{Marks, Unit};

/// The cut of one held task's work.
#[derive(Debug)]
pub(crate) struct TaskCut {
    /// The reason the work's turns are cut with, once it is cut.
    reason: watch::Sender<Option<&'static str>>,
    /// How many reads of the work's turns' streams run now.
    reads: watch::Sender<usize>,
}

impl Default for TaskCut {
    fn default() -> Self {
        Self {
            reason: watch::Sender::new(None),
            reads: watch::Sender::new(0),
        }
    }
}

impl TaskCut {
    /// Cuts the work, its turns cut with `reason`; a work cut already
    /// keeps its first reason.
    pub(crate) fn cut(&self, reason: &'static str) {
        self.reason.send_if_modified(|cut| {
            let first = cut.is_none();
            if first {
                *cut = Some(reason);
            }
            first
        });
    }

    /// The signal a read of the work's turns stops on: the reason once
    /// the work is cut.
    pub(crate) fn signal(&self) -> watch::Receiver<Option<&'static str>> {
        self.reason.subscribe()
    }

    /// Counts a read of one of the work's turns' streams until the guard
    /// drops: the work is not dropped while it runs.
    pub(crate) fn reading(self: &Arc<Self>) -> TaskRead {
        self.reads.send_modify(|reads| *reads += 1);
        TaskRead(Arc::clone(self))
    }

    /// Runs `work` to its end, or until it is cut while none of its reads
    /// runs: dropped then, where it waits. A work cut before its first
    /// step never starts.
    pub(crate) async fn run(&self, work: impl Future<Output = ()>) {
        let (mut reason, mut reads) = (self.reason.subscribe(), self.reads.subscribe());
        let dropped = async move {
            loop {
                let cut = reason.borrow_and_update().is_some();
                if cut && *reads.borrow_and_update() == 0 {
                    return;
                }
                // Both senders live as long as `self`: neither closes.
                tokio::select! {
                    _ = reason.changed() => {}
                    _ = reads.changed() => {}
                }
            }
        };
        tokio::select! {
            biased;
            () = dropped => {}
            () = work => {}
        }
    }
}

/// A read of a held task's turn's stream, counted while it lasts.
#[derive(Debug)]
pub(crate) struct TaskRead(Arc<TaskCut>);

impl Drop for TaskRead {
    fn drop(&mut self) {
        self.0
            .reads
            .send_modify(|reads| *reads = reads.saturating_sub(1));
    }
}

/// What a unit is to the reads within it.
#[derive(Debug)]
enum Mark {
    /// A held task's work runs in it, cut with this.
    Cut(Arc<TaskCut>),
    /// A run set apart from the work that started it runs in it.
    Apart,
}

/// The cuts of the held tasks' works, by the unit each work runs in, and
/// the runs set apart from them.
#[derive(Debug, Default)]
pub(crate) struct TaskCuts {
    marks: Mutex<HashMap<Unit, Mark>>,
}

impl TaskCuts {
    /// Registers `cut` as the cut of the work running in `unit` until the
    /// guard drops.
    pub(crate) fn register(self: &Arc<Self>, unit: Unit, cut: Arc<TaskCut>) -> Registered {
        self.mark(unit, Mark::Cut(cut))
    }

    /// Sets the run in the innermost unit of `marks` apart from the work
    /// that started it until the guard drops: no cut of a task that work
    /// belongs to reaches the reads within it.
    pub(crate) fn apart(self: &Arc<Self>, marks: &Marks) -> Option<Registered> {
        Some(self.mark(marks.innermost()?, Mark::Apart))
    }

    /// The cut of the held task whose work the code of `marks` runs
    /// within, the innermost: a background task a task's work started
    /// runs within that work, and only its own cut stops it; none past a
    /// run set apart.
    pub(crate) fn of(&self, marks: &Marks) -> Option<Arc<TaskCut>> {
        let held = self.lock();
        for unit in marks.units().iter().rev() {
            match held.get(unit) {
                Some(Mark::Cut(cut)) => return Some(Arc::clone(cut)),
                Some(Mark::Apart) => return None,
                None => {}
            }
        }
        None
    }

    fn mark(self: &Arc<Self>, unit: Unit, mark: Mark) -> Registered {
        self.lock().insert(unit, mark);
        Registered {
            cuts: Arc::clone(self),
            unit,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Unit, Mark>> {
        self.marks.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A unit's mark registered, until it drops.
#[derive(Debug)]
pub(crate) struct Registered {
    cuts: Arc<TaskCuts>,
    unit: Unit,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.cuts.lock().remove(&self.unit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_finds_the_cut_of_its_innermost_held_work_short_of_a_run_apart() {
        let cuts = Arc::new(TaskCuts::default());
        let (outer, inner) = (Unit::new(), Unit::new());
        let (outer_cut, inner_cut) = (Arc::new(TaskCut::default()), Arc::new(TaskCut::default()));
        let _outer = cuts.register(outer, Arc::clone(&outer_cut));
        let held = cuts.register(inner, Arc::clone(&inner_cut));
        let marks = Marks::default().with(outer).with(Unit::new()).with(inner);
        assert!(Arc::ptr_eq(&cuts.of(&marks).unwrap(), &inner_cut));
        drop(held);
        assert!(Arc::ptr_eq(&cuts.of(&marks).unwrap(), &outer_cut));
        let deferred = Marks::default().with(outer).with(Unit::new());
        let apart = cuts.apart(&deferred);
        let within = deferred.with(Unit::new());
        assert!(cuts.of(&within).is_none());
        drop(apart);
        assert!(cuts.of(&within).is_some());
    }
}
