//! The marks of the kit's own work: the units of it the current code runs
//! within (RFC §12.10.4).
//!
//! A unit is one piece of the kit's work, which its close waits for and
//! cuts as one: an inbound run, a hook's body run in the background, a
//! turn with its tasks (its owner's pump, its executor, their beats and
//! the verbs they serve), a write of the store. A task the kit tracks runs
//! in one unit, and within the units of the code that started it, so the
//! marks of the current code name every unit it runs within, the innermost
//! last, as the Python implementation's context names the held runs a task
//! inherited when it was created. A `close()` called from inside them
//! spares them: the kit's shutdown neither waits for them nor cuts them,
//! since they cannot move on before the call returns.
//!
//! A task-local: the kit's tasks are started within their marks, the calls
//! a run makes within the run's, and [`keep_context`](crate::keep_context)
//! carries them into a task the kit's work spawns.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

tokio::task_local! {
    static MARKS: Marks;
}

/// One unit of the kit's own work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Unit(u64);

impl Unit {
    /// A unit no other piece of work is in, in this process.
    pub(crate) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// The units of the kit's work the current code runs within, the
/// innermost last.
#[derive(Clone, Debug, Default)]
pub(crate) struct Marks(Arc<[Unit]>);

impl Marks {
    /// The marks of the current code: none outside the kit's work.
    pub(crate) fn current() -> Self {
        MARKS.try_with(Clone::clone).unwrap_or_default()
    }

    /// These marks, and `unit` within them.
    pub(crate) fn with(&self, unit: Unit) -> Self {
        Self(self.0.iter().copied().chain([unit]).collect())
    }

    /// The units, the innermost last.
    pub(crate) fn units(&self) -> &[Unit] {
        &self.0
    }

    /// The unit the current code runs in itself.
    pub(crate) fn innermost(&self) -> Option<Unit> {
        self.0.last().copied()
    }

    /// Whether the code runs within `unit`.
    pub(crate) fn contains(&self, unit: Unit) -> bool {
        self.0.contains(&unit)
    }

    /// `future`, run within these marks.
    pub(crate) fn scoped<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        MARKS.scope(self, future)
    }

    /// `f`, run within these marks.
    pub(crate) fn sync_scope<R>(self, f: impl FnOnce() -> R) -> R {
        MARKS.sync_scope(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn code_outside_the_kit_s_work_has_no_marks() {
        assert_eq!(Marks::current().units(), []);
    }

    #[tokio::test]
    async fn a_unit_opened_within_another_runs_within_both() {
        let (outer, inner) = (Unit::new(), Unit::new());
        let marks = Marks::default()
            .with(outer)
            .scoped(async {
                Marks::current()
                    .with(inner)
                    .scoped(async { Marks::current() })
                    .await
            })
            .await;
        assert_eq!(
            (marks.units(), marks.innermost()),
            (&[outer, inner][..], Some(inner))
        );
    }
}
