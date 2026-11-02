//! The runs of delegated tasks the kit holds until each ends (RFC §23.3):
//! a background task's, on [`InMemoryTaskRunner`](super::InMemoryTaskRunner),
//! and an inline delegation's, on the kit. Python's runner keeps them as
//! `_tasks`, `_cancelled_ends` and `_ends`, and an inline run as the task
//! `hold_task` holds.
//!
//! A task ends once, by whoever takes its end first: its work once it ran,
//! or a cancel, which stops the work and ends the task cancelled on a run
//! of its own, tracked by the kit, so that a cancel whose caller stops
//! waiting still ends it. A cancel that finds the end taken waits for the
//! task to end as it stands, unless it runs within one of the task's own
//! runs (an `on_complete` that closes the kit), which would wait for
//! itself.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{Kind, Marks, Tasks, Unit};
use crate::channel::BoxFuture;

/// The task runs the kit holds, by task id.
#[derive(Clone, Default)]
pub(crate) struct HeldTasks {
    held: Arc<Mutex<HashMap<String, Held>>>,
}

/// A task held: its work, its end once cancelled, which whoever takes it
/// runs, and what tells its end.
struct Held {
    tasks: Arc<Tasks>,
    /// The work, until a cancel takes it to stop it.
    work: Option<JoinHandle<()>>,
    /// How the task ends cancelled, until its work or a cancel takes it.
    end_cancelled: Option<BoxFuture<'static, ()>>,
    /// Told once the task ended, however it ended; dropped with the entry.
    ended: watch::Sender<bool>,
    /// The units the task's runs work in: its work's, its end's.
    units: Vec<Unit>,
}

/// What a task's work holds of its entry.
#[derive(Clone)]
pub(crate) struct Claim {
    held: HeldTasks,
    task_id: String,
}

impl Claim {
    /// The task's end, when the work takes it, no cancel having taken it
    /// first: the work then ends the task as it ran, the end told as the
    /// returned [`Ending`] drops. Once the kit's close began, the work
    /// leaves the end to the close, which cancels every task held, as
    /// Python's close cancels the run before its turn could end: the close
    /// cut the turn the work ran.
    pub(crate) fn take(&self) -> Option<Ending> {
        // Its cancelled end is not run: the work ends the task as it ran.
        drop(self.held.take_end(&self.task_id)?);
        Some(Ending {
            held: self.held.clone(),
            task_id: self.task_id.clone(),
        })
    }
}

/// A task's end, taken by the run that ends it: dropped, however that run
/// stops, the close's cut among the ways, it tells the task's end, so that
/// a cancel waiting for it goes on, as Python's waits until the task's runs
/// are over.
pub(crate) struct Ending {
    held: HeldTasks,
    task_id: String,
}

impl Drop for Ending {
    fn drop(&mut self) {
        self.held.ended(&self.task_id);
    }
}

/// What a cancel found of a task.
enum Found {
    /// Its end, to run once its work is stopped.
    End {
        tasks: Arc<Tasks>,
        work: Option<JoinHandle<()>>,
        end: BoxFuture<'static, ()>,
        ended: watch::Receiver<bool>,
    },
    /// Its end taken already: the task is ending.
    Ending {
        ended: watch::Receiver<bool>,
        units: Vec<Unit>,
    },
}

impl HeldTasks {
    /// Holds `task_id`, ending cancelled with `end_cancelled` unless its
    /// work, `work(claim)`, run as a turn's task the kit tracks, takes its
    /// end first. What tells its end; `None` once the kit spawns no more
    /// work, nothing held.
    pub(crate) fn hold<F>(
        &self,
        tasks: &Arc<Tasks>,
        task_id: &str,
        end_cancelled: BoxFuture<'static, ()>,
        work: impl FnOnce(Claim) -> F,
    ) -> Option<watch::Receiver<bool>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (ended, told) = watch::channel(false);
        let claim = Claim {
            held: self.clone(),
            task_id: task_id.to_owned(),
        };
        let noted = claim.clone();
        let work = work(claim);
        let run = async move {
            noted.held.note_run(&noted.task_id);
            work.await;
        };
        // Held while the work is spawned: a cancel finds its work there.
        let mut held = self.lock();
        held.insert(
            task_id.to_owned(),
            Held {
                tasks: Arc::clone(tasks),
                work: None,
                end_cancelled: Some(end_cancelled),
                ended,
                units: Vec::new(),
            },
        );
        let Some(spawned) = tasks.spawn(Kind::Turn, run) else {
            held.remove(task_id);
            return None;
        };
        if let Some(entry) = held.get_mut(task_id) {
            entry.work = Some(spawned);
        }
        Some(told)
    }

    /// Cancels `task_id`: its work stopped, it ends cancelled on a run of
    /// its own, which this call waits for; `true` then. A task ending as
    /// it stands is waited for, `false`; so is one the runner does not
    /// hold, at once.
    pub(crate) async fn cancel(&self, task_id: &str) -> bool {
        match self.find(task_id) {
            None => false,
            Some(Found::Ending { ended, units }) => {
                let current = Marks::current();
                if !units.iter().any(|unit| current.contains(*unit)) {
                    until_ended(ended).await;
                }
                false
            }
            Some(Found::End {
                tasks,
                work,
                end,
                ended,
            }) => {
                self.end_apart(task_id, &tasks, work, end).await;
                until_ended(ended).await;
                true
            }
        }
    }

    /// Cancels `task_id` without waiting for its end: what the caller of
    /// an inline delegation that stops waiting leaves (Python's cancelled
    /// `hold_task`).
    pub(crate) fn cancel_apart(&self, task_id: &str) {
        let Some(Found::End {
            tasks, work, end, ..
        }) = self.find(task_id)
        else {
            return;
        };
        let ending = self.ending(task_id, work, end);
        if tasks.spawn_alone(Kind::Turn, ending).is_none() {
            tracing::debug!(target: "roomkit::tasks", task = %task_id, "the kit is shut down: a cancelled task does not end");
        }
    }

    /// Cancels every task held, one at a time, each waited for, the tasks
    /// their ends delegate among them.
    pub(crate) async fn close(&self) {
        let mut asked = HashSet::new();
        loop {
            let next = self.lock().keys().find(|id| !asked.contains(*id)).cloned();
            let Some(task_id) = next else {
                return;
            };
            self.cancel(&task_id).await;
            asked.insert(task_id);
        }
    }

    /// How many tasks are held.
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Runs `end`, `task_id`'s end a cancel took, once `work` is stopped,
    /// as a run of its own the kit tracks; here, when the kit spawns no
    /// more work.
    async fn end_apart(
        &self,
        task_id: &str,
        tasks: &Arc<Tasks>,
        work: Option<JoinHandle<()>>,
        end: BoxFuture<'static, ()>,
    ) {
        let ending: BoxFuture<'static, ()> = Box::pin(self.ending(task_id, work, end));
        let slot = Arc::new(Mutex::new(Some(ending)));
        let spawned = Arc::clone(&slot);
        let run = async move {
            if let Some(ending) = take(&spawned) {
                ending.await;
            }
        };
        if tasks.spawn_alone(Kind::Turn, run).is_none()
            && let Some(ending) = take(&slot)
        {
            ending.await;
        }
    }

    /// `task_id`'s end a cancel took: `work` stopped, then `end`, then
    /// the task's waiters told.
    fn ending(
        &self,
        task_id: &str,
        work: Option<JoinHandle<()>>,
        end: BoxFuture<'static, ()>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let ending = Ending {
            held: self.clone(),
            task_id: task_id.to_owned(),
        };
        async move {
            ending.held.note_run(&ending.task_id);
            stop(work).await;
            end.await;
            drop(ending);
        }
    }

    /// What a cancel finds of `task_id`, its end taken when it is there.
    fn find(&self, task_id: &str) -> Option<Found> {
        let mut held = self.lock();
        let entry = held.get_mut(task_id)?;
        let ended = entry.ended.subscribe();
        Some(match entry.end_cancelled.take() {
            Some(end) => Found::End {
                tasks: Arc::clone(&entry.tasks),
                work: entry.work.take(),
                end,
                ended,
            },
            None => Found::Ending {
                ended,
                units: entry.units.clone(),
            },
        })
    }

    fn take_end(&self, task_id: &str) -> Option<BoxFuture<'static, ()>> {
        let mut held = self.lock();
        let entry = held.get_mut(task_id)?;
        if entry.tasks.is_closing() {
            return None;
        }
        entry.end_cancelled.take()
    }

    /// Notes the unit the current code runs in as one of `task_id`'s runs.
    fn note_run(&self, task_id: &str) {
        let Some(unit) = Marks::current().innermost() else {
            return;
        };
        if let Some(entry) = self.lock().get_mut(task_id) {
            entry.units.push(unit);
        }
    }

    /// `task_id` ended: its entry goes, and its waiters with it.
    fn ended(&self, task_id: &str) {
        if let Some(entry) = self.lock().remove(task_id) {
            entry.ended.send_replace(true);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Held>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Stops `work` and waits until it is over.
async fn stop(work: Option<JoinHandle<()>>) {
    if let Some(work) = work {
        work.abort();
        // Its outcome is its abort, or its own end: either way it is over.
        drop(work.await);
    }
}

/// Waits until the task `ended` tells of ended, or its entry went.
pub(crate) async fn until_ended(mut ended: watch::Receiver<bool>) {
    // A closed channel is an entry gone: the task ended.
    drop(ended.wait_for(|ended| *ended).await);
}

fn take(slot: &Mutex<Option<BoxFuture<'static, ()>>>) -> Option<BoxFuture<'static, ()>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner).take()
}
