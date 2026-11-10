//! The runs of delegated tasks the kit holds until each ends (RFC §23.3):
//! a background task's, on [`InMemoryTaskRunner`](super::InMemoryTaskRunner),
//! and an inline delegation's, on the kit. Python's runner keeps them as
//! `_tasks`, `_cancelled_ends` and `_ends`, and an inline run as the task
//! `hold_task` holds.
//!
//! A task ends once, by whoever takes its end first: its work once it ran,
//! or a cancel, which cuts the work ([`TaskCut`]) and ends the task
//! cancelled on a run of its own, tracked by the kit, so that a cancel
//! whose caller stops waiting still ends it. The cut work stores what its
//! turn said before it stops, as Python's cancelled reader does. A cancel
//! that finds the end taken waits for the task to end as it stands, unless
//! it runs within one of the task's own runs (an `on_complete` that closes
//! the kit), which would wait for itself.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{Kind, Marks, TaskCut, Tasks, Unit};
use crate::channel::BoxFuture;
use crate::stream::{CALLER_CANCELLED, KIT_CLOSED};

#[cfg(test)]
mod tests;

/// The task runs the kit holds, by task id.
#[derive(Clone, Default)]
pub(crate) struct HeldTasks {
    held: Arc<Mutex<HashMap<String, Held>>>,
}

/// A task held: its work, its end once cancelled, which whoever takes it
/// runs, and what tells its end.
struct Held {
    tasks: Arc<Tasks>,
    /// The work, until a cancel takes it to wait for its end.
    work: Option<JoinHandle<()>>,
    /// What cuts the work.
    cut: Arc<TaskCut>,
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
        let cut = Arc::new(TaskCut::default());
        let (cuts, cutting) = (Arc::clone(&tasks.task_cuts), Arc::clone(&cut));
        let run = async move {
            let unit = noted.held.note_run(&noted.task_id);
            // The reads of the work's turns find its cut by its unit.
            let _registered = unit.map(|unit| cuts.register(unit, Arc::clone(&cutting)));
            cutting.run(work).await;
        };
        // Held while the work is spawned: a cancel finds its work there.
        let mut held = self.lock();
        held.insert(
            task_id.to_owned(),
            Held {
                tasks: Arc::clone(tasks),
                work: None,
                cut,
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

    /// Cancels `task_id`: its work cut, it ends cancelled on a run of its
    /// own, which this call waits for; `true` then. A task ending as it
    /// stands is waited for, `false`; so is one the runner does not hold,
    /// at once.
    pub(crate) async fn cancel(&self, task_id: &str) -> bool {
        match self.find(task_id, CALLER_CANCELLED) {
            None => false,
            Some(Found::Ending { ended, units }) => {
                wait_unless_within(ended, &units).await;
                false
            }
            Some(Found::End {
                tasks,
                work,
                end,
                ended,
            }) => {
                if let Err(here) = spawn_apart(&tasks, self.ending(task_id, work, end)) {
                    here.await;
                }
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
        }) = self.find(task_id, CALLER_CANCELLED)
        else {
            return;
        };
        if spawn_apart(&tasks, self.ending(task_id, work, end)).is_err() {
            tracing::debug!(target: "roomkit::tasks", task = %task_id, "the kit is shut down: a cancelled task does not end");
        }
    }

    /// Cancels every task held, the tasks their ends delegate among them:
    /// each one's end started at once, then each waited for, so that no
    /// end outlasting the close's grace keeps another from starting
    /// (Python's close cancels the runs it holds together).
    pub(crate) async fn close(&self) {
        let mut asked = HashSet::new();
        loop {
            let next: Vec<String> = self
                .lock()
                .keys()
                .filter(|id| !asked.contains(*id))
                .cloned()
                .collect();
            if next.is_empty() {
                return;
            }
            let (mut here, mut waits) = (Vec::new(), Vec::new());
            for task_id in next {
                match self.find(&task_id, KIT_CLOSED) {
                    None => {}
                    Some(Found::End {
                        tasks,
                        work,
                        end,
                        ended,
                    }) => {
                        if let Err(ending) = spawn_apart(&tasks, self.ending(&task_id, work, end)) {
                            here.push(ending);
                        }
                        waits.push((ended, Vec::new()));
                    }
                    Some(Found::Ending { ended, units }) => waits.push((ended, units)),
                }
                asked.insert(task_id);
            }
            for ending in here {
                ending.await;
            }
            for (ended, units) in waits {
                wait_unless_within(ended, &units).await;
            }
        }
    }

    /// How many tasks are held.
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// `task_id`'s end a cancel took: `work`, cut, waited for, then `end`,
    /// then the task's waiters told.
    fn ending(
        &self,
        task_id: &str,
        work: Option<JoinHandle<()>>,
        end: BoxFuture<'static, ()>,
    ) -> BoxFuture<'static, ()> {
        let ending = Ending {
            held: self.clone(),
            task_id: task_id.to_owned(),
        };
        Box::pin(async move {
            ending.held.note_run(&ending.task_id);
            over(work).await;
            end.await;
            drop(ending);
        })
    }

    /// What a cancel finds of `task_id`, its end taken when it is there,
    /// its work cut then with `reason`, before the work's first step: a
    /// task cut before it ran never begins.
    fn find(&self, task_id: &str, reason: &'static str) -> Option<Found> {
        let mut held = self.lock();
        let entry = held.get_mut(task_id)?;
        let ended = entry.ended.subscribe();
        Some(match entry.end_cancelled.take() {
            Some(end) => {
                entry.cut.cut(reason);
                Found::End {
                    tasks: Arc::clone(&entry.tasks),
                    work: entry.work.take(),
                    end,
                    ended,
                }
            }
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

    /// Notes the unit the current code runs in as one of `task_id`'s runs:
    /// that unit.
    fn note_run(&self, task_id: &str) -> Option<Unit> {
        let unit = Marks::current().innermost()?;
        if let Some(entry) = self.lock().get_mut(task_id) {
            entry.units.push(unit);
        }
        Some(unit)
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

/// Waits until `work`, cut, is over: dropped where it waits, or once the
/// reads of its turns stored what they said.
async fn over(work: Option<JoinHandle<()>>) {
    if let Some(work) = work {
        // Its outcome is its end, or its abort by the close's cut: either
        // way it is over.
        drop(work.await);
    }
}

/// Runs `work` as a run of its own the kit tracks, apart from its caller,
/// which may stop waiting for it: its handle; `work` back when the kit
/// spawns no more work, for the caller to run in place.
pub(crate) fn spawn_apart(
    tasks: &Arc<Tasks>,
    work: BoxFuture<'static, ()>,
) -> Result<JoinHandle<()>, BoxFuture<'static, ()>> {
    let slot = Arc::new(Mutex::new(Some(work)));
    let spawned = Arc::clone(&slot);
    let run = async move {
        if let Some(work) = take(&spawned) {
            work.await;
        }
    };
    match tasks.spawn_alone(Kind::Turn, run) {
        Some(handle) => Ok(handle),
        // Refused, the run was dropped unpolled: the work is still there.
        None => Err(take(&slot).unwrap_or_else(|| Box::pin(async {}))),
    }
}

/// Waits for the task `ended` tells of, unless the current code runs
/// within one of its `units`, which would wait for itself.
async fn wait_unless_within(ended: watch::Receiver<bool>, units: &[Unit]) {
    let current = Marks::current();
    if !units.iter().any(|unit| current.contains(*unit)) {
        until_ended(ended).await;
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
