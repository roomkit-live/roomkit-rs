//! The mutable task objects a plan's observers share with its stored plan.

use std::sync::{Arc, PoisonError, RwLock};

use crate::model::Metadata;

/// An open task object in [`super::PlanUpdatedEvent::tasks`]. Clones share
/// the object, including the one the planner reads on later turns. The
/// event's list is independent: changing that list does not change the plan.
///
/// [`get`](Self::get) returns an owned snapshot, including nested JSON
/// values. Edit the shared object atomically with [`with_mut`](Self::with_mut),
/// or replace it whole with [`replace`](Self::replace). This does not validate
/// its fields: the planning tool validates model arguments before storage;
/// a host may edit the open objects its observers receive.
#[derive(Clone, Debug)]
pub struct PlanTaskHandle(Arc<RwLock<Metadata>>);

impl PlanTaskHandle {
    /// A new task object, taking ownership of its fields without validation.
    pub fn new(task: Metadata) -> Self {
        Self(Arc::new(RwLock::new(task)))
    }

    /// An owned snapshot of this object and all its nested JSON values.
    pub fn get(&self) -> Metadata {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the shared object whole. Use [`with_mut`](Self::with_mut)
    /// when editing fields concurrently with another observer: separate
    /// `get`/`replace` operations do not merge concurrent writes.
    pub fn replace(&self, task: Metadata) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = task;
    }

    /// Edits this shared object atomically, including its nested values.
    /// The callback must be short and synchronous, and must not access the
    /// same handle (or one of its clones) again while it holds the object.
    pub fn with_mut(&self, change: impl FnOnce(&mut Metadata)) {
        change(&mut self.0.write().unwrap_or_else(PoisonError::into_inner));
    }
}

impl PartialEq for PlanTaskHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.get() == other.get()
    }
}
