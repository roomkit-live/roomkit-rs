//! Inline completions that belong to one owner turn.

use std::sync::Arc;

use super::{HeldTasks, Marks};

impl HeldTasks {
    /// End only inline delegations made by this owner's turn. Their existing
    /// completion runs once; other turns and background tasks are untouched.
    pub(crate) async fn cancel_in_turn(&self, turn_id: &str) {
        let ids: Vec<String> = self
            .lock()
            .iter()
            .filter(|(_, held)| held.caller_turn.as_deref() == Some(turn_id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if !self.spared(id) {
                self.cancel_apart(id);
            }
        }
        for id in ids {
            self.join_unspared(&id).await;
        }
    }

    fn spared(&self, task_id: &str) -> bool {
        let Some((tasks, units)) = self
            .lock()
            .get(task_id)
            .map(|held| (Arc::clone(&held.tasks), held.units.clone()))
        else {
            return true;
        };
        // Read the closing state after releasing the held-map lock; no
        // registry lock is retained while another subsystem is consulted.
        let current = Marks::current();
        units
            .iter()
            .any(|unit| current.contains(*unit) || tasks.spares(&Marks::default().with(*unit)))
    }

    async fn join_unspared(&self, task_id: &str) {
        let Some((tasks, mut ended)) = self
            .lock()
            .get(task_id)
            .map(|held| (Arc::clone(&held.tasks), held.ended.subscribe()))
        else {
            return;
        };
        loop {
            let changed = tasks.changes();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if *ended.borrow() || self.spared(task_id) {
                return;
            }
            tokio::select! {
                result = ended.changed() => { if result.is_err() { return; } }
                () = changed => {}
            }
        }
    }
}
