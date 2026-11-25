//! The listeners an executor runs once it closed (RFC §10.6.7).
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::turns::{ExecutorsListener, lock};

/// Closing an executor runs each listener it was given once: a backend that
/// serves it learns it no longer covers its channels. Neither direction
/// keeps an owner or a backend alive.
#[derive(Default)]
pub(crate) struct ClosedListeners(Mutex<Vec<ExecutorsListener>>);

impl ClosedListeners {
    /// Keep `listener`; one given again is kept once, as Python's is.
    pub(crate) fn watch(&self, listener: ExecutorsListener) {
        let mut listeners = lock(&self.0);
        let given = Arc::as_ptr(&listener);
        if !listeners
            .iter()
            .any(|kept| std::ptr::addr_eq(Arc::as_ptr(kept), given))
        {
            listeners.push(listener);
        }
    }

    pub(crate) async fn closed(&self) {
        let listeners = std::mem::take(&mut *lock(&self.0));
        for work in listeners.iter().filter_map(|listener| listener()) {
            work.await;
        }
    }
}

impl fmt::Debug for ClosedListeners {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClosedListeners").finish_non_exhaustive()
    }
}
