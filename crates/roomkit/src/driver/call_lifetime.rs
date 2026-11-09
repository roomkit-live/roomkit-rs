//! A direct call can be abandoned before its timer write; after it starts,
//! the timer's records and notifications must finish (RFC §5.1, §13.6).

use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::Notify;

const LIVE: u8 = 0;
const FINISHING: u8 = 1;
const ABANDONED: u8 = 2;
const GONE: u8 = 3;

#[derive(Debug, Default)]
pub(super) struct CallLifetime {
    state: AtomicU8,
    gone: Notify,
}

impl CallLifetime {
    /// The caller's drop and the start of the write compete for one decision:
    /// a dropped caller cannot race an abort with a write starting beside it.
    pub(super) fn finish_timers(&self) -> bool {
        matches!(
            self.state
                .compare_exchange(LIVE, FINISHING, Ordering::SeqCst, Ordering::SeqCst),
            Ok(_) | Err(FINISHING | GONE)
        )
    }

    /// Returns whether the caller may abort its driver. A protected timer
    /// finishes, but the operation must give up the caller's later work.
    pub(super) fn abandon(&self) -> bool {
        match self
            .state
            .compare_exchange(LIVE, ABANDONED, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => true,
            Err(FINISHING) => {
                self.state.store(GONE, Ordering::SeqCst);
                self.gone.notify_one();
                false
            }
            Err(_) => false,
        }
    }

    pub(super) async fn gone(&self) {
        self.gone.notified().await;
    }
}
