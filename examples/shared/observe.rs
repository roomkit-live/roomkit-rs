//! What an example observes while it runs: what its hooks and channels
//! note, and the framework events the kit publishes. What the kit runs in
//! the background, an observer hook or a call that went on detached, is
//! waited for with `RoomKit::idle` before it is read.

use roomkit::{ConversationStore, FrameworkEvent, RoomKit, RoomKitError};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

/// How long an example waits for a hook of a call still running to note
/// something: far longer than it takes.
const PATIENCE: Duration = Duration::from_secs(1);

/// What the example's hooks and channels note while it runs, in the order
/// they note it. Cloning it shares it: the example keeps a clone and gives
/// another to the callback that writes to it.
#[derive(Debug)]
pub struct Log<T> {
    entries: Arc<watch::Sender<Vec<T>>>,
}

impl<T: Clone> Log<T> {
    /// An empty log.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(watch::Sender::new(Vec::new())),
        }
    }

    /// Notes `entry`.
    pub fn push(&self, entry: T) {
        self.entries.send_modify(|entries| entries.push(entry));
    }

    /// What was noted so far.
    pub fn entries(&self) -> Vec<T> {
        self.entries.borrow().clone()
    }

    /// What was noted, once `count` entries are, or after a second at most:
    /// for an example that goes on once a hook of a call still running
    /// noted it started. Past the wait, the entries are what they are, and
    /// the example's checks say what is missing.
    pub async fn wait_for(&self, count: usize) -> Vec<T> {
        let mut changes = self.entries.subscribe();
        let enough = async {
            changes
                .wait_for(|entries| entries.len() >= count)
                .await
                .map(|entries| entries.clone())
        };
        match tokio::time::timeout(PATIENCE, enough).await {
            Ok(Ok(entries)) => entries,
            Ok(Err(_)) | Err(_) => self.entries(),
        }
    }
}

impl<T> Clone for Log<T> {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
        }
    }
}

impl<T: Clone> Default for Log<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The framework events of `types` the kit emits from now on (RFC §8.2),
/// noted in order by a handler [`RoomKit::on`] registers for each type. The
/// call that emits an event waits for its handlers: those of a call the
/// example makes are there once the call returned, and those of the work it
/// runs in the background once that work is over.
///
/// # Errors
///
/// Returns [`RoomKitError::KitClosed`] once the kit's close started.
pub fn framework_events<S: ConversationStore>(
    kit: &RoomKit<S>,
    types: &[&str],
) -> Result<Log<FrameworkEvent>, RoomKitError> {
    let log = Log::new();
    for event_type in types {
        let noted = log.clone();
        kit.on(*event_type, move |event| {
            noted.push(event);
            std::future::ready(())
        })?;
    }
    Ok(log)
}
