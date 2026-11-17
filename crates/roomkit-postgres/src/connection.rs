//! A task's current store connection and the cancellation barrier before reuse.

mod release;

use std::sync::Arc;

use deadpool_postgres::Object;
use roomkit::StoreError;
use tokio::sync::{Mutex, OwnedMutexGuard, oneshot};
use tokio::time::Instant;

use crate::PostgresCancelQuery;
use crate::lent::Lent;
use crate::store::Shared;

tokio::task_local! {
    /// Only the innermost binding is visible, as Python's ContextVar is.
    static BOUND: Arc<Bound>;
}

/// The store owns its identity; clones join, another store masks it temporarily.
pub(crate) struct Bound {
    owner: Arc<Shared>,
    state: Arc<Mutex<State>>,
    cancel: PostgresCancelQuery,
    runtime: tokio::runtime::Handle,
}

struct State {
    releasing: oneshot::Receiver<Instant>,
    client: Option<Object>,
    failure: Option<StoreError>,
    failed: bool,
}

impl State {
    fn check(&mut self) -> Result<(), StoreError> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        if self.client.is_none() {
            return Err(StoreError::Value(
                "the scoped connection is closed".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Bound {
    pub(crate) async fn run<T, E: From<StoreError>>(
        owner: Arc<Shared>,
        client: Object,
        cancel: PostgresCancelQuery,
        runtime: tokio::runtime::Handle,
        work: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let (scope, releasing) = release::Owner::for_checkout();
        let bound = Arc::new(Self {
            owner,
            state: Arc::new(Mutex::new(State {
                releasing,
                client: Some(client),
                failure: None,
                failed: false,
            })),
            cancel,
            runtime,
        });
        let result = BOUND.scope(Arc::clone(&bound), work).await;
        drop(scope);
        bound.finish().await?;
        result
    }

    /// Taking the lock joins any cancellation left by the preceding operation.
    pub(crate) async fn client(&self) -> Result<Lent, StoreError> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        state.check()?;
        let client = state.client.take().unwrap_or_else(|| unreachable!());
        Ok(Lent::scoped(
            client,
            Lease {
                state,
                cancel: Arc::clone(&self.cancel),
                runtime: self.runtime.clone(),
            },
        ))
    }

    /// The body can catch a timeout and return without another store call.
    /// Its exit still waits for the cancellation and `ReadyForQuery` barriers.
    pub(crate) async fn finish(&self) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        if state.failed && state.failure.is_none() {
            // The body has already received the cleanup error. A closed lease
            // needs no further release and must not raise that error twice.
            return Ok(());
        }
        state.check()
    }
}

pub(crate) fn current(owner: &Arc<Shared>) -> Option<Arc<Bound>> {
    BOUND
        .try_with(|bound| Arc::ptr_eq(&bound.owner, owner).then(|| Arc::clone(bound)))
        .ok()
        .flatten()
}

/// Exclusive use of the scope's connection, retained while a cancel is drained.
pub(crate) struct Lease {
    state: OwnedMutexGuard<State>,
    cancel: PostgresCancelQuery,
    runtime: tokio::runtime::Handle,
}

impl Lease {
    pub(crate) fn returned(mut self, client: Object) {
        self.state.client = Some(client);
    }

    pub(crate) fn cut(mut self, client: Object) {
        // The caller may drop a Send future on a thread outside Tokio while
        // the runtime that entered the block is still alive.
        let runtime = self.runtime.clone();
        let mut closing = Closing(Some(client));
        runtime.spawn(async move {
            let client = closing.0.as_ref().unwrap_or_else(|| unreachable!());
            let cleanup = async {
                (self.cancel)(client.cancel_token()).await?;
                // EOF on the cancel socket precedes this Sync/ReadyForQuery.
                // Neither barrier by itself allows safe reuse of the connection.
                client
                    .check_connection()
                    .await
                    .map_err(StoreError::unavailable)
            };
            match release::drain(cleanup, &mut self.state.releasing).await {
                Ok(()) => self.state.client = closing.0.take(),
                Err(error) => {
                    self.state.failed = true;
                    self.state.failure = Some(error);
                }
            }
            // The connection is either back in the scope or detached and closed
            // before another operation can acquire this state's lock.
            drop(closing);
        });
    }
}

/// Never return an unclean connection, including when runtime shutdown drops
/// the background cleanup before its barriers complete.
struct Closing(Option<Object>);

impl Drop for Closing {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(Object::take(client));
        }
    }
}
