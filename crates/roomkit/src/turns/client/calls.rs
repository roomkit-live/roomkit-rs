//! A close interrupts the owner's outstanding between-turn calls, except its caller.
use crate::RoomKitError;
use crate::tasks::{Marks, Unit};
use crate::turn::TurnKit;
use crate::turns::TurnFuture;
use crate::turns::lock;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Default)]
pub(super) struct Calls(Arc<Mutex<HashMap<Unit, watch::Sender<bool>>>>);
struct Active {
    calls: Arc<Mutex<HashMap<Unit, watch::Sender<bool>>>>,
    unit: Unit,
}
impl Drop for Active {
    fn drop(&mut self) {
        lock(&self.calls).remove(&self.unit);
    }
}
impl Calls {
    pub(super) async fn run<T: Send + 'static>(
        &self,
        runtime: &dyn TurnKit,
        closed: impl FnOnce() -> bool,
        work: TurnFuture<'static, T>,
    ) -> Result<T, RoomKitError> {
        let unit = Unit::new();
        let mut stop = {
            let mut calls = lock(&self.0);
            if closed() {
                return Err(RoomKitError::Runtime("The room owner has closed".into()));
            }
            let (send, receive) = watch::channel(false);
            calls.insert(unit, send);
            receive
        };
        let active = Active {
            calls: Arc::clone(&self.0),
            unit,
        };
        let (send, receive) = tokio::sync::oneshot::channel();
        let task = async move {
            let _active = active;
            let result = tokio::select! {biased; _=stop.wait_for(|stopped|*stopped)=>Err(RoomKitError::Cancelled),output=work=>Ok(output)};
            drop(send.send(result));
        };
        let Some((task, _)) = Marks::current()
            .with(unit)
            .sync_scope(|| runtime.spawn_owner_call(Box::pin(task)))
        else {
            return Err(RoomKitError::KitClosed);
        };
        let _cancel = AbortOnDrop(task.abort_handle());
        receive.await.map_err(|_| RoomKitError::Cancelled)?
    }
    pub(super) async fn stop(&self) {
        let marks = Marks::current();
        let pending: Vec<_> = lock(&self.0)
            .iter()
            .filter(|(unit, _)| !marks.units().contains(unit))
            .map(|(_, stop)| {
                stop.send_replace(true);
                stop.clone()
            })
            .collect();
        for signal in pending {
            signal.closed().await;
        }
    }
}

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
