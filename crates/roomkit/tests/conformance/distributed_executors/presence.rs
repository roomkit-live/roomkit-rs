//! A shared realtime bus and failures at the owner-side presence contracts.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use roomkit::RoomKitError;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::turns::DistributedTurnBackend;
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorsListener, RoomOwner, ServedExecutor,
    TurnBackend, TurnBackendHandle, TurnFuture, TurnLink, TurnRequest, TurnTimeouts,
};
use serde_json::Value;

use crate::run::{OwnerConnection, lock};

#[derive(Default)]
struct Fault {
    taken: bool,
    gate: Option<String>,
}

struct Faults {
    armed: Mutex<BTreeMap<String, Fault>>,
    slot: crate::identity::RunSlot,
}

impl Faults {
    fn check(&self, operation: &str) -> Result<(), RoomKitError> {
        if let Some(fault) = lock(&self.armed).get_mut(operation) {
            fault.taken = true;
            if let Some(gate) = fault.gate.take()
                && let Some(run) = self.slot.run()
            {
                run.gates.reach(&gate);
            }
            return Err(RoomKitError::Runtime(format!(
                "scripted failure of {operation}"
            )));
        }
        Ok(())
    }
}

pub(super) struct Presence {
    bus: Option<Arc<InMemoryRealtime>>,
    faults: Option<Arc<Faults>>,
    catalogue: bool,
    /// The kit is given a transport written on the public contract alone
    /// (README, `kit.turn_backend_contract_only`).
    contract_only: bool,
}

impl Presence {
    pub(super) fn new(kit: &Value, slot: crate::identity::RunSlot) -> Self {
        Self {
            bus: (kit["shared_realtime"] == true).then(Arc::default),
            faults: (kit["turn_backend_faults"] == true).then(|| {
                Arc::new(Faults {
                    slot,
                    armed: Mutex::default(),
                })
            }),
            catalogue: kit["executor_catalogue"] != false,
            contract_only: kit["turn_backend_contract_only"] == true,
        }
    }

    pub(super) fn connection(&self, node: DistributedTurnBackend) -> OwnerConnection {
        let turns = if self.catalogue && self.faults.is_none() && !self.contract_only {
            TurnBackendHandle::new(Arc::new(node))
        } else {
            TurnBackendHandle::new(Arc::new(PresenceBackend {
                inner: node,
                faults: self.faults.clone(),
                catalogue: self.catalogue,
            }))
        };
        let realtime: Option<Arc<dyn RealtimeBackend>> =
            if self.faults.is_some() || self.bus.is_some() {
                Some(Arc::new(OwnerRealtime {
                    bus: self.bus.clone().unwrap_or_default(),
                    subscriptions: Mutex::default(),
                    faults: self.faults.clone(),
                }))
            } else {
                None
            };
        OwnerConnection { turns, realtime }
    }

    pub(super) fn fault(
        &self,
        operation: &str,
        enabled: bool,
        gate: Option<String>,
    ) -> Result<(), RoomKitError> {
        let faults = self
            .faults
            .as_ref()
            .ok_or_else(|| RoomKitError::Value("turn_backend_faults not enabled".into()))?;
        let mut armed = lock(&faults.armed);
        if enabled {
            armed.insert(operation.to_owned(), Fault { taken: false, gate });
        } else if armed.get(operation).is_some_and(|fault| fault.taken) {
            armed.remove(operation);
        } else {
            return Err(RoomKitError::Runtime(format!(
                "fault {operation} was never taken"
            )));
        }
        Ok(())
    }

    pub(super) fn untaken(&self) -> Vec<String> {
        self.faults
            .as_ref()
            .map(|faults| {
                lock(&faults.armed)
                    .iter()
                    .filter(|(_, fault)| !fault.taken)
                    .map(|(operation, _)| format!("untaken {operation} fault"))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The runner's own transport: each operation of the public contract handed
/// to the distributed backend, a claim failing while its fault is armed.
struct PresenceBackend {
    inner: DistributedTurnBackend,
    faults: Option<Arc<Faults>>,
    catalogue: bool,
}

impl TurnBackend for PresenceBackend {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        TurnBackend::submit(&self.inner, request)
    }
    fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        TurnBackend::submit_to(&self.inner, request, executor)
    }
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::serve(&self.inner, executor)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        TurnBackend::control(&self.inner, message)
    }
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::attend(&self.inner, owner)
    }
    fn claim_presence<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        Some(Box::pin(async move {
            if online.is_some()
                && let Some(faults) = &self.faults
            {
                faults.check("claim_presence")?;
            }
            self.inner.claim_presence(channel, room, online).await
        }))
    }
    fn watch_executors(&self, listener: ExecutorsListener) {
        TurnBackend::watch_executors(&self.inner, listener);
    }
    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        if self.catalogue {
            TurnBackend::executors(&self.inner)
        } else {
            None
        }
    }
    fn timeouts(&self) -> Option<TurnTimeouts> {
        Some(self.inner.timeouts())
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::close(&self.inner)
    }
}

struct OwnerRealtime {
    subscriptions: Mutex<BTreeSet<String>>,
    bus: Arc<InMemoryRealtime>,
    faults: Option<Arc<Faults>>,
}

impl RealtimeBackend for OwnerRealtime {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), roomkit::ChannelError>> {
        Box::pin(async move {
            if matches!(
                event.r#type,
                EphemeralEventType::PresenceOnline | EphemeralEventType::PresenceOffline
            ) && let Some(faults) = &self.faults
            {
                faults.check("publish_presence")?;
            }
            self.bus.publish(channel, event).await
        })
    }
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        Box::pin(async move {
            let id = self.bus.subscribe(channel, callback).await;
            lock(&self.subscriptions).insert(id.clone());
            id
        })
    }
    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        lock(&self.subscriptions).remove(subscription_id);
        self.bus.unsubscribe(subscription_id)
    }
    fn close(&self) -> RealtimeFuture<'_, ()> {
        Box::pin(async move {
            let subscriptions = std::mem::take(&mut *lock(&self.subscriptions));
            for id in subscriptions {
                self.bus.unsubscribe(&id).await;
            }
        })
    }
}
