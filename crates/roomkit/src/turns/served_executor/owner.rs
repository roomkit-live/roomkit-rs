//! A backend can keep the identity of the kit's executor without keeping the kit alive.
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Weak;

use roomkit_core::model::ChannelId;

use super::{
    ChannelControl, ChannelResult, ExecutorLink, ServedExecutor, TakesOptions, TurnFuture,
    TurnTimeouts,
};
use crate::turns::RoomOwner;
use crate::{ConversationStore, RoomKit, RoomKitError, TurnExecutor, WeakRoomKit};

struct OwnerExecutor<S> {
    kit: Arc<WeakRoomKit<S>>,
    id: String,
    timeouts: TurnTimeouts,
    lifecycle: Weak<crate::turns::executor::Lifecycle>,
    #[cfg(feature = "redis")]
    identity: Weak<Self>,
}

pub(crate) fn owner_executor<S: ConversationStore>(kit: &RoomKit<S>) -> Arc<dyn ServedExecutor> {
    let executor = kit.turn_executor();
    Arc::new_cyclic(|identity| {
        #[cfg(not(feature = "redis"))]
        let _ = identity;
        OwnerExecutor {
            kit: Arc::new(kit.downgrade()),
            id: executor.executor_id().to_owned(),
            timeouts: executor.timeouts(),
            lifecycle: executor.lifecycle(),
            #[cfg(feature = "redis")]
            identity: Weak::clone(identity),
        }
    })
}

impl<S: ConversationStore> OwnerExecutor<S> {
    fn worker(&self) -> Option<TurnExecutor<S>> {
        self.kit.upgrade().map(|kit| kit.turn_executor())
    }
}

impl<S: ConversationStore> super::private::Sealed for OwnerExecutor<S> {
    fn apply_room_hooks(&self, room: &roomkit_core::model::RoomId, names: Vec<String>) {
        if let Some(worker) = self.worker() {
            worker.hooks().use_room_hooks(room, names);
        }
    }
    fn transport(&self, _: super::native::Key) -> &dyn super::native::Native {
        self
    }
}

impl<S: ConversationStore> ServedExecutor for OwnerExecutor<S> {
    fn executor_id(&self) -> &str {
        &self.id
    }

    fn closed(&self) -> bool {
        self.worker().is_none_or(|worker| worker.closed())
    }

    fn close(&self) -> TurnFuture<'_, ()> {
        Box::pin(async move {
            if let Some(worker) = self.worker() {
                worker.close().await;
            }
        })
    }

    fn declares(&self, channel_id: &ChannelId) -> bool {
        self.worker()
            .is_some_and(|worker| worker.declares(channel_id))
    }

    fn hosts_channel(&self, channel_id: &ChannelId) -> bool {
        self.worker()
            .is_some_and(|worker| worker.hosts_channel(channel_id))
    }

    fn hosted(&self) -> Option<BTreeSet<String>> {
        self.worker().and_then(|worker| worker.hosted())
    }

    fn organizations(&self) -> Option<BTreeSet<String>> {
        self.worker().and_then(|worker| worker.organizations())
    }

    fn serves(&self, organization: Option<&str>) -> bool {
        self.worker()
            .is_some_and(|worker| worker.serves(organization))
    }

    fn room_hooks(&self) -> BTreeSet<String> {
        self.worker()
            .map_or_else(BTreeSet::new, |worker| worker.room_hooks())
    }

    fn takes(&self, channel_id: &ChannelId, options: TakesOptions) -> bool {
        self.worker()
            .is_some_and(|worker| worker.takes(channel_id, options))
    }

    fn timeouts(&self) -> TurnTimeouts {
        self.timeouts
    }

    fn take(&self, link: Arc<dyn ExecutorLink>) -> Result<(), RoomKitError> {
        self.worker().ok_or(RoomKitError::KitClosed)?.take(link)
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async move {
            self.worker()
                .ok_or(RoomKitError::KitClosed)?
                .control(message)
                .await
        })
    }

    fn reach_owner(&self, owner: Arc<dyn RoomOwner>) {
        if let Some(worker) = self.worker() {
            worker.reach_owner(owner);
        }
    }
}

impl<S: ConversationStore> crate::turns::LocalHosts for WeakRoomKit<S> {
    fn hosts_name(&self, id: &str) -> bool {
        self.upgrade().is_some_and(|kit| kit.inner.hosts_name(id))
    }
}

impl<S: ConversationStore> super::native::Native for OwnerExecutor<S> {
    fn local_hosts(&self) -> Arc<dyn crate::turns::LocalHosts> {
        Arc::<WeakRoomKit<S>>::clone(&self.kit)
    }

    #[cfg(feature = "redis")]
    fn look_up(&self) -> crate::turns::redis::WorkerLookup {
        let identity = Weak::clone(&self.identity);
        Box::new(move || Some(identity.upgrade()?))
    }

    fn lifecycle(&self) -> Weak<crate::turns::executor::Lifecycle> {
        Weak::clone(&self.lifecycle)
    }

    #[cfg(feature = "redis")]
    fn served_by(&self, backend: &crate::RedisTurnBackend) {
        if let Some(worker) = self.worker() {
            worker.served_by(backend);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, owner_executor};
    use crate::model::ChannelId;
    use crate::testing::{EchoAgent, EchoAgentOptions};
    use crate::{RoomKit, RoomKitOptions};

    #[tokio::test]
    async fn holding_the_owners_executor_preserves_its_identity_without_retaining_its_kit() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let weak = kit.downgrade();
        let agent = ChannelId::new("live").unwrap();
        let view = owner_executor(&kit);
        let held = Arc::downgrade(&view);
        assert_eq!(view.executor_id(), kit.turn_executor().executor_id());
        assert!(!view.hosts_channel(&agent));
        kit.register_channel(EchoAgent::new(
            agent.clone(),
            "here",
            EchoAgentOptions::default(),
        ))
        .unwrap();
        assert!(view.declares(&agent));
        #[cfg(feature = "redis")]
        let selection = {
            let native = view.native();
            let recovered = native.look_up()().unwrap();
            assert!(Arc::ptr_eq(&recovered, &view));
            assert!(native.lifecycle().ptr_eq(&kit.turn_executor().lifecycle()));
            let selector = native.local_hosts();
            assert!(Arc::ptr_eq(&selector, &native.local_hosts()));
            selector
        };
        view.close().await;
        assert!(view.closed() && view.hosts_channel(&agent) && !view.declares(&agent));
        drop(kit);
        assert!(weak.upgrade().is_none());
        assert!(view.closed() && !view.hosts_channel(&agent));
        #[cfg(feature = "redis")]
        assert!(!selection.hosts_name(agent.as_str()));
        drop(view);
        assert!(held.upgrade().is_none());
    }
}
