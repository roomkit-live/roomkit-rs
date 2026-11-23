//! The transport can keep the native admission and shutdown path after erasing the store.
use std::sync::Weak;

#[cfg(feature = "redis")]
use crate::RedisTurnBackend;
use crate::turns::executor::Lifecycle;
use crate::{ConversationStore, TurnExecutor};

use super::ServedExecutor;

/// The type is unnameable outside this module's parent.
#[derive(Debug)]
pub(crate) struct Key(());

pub(crate) trait Native {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts>;
    #[cfg(feature = "redis")]
    fn look_up(&self) -> crate::turns::redis::WorkerLookup;
    fn lifecycle(&self) -> Weak<Lifecycle>;
    #[cfg(feature = "redis")]
    fn served_by(&self, backend: &RedisTurnBackend);
}

impl<S: ConversationStore> Native for TurnExecutor<S> {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts> {
        self.local_hosts()
    }
    #[cfg(feature = "redis")]
    fn look_up(&self) -> crate::turns::redis::WorkerLookup {
        self.look_up()
    }
    fn lifecycle(&self) -> Weak<Lifecycle> {
        self.lifecycle()
    }
    #[cfg(feature = "redis")]
    fn served_by(&self, backend: &RedisTurnBackend) {
        self.served_by(backend);
    }
}

impl dyn ServedExecutor {
    pub(crate) fn native(&self) -> &dyn Native {
        self.transport(Key(()))
    }
}
