//! The transport can keep the native admission and shutdown path after erasing the store.
use std::sync::Weak;

use crate::turns::executor::{Lifecycle, WorkerOrigin};
use crate::turns::{CarriedRequest, WorkerTurn};
use crate::{ConversationStore, RedisTurnBackend, RoomKitError, TurnExecutor};

use super::ServedExecutor;

/// The type is unnameable outside this module's parent.
#[derive(Debug)]
pub(crate) struct Key(());

pub(crate) trait Native {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts>;
    fn look_up(&self) -> crate::turns::redis::WorkerLookup;
    fn lifecycle(&self) -> Weak<Lifecycle>;
    fn served_by(&self, backend: &RedisTurnBackend);
    fn take_from(
        &self,
        request: CarriedRequest,
        origin: WorkerOrigin,
    ) -> Result<WorkerTurn, RoomKitError>;
}

impl<S: ConversationStore> Native for TurnExecutor<S> {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts> {
        self.local_hosts()
    }
    fn look_up(&self) -> crate::turns::redis::WorkerLookup {
        self.look_up()
    }
    fn lifecycle(&self) -> Weak<Lifecycle> {
        self.lifecycle()
    }
    fn served_by(&self, backend: &RedisTurnBackend) {
        self.served_by(backend);
    }
    fn take_from(
        &self,
        request: CarriedRequest,
        origin: WorkerOrigin,
    ) -> Result<WorkerTurn, RoomKitError> {
        self.take_from(request, origin)
    }
}

impl dyn ServedExecutor {
    pub(crate) fn native(&self) -> &dyn Native {
        self.transport(Key(()))
    }
}
