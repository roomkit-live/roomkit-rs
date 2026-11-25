//! The transport can keep the native admission and shutdown path after erasing the store.
use std::sync::Weak;

use crate::turns::executor::Lifecycle;
use crate::{ConversationStore, TurnExecutor};

use super::ServedExecutor;

/// The type is unnameable outside this module's parent.
#[derive(Debug)]
pub(crate) struct Key(());

pub(crate) trait Native {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts>;
    fn look_up(&self) -> crate::turns::distributed::WorkerLookup;
    fn lifecycle(&self) -> Weak<Lifecycle>;
}

impl<S: ConversationStore> Native for TurnExecutor<S> {
    fn local_hosts(&self) -> std::sync::Arc<dyn crate::turns::LocalHosts> {
        self.local_hosts()
    }
    fn look_up(&self) -> crate::turns::distributed::WorkerLookup {
        self.look_up()
    }
    fn lifecycle(&self) -> Weak<Lifecycle> {
        self.lifecycle()
    }
}

impl dyn ServedExecutor {
    pub(crate) fn native(&self) -> &dyn Native {
        self.transport(Key(()))
    }
}
