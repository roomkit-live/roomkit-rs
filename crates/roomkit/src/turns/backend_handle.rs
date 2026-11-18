//! The backend object shared by room owners and standalone clients.
use super::{ExecutorInfo, LocalPresenceClaims, TurnBackend};
use crate::RoomKitError;
use std::{fmt, ops::Deref, sync::Arc};

struct Shared {
    transport: Arc<dyn TurnBackend>,
    presence: LocalPresenceClaims,
}

/// A transport and its default presence state. Clone the same handle for each
/// owner or client using that backend (RFC §10.6.7).
#[derive(Clone)]
pub struct TurnBackendHandle(Arc<Shared>);

impl TurnBackendHandle {
    /// Wrap an application transport. Each new handle starts with empty local claims.
    pub fn new(transport: Arc<dyn TurnBackend>) -> Self {
        Self(Arc::new(Shared {
            transport,
            presence: LocalPresenceClaims::default(),
        }))
    }

    /// The workers listed by this transport. A backend without a catalogue returns an empty list.
    ///
    /// # Errors
    /// Returns the transport's error if its catalogue could not be read.
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        match self.0.transport.executors() {
            Some(read) => read.await,
            None => Ok(Vec::new()),
        }
    }

    pub(crate) fn lists_executors(&self) -> bool {
        self.0.transport.executors().is_some()
    }

    /// Claim a change once per room and channel. Passing `None` forgets the pair.
    ///
    /// # Errors
    /// Returns the transport's error if its shared presence store cannot be reached.
    pub async fn claim_presence(
        &self,
        channel_id: &str,
        room_id: &str,
        online: Option<bool>,
    ) -> Result<bool, RoomKitError> {
        match self.0.transport.claim_presence(channel_id, room_id, online) {
            Some(claim) => claim.await,
            None => Ok(self.0.presence.claim(channel_id, room_id, online)),
        }
    }
}

impl Deref for TurnBackendHandle {
    type Target = dyn TurnBackend;
    fn deref(&self) -> &Self::Target {
        self.0.transport.as_ref()
    }
}

impl fmt::Debug for TurnBackendHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnBackendHandle").finish_non_exhaustive()
    }
}
