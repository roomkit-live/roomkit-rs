//! The fresh room reader a local turn reaches through its owner.

use std::sync::Weak;

use roomkit_core::model::{Metadata, RoomContext, RoomId};

use super::{BoxFuture, TurnHost, decoded};
use crate::kit::Inner;
use crate::turns::wire::{ROOM_CONTEXT, ReadRoomContext};
use crate::{ConversationStore, RoomKitError};

impl TurnHost {
    /// The room as its owner builds it now, as JSON data (RFC §10.6.5,
    /// §10.6.6): Python's `TurnHost.room_context()`.
    ///
    /// Re-reads the room, bindings, participants and recent history, including
    /// history when this channel's window is zero. The returned mapping owns
    /// its nested data independently of the store and of later reads.
    ///
    /// # Errors
    ///
    /// Propagates a missing room, store failure or failed owner link. After
    /// the turn the reader is refused with [`RoomKitError::Runtime`].
    pub async fn room_context(&self) -> Result<Metadata, RoomKitError> {
        let context = self.verb(ROOM_CONTEXT, &ReadRoomContext::default()).await?;
        decoded(context)
    }
}

pub(super) fn read_context<S: ConversationStore>(
    kit: Weak<Inner<S>>,
    room_id: RoomId,
) -> BoxFuture<'static, Result<RoomContext, RoomKitError>> {
    Box::pin(async move {
        let inner = kit.upgrade().ok_or(RoomKitError::KitClosed)?;
        let recent = inner.config().recent_events().read_by_the_caller();
        crate::driver::load_context(&*inner.store, &room_id, recent)
            .await?
            .ok_or(RoomKitError::RoomNotFound(room_id))
    })
}
