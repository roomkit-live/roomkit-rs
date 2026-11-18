//! A detach's notification and cleanup retain the failure that actually occurred.

use std::sync::Arc;

use roomkit_core::model::{ChannelBinding, ChannelCategory};
use roomkit_core::ops::{ChannelOutcome, Outcome};

use crate::kit::Inner;
use crate::kit::channel_detach::DetachCleanup;
use crate::{ChannelError, ConversationStore, RoomKitError};

/// Intelligence callbacks are logged by their executor; cleanup failures and
/// ordinary channel callbacks still fail the announced detach (RFC §6.1).
pub(super) async fn tell<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    binding: ChannelBinding,
) -> Outcome {
    let channel = inner.channel(&binding.channel_id);
    if binding.category == ChannelCategory::Intelligence {
        let (channel_id, room_id) = (&binding.channel_id, &binding.room_id);
        let organization =
            match super::control_organization(inner, room_id, channel.as_deref()).await {
                Ok(organization) => organization,
                Err(error) => return failed(error),
            };
        let cleanup = match DetachCleanup::begin(inner, channel_id, room_id).await {
            Ok(cleanup) => cleanup,
            Err(error) => return failed(error),
        };
        if !inner.turns.carries_controls(channel.as_deref()) {
            inner.wiring().env.room_left(room_id, channel_id);
        }
        let notified = inner
            .turns
            .detached(channel_id, room_id, organization, channel)
            .await;
        if let Err(error) = cleanup.finish().await {
            return failed(error);
        }
        return match notified {
            Ok(()) => ChannelOutcome::Told.into(),
            Err(error) => failed(error),
        };
    }
    let Some(channel) = channel else {
        return ChannelOutcome::Told.into();
    };
    super::told(channel.on_room_detached(binding.room_id).await)
}

/// `StoreFailure` is the Rust wrapper for the exception Python propagates. The
/// wrapper's generic display must not replace that cause's class and message.
pub(super) fn failed(error: RoomKitError) -> Outcome {
    let cause: ChannelError = if let RoomKitError::Store(failure) = error {
        if let Some(record) = crate::error::stored_record(&*failure) {
            return Outcome::Failed(record.clone());
        }
        failure.into_inner()
    } else {
        Box::new(error)
    };
    super::channel_failed(&cause)
}
