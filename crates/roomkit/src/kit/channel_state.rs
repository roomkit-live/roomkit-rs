//! Per-room channel verbs and their stored state (RFC §10.6.2–3).

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use serde_json::Value;
use tokio::task::JoinHandle;

use super::{GetRoomOptions, RoomKit};
use crate::RoomKitError;
use crate::store::{ConversationStore, PatchRoomMetadataOptions};
use crate::tasks::{Kind, Marks, Phase, Unit};
use crate::turns::{ChannelCall, state_key, stored_state};

/// Python's `channel_call(..., *, organization_id=None, **arguments)`.
#[derive(Clone, Debug, Default)]
pub struct ChannelCallOptions {
    /// The organization the call is scoped to.
    pub organization_id: Option<String>,
    /// The keyword arguments the channel's verb receives.
    pub arguments: Metadata,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Run a per-room verb of an attached channel on its executor.
    /// Changed room state is stored before returning or raising the verb's
    /// error; a response after detach cannot recreate stored state.
    ///
    /// # Errors
    /// Refuses a missing room, another organization's room, an unregistered
    /// or unbound channel, a closed kit or executor, or the channel's error.
    pub async fn channel_call(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        verb: &str,
        options: ChannelCallOptions,
    ) -> Result<Value, RoomKitError> {
        self.inner.open()?;
        let (kit, channel_id, room_id, verb) = (
            self.clone(),
            channel_id.clone(),
            room_id.clone(),
            verb.to_owned(),
        );
        let work = async move {
            kit.channel_call_until_cut(channel_id, room_id, verb, options)
                .await
        };
        // Preserve the caller's turn/host/tool scopes and give this call
        // its own tracked unit, so a close it makes can spare that unit.
        let task = Marks::current()
            .with(Unit::new())
            .sync_scope(|| {
                self.inner
                    .tasks
                    .spawn_joined(Kind::Call, crate::keep_context(work))
            })
            .ok_or(RoomKitError::KitClosed)?;
        let mut held = HeldCall(task);
        match (&mut held.0).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(RoomKitError::Cancelled),
        }
    }

    async fn channel_call_until_cut(
        &self,
        channel_id: ChannelId,
        room_id: RoomId,
        verb: String,
        options: ChannelCallOptions,
    ) -> Result<Value, RoomKitError> {
        let mut phases = self.inner.tasks.phases();
        let marks = Marks::current();
        let run = self.run_channel_call(channel_id, room_id, verb, options);
        tokio::pin!(run);
        loop {
            tokio::select! {
                biased;
                changed = phases.changed() => {
                    if changed.is_err() || (*phases.borrow() >= Phase::Cut
                        && !self.inner.tasks.spares(&marks)) {
                        return Err(RoomKitError::Cancelled);
                    }
                }
                result = &mut run => return result,
            }
        }
    }

    async fn run_channel_call(
        &self,
        channel_id: ChannelId,
        room_id: RoomId,
        verb: String,
        options: ChannelCallOptions,
    ) -> Result<Value, RoomKitError> {
        let mut room = self
            .get_room(
                &room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        let channel = self
            .inner
            .channel(&channel_id)
            .ok_or_else(|| RoomKitError::channel_not_registered(channel_id.clone()))?;
        if self
            .store()
            .get_binding(&room_id, &channel_id)
            .await?
            .is_none()
        {
            return Err(RoomKitError::ChannelNotFound {
                room_id,
                channel_id,
            });
        }
        if self
            .inner
            .turns
            .pending_detaches
            .wait(&channel_id, &room_id)
            .await
        {
            room = self
                .get_room(
                    &room_id,
                    GetRoomOptions {
                        organization_id: room.organization_id,
                    },
                )
                .await?;
            if self
                .store()
                .get_binding(&room_id, &channel_id)
                .await?
                .is_none()
            {
                return Err(RoomKitError::ChannelNotFound {
                    room_id,
                    channel_id,
                });
            }
        }
        let call = ChannelCall {
            call_id: format!("call-{}", uuid::Uuid::new_v4().simple()),
            room_state: stored_state(&room.metadata, &channel_id),
            organization_id: room.organization_id,
            channel_id: channel_id.clone(),
            room_id: room_id.clone(),
            verb,
            arguments: options.arguments,
            previous_turn: None,
        };
        let answer = self.inner.turns.channel_call(call, channel).await?;
        if let Some(state) = answer.room_state {
            self.keep_channel_state(&room_id, &channel_id, state)
                .await?;
        }
        match answer.error {
            Some(error) => Err(crate::turns::rebuild_kit_error(&error)),
            None => Ok(answer.result),
        }
    }

    async fn keep_channel_state(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        state: Metadata,
    ) -> Result<(), RoomKitError> {
        let held = self.inner.locks.lock(room_id).await?;
        let written = async {
            if self.inner.tasks.is_sealed() {
                return Err(RoomKitError::KitClosed);
            }
            if self
                .store()
                .get_binding(room_id, channel_id)
                .await?
                .is_some()
            {
                store_state(&*self.inner.store, room_id, channel_id, state).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = held.release().await {
            tracing::warn!(target: "roomkit::locks", %error, %room_id, "releasing a room lock failed");
        }
        written
    }
}

/// Called under the room lock, also by the intelligence detach path.
pub(crate) async fn store_state<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    channel_id: &ChannelId,
    state: Metadata,
) -> Result<(), RoomKitError> {
    let key = state_key(channel_id);
    let (patch, unset) = if state.is_empty() {
        (Metadata::new(), vec![key])
    } else {
        (
            [(key, Value::Object(state))].into_iter().collect(),
            Vec::new(),
        )
    };
    store
        .patch_room_metadata(room_id, patch, PatchRoomMetadataOptions { unset })
        .await?;
    Ok(())
}

struct HeldCall<T>(JoinHandle<T>);

impl<T> Drop for HeldCall<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
