//! What a provider reports of the messages the rooms sent (RFC §10.4, A
//! delivery status follows its message), and its webhooks: Python's
//! `process_delivery_status`, `on_delivery_status` and `process_webhook`.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, DeliveryStatus, RoomContext, RoomId};
use roomkit_core::rules::hook_registry::{HookExecution, HookSubject, HookTrigger};

use super::options::ProcessInboundOptions;
use super::{Inner, RoomKit, read};
use crate::delivery::status::room_of_status;
use crate::driver;
use crate::error::RoomKitError;
use crate::hooks::{self, HookOptions, Kept};
use crate::providers::sms::WebhookMeta;
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// What [`RoomKit::process_delivery_status`] takes beside the status:
/// Python's `process_delivery_status(status, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessDeliveryStatusOptions {
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2). A status naming no room is routed among its rooms only,
    /// and a room the status names that is another organization's is not
    /// found; `None` routes among every room.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::process_webhook`] takes beside the webhook and the
/// channel: Python's `process_webhook(meta, channel_id,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessWebhookOptions {
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2), the room a message lands in and the room a delivery
    /// status is dispatched in being one of its rooms; `None` reads rooms
    /// unscoped.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Runs `handler` on every delivery status the kit processes from now
    /// on: Python's `@kit.on_delivery_status`, an ASYNC hook on
    /// `ON_DELIVERY_STATUS` (RFC §9.2), which every status dispatched in a
    /// room runs, the room's context set aside. The hook is named as an
    /// unnamed hook is: `hook`, then `hook#2`, ...
    ///
    /// ```
    /// use roomkit::RoomKit;
    /// use roomkit::model::DeliveryStatus;
    ///
    /// let kit = RoomKit::new(Default::default());
    /// kit.on_delivery_status(|status: DeliveryStatus| async move {
    ///     if status.status == "failed" {
    ///         eprintln!("message {} failed: {:?}", status.message_id, status.error_message);
    ///     }
    /// })?;
    /// # Ok::<(), roomkit::RoomKitError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// As [`hook`](Self::hook): [`RoomKitError::KitClosed`] once the kit's
    /// close started.
    pub fn on_delivery_status<F, Fut>(&self, r#fn: F) -> Result<(), RoomKitError>
    where
        F: Fn(DeliveryStatus) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let options = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        let handler = move |status: DeliveryStatus, _context: RoomContext| r#fn(status);
        self.hook(HookTrigger::OnDeliveryStatus, handler, options)
    }

    /// Handles a provider's webhook, once its payload was read into `meta`:
    /// a message received is processed as one on the channel `channel_id`
    /// ([`process_inbound`](Self::process_inbound), its hooks and turns
    /// included), an outbound message's delivery status is processed as
    /// the channel's ([`process_delivery_status`](Self::process_delivery_status)),
    /// and any other webhook is acknowledged and ignored. Python's
    /// `process_webhook(meta, channel_id, organization_id=None)`.
    ///
    /// It does not authenticate the webhook: the endpoint verifies that the
    /// payload came from the provider before it calls this, as nothing
    /// downstream can tell a forged webhook from a real one (RFC §17.1).
    ///
    /// # Errors
    ///
    /// What [`process_inbound`](Self::process_inbound) or
    /// [`process_delivery_status`](Self::process_delivery_status) raises.
    pub async fn process_webhook(
        &self,
        meta: WebhookMeta,
        channel_id: &ChannelId,
        options: ProcessWebhookOptions,
    ) -> Result<(), RoomKitError> {
        let organization_id = options.organization_id;
        if meta.is_inbound() {
            let inbound = meta.to_inbound(channel_id)?;
            let options = ProcessInboundOptions {
                organization_id,
                ..ProcessInboundOptions::default()
            };
            self.process_inbound(inbound, options).await?;
        } else if meta.is_status() {
            let status = DeliveryStatus {
                channel_id: Some(channel_id.clone()),
                ..meta.to_status()
            };
            let options = ProcessDeliveryStatusOptions { organization_id };
            self.process_delivery_status(status, options).await?;
        }
        Ok(())
    }

    /// Dispatches a provider's delivery status to the `ON_DELIVERY_STATUS`
    /// hooks of its room, with the room's context, and returns while they
    /// run: Python's `process_delivery_status(status, *,
    /// organization_id=None)`.
    ///
    /// Its room is the one `status.room_id` names, else the one its
    /// recipient's binding of `status.channel_id` names (RFC §10.4, A
    /// delivery status follows its message): the newest ACTIVE or PAUSED
    /// room whose binding of the channel names the recipient, in the
    /// channel's form, as step 1 finds a sender's, else the one ACTIVE room
    /// bound to the channel unless its binding names another
    /// correspondent, else none. A status for which no room is found, or
    /// whose room does not exist or is another organization's than the one
    /// `options` name (RFC §17.2), is logged and dispatched nowhere. A
    /// status is no activity: it resumes no room (RFC §5.1). Every hook on
    /// the trigger runs, the kit's and the room's, its filter unread.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Store`] for a read the store failed.
    pub async fn process_delivery_status(
        &self,
        status: DeliveryStatus,
        options: ProcessDeliveryStatusOptions,
    ) -> Result<(), RoomKitError> {
        let organization_id = options.organization_id.as_deref();
        let Some(room_id) = self.room_of(&status, organization_id).await? else {
            tracing::warn!(
                target: "roomkit::delivery",
                message_id = %status.message_id,
                "Cannot dispatch ON_DELIVERY_STATUS: no room_id resolved"
            );
            return Ok(());
        };
        let inner = &self.inner;
        let recent = inner.config().recent_events();
        let Some(context) = driver::load_context(&*inner.store, &room_id, recent).await? else {
            return Ok(());
        };
        if organization_id.is_some_and(|organization| {
            context.room.organization_id.as_deref() != Some(organization)
        }) {
            tracing::warn!(
                target: "roomkit::delivery",
                message_id = %status.message_id,
                room_id = %room_id,
                "Cannot dispatch ON_DELIVERY_STATUS: the room is not found"
            );
            return Ok(());
        }
        inner.start_status_hooks(&room_id, &status, &context);
        Ok(())
    }

    /// The room `status` names, else the one its channel and recipient
    /// route it to (RFC §10.4); `None` for a status naming neither a room
    /// nor a channel.
    async fn room_of(
        &self,
        status: &DeliveryStatus,
        organization_id: Option<&str>,
    ) -> Result<Option<RoomId>, RoomKitError> {
        if let Some(room_id) = &status.room_id {
            return Ok(Some(room_id.clone()));
        }
        let Some(channel_id) = &status.channel_id else {
            return Ok(None);
        };
        let channel = self.get_channel(channel_id);
        let store = &*self.inner.store;
        let found = room_of_status(
            store,
            status,
            channel_id,
            channel.as_deref(),
            organization_id,
        );
        Ok(found.await?)
    }
}

impl<S: ConversationStore> Inner<S> {
    /// Starts the `ON_DELIVERY_STATUS` hooks of the room `room_id`, the
    /// kit's and the room's, on `status` with `context`, each in the
    /// background, within its timeout, as observers run (RFC §9.3): the
    /// call that dispatched the status does not wait for them. Once the
    /// kit is shut down, none runs.
    fn start_status_hooks(
        self: &Arc<Self>,
        room_id: &RoomId,
        status: &DeliveryStatus,
        context: &RoomContext,
    ) {
        let trigger = HookTrigger::OnDeliveryStatus;
        let channel_id = status.channel_id.clone().unwrap_or_else(ChannelId::system);
        let channel_type = roomkit_core::model::ChannelType::SYSTEM;
        let subject = HookSubject::of_channel(&channel_id, &channel_type).in_room(room_id);
        let hooks = read(&self.config).hooks.observers(trigger, subject);
        for hook in hooks {
            let Some((Kept::Status(handler), spec)) = self.hook_with_spec(hook) else {
                continue;
            };
            let (inner, room_id) = (Arc::clone(self), room_id.clone());
            let (status, context) = (status.clone(), context.clone());
            let observing = async move {
                let outcome = hooks::run(handler, status, context, spec.timeout, |_| None).await;
                inner.report_observer(&spec, trigger, room_id, &outcome);
            };
            if self.tasks.spawn_alone(Kind::Observer, observing).is_none() {
                tracing::debug!(
                    target: "roomkit::hooks",
                    ?trigger,
                    "the kit is shut down: a delivery status's hook is not run"
                );
            }
        }
    }
}
