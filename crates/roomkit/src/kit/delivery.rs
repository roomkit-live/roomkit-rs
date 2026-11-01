//! Proactive delivery (RFC §22): Python's `deliver`; and what a provider
//! reports of the messages the rooms sent (RFC §10.4, A delivery status
//! follows its message), and its webhooks: Python's
//! `process_delivery_status`, `on_delivery_status` and `process_webhook`.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, DeliveryStatus, Metadata, RoomContext, RoomId};
use roomkit_core::rules::hook_registry::{HookExecution, HookSubject, HookTrigger};

use super::options::{GetRoomOptions, ProcessInboundOptions};
use super::{Inner, RoomKit, read};
use crate::delivery::execute::reject_invalid;
use crate::delivery::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use crate::delivery::status::room_of_status;
use crate::delivery::{ExecuteDeliveryOptions, execute_delivery, serialize_strategy};
use crate::driver;
use crate::error::RoomKitError;
use crate::hooks::{self, HookOptions, Kept};
use crate::providers::sms::WebhookMeta;
use crate::store::{ConversationStore, InMemoryStore};
use crate::tasks::Kind;
use crate::{DeliveryBackend, DeliveryItem, DeliveryStrategy, Immediate};

/// What [`RoomKit::deliver`] takes beside the room and the content:
/// Python's `deliver(room_id, content, *, channel_id=None, strategy=None,
/// metadata=None, addressed_to=None, idempotency_key=None, session_id=None,
/// chain_depth=0, instruction=False, organization_id=None)`.
pub struct DeliverOptions<S = InMemoryStore> {
    /// Python `channel_id=None`: the transport that carries the content;
    /// `None` takes the room's, a voice one first. An intelligence channel
    /// named here is carried by the room's transport.
    pub channel_id: Option<ChannelId>,
    /// Python `strategy=None`: when it goes out (RFC §22.2); `None` takes
    /// the kit's, [`RoomKitOptions::delivery_strategy`](crate::RoomKitOptions::delivery_strategy),
    /// else [`Immediate`]. Python's shorthand names (`"queued"`) are the
    /// strategies' values here (`Queued::default()`).
    pub strategy: Option<Arc<dyn DeliveryStrategy<S>>>,
    /// Python `metadata=None`: the metadata of the event it publishes.
    pub metadata: Metadata,
    /// Python `addressed_to=None`: the intelligence channels asked to act
    /// (RFC §19.3). `None` keeps routing, an empty list solicits no agent;
    /// it changes no visibility.
    pub addressed_to: Option<Vec<ChannelId>>,
    /// Python `idempotency_key=None`: the key naming one publication in the
    /// room, whatever transport carries it (RFC §22.1). A blank one is
    /// refused.
    pub idempotency_key: Option<String>,
    /// Python `session_id=None`: the realtime session the content is
    /// injected into; this implementation has no realtime session, so a
    /// delivery naming one is refused.
    pub session_id: Option<String>,
    /// Python `chain_depth=0`: the chain the content continues (RFC §8.3);
    /// made within a turn, at least the turn's response's.
    pub chain_depth: u32,
    /// Python `instruction=False`: the content is the application's
    /// direction to the agents it addresses, an `INSTRUCTION` never stored
    /// (RFC §10.1.1), which needs `addressed_to` and no key.
    pub instruction: bool,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2): the room is read with it before any hook runs.
    pub organization_id: Option<String>,
}

impl<S> Default for DeliverOptions<S> {
    fn default() -> Self {
        Self {
            channel_id: None,
            strategy: None,
            metadata: Metadata::new(),
            addressed_to: None,
            idempotency_key: None,
            session_id: None,
            chain_depth: 0,
            instruction: false,
            organization_id: None,
        }
    }
}

impl<S> Clone for DeliverOptions<S> {
    fn clone(&self) -> Self {
        Self {
            channel_id: self.channel_id.clone(),
            strategy: self.strategy.clone(),
            metadata: self.metadata.clone(),
            addressed_to: self.addressed_to.clone(),
            idempotency_key: self.idempotency_key.clone(),
            session_id: self.session_id.clone(),
            chain_depth: self.chain_depth,
            instruction: self.instruction,
            organization_id: self.organization_id.clone(),
        }
    }
}

impl<S> std::fmt::Debug for DeliverOptions<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliverOptions")
            .field("channel_id", &self.channel_id)
            .field(
                "strategy",
                &self.strategy.as_ref().map(|_| "DeliveryStrategy"),
            )
            .field("addressed_to", &self.addressed_to)
            .field("idempotency_key", &self.idempotency_key)
            .field("session_id", &self.session_id)
            .field("chain_depth", &self.chain_depth)
            .field("instruction", &self.instruction)
            .field("organization_id", &self.organization_id)
            .finish_non_exhaustive()
    }
}

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
    /// Brings `content` into the room `room_id` from outside it, for the
    /// room's agents (RFC §22): a background task's result, an external
    /// event, a scheduled nudge. Python's `deliver(room_id, content, ...)`,
    /// its keywords in `options`, with the immediate strategy.
    ///
    /// The content enters the room as a message the framework's own sender,
    /// `system`, writes on the room's transport (or the one `options`
    /// name), through the inbound pipeline: the room's agents read it and
    /// act, the correspondent receives their answer, never the content
    /// itself. A message for the correspondent is the host's own words:
    /// [`post`](Self::post). `BEFORE_DELIVER` runs first and may block it
    /// or rewrite it; `AFTER_DELIVER` observes the outcome, whatever it is
    /// (RFC §22.3).
    ///
    /// Returns the actual outcome (RFC §22.1): `sent` once the turn the
    /// publication asked for is over (`turn_complete`), `blocked` by a hook,
    /// the room or the request's form, `unavailable` for a destination it
    /// cannot reach, `failed`; the published event, a keyed replay's
    /// original as a `duplicate`, and the inbound result in `inbound`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::RoomNotFound`] when `options` name an organization
    /// and the room is missing or another organization's: nothing is
    /// delivered or fired. Any other failure is the outcome's.
    pub async fn deliver(
        &self,
        room_id: &RoomId,
        content: impl Into<String>,
        options: DeliverOptions<S>,
    ) -> Result<DeliveryOutcome, RoomKitError> {
        let DeliverOptions {
            channel_id,
            strategy,
            metadata,
            addressed_to,
            idempotency_key,
            session_id,
            chain_depth,
            instruction,
            organization_id,
        } = options;
        if organization_id.is_some() {
            self.get_room(room_id, GetRoomOptions { organization_id })
                .await?;
        }
        let strategy = strategy
            .or_else(|| self.inner.delivery_strategy.clone())
            .unwrap_or_else(|| Arc::new(Immediate));
        let mut item = DeliveryItem {
            channel_id,
            metadata,
            addressed_to,
            idempotency_key,
            session_id,
            chain_depth,
            instruction,
            strategy: serialize_strategy(Some(&*strategy)),
            ..DeliveryItem::new(room_id.clone(), content)
        };
        let Some(backend) = self.inner.delivery_backend.clone() else {
            let options = ExecuteDeliveryOptions {
                strategy: Some(strategy),
                hook_engine: None,
            };
            return Ok(execute_delivery(self, &mut item, options).await);
        };
        Ok(self.enqueue(&*backend, item).await)
    }

    /// Enqueues `item` in `backend`, its form checked first: the outcome
    /// `queued` naming the item, a refusal of its form, or the enqueue's
    /// failure.
    async fn enqueue(
        &self,
        backend: &dyn DeliveryBackend<S>,
        mut item: DeliveryItem,
    ) -> DeliveryOutcome {
        if let Some(refusal) = reject_invalid(self, &mut item).await {
            return refusal;
        }
        let id = item.id.clone();
        match backend.enqueue(item).await {
            Ok(()) => DeliveryOutcome {
                delivery_item_id: Some(id),
                ..DeliveryOutcome::new(DeliveryOutcomeStatus::Queued)
            },
            Err(error) => {
                let error = roomkit_core::model::DeliveryError::new(
                    "DeliveryBackendError",
                    error.to_string(),
                );
                DeliveryOutcome {
                    delivery_item_id: Some(id),
                    ..DeliveryOutcome::failed("enqueue_failed", error)
                }
            }
        }
    }

    /// The kit's delivery backend, if it was built with one: Python's
    /// `RoomKit.delivery_backend`.
    pub fn delivery_backend(&self) -> Option<Arc<dyn DeliveryBackend<S>>> {
        self.inner.delivery_backend.clone()
    }

    /// Starts the kit's own work that runs once it is entered: its delivery
    /// backend's worker (RFC §22.2), Python's `async with RoomKit(...) as
    /// kit` entering the kit; [`close`](Self::close) stops it. Rust has no
    /// async context manager, so the kit is started by this call. Starting
    /// it again starts nothing more.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close started, as Python's
    /// `async with` on a closed kit raises; [`RoomKitError::Value`] for the
    /// backend's failure to start.
    pub async fn start(&self) -> Result<(), RoomKitError> {
        self.inner.open()?;
        if let Some(backend) = self.inner.delivery_backend.clone() {
            backend
                .start(self.clone())
                .await
                .map_err(|error| RoomKitError::Value(error.to_string()))?;
        }
        Ok(())
    }

    /// Publishes `message`, the delivery path's, into the room `room_id`,
    /// its delivery deferred, its key `publishing` admitted (RFC §22.1).
    pub(crate) async fn publish_delivery(
        &self,
        message: roomkit_core::model::InboundMessage,
        room_id: &RoomId,
        publishing: Option<String>,
    ) -> Result<crate::inbound::InboundResult, RoomKitError> {
        let options = ProcessInboundOptions {
            room_id: Some(room_id.clone()),
            defer_delivery: true,
            organization_id: None,
        };
        self.run_message(message, options, publishing).await
    }

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
