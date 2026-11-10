//! A message's entry: scope and work budget precede the channel's callbacks.

use roomkit_core::model::{ChannelType, InboundMessage};
use roomkit_core::ops::{Entry, Inbound, InboundTarget};
use roomkit_core::rules::chain_depth::Draw;

use super::inject::Entered;
use super::telemetry;
use super::{ProcessInboundOptions, RoomKit};
use crate::context::{self, ExecutionContext};
use crate::deadline::CallDeadline;
use crate::delivery_keys::NamedGuard;
use crate::driver;
use crate::error::RoomKitError;
use crate::inbound::InboundResult;
use crate::store::ConversationStore;

const PROCESS_INBOUND: &str = "process_inbound";

enum Preflight {
    Ready(Option<Draw>),
    Refused(Box<InboundResult>),
}

/// What belongs to this call before any await, kept through its arrival.
struct MessageCall {
    deadline: CallDeadline,
    named: NamedGuard,
    charged: Option<Draw>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The public inbound path, also used by a delivery publication.
    pub(crate) async fn run_message(
        &self,
        mut message: InboundMessage,
        options: ProcessInboundOptions,
        publication: Option<String>,
    ) -> Result<InboundResult, RoomKitError> {
        let deadline = CallDeadline::new(self.config().process_timeout);
        message.content.check()?;
        self.inner.open()?;
        let named = self.inner.delivery_keys.naming(
            message.delivery_key.as_deref(),
            options.organization_id.as_deref(),
        );
        let charged = match self.precharge_named(&message, &options, deadline).await? {
            Preflight::Ready(draw) => draw,
            Preflight::Refused(refused) => {
                let span =
                    self.inner
                        .inbound_span(&message, &message.sender_id, options.room_id.as_ref());
                let result = Ok(*refused);
                telemetry::end_inbound_span(span, &result, options.defer_delivery);
                return result;
            }
        };
        let call = MessageCall {
            deadline,
            named,
            charged,
        };
        if let Some(limit) = &self.inner.inbound_limit
            && !limit.acquire(&message.channel_id)
        {
            return Ok(InboundResult {
                blocked: true,
                reason: Some(crate::sends::RATE_LIMITED.to_owned()),
                ..InboundResult::default()
            });
        }
        let is_host = self
            .config()
            .channel(&message.channel_id)
            .is_some_and(|channel| channel.channel_type == ChannelType::HOST);
        let channel = self.inner.channel(&message.channel_id);
        let (Some(channel), false) = (channel, is_host) else {
            // The core reports an unknown/host channel, without callbacks or
            // a span. A cap refusal was already reported with the raw sender.
            return Box::pin(self.traced_message(message, options, publication, call, false)).await;
        };
        message.sender_id = channel.normalize_address(&message.sender_id);
        let span = self
            .inner
            .inbound_span(&message, &message.sender_id, options.room_id.as_ref());
        let deferred = options.defer_delivery;
        let result = span
            .within(Box::pin(self.traced_message(
                message,
                options,
                publication,
                call,
                true,
            )))
            .await;
        telemetry::end_inbound_span(span, &result, deferred);
        result
    }

    /// Capture and charge the room's current work once. A scoped read comes
    /// first; a missing room is left for routing to create, as in Python.
    async fn precharge_named(
        &self,
        message: &InboundMessage,
        options: &ProcessInboundOptions,
        deadline: CallDeadline,
    ) -> Result<Preflight, RoomKitError> {
        let Some(room) = options.room_id.as_ref() else {
            return Ok(Preflight::Ready(None));
        };
        let Some(draw) = ExecutionContext::current().work_budget(room) else {
            return Ok(Preflight::Ready(None));
        };
        if let Some(organization) = options.organization_id.as_deref() {
            match deadline.until(self.store().get_room(room)).await {
                Some(Ok(Some(found))) if found.organization_id.as_deref() != Some(organization) => {
                    return Err(RoomKitError::RoomNotFound(room.clone()));
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
                None => {
                    let entry = Entry::Message {
                        room_id: room.clone(),
                        organization_id: options.organization_id.clone(),
                        message: Box::new(message.clone()),
                        answers: None,
                    };
                    let ended = self.entry_timed_out(&entry).await?;
                    return Ok(Preflight::Refused(Box::new(
                        ended.into_result(options.defer_delivery, &self.inner.tasks),
                    )));
                }
            }
        }
        if !draw.take() {
            return Ok(Preflight::Refused(Box::new(InboundResult {
                blocked: true,
                reason: Some("reentry_loop_cap".to_owned()),
                ..Default::default()
            })));
        }
        Ok(Preflight::Ready(Some(draw)))
    }

    /// The remaining inbound pipeline under its span, with its acquired charge.
    async fn traced_message(
        &self,
        mut message: InboundMessage,
        options: ProcessInboundOptions,
        publication: Option<String>,
        call: MessageCall,
        ask_channel: bool,
    ) -> Result<InboundResult, RoomKitError> {
        let answers = ask_channel
            .then(|| self.inner.answer(&message))
            .flatten()
            .map(|mut answers| {
                answers.publication = publication;
                answers
            });
        let ProcessInboundOptions {
            room_id,
            defer_delivery,
            organization_id,
        } = options;
        let MessageCall {
            deadline,
            named,
            charged,
        } = call;
        let entered = if let Some(room_id) = room_id {
            let entry = Entry::Message {
                room_id,
                organization_id,
                message: Box::new(message),
                answers: answers.map(Box::new),
            };
            self.enter_named(
                entry,
                PROCESS_INBOUND,
                defer_delivery,
                Some(named),
                deadline,
                charged,
            )
            .await?
        } else {
            context::as_deep_as_its_turn(&mut message.chain_depth);
            self.inner.open()?;
            let target = InboundTarget::Routed;
            let routed = match answers {
                Some(answers) => {
                    Inbound::answered(self.config(), target, organization_id, message, answers)
                }
                None => Inbound::new(self.config(), target, organization_id, message),
            };
            let ran = driver::run_inbound(
                &self.inner,
                PROCESS_INBOUND,
                None,
                (routed, deadline),
                defer_delivery,
                Some(named),
            )
            .await;
            Entered::ran(ran, RoomKitError::from)?
        };
        Ok(entered.into_result(defer_delivery, &self.inner.tasks))
    }
}
