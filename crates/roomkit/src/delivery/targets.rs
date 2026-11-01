//! Where a proactive delivery goes and what it came to (RFC §22.1), as the
//! Python implementation's `_delivery_targets` resolves and reports it: the
//! transport that carries a text delivery into its room, its publication
//! through the inbound pipeline, and the outcome its inbound result reads
//! as. Rust has no realtime voice session: a delivery is always a text
//! publication.

use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelType, DeliveryError, DeliveryResultStatus, EventStatus,
    EventType, InboundMessage, Metadata, RoomId, TextContent,
};
use roomkit_core::rules::publications;

use super::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use crate::error::RoomKitError;
use crate::inbound::{DeliveryHandle, InboundResult};
use crate::kit::RoomKit;
use crate::store::ConversationStore;

/// The framework's own sender, who writes a delivery into its room: the host
/// speaking, never a correspondent of the room's channel (RFC §10.4).
pub(crate) const SYSTEM_SENDER_ID: &str = "system";

/// One delivery request, its destination as resolved so far: Python's
/// `DeliveryContext` without its kit and its voice sessions.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) room_id: RoomId,
    pub(crate) content: String,
    pub(crate) channel_id: Option<ChannelId>,
    pub(crate) metadata: Metadata,
    pub(crate) addressed_to: Option<Vec<ChannelId>>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) chain_depth: u32,
    pub(crate) instruction: bool,
    /// Whether the call waits for the turn its publication asks for; a
    /// batch a strategy delivers for others does not.
    pub(crate) wait_for_turn: bool,
}

impl Request {
    /// Whether it is a keyed publication whose key names one publication in
    /// its room (RFC §22.1): a key, no explicit realtime session, and no
    /// instruction, which takes no key.
    fn keyed_publication(&self) -> bool {
        self.idempotency_key.is_some() && self.session_id.is_none() && !self.instruction
    }
}

/// The voice channel types, which a delivery prefers to reach the room by.
fn is_voice(channel_type: &ChannelType) -> bool {
    [
        ChannelType::VOICE,
        ChannelType::REALTIME_VOICE,
        ChannelType::REALTIME_AUDIO_VIDEO,
    ]
    .contains(channel_type)
}

/// What a delivery resolved: the channel that carries it, or its outcome
/// already.
pub(crate) enum Prepared {
    Channel(ChannelId),
    Done(Box<DeliveryOutcome>),
}

impl<S: ConversationStore> RoomKit<S> {
    /// The transport a delivery into `room_id` takes when it names none:
    /// a voice channel, else the first other transport bound to the room.
    /// An intelligence channel attached as a transport is not one: an
    /// instruction delivered through it would re-enter the channel it is
    /// for.
    pub(crate) async fn find_transport_channel_id(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<ChannelId>, RoomKitError> {
        let bindings = self.store().list_bindings(room_id).await?;
        let (mut voice, mut text) = (None, None);
        for binding in bindings {
            if binding.category != ChannelCategory::Transport
                || self.is_intelligence(&binding.channel_id)
            {
                continue;
            }
            if is_voice(&binding.channel_type) {
                voice = Some(binding.channel_id);
            } else if text.is_none() {
                text = Some(binding.channel_id);
            }
        }
        Ok(voice.or(text))
    }

    fn is_intelligence(&self, channel_id: &ChannelId) -> bool {
        self.get_channel(channel_id)
            .is_some_and(|channel| channel.category() == ChannelCategory::Intelligence)
    }

    /// The original outcome of a keyed publication the room already holds,
    /// or `None` (RFC §22.1, §13.4): looked up within the room, whatever
    /// channel committed it, before any transport is resolved, so a retry
    /// that would resolve another transport, or none, is still that
    /// publication. A store that only answers "seen" gives a blocked
    /// duplicate.
    async fn replay_publication(
        &self,
        request: &Request,
    ) -> Result<Option<DeliveryOutcome>, RoomKitError> {
        let Some(key) = request
            .idempotency_key
            .as_deref()
            .filter(|_| request.keyed_publication())
        else {
            return Ok(None);
        };
        let stored = publications::publication_key(key);
        let store = self.store();
        let room_id = &request.room_id;
        if let Some(original) = store
            .get_event_by_idempotency_key(room_id, &stored, None)
            .await?
        {
            let blocked = original.status == EventStatus::Blocked;
            let replay = InboundResult {
                reason: if blocked {
                    original.blocked_by.clone()
                } else {
                    None
                },
                event: Some(original),
                duplicate: true,
                blocked,
                ..InboundResult::default()
            };
            return Ok(Some(text_outcome(replay)));
        }
        if store.check_idempotency(room_id, &stored, None).await? {
            let replay = InboundResult {
                blocked: true,
                reason: Some("duplicate".to_owned()),
                duplicate: true,
                ..InboundResult::default()
            };
            return Ok(Some(text_outcome(replay)));
        }
        Ok(None)
    }

    /// Resolves and checks the channel that carries `request`: `channel_id`,
    /// else the one it names, else the room's transport. A keyed
    /// publication the room holds already gives its original outcome
    /// first, before any transport is resolved (RFC §22.1).
    pub(crate) async fn prepare_delivery(
        &self,
        request: &Request,
        channel_id: Option<ChannelId>,
    ) -> Result<Prepared, RoomKitError> {
        if channel_id.is_none()
            && let Some(published) = self.replay_publication(request).await?
        {
            return Ok(Prepared::Done(Box::new(published)));
        }
        let resolved = match channel_id.or_else(|| request.channel_id.clone()) {
            Some(channel_id) => Some(channel_id),
            None => self.find_transport_channel_id(&request.room_id).await?,
        };
        let Some(channel_id) = resolved else {
            return Ok(done(DeliveryOutcome::unavailable(
                "no_transport",
                Vec::new(),
            )));
        };
        let bound = self
            .store()
            .get_binding(&request.room_id, &channel_id)
            .await?
            .is_some();
        let Some(channel) = self.get_channel(&channel_id).filter(|_| bound) else {
            let targets = vec![channel_id.to_string()];
            return Ok(done(DeliveryOutcome::unavailable(
                "channel_unavailable",
                targets,
            )));
        };
        if channel.category() == ChannelCategory::Intelligence {
            // The room's transport carries it; never an intelligence channel,
            // so the instruction cannot re-enter the channel it is for.
            let Some(transport) = self.find_transport_channel_id(&request.room_id).await? else {
                return Ok(done(DeliveryOutcome::unavailable(
                    "no_transport",
                    Vec::new(),
                )));
            };
            return Box::pin(self.prepare_delivery(request, Some(transport))).await;
        }
        // No channel of this implementation hosts a realtime model: a
        // session names nothing a text publication reaches.
        if request.session_id.is_some() {
            return Ok(done(DeliveryOutcome::blocked("session_requires_realtime")));
        }
        Ok(Prepared::Channel(channel_id))
    }

    /// Publishes `request` into its room through `channel_id`, as a message
    /// the framework's own sender writes on it, and reports what it came
    /// to: Python's `deliver_to_channel`. A keyed publication is looked up
    /// within the room first, and once more by the pipeline under the room
    /// lock, so a concurrent replay finds the first's key (RFC §22.1). The
    /// call waits for the turn its publication asks for, unless the request
    /// says not to; dropped meanwhile, it cancels that turn.
    pub(crate) async fn deliver_to_channel(
        &self,
        request: &Request,
        channel_id: ChannelId,
    ) -> Result<DeliveryOutcome, RoomKitError> {
        if let Some(published) = self.replay_publication(request).await? {
            return Ok(published);
        }
        let channel_id = match self.prepare_delivery(request, Some(channel_id)).await? {
            Prepared::Channel(channel_id) => channel_id,
            Prepared::Done(outcome) => return Ok(*outcome),
        };
        let (message, publishing) = publication(request, channel_id);
        let result = self
            .publish_delivery(message, &request.room_id, publishing)
            .await?;
        let result = match result.delivery.clone() {
            Some(delivery) if request.wait_for_turn => CancelOnDrop(Some(delivery)).wait().await?,
            Some(_) | None => result,
        };
        Ok(text_outcome(result))
    }
}

/// A delivery's turn the call that waits for it cancels when it is dropped
/// before the turn is over, as Python's `deliver_to_channel` cancels it on
/// the caller's cancellation.
struct CancelOnDrop(Option<DeliveryHandle>);

impl CancelOnDrop {
    async fn wait(mut self) -> Result<InboundResult, RoomKitError> {
        let waited = match &self.0 {
            Some(delivery) => delivery.wait().await,
            None => Ok(InboundResult::default()),
        };
        self.0 = None;
        waited
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(delivery) = self.0.take() {
            delivery.request_cancel();
        }
    }
}

fn done(outcome: DeliveryOutcome) -> Prepared {
    Prepared::Done(Box::new(outcome))
}

/// The message that publishes `request` into its room through
/// `channel_id`, from the framework's own sender, its key in the
/// publication namespace; and that stored key, which the pipeline admits
/// on it alone. An instruction keeps its key as given, which the pipeline
/// refuses (RFC §10.1.1).
fn publication(request: &Request, channel_id: ChannelId) -> (InboundMessage, Option<String>) {
    let stored = match &request.idempotency_key {
        Some(key) if !request.instruction => Some(publications::publication_key(key)),
        key => key.clone(),
    };
    let message = InboundMessage {
        event_type: if request.instruction {
            EventType::Instruction
        } else {
            EventType::Message
        },
        metadata: request.metadata.clone(),
        addressed_to: request.addressed_to.clone(),
        idempotency_key: stored.clone(),
        chain_depth: request.chain_depth,
        ..InboundMessage::new(
            channel_id,
            SYSTEM_SENDER_ID,
            TextContent::new(request.content.clone()),
        )
    };
    let publishing = stored.filter(|key| publications::is_reserved(key));
    (message, publishing)
}

/// What a text publication's inbound result reads as (RFC §22.1): sent,
/// blocked by the room or a hook, failed by its turn or its delivery,
/// unknown with no event, or unavailable for an addressed target it could
/// not reach. A replay identifies the original publication, whose turn's
/// completion it does not know.
pub(crate) fn text_outcome(result: InboundResult) -> DeliveryOutcome {
    let event_id = result.event.as_ref().map(|event| event.id.clone());
    let committed = event_id.is_some();
    let blocked = result.blocked
        || result
            .event
            .as_ref()
            .is_some_and(|event| event.status == EventStatus::Blocked);
    let turn_complete =
        !result.duplicate && result.delivery.as_ref().is_some_and(DeliveryHandle::done);
    let mut outcome = DeliveryOutcome {
        event_id,
        duplicate: result.duplicate,
        turn_complete,
        ..DeliveryOutcome::new(DeliveryOutcomeStatus::Sent)
    };
    if blocked {
        outcome.status = DeliveryOutcomeStatus::Blocked;
        outcome.reason = Some(
            result
                .reason
                .clone()
                .unwrap_or_else(|| "inbound_blocked".to_owned()),
        );
        return with_inbound(outcome, result);
    }
    if result.error.is_some() || result.cancellation_reason.is_some() {
        let (code, message) = match &result.error {
            Some(error) => (error.error_type.clone(), error.message.clone()),
            None => (
                "turn_cancelled".to_owned(),
                result.cancellation_reason.clone().unwrap_or_default(),
            ),
        };
        outcome.status = DeliveryOutcomeStatus::Failed;
        outcome.reason = Some(
            result
                .cancellation_reason
                .clone()
                .unwrap_or_else(|| "inbound_failed".to_owned()),
        );
        outcome.error = Some(DeliveryError {
            retryable: !committed,
            ..DeliveryError::new(code, message)
        });
        return with_inbound(outcome, result);
    }
    let failed = result
        .delivery_results
        .values()
        .find(|delivered| delivered.status == DeliveryResultStatus::Failed);
    if let Some(failed) = failed {
        let error = failed
            .error
            .clone()
            .unwrap_or_else(|| DeliveryError::new("delivery_failed", "Channel delivery failed"));
        outcome.status = DeliveryOutcomeStatus::Failed;
        outcome.reason = Some("channel_delivery_failed".to_owned());
        outcome.error = Some(DeliveryError {
            retryable: !committed,
            ..error
        });
        return with_inbound(outcome, result);
    }
    if !committed {
        outcome.status = DeliveryOutcomeStatus::Unknown;
        outcome.reason = Some("no_publication_result".to_owned());
        return with_inbound(outcome, result);
    }
    if result.duplicate {
        outcome.reason = Some("duplicate_publication".to_owned());
    }
    let unavailable = result
        .unavailable_targets
        .clone()
        .filter(|targets| !targets.is_empty());
    if let Some(targets) = unavailable {
        outcome.status = DeliveryOutcomeStatus::Unavailable;
        outcome.reason = Some("addressed_targets_unavailable".to_owned());
        outcome.unavailable_targets = targets.iter().map(ToString::to_string).collect();
        outcome.error = Some(DeliveryError {
            retryable: false,
            ..DeliveryError::new(
                "addressed_targets_unavailable",
                "The event was published but an addressed target was unavailable",
            )
        });
    }
    with_inbound(outcome, result)
}

fn with_inbound(outcome: DeliveryOutcome, result: InboundResult) -> DeliveryOutcome {
    DeliveryOutcome {
        inbound: Some(result),
        ..outcome
    }
}
