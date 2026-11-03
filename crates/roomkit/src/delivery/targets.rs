//! Where a proactive delivery goes and what it came to (RFC §22.1), as the
//! Python implementation's `_delivery_targets` resolves and reports it: the
//! transport that carries a text delivery into its room, its publication
//! through the inbound pipeline, and the outcome its inbound result reads
//! as. Rust has no realtime voice session: a delivery is always a text
//! publication.

use std::time::Duration;

use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelType, DeliveryError, DeliveryResultStatus, EventStatus,
    EventType, InboundMessage, RoomId, TextContent,
};
use roomkit_core::ops::FrameworkEvent as CoreEvent;
use roomkit_core::rules::publications;

use super::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use super::strategy::DeliveryContext;
use crate::context::ExecutionContext;
use crate::driver;
use crate::error::RoomKitError;
use crate::inbound::{DeliveryHandle, InboundResult};
use crate::kit::RoomKit;
use crate::locks::HeldLock;
use crate::store::ConversationStore;

/// The reason a call `process_timeout` stopped is refused with (RFC §13.6).
const PROCESS_TIMEOUT: &str = "process_timeout";

/// The framework's own sender, who writes a delivery into its room: the host
/// speaking, never a correspondent of the room's channel (RFC §10.4).
pub(crate) const SYSTEM_SENDER_ID: &str = "system";

impl<S> DeliveryContext<S> {
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
        request: &DeliveryContext<S>,
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
        request: &DeliveryContext<S>,
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
    /// to: Python's `deliver_to_channel`. A keyed publication looks its key
    /// up within the room, checks its transport and commits in one section
    /// under the room lock, so a concurrent replay finds the first's key
    /// and a detach seen there writes nothing (RFC §22.1). The call waits
    /// for the turn its publication asks for, once the lock is released,
    /// unless the request says not to; dropped meanwhile, it cancels that
    /// turn.
    pub(crate) async fn deliver_to_channel(
        &self,
        request: &DeliveryContext<S>,
        channel_id: ChannelId,
    ) -> Result<DeliveryOutcome, RoomKitError> {
        let section = match self.publication_section(request).await? {
            Ok(section) => section,
            Err(refused) => return Ok(*refused),
        };
        if let Some(published) = self.replay_publication(request).await? {
            return Ok(published);
        }
        let channel_id = match self.prepare_delivery(request, Some(channel_id)).await? {
            Prepared::Channel(channel_id) => channel_id,
            Prepared::Done(outcome) => return Ok(*outcome),
        };
        let (message, publishing) = publication(request, channel_id);
        let result = self
            .publish_within(section, message, &request.room_id, publishing)
            .await?;
        let (result, waited) = match result.delivery.clone() {
            Some(delivery) if request.wait_for_turn => {
                (CancelOnDrop(Some(delivery)).wait().await?, true)
            }
            Some(_) | None => (result, false),
        };
        let outcome = text_outcome(result);
        // A turn the call did not wait for is not complete at its
        // decision, as the Python implementation reads `done` then, with
        // nothing run in between; here the detached turn may have ended
        // meanwhile.
        Ok(DeliveryOutcome {
            turn_complete: waited && outcome.turn_complete,
            ..outcome
        })
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// The room lock a keyed publication's section holds (RFC §22.1),
    /// waited for within `process_timeout`, as the pipeline's own wait is
    /// bounded (RFC §13.6), as Python's `deliver_to_channel` takes it,
    /// whatever work of the room its caller runs in: `None` for any other
    /// request, and for a caller that holds the lock already, a locked
    /// hook, whose section adopts the publication. Past the timeout, the
    /// refusal; the lock manager's failure, the call's error.
    async fn publication_section(
        &self,
        request: &DeliveryContext<S>,
    ) -> Result<Result<Option<HeldLock>, Box<DeliveryOutcome>>, RoomKitError> {
        let room_id = &request.room_id;
        if !request.keyed_publication() || ExecutionContext::current().lease(room_id).is_some() {
            return Ok(Ok(None));
        }
        let locking = self.inner.locks.lock(room_id);
        let Some(timeout) = self.inner.config().process_timeout else {
            return Ok(Ok(Some(locking.await?)));
        };
        match tokio::time::timeout(timeout, locking).await {
            Ok(guard) => Ok(Ok(Some(guard?))),
            Err(_elapsed) => Ok(Err(Box::new(text_outcome(
                self.refuse_on_timeout(room_id, timeout).await,
            )))),
        }
    }

    /// Refuses what `process_timeout` stopped before the room's lock, with
    /// the `process_timeout` framework event (RFC §8.2, §13.6): Python's
    /// `_refuse_on_timeout(room_id)`.
    async fn refuse_on_timeout(&self, room_id: &RoomId, timeout: Duration) -> InboundResult {
        tracing::error!(target: "roomkit::delivery", room_id = %room_id, timeout = timeout.as_secs_f64(), "a keyed publication timed out waiting for the room lock");
        let notice =
            CoreEvent::process_timeout(Some(room_id.clone()), None, None, timeout.as_secs_f64());
        if let Some(handled) = self.inner.emit(notice) {
            handled.wait().await;
        }
        InboundResult {
            blocked: true,
            reason: Some(PROCESS_TIMEOUT.to_owned()),
            ..InboundResult::default()
        }
    }

    /// Publishes `message` into `room_id`, handing `section`, the room's
    /// lock the delivery holds, when it holds it, to the run of its
    /// pipeline, which holds it from its start: the call is nested under it
    /// (`Inbound::under_lock`), and its commit is the section's (RFC
    /// §22.1). A call refused before any run takes the lock, the kit closed
    /// among others, leaves it here, released once the call returns.
    async fn publish_within(
        &self,
        section: Option<HeldLock>,
        message: InboundMessage,
        room_id: &RoomId,
        publishing: Option<String>,
    ) -> Result<InboundResult, RoomKitError> {
        let Some(guard) = section else {
            return self.publish_delivery(message, room_id, publishing).await;
        };
        let (context, hand) = ExecutionContext::current().handing(room_id.clone(), guard);
        let published = context
            .scope(self.publish_delivery(message, room_id, publishing))
            .await;
        drop(hand.take_back());
        published
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
            delivery.cancel_unwaited(driver::CALLER_CANCELLED);
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
fn publication<S>(
    request: &DeliveryContext<S>,
    channel_id: ChannelId,
) -> (InboundMessage, Option<String>) {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use roomkit_core::model::{ChannelType, RoomId};

    use super::*;
    use crate::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
    use crate::{AttachChannelOptions, CreateRoomOptions, DeliverOptions, RoomKitOptions};

    // Python: tests/test_proactive_delivery.py::TestAKeyNamesOnePublicationInItsRoom::test_a_room_lock_held_past_process_timeout_refuses_the_publication
    #[tokio::test]
    async fn a_room_lock_held_past_process_timeout_refuses_the_publication() {
        let kit = RoomKit::new(RoomKitOptions {
            process_timeout: Duration::from_millis(50),
            ..RoomKitOptions::default()
        });
        let (room, text) = (RoomId::new("r1").unwrap(), ChannelId::new("text").unwrap());
        let transport = RecordingTransport::new(
            text.clone(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport).unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        kit.attach_channel(&room, &text, AttachChannelOptions::default())
            .await
            .unwrap();
        let tap = EventTap::on(&kit, &["process_timeout"]);
        let held = kit.inner.locks.lock(&room).await;
        let keyed = DeliverOptions {
            idempotency_key: Some("k".to_owned()),
            ..DeliverOptions::default()
        };
        let outcome = kit.deliver(&room, "done", keyed).await.unwrap();
        drop(held);
        assert_eq!(outcome.status, DeliveryOutcomeStatus::Blocked);
        assert_eq!(outcome.reason.as_deref(), Some("process_timeout"));
        crate::testing::idle(&kit).await;
        let reported: Vec<_> = tap
            .events()
            .into_iter()
            .map(|event| (event.room_id, event.data["timeout"].as_f64()))
            .collect();
        assert_eq!(reported, [(Some(room.clone()), Some(0.05))]);
        let options = crate::store::ListEventsOptions::default();
        let stored = kit.store().list_events(&room, options).await.unwrap();
        let published = stored
            .iter()
            .filter(|event| event.source.participant_id.is_some());
        assert_eq!(published.count(), 0);
        kit.close().await.unwrap();
    }

    // A keyed publication waiting for its section's lock when the kit closes
    // is refused once it holds the lock, as Python's process_inbound raises
    // KitClosedError, which its strategy reports as strategy_failed: nothing
    // published under its key.
    #[tokio::test]
    async fn a_keyed_publication_waiting_for_its_section_as_the_kit_closes_is_refused() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let (room, text) = (RoomId::new("r1").unwrap(), ChannelId::new("text").unwrap());
        let transport = RecordingTransport::new(
            text.clone(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport).unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        kit.attach_channel(&room, &text, AttachChannelOptions::default())
            .await
            .unwrap();
        let held = kit.inner.locks.lock(&room).await;
        let delivering = tokio::spawn({
            let (kit, room) = (kit.clone(), room.clone());
            async move {
                let keyed = DeliverOptions {
                    idempotency_key: Some("k".to_owned()),
                    ..DeliverOptions::default()
                };
                kit.deliver(&room, "done", keyed).await
            }
        });
        while kit.inner.locks.waiting(&room) == 0 {
            tokio::task::yield_now().await;
        }
        kit.close().await.unwrap();
        drop(held);
        let refused = tokio::time::timeout(Duration::from_secs(5), delivering)
            .await
            .expect("the delivery ends once it holds the lock")
            .unwrap()
            .unwrap();
        assert_eq!(refused.status, DeliveryOutcomeStatus::Failed);
        assert_eq!(refused.reason.as_deref(), Some("strategy_failed"));
        let error = refused.error.expect("the refusal's error");
        assert_eq!(error.code, "KitClosedError");
        let options = crate::store::ListEventsOptions::default();
        let stored = kit.store().list_events(&room, options).await.unwrap();
        let published = stored
            .iter()
            .filter(|event| event.source.participant_id.is_some());
        assert_eq!(published.count(), 0);
    }
}
