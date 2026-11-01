//! A room's members (RFC §5.5, Appendix B.3), as the Python implementation
//! changes them: under the room's lock, no due timer applied first, the
//! participant written, the record of the change committed (none for a
//! change that is bookkeeping), and its trigger's hooks fired once the lock
//! is released, their payload the system event of the change.

mod change;

use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;

pub use self::change::{MemberChange, Membered};
use super::io::Io;
use super::lifecycle::locked::{LockedSection, SectionError};
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use super::{Cx, Environment, KitConfig, Progress};
use crate::model::{
    ChannelId, ChannelType, ErrorRecord, EventContent, EventSource, EventStatus, IdentityId,
    Metadata, Participant, ParticipantId, RoomEvent, RoomId, SystemContent, Visibility,
};

/// Why a member call failed. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MemberError {
    /// The room does not exist, or belongs to another organization than
    /// the one the caller acts for (RFC §17.2).
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The room has no participant of that id, Python's
    /// `ParticipantNotFoundError`.
    #[error("Participant {participant_id} not found in room {room_id}")]
    ParticipantNotFound {
        /// The room.
        room_id: RoomId,
        /// The participant named.
        participant_id: ParticipantId,
    },
    /// The store holds no such identity, or another organization's,
    /// Python's `IdentityNotFoundError`.
    #[error("Identity {0} not found")]
    IdentityNotFound(IdentityId),
    /// The store refused or failed the change.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken.
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
}

/// One change of a room's members ([`MemberChange`]) under the room's
/// lock. Its output is the participant as the change left it.
#[derive(Debug)]
pub struct ChangeMember {
    config: Arc<KitConfig>,
    io: Io,
    section: LockedSection<MemberChange>,
    /// The participant a call that reads no room names (Python's
    /// `remove_member`, `rename_member` and `resolve_participant` naming no
    /// organization): a room missing then is that participant not found.
    unscoped: Option<ParticipantId>,
    result: Option<Result<Participant, MemberError>>,
}

impl ChangeMember {
    /// `change` of `room_id`, for a caller acting for `organization_id`: a
    /// room of another organization, or of none, is not found, nothing
    /// written (RFC §17.2); `None` reads the room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        change: MemberChange,
    ) -> Self {
        let reads_no_room = organization_id.is_none()
            && matches!(
                change,
                MemberChange::Remove { .. }
                    | MemberChange::Rename { .. }
                    | MemberChange::Resolve { .. }
            );
        let unscoped = reads_no_room.then(|| change.participant_id().clone());
        Self {
            config,
            io: Io::new(),
            section: LockedSection::changing(room_id, organization_id, change),
            unscoped,
            result: None,
        }
    }
}

impl Operation for ChangeMember {
    type Output = Result<Participant, MemberError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let Self {
            config,
            io,
            section,
            unscoped,
            result,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        section.start(&mut cx);
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            if let Progress::Done(ended) = section.accept(&mut cx, effect, outcome) {
                *result = Some(ended_with(&mut cx, unscoped.as_ref(), ended.result));
            }
        }
        let ready = result.take();
        self.io.step(ready)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}

/// The participant the change left, its trigger's hooks fired now that the
/// lock is released, with the room as the change left it; or why it failed.
fn ended_with(
    cx: &mut Cx<'_>,
    unscoped: Option<&ParticipantId>,
    ended: Result<Membered, SectionError<MemberError>>,
) -> Result<Participant, MemberError> {
    match ended {
        Ok(Membered {
            participant,
            report,
            context,
        }) => {
            if let Some(report) = report {
                let payload = payload(cx, &context.room.id, &report);
                cx.fire_observers(report.trigger, &payload, &context);
            }
            Ok(participant)
        }
        Err(SectionError::RoomNotFound(room_id)) => Err(match unscoped {
            Some(participant_id) => MemberError::ParticipantNotFound {
                room_id,
                participant_id: participant_id.clone(),
            },
            None => MemberError::RoomNotFound(room_id),
        }),
        Err(SectionError::Store(error)) => Err(MemberError::Store(error)),
        Err(SectionError::Lock(error)) => Err(MemberError::Lock(error)),
        Err(SectionError::Refused(refusal)) => Err(refusal),
    }
}

/// The payload of a member change's hooks, as Python's
/// `_fire_lifecycle_hook` writes it: an event of the change's type, from
/// the framework's `system` source, `DELIVERED` and `INTERNAL`, whose
/// system content's code names the change and whose data holds what it
/// changed (RFC §5.1, §9.2).
fn payload(cx: &mut Cx<'_>, room_id: &RoomId, report: &change::Report) -> RoomEvent {
    let data = if let Value::Object(members) = &report.data {
        members.clone()
    } else {
        Metadata::new()
    };
    let content = SystemContent {
        body: report.body.to_owned(),
        code: Some(report.code.to_owned()),
        data,
    };
    let source = EventSource::new(ChannelId::system(), ChannelType::SYSTEM);
    let mut event = RoomEvent::at(
        cx.env.new_event_id(),
        room_id.clone(),
        report.event_type,
        source,
        EventContent::System(content),
        cx.env.now(),
    );
    event.status = EventStatus::Delivered;
    event.visibility = Visibility::Internal;
    event
}

/// The `connected_via` to store now that `channel_id` reached `existing`
/// (RFC §5.5): the primary channel first, the rest in their order of first
/// sight; `None` when the record lists them all already. Python's
/// `channels_reached`.
pub(crate) fn channels_reached(
    existing: &Participant,
    channel_id: &ChannelId,
) -> Option<Vec<ChannelId>> {
    let mut channels = vec![existing.channel_id.clone()];
    for reached in existing
        .connected_via
        .iter()
        .chain(std::iter::once(channel_id))
    {
        if !channels.contains(reached) {
            channels.push(reached.clone());
        }
    }
    (channels != existing.connected_via).then_some(channels)
}

/// Says that `channel_id` was handed a record homed on another channel
/// (RFC §5.5: logged at warning level, naming both channels), as Python's
/// `warn_cross_channel`: nothing when the caller named the record's own
/// channel. `rehomed` is a deliberate join's, which moves the primary
/// channel to the one joined through.
pub(crate) fn warn_cross_channel(existing: &Participant, channel_id: &ChannelId, rehomed: bool) {
    if *channel_id == existing.channel_id {
        return;
    }
    if rehomed {
        tracing::warn!(
            target: "roomkit::core::members",
            room_id = %existing.room_id,
            "Participant {} of room {} is joining through channel '{}'; the primary channel of \
             their record was '{}' and becomes '{}' — one record still, and '{}' stays in \
             connected_via (RFC 5.5).",
            existing.id,
            existing.room_id,
            channel_id,
            existing.channel_id,
            channel_id,
            existing.channel_id,
        );
        return;
    }
    tracing::warn!(
        target: "roomkit::core::members",
        room_id = %existing.room_id,
        "Participant {} of room {} is recorded on channel '{}'; '{}' asked for them and gets \
         that record as it stands, primary channel included (RFC 5.5). Reaching one \
         participant on several channels is the point — but a lifecycle or a status kept on \
         this record from '{}' moves it for '{}' too.",
        existing.id,
        existing.room_id,
        existing.channel_id,
        channel_id,
        channel_id,
        existing.channel_id,
    );
}
