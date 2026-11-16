//! What each member call changes of the room's record of a participant,
//! decided on the room read under its lock (RFC §5.5), as the Python
//! implementation's `ensure_participant` and `resolve_participant`
//! (`room_lifecycle.py`), `add_member`, `remove_member` and `rename_member`
//! (`membership.py`) write it.

use std::collections::VecDeque;

use serde_json::{Value, json};

use super::MemberError;
use crate::model::{
    ChannelBinding, ChannelId, ErrorRecord, EventType, IdentificationStatus, Identity, IdentityId,
    Participant, ParticipantId, ParticipantRole, ParticipantStatus, RoomContext,
};
use crate::ops::Cx;
use crate::ops::lifecycle::change::{Decision, SectionChange, write_records};
use crate::ops::lifecycle::report::Reported;
use crate::ops::protocol::{ChannelRequest, Outcome, Request, StoreRequest};
use crate::rules::addresses::{AddressForm, Held};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::records::Record;
use crate::rules::routing::own_record_id;

/// A change of a room's members.
#[derive(Clone, Debug)]
pub enum MemberChange {
    /// Python's `ensure_participant`: the participant, created reached
    /// through the channel, or the room's record of that id with the
    /// channel recorded, never re-homed. Bookkeeping: no record, no hook.
    Ensure {
        /// The channel a record created is primarily reached on.
        channel_id: ChannelId,
        /// The participant.
        participant_id: ParticipantId,
        /// The name of a record created.
        display_name: Option<String>,
    },
    /// Python's `resolve_participant`: the participant identified as the
    /// identity the store holds, named on the binding of its channel unless
    /// that binding is someone else's conversation.
    Resolve {
        /// The participant.
        participant_id: ParticipantId,
        /// The identity named.
        identity_id: IdentityId,
        /// The identity as the store holds it, read before the lock; `None`
        /// when it holds none.
        identity: Option<Box<Identity>>,
        /// Who resolved it.
        resolved_by: String,
        /// The organization the call acts for, whose identities only it
        /// reaches (RFC §17.2).
        organization_id: Option<String>,
        /// Who the binding of its channel delivers to, as the channel
        /// answers under the lock; [`RecipientAnswer::Unasked`] when built.
        recipient: RecipientAnswer,
    },
    /// Python's `add_member`: a deliberate join.
    Add {
        /// The channel joined through.
        channel_id: ChannelId,
        /// The member, in the channel's form.
        participant_id: ParticipantId,
        /// The identity it is, which makes it IDENTIFIED.
        identity_id: Option<IdentityId>,
        /// Its display name.
        display_name: Option<String>,
        /// Its role.
        role: ParticipantRole,
    },
    /// Python's `remove_member`: a soft status flip.
    Remove {
        /// The participant.
        participant_id: ParticipantId,
        /// LEFT, or BANNED.
        status: ParticipantStatus,
    },
    /// Python's `rename_member`: the display name, nothing else.
    Rename {
        /// The participant.
        participant_id: ParticipantId,
        /// The new name.
        display_name: String,
    },
}

/// What a member change reports once the room's lock is released: its
/// trigger, and its payload's type, code, body and data, as Python's
/// `_fire_lifecycle_hook` is called with them.
/// What the channel answered, under the room's lock, of who the binding a
/// resolution would name the identity on delivers to (Python's
/// `routed_recipient`, RFC §5.7): a binding naming no participant and not
/// declared `group` is its recipient's conversation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RecipientAnswer {
    /// Not asked yet.
    #[default]
    Unasked,
    /// The channel's answer, in its form; `None` when it reads none, or
    /// the driver no longer holds it.
    Answered(Option<String>),
}

#[derive(Clone, Debug)]
pub(super) struct Report {
    pub(super) trigger: HookTrigger,
    pub(super) event_type: EventType,
    pub(super) code: &'static str,
    pub(super) body: &'static str,
    pub(super) data: Value,
}

/// What a member change did: the participant as it left it, what it
/// reports, and the room as it left it.
#[derive(Debug)]
pub struct Membered {
    pub(super) participant: Participant,
    pub(super) report: Option<Report>,
    pub(super) context: Box<RoomContext>,
}

impl MemberChange {
    /// The participant the change names.
    pub(super) fn participant_id(&self) -> &ParticipantId {
        match self {
            Self::Ensure { participant_id, .. }
            | Self::Resolve { participant_id, .. }
            | Self::Add { participant_id, .. }
            | Self::Remove { participant_id, .. }
            | Self::Rename { participant_id, .. } => participant_id,
        }
    }
}

impl SectionChange for MemberChange {
    type Done = Membered;
    type Refusal = MemberError;

    fn decide(
        &self,
        context: Box<RoomContext>,
        cx: &Cx<'_>,
    ) -> Result<Decision<Membered>, MemberError> {
        let mut writes = VecDeque::new();
        let mut members = Members {
            context,
            writes: &mut writes,
        };
        let (participant, report) = match self {
            Self::Ensure {
                channel_id,
                participant_id,
                display_name,
            } => members.ensure(cx, channel_id, participant_id, display_name.as_ref()),
            Self::Resolve {
                participant_id,
                identity_id,
                identity,
                resolved_by,
                organization_id,
                recipient,
            } => {
                // The participant first, then the identity, as Python's.
                members.found(participant_id)?;
                let identity = identity
                    .as_deref()
                    .filter(|identity| reaches(organization_id.as_deref(), identity))
                    .ok_or_else(|| MemberError::IdentityNotFound(identity_id.clone()))?;
                members.resolve(cx, participant_id, identity, resolved_by, recipient)?
            }
            Self::Add {
                channel_id,
                participant_id,
                identity_id,
                display_name,
                role,
            } => members.add(
                cx,
                channel_id,
                participant_id,
                identity_id.as_ref(),
                display_name.as_ref(),
                *role,
            ),
            Self::Remove {
                participant_id,
                status,
            } => members.remove(cx, participant_id, *status)?,
            Self::Rename {
                participant_id,
                display_name,
            } => members.rename(cx, participant_id, display_name)?,
        };
        let context = members.context;
        Ok(Decision {
            writes,
            done: Membered {
                participant,
                report,
                context,
            },
            telling: None,
        })
    }

    /// A member change reports through its own observers (`ChangeMember`).
    fn reported(_done: &Membered) -> Option<(Reported<'_>, &RoomContext)> {
        None
    }

    /// The Python implementation applies no due timer before it writes.
    fn applies_timers(&self) -> bool {
        false
    }

    /// A resolution asks the channel, once, who the binding it would name
    /// the identity on delivers to, when that binding names no participant
    /// and is not declared `group`: its recipient, as routing reads it
    /// (RFC §5.5, §5.7). Not for a participant or an identity the change
    /// refuses.
    fn next_read(&self, context: &RoomContext) -> Option<Request> {
        let binding = self.binding_to_read(context)?;
        Some(
            ChannelRequest::RoutedRecipient {
                binding: binding.clone(),
            }
            .into(),
        )
    }

    fn take_read(&mut self, context: &RoomContext, outcome: Outcome) -> Result<(), ErrorRecord> {
        if self.binding_to_read(context).is_none() {
            return Ok(());
        }
        if let Self::Resolve { recipient, .. } = self {
            *recipient = RecipientAnswer::Answered(outcome.into_recipient()?);
        }
        Ok(())
    }
}

impl MemberChange {
    /// The binding whose recipient a resolution still reads under the lock
    /// ([`SectionChange::next_read`]).
    fn binding_to_read<'c>(&self, context: &'c RoomContext) -> Option<&'c ChannelBinding> {
        let Self::Resolve {
            participant_id,
            identity,
            organization_id,
            recipient: RecipientAnswer::Unasked,
            ..
        } = self
        else {
            return None;
        };
        identity
            .as_deref()
            .filter(|identity| reaches(organization_id.as_deref(), identity))?;
        let participant = context
            .participants
            .iter()
            .find(|participant| participant.id == *participant_id)?;
        context
            .get_binding(&participant.channel_id)
            .filter(|binding| !binding.group && binding.participant_id.is_none())
    }
}

/// Whether `binding` is the conversation of someone other than
/// `participant`, resolved as `identity_id` (RFC §5.5, §5.7), the Python
/// implementation's `delivers_to_another_than`: it names another
/// correspondent, the framework's own name included, or names none and
/// routing reads its recipient, in `form`, as another address, or as a value
/// that is not a recipient. On a channel not registered (`form` is `None`)
/// nothing reads that recipient: a binding naming no one is read as
/// another's then. The participant
/// is known by their id, the identity they had and the one they get, and the
/// address whose own record they are. A `group` binding names no
/// correspondent, and one delivering to no one is no one's: neither is
/// another's.
fn delivers_to_another(
    binding: &ChannelBinding,
    form: Option<&AddressForm>,
    participant: &Participant,
    identity_id: &IdentityId,
) -> bool {
    if binding.group {
        return false;
    }
    let named = match (&binding.participant_id, form.map(|form| form.held(binding))) {
        (Some(named), _) => named.as_str().to_owned(),
        // No channel to read the recipient, or a value no one is reached
        // at: someone else's.
        (None, None | Some(Held::NotARecipient(_))) => return true,
        (None, Some(Held::Nothing)) => return false,
        (None, Some(Held::Recipient(recipient))) => recipient,
    };
    let theirs = named == participant.id.as_str()
        || named == identity_id.as_str()
        || participant
            .identity_id
            .as_ref()
            .is_some_and(|had| named == had.as_str());
    !theirs && own_record_id(binding.channel_id.as_str(), &named) != participant.id.as_str()
}

/// Whether a caller acting for `organization_id` reaches `identity`: every
/// identity when it names none (RFC §17.2).
fn reaches(organization_id: Option<&str>, identity: &Identity) -> bool {
    organization_id
        .is_none_or(|organization| identity.organization_id.as_deref() == Some(organization))
}

/// What `add_member` joins a participant with.
struct Joining<'a> {
    channel_id: &'a ChannelId,
    identity_id: Option<&'a IdentityId>,
    display_name: Option<&'a String>,
    role: ParticipantRole,
}

impl Joining<'_> {
    /// IDENTIFIED with an identity, PENDING without one, as Python's
    /// `add_member` sets it on a new member.
    fn identification(&self) -> IdentificationStatus {
        if self.identity_id.is_some() {
            IdentificationStatus::Identified
        } else {
            IdentificationStatus::Pending
        }
    }
}

/// `name` unless it is empty: an empty display name is none, as the Python
/// implementation's `or` reads it.
fn named(name: Option<&String>) -> Option<&String> {
    name.filter(|name| !name.is_empty())
}

/// The room's participants under the lock, and the writes the change makes.
struct Members<'w> {
    context: Box<RoomContext>,
    writes: &'w mut VecDeque<StoreRequest>,
}

type Changed = (Participant, Option<Report>);

impl Members<'_> {
    fn find(&self, participant_id: &ParticipantId) -> Option<Participant> {
        self.context
            .participants
            .iter()
            .find(|participant| participant.id == *participant_id)
            .cloned()
    }

    fn found(&self, participant_id: &ParticipantId) -> Result<Participant, MemberError> {
        self.find(participant_id)
            .ok_or_else(|| MemberError::ParticipantNotFound {
                room_id: self.context.room.id.clone(),
                participant_id: participant_id.clone(),
            })
    }

    /// Writes `participant` over the room's record of it.
    fn update(&mut self, participant: &Participant) {
        if let Some(record) = self
            .context
            .participants
            .iter_mut()
            .find(|record| record.id == participant.id)
        {
            record.clone_from(participant);
        }
        self.writes.push_back(StoreRequest::UpdateParticipant {
            participant: Box::new(participant.clone()),
        });
    }

    /// Adds `participant` to the room.
    fn add_record(&mut self, participant: &Participant) {
        self.context.participants.push(participant.clone());
        self.writes.push_back(StoreRequest::AddParticipant {
            participant: Box::new(participant.clone()),
        });
    }

    fn record(&mut self, cx: &Cx<'_>, record: &Record) {
        write_records(
            cx,
            self.writes,
            &mut self.context,
            std::slice::from_ref(record),
        );
    }

    fn ensure(
        &mut self,
        cx: &Cx<'_>,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        display_name: Option<&String>,
    ) -> Changed {
        if let Some(mut existing) = self.find(participant_id) {
            super::warn_cross_channel(&existing, channel_id, false);
            if let Some(channels) = super::channels_reached(&existing, channel_id) {
                existing.connected_via = channels;
                self.update(&existing);
            }
            return (existing, None);
        }
        let mut participant = Participant::at(
            participant_id.clone(),
            self.context.room.id.clone(),
            channel_id.clone(),
            cx.env.now(),
        );
        participant.display_name = display_name.cloned();
        self.add_record(&participant);
        (participant, None)
    }

    fn resolve(
        &mut self,
        cx: &Cx<'_>,
        participant_id: &ParticipantId,
        identity: &Identity,
        resolved_by: &str,
        recipient: &RecipientAnswer,
    ) -> Result<Changed, MemberError> {
        let mut participant = self.found(participant_id)?;
        let pending = participant.clone();
        participant.identification = IdentificationStatus::Identified;
        participant.identity_id = Some(identity.id.clone());
        participant.resolved_at = Some(cx.env.now());
        participant.resolved_by = Some(resolved_by.to_owned());
        participant.candidates = None;
        if let Some(name) = named(identity.display_name.as_ref()) {
            participant.display_name = Some(name.clone());
        }
        self.update(&participant);
        self.name_on_binding(cx, &pending, &identity.id, recipient);
        self.record(
            cx,
            &Record::ParticipantIdentified {
                participant_id: participant_id.clone(),
                display_name: identity.display_name.clone(),
                identity_id: identity.id.clone(),
                resolved_by: resolved_by.to_owned(),
            },
        );
        let report = Report {
            trigger: HookTrigger::OnParticipantIdentified,
            event_type: EventType::ParticipantIdentified,
            code: "participant_identified",
            body: "Participant identified",
            data: json!({ "participant_id": participant_id, "identity_id": identity.id }),
        };
        Ok((participant, Some(report)))
    }

    /// The binding of `participant`'s primary channel names `identity_id`
    /// from now on, when the room has one, unless it is someone else's
    /// conversation (RFC §5.5): the advisor identifies a person, and does
    /// not choose whom the room answers. `participant` is the record as it
    /// was before the resolution, `recipient` the channel's answer of who
    /// the binding delivers to.
    fn name_on_binding(
        &mut self,
        cx: &Cx<'_>,
        participant: &Participant,
        identity_id: &IdentityId,
        recipient: &RecipientAnswer,
    ) {
        let Ok(named) = ParticipantId::new(identity_id.as_str()) else {
            return;
        };
        let Some(binding) = self
            .context
            .bindings
            .iter_mut()
            .find(|binding| binding.channel_id == participant.channel_id)
        else {
            return;
        };
        let form = cx.config.channel(&binding.channel_id).map(|channel| {
            let mut form = channel.form();
            if let RecipientAnswer::Answered(answered) = recipient {
                form.answered_recipient.clone_from(answered);
            }
            form
        });
        if delivers_to_another(binding, form.as_ref(), participant, identity_id) {
            return;
        }
        binding.participant_id = Some(named);
        self.writes.push_back(StoreRequest::StoreBinding {
            binding: binding.clone(),
        });
    }

    fn add(
        &mut self,
        cx: &Cx<'_>,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        display_name: Option<&String>,
        role: ParticipantRole,
    ) -> Changed {
        let existing = self.find(participant_id);
        if let Some(mut active) = existing
            .clone()
            .filter(|participant| participant.status == ParticipantStatus::Active)
        {
            super::warn_cross_channel(&active, channel_id, false);
            if let Some(channels) = super::channels_reached(&active, channel_id) {
                active.connected_via = channels;
                self.update(&active);
            }
            return (active, None);
        }
        let joining = Joining {
            channel_id,
            identity_id,
            display_name,
            role,
        };
        let participant = match existing {
            Some(rejoined) => self.rejoin(rejoined, &joining),
            None => self.join(cx, participant_id, &joining),
        };
        self.record(
            cx,
            &Record::MemberJoined {
                participant_id: participant_id.clone(),
                identity_id: identity_id.cloned(),
            },
        );
        let report = Report {
            trigger: HookTrigger::OnParticipantJoined,
            event_type: EventType::ParticipantJoined,
            code: "participant_joined",
            body: "Participant joined",
            data: json!({ "participant_id": participant_id, "identity_id": identity_id }),
        };
        (participant, Some(report))
    }

    /// A participant who is not ACTIVE joins again through `joining`'s
    /// channel, which becomes their primary one, the one it replaces kept
    /// (RFC §5.5); `joined_at` is kept, and so are the identification and
    /// the identity of a re-join that names none.
    fn rejoin(&mut self, mut rejoined: Participant, joining: &Joining<'_>) -> Participant {
        let channel_id = joining.channel_id;
        super::warn_cross_channel(&rejoined, channel_id, true);
        let reached = super::channels_reached(&rejoined, channel_id)
            .unwrap_or_else(|| rejoined.connected_via.clone());
        let mut channels = vec![channel_id.clone()];
        channels.extend(reached.into_iter().filter(|reached| reached != channel_id));
        rejoined.status = ParticipantStatus::Active;
        rejoined.channel_id = channel_id.clone();
        rejoined.connected_via = channels;
        rejoined.role = joining.role;
        // A re-join without an identity keeps the identification the record
        // had, which follows the identity it keeps.
        if let Some(identity_id) = joining.identity_id {
            rejoined.identity_id = Some(identity_id.clone());
            rejoined.identification = IdentificationStatus::Identified;
        }
        if let Some(name) = named(joining.display_name) {
            rejoined.display_name = Some(name.clone());
        }
        self.update(&rejoined);
        rejoined
    }

    /// A participant new to the room joins through `joining`'s channel.
    fn join(
        &mut self,
        cx: &Cx<'_>,
        participant_id: &ParticipantId,
        joining: &Joining<'_>,
    ) -> Participant {
        let mut joined = Participant::at(
            participant_id.clone(),
            self.context.room.id.clone(),
            joining.channel_id.clone(),
            cx.env.now(),
        );
        joined.display_name = joining.display_name.cloned();
        joined.role = joining.role;
        joined.identification = joining.identification();
        joined.identity_id = joining.identity_id.cloned();
        self.add_record(&joined);
        joined
    }

    fn remove(
        &mut self,
        cx: &Cx<'_>,
        participant_id: &ParticipantId,
        status: ParticipantStatus,
    ) -> Result<Changed, MemberError> {
        let mut participant = self.found(participant_id)?;
        participant.status = status;
        self.update(&participant);
        self.record(
            cx,
            &Record::MemberLeft {
                participant_id: participant_id.clone(),
                status,
            },
        );
        let report = Report {
            trigger: HookTrigger::OnParticipantLeft,
            event_type: EventType::ParticipantLeft,
            code: "participant_left",
            body: "Participant left",
            data: json!({ "participant_id": participant_id, "status": status }),
        };
        Ok((participant, Some(report)))
    }

    fn rename(
        &mut self,
        cx: &Cx<'_>,
        participant_id: &ParticipantId,
        display_name: &str,
    ) -> Result<Changed, MemberError> {
        let mut participant = self.found(participant_id)?;
        if participant.display_name.as_deref() == Some(display_name) {
            return Ok((participant, None));
        }
        participant.display_name = Some(display_name.to_owned());
        self.update(&participant);
        self.record(
            cx,
            &Record::MemberRenamed {
                participant_id: participant_id.clone(),
                display_name: display_name.to_owned(),
            },
        );
        let report = Report {
            trigger: HookTrigger::OnParticipantUpdated,
            event_type: EventType::ParticipantUpdated,
            code: "participant_updated",
            body: "Participant renamed",
            data: json!({ "participant_id": participant_id, "display_name": display_name }),
        };
        Ok((participant, Some(report)))
    }
}
