//! A room's members (RFC §5.5, Appendix B.3): the host's get-or-create of a
//! participant, an advisor's resolution of a pending one, and the deliberate
//! join, leave and rename, as the Python implementation's
//! `ensure_participant`, `resolve_participant` (`room_lifecycle.py`),
//! `add_member`, `remove_member`, `rename_member` and `is_member`
//! (`membership.py`).

use roomkit_core::model::{
    ChannelId, IdentityId, Participant, ParticipantId, ParticipantRole, ParticipantStatus, RoomId,
};
use roomkit_core::ops::{ChangeMember, MemberChange};

use super::RoomKit;
use crate::driver;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// What [`RoomKit::ensure_participant`] takes beside the room, the channel
/// and the participant, Python's `ensure_participant(room_id, channel_id,
/// participant_id, display_name=None, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnsureParticipantOptions {
    /// The display name of a record created; a record the room has keeps
    /// its own.
    pub display_name: Option<String>,
    /// The organization the call acts for (RFC §17.2).
    pub organization_id: Option<String>,
}

/// What [`RoomKit::resolve_participant`] takes beside the room, the
/// participant and the identity, Python's `resolve_participant(room_id,
/// participant_id, identity_id, resolved_by="manual", *,
/// organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveParticipantOptions {
    /// Who resolved the participant: `"manual"` by default.
    pub resolved_by: String,
    /// The organization the call acts for, whose rooms and identities only
    /// it reaches (RFC §17.2).
    pub organization_id: Option<String>,
}

impl Default for ResolveParticipantOptions {
    fn default() -> Self {
        Self {
            resolved_by: "manual".to_owned(),
            organization_id: None,
        }
    }
}

/// What [`RoomKit::add_member`] takes beside the room, the channel and the
/// member, Python's `add_member(room_id, channel_id, participant_id, *,
/// identity_id=None, display_name=None, role=ParticipantRole.MEMBER,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddMemberOptions {
    /// The identity the member is, which makes them IDENTIFIED; PENDING
    /// without one.
    pub identity_id: Option<IdentityId>,
    /// The member's display name.
    pub display_name: Option<String>,
    /// The member's role: MEMBER by default.
    pub role: ParticipantRole,
    /// The organization the call acts for (RFC §17.2).
    pub organization_id: Option<String>,
}

/// What [`RoomKit::remove_member`] takes beside the room and the
/// participant, Python's `remove_member(room_id, participant_id, *,
/// status=ParticipantStatus.LEFT, organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveMemberOptions {
    /// The status the participant is left with: LEFT by default, BANNED to
    /// ban them.
    pub status: ParticipantStatus,
    /// The organization the call acts for (RFC §17.2).
    pub organization_id: Option<String>,
}

impl Default for RemoveMemberOptions {
    fn default() -> Self {
        Self {
            status: ParticipantStatus::Left,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::rename_member`] takes beside the room, the participant
/// and the name, Python's `rename_member(room_id, participant_id,
/// display_name, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenameMemberOptions {
    /// The organization the call acts for (RFC §17.2).
    pub organization_id: Option<String>,
}

/// What [`RoomKit::is_member`] takes beside the room and the identity,
/// Python's `is_member(room_id, identity_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IsMemberOptions {
    /// The organization the call acts for (RFC §17.2).
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The room's participant `participant_id`, created reached through
    /// `channel_id`, or the room's record of that id with the channel
    /// recorded among those it was reached through, never re-homed, a
    /// channel other than its primary one logged at warning level (RFC
    /// §5.5). Bookkeeping: no record of a join, no hook.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the call does not
    /// reach; the store's error when it fails.
    pub async fn ensure_participant(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        options: EnsureParticipantOptions,
    ) -> Result<Participant, RoomKitError> {
        let change = MemberChange::Ensure {
            channel_id: channel_id.clone(),
            participant_id: participant_id.clone(),
            display_name: options.display_name,
        };
        self.change_member(
            room_id,
            options.organization_id,
            change,
            "ensure_participant",
        )
        .await
    }

    /// Resolves the pending participant `participant_id` as the identity
    /// `identity_id` the store holds (RFC §5.5, Appendix B.3): IDENTIFIED,
    /// its candidates dropped, the identity's display name taken when it has
    /// one, the binding of its primary channel naming the identity, the
    /// record of the change committed and `ON_PARTICIPANT_IDENTIFIED` fired.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::ParticipantNotFound`] for a participant the
    /// room does not have, [`RoomKitError::IdentityNotFound`] for an
    /// identity the store does not hold or another organization's, and
    /// [`RoomKitError::RoomNotFound`] for a room of another organization;
    /// the store's failure to read the identity, a record the model cannot
    /// read among them (`RoomKitError::Value`), once the room and the
    /// participant are found; nothing is written.
    pub async fn resolve_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
        identity_id: &IdentityId,
        options: ResolveParticipantOptions,
    ) -> Result<Participant, RoomKitError> {
        // Read before the lock, its failure told only once the room and the
        // participant are found, as Python reads the identity after them: a
        // caller of another organization, or naming a participant the room
        // lacks, learns nothing of an identity whose record cannot be read
        // (HITL 70fef50d).
        let (identity, unread) = match self.store().get_identity(identity_id).await {
            Ok(identity) => (identity, None),
            Err(error) => (None, Some(error)),
        };
        let change = MemberChange::Resolve {
            participant_id: participant_id.clone(),
            identity_id: identity_id.clone(),
            identity: identity.map(Box::new),
            resolved_by: options.resolved_by,
            organization_id: options.organization_id.clone(),
        };
        let resolved = self
            .change_member(
                room_id,
                options.organization_id,
                change,
                "resolve_participant",
            )
            .await;
        match (resolved, unread) {
            (Err(RoomKitError::IdentityNotFound(_)), Some(error)) => Err(self.read_failed(error)),
            (resolved, _) => resolved,
        }
    }

    /// Adds `participant_id`, written in the channel's form
    /// (`normalize_address`), as a member of the room reached through
    /// `channel_id` (RFC §5.5): a deliberate join. An ACTIVE member is left
    /// as they are, a channel newly reached recorded; anyone else joins
    /// ACTIVE, a re-join keeping `joined_at` and moving the primary channel
    /// to the one joined through, with the record of the join and
    /// `ON_PARTICIPANT_JOINED`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room that does not exist
    /// or that the call does not reach; nothing is written.
    pub async fn add_member(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        options: AddMemberOptions,
    ) -> Result<Participant, RoomKitError> {
        let participant_id = match self.inner.channel(channel_id) {
            Some(channel) => {
                let written = channel.normalize_address(participant_id.as_str());
                ParticipantId::new(written).unwrap_or_else(|_| participant_id.clone())
            }
            None => participant_id.clone(),
        };
        let change = MemberChange::Add {
            channel_id: channel_id.clone(),
            participant_id,
            identity_id: options.identity_id,
            display_name: options.display_name,
            role: options.role,
        };
        self.change_member(room_id, options.organization_id, change, "add_member")
            .await
    }

    /// Sets the participant's status, LEFT by default, keeping the record
    /// (RFC §5.5), with the record of the leave and `ON_PARTICIPANT_LEFT`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::ParticipantNotFound`] for a participant the
    /// room does not have, [`RoomKitError::RoomNotFound`] for a room the
    /// organization the call names does not reach; nothing is written.
    pub async fn remove_member(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
        options: RemoveMemberOptions,
    ) -> Result<Participant, RoomKitError> {
        let change = MemberChange::Remove {
            participant_id: participant_id.clone(),
            status: options.status,
        };
        self.change_member(room_id, options.organization_id, change, "remove_member")
            .await
    }

    /// Changes what a member is called, never who they are (RFC §5.5): the
    /// display name in place, with the record of the change and
    /// `ON_PARTICIPANT_UPDATED`; a rename to the name held writes nothing.
    ///
    /// # Errors
    ///
    /// As [`remove_member`](Self::remove_member).
    pub async fn rename_member(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
        display_name: &str,
        options: RenameMemberOptions,
    ) -> Result<Participant, RoomKitError> {
        let change = MemberChange::Rename {
            participant_id: participant_id.clone(),
            display_name: display_name.to_owned(),
        };
        self.change_member(room_id, options.organization_id, change, "rename_member")
            .await
    }

    /// Whether an ACTIVE participant of the room is the identity
    /// `identity_id` (RFC §5.5). The room is checked only for a call that
    /// names an organization.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the organization
    /// does not reach; the store's error when it fails.
    pub async fn is_member(
        &self,
        room_id: &RoomId,
        identity_id: &IdentityId,
        options: IsMemberOptions,
    ) -> Result<bool, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let participants = self
            .store()
            .list_participants(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        Ok(participants.iter().any(|participant| {
            participant.identity_id.as_ref() == Some(identity_id)
                && participant.status == ParticipantStatus::Active
        }))
    }

    /// Runs `change` of the room's members under the room's lock.
    async fn change_member(
        &self,
        room_id: &RoomId,
        organization_id: Option<String>,
        change: MemberChange,
        name: &'static str,
    ) -> Result<Participant, RoomKitError> {
        let operation = ChangeMember::new(self.config(), room_id.clone(), organization_id, change);
        Ok(driver::run(&self.inner, name, operation).await??)
    }
}
