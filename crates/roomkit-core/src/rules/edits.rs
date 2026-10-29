//! Edit and delete (RFC §10.1 step 8a, §10.3, §5.3, §5.5): whether an event
//! may change the event it targets, and what the commit then writes on the
//! target.
//!
//! An event is an edit when its type is `EDIT` and its content an
//! [`EditContent`], a delete when its type is `DELETE` and its content a
//! [`DeleteContent`]. A type and a content that disagree name no target the
//! pipeline can check, and are refused as an event whose target is not
//! found ([`mutation`], gap G-062). So is an edit or a delete held anywhere
//! else: an edit's new content, or a composite's part (`holds_change`).
//!
//! Step 8a checks, in order (`validate`):
//!
//! 1. the target: an event of the same room, received (not `BLOCKED`), not
//!    deleted, and itself neither an edit nor a delete;
//! 2. who may make the change, by the authority it claims ([`Authority`]):
//!    - its author, for a `SENDER` delete and an edit whose `edit_source` is
//!      `sender` or null: the same participant on their channel (RFC §5.5),
//!      or the same sender id on the same channel (`author`);
//!    - an administrator, for an `ADMIN` delete: an OWNER or an AGENT, as
//!      the participant the sender is on their channel;
//!    - the system, for a `SYSTEM` delete and an edit whose `edit_source` is
//!      `system`: a SYSTEM channel, or a hook.
//!
//! An event a hook injected is the hook's (gap G-031): it may make a
//! `SYSTEM` change only, so a hook never edits or deletes in a
//! participant's name. Each failed check is one of the RFC's four reasons
//! ([`EditRefusal`]).
//!
//! The target is changed at the commit, never before (`target_updates`):
//! an edit writes its new content and `metadata.edited = true`, a delete
//! `metadata.deleted = true`. The change reaches no one who could not see
//! its target (`visibility_within`).

use serde_json::Value;

use crate::model::{
    ChannelCategory, ChannelId, ChannelType, DeleteContent, DeleteType, EditContent, EditSource,
    EventContent, EventDirection, EventId, EventStatus, EventType, EventUpdates, Participant,
    ParticipantId, ParticipantRole, ParticipantStatus, RoomEvent, Visibility,
};

/// The metadata key an accepted edit sets on its target (RFC §10.3).
pub(crate) const EDITED: &str = "edited";

/// The metadata key an accepted delete sets on its target (RFC §10.3).
pub const DELETED: &str = "deleted";

/// Why step 8a refuses an edit or a delete (RFC §10.1 step 8a). Nothing of
/// the event is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EditRefusal {
    /// The target is not an event of the room the change may target.
    TargetNotFound,
    /// The sender, or the target's author, cannot be told apart from anyone
    /// else: neither names a sender.
    IdentityRequired,
    /// The sender lacks the authority the change claims.
    NotAuthorized,
    /// The sender is not the target's author.
    NotOriginalAuthor,
}

impl EditRefusal {
    /// The reason the refusal reports (RFC §10.1 step 8a).
    pub fn reason(self) -> &'static str {
        match self {
            Self::TargetNotFound => "target_event_not_found",
            Self::IdentityRequired => "identity_required_for_edit",
            Self::NotAuthorized => "not_authorized",
            Self::NotOriginalAuthor => "not_original_author",
        }
    }
}

/// The change an edit or a delete makes to its target.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mutation<'event> {
    /// An edit: the target's content replaced.
    Edit(&'event EditContent),
    /// A delete: the target marked deleted.
    Delete(&'event DeleteContent),
}

impl Mutation<'_> {
    /// The event the change targets.
    pub fn target_event_id(&self) -> &EventId {
        match self {
            Self::Edit(edit) => &edit.target_event_id,
            Self::Delete(delete) => &delete.target_event_id,
        }
    }

    /// The authority the change claims (RFC §10.3 validation rules 2 to 4).
    /// An edit with no `edit_source` is the sender's (gap G-062).
    pub fn authority(&self) -> Authority {
        match self {
            Self::Edit(edit) => match edit.edit_source {
                Some(EditSource::Sender) | None => Authority::Author,
                Some(EditSource::System) => Authority::System,
            },
            Self::Delete(delete) => match delete.delete_type {
                DeleteType::Sender => Authority::Author,
                DeleteType::Admin => Authority::Admin,
                DeleteType::System => Authority::System,
            },
        }
    }
}

/// The authority an edit or a delete claims (RFC §10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Authority {
    /// The target's author.
    Author,
    /// A room administrator.
    Admin,
    /// The system: a SYSTEM channel, or a hook.
    System,
}

/// Who brought the event to the pass that checks it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Origin {
    /// A channel: an inbound message, or a channel's response.
    Channel,
    /// A hook, which injected the event (RFC §9.4).
    Hook,
}

/// Who wrote an event, as authorship is compared (RFC §10.3 validation rule
/// 2, §5.5).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Author {
    /// A participant of the room, named on a channel it was reached through.
    Participant(ParticipantId),
    /// A sender who names no participant: their id on their channel.
    Sender {
        /// The channel.
        channel_id: ChannelId,
        /// The sender's id there.
        sender_id: ParticipantId,
    },
    /// A channel's own output, an event that travelled outbound: an agent's
    /// response (gap G-062).
    Channel(ChannelId),
}

/// The edit or the delete `event` makes; `None` for any other event.
///
/// # Errors
///
/// Returns [`EditRefusal::TargetNotFound`] when the event's type and content
/// disagree: an `EDIT` or a `DELETE` without its content, or an edit's or a
/// delete's content under another type, names no target that can be
/// checked (gap G-062). So does an edit or a delete held deeper, in an
/// edit's new content or in a composite's part, which step 8a would never
/// check: the commit would write it on the target, or a channel would apply
/// it natively.
pub fn mutation(event: &RoomEvent) -> Result<Option<Mutation<'_>>, EditRefusal> {
    match (event.event_type, &event.content) {
        (EventType::Edit, EventContent::Edit(edit)) if !holds_change(&edit.new_content) => {
            Ok(Some(Mutation::Edit(edit)))
        }
        (EventType::Delete, EventContent::Delete(delete)) => Ok(Some(Mutation::Delete(delete))),
        (EventType::Edit | EventType::Delete, _) => Err(EditRefusal::TargetNotFound),
        (_, content) if holds_change(content) => Err(EditRefusal::TargetNotFound),
        _ => Ok(None),
    }
}

/// Whether `content` is, or holds, an edit or a delete: itself, a part of a
/// composite, or an edit's new content (gap G-062). Only the whole content
/// of an `EDIT` or a `DELETE` event may be one.
pub(crate) fn holds_change(content: &EventContent) -> bool {
    match content {
        EventContent::Edit(_) | EventContent::Delete(_) => true,
        EventContent::Composite(composite) => composite.parts().iter().any(holds_change),
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Media(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::ToolCall(_) => false,
    }
}

/// Whether `event`, making `mutation`, may change `target`, the event the
/// store holds under the targeted id (RFC §10.1 step 8a, §10.3).
///
/// The room's `participants` say who a sender is (RFC §5.5); `origin` says
/// whether a channel or a hook brought the event.
///
/// # Errors
///
/// Returns the first check that fails, as an [`EditRefusal`].
pub(crate) fn validate(
    event: &RoomEvent,
    mutation: &Mutation<'_>,
    target: Option<&RoomEvent>,
    origin: Origin,
    participants: &[Participant],
) -> Result<(), EditRefusal> {
    let target = check_target(event, target)?;
    match (mutation.authority(), origin) {
        (Authority::System, Origin::Hook) => Ok(()),
        (Authority::Author | Authority::Admin, Origin::Hook) => Err(EditRefusal::NotAuthorized),
        (Authority::System, Origin::Channel) => check_system_channel(event),
        (Authority::Admin, Origin::Channel) => check_administrator(event, participants),
        (Authority::Author, Origin::Channel) => check_author(event, target, participants),
    }
}

/// The target, when the change may target it: an event of the same room,
/// received, not deleted, and itself neither an edit nor a delete (gap
/// G-062).
fn check_target<'target>(
    event: &RoomEvent,
    target: Option<&'target RoomEvent>,
) -> Result<&'target RoomEvent, EditRefusal> {
    let target = target.ok_or(EditRefusal::TargetNotFound)?;
    let targetable = target.room_id == event.room_id
        && target.status != EventStatus::Blocked
        && !is_deleted(target)
        && !matches!(target.event_type, EventType::Edit | EventType::Delete);
    if targetable {
        Ok(target)
    } else {
        Err(EditRefusal::TargetNotFound)
    }
}

/// A `SYSTEM` change from a channel comes from a SYSTEM channel (RFC §10.3
/// validation rule 4).
fn check_system_channel(event: &RoomEvent) -> Result<(), EditRefusal> {
    if event.source.channel_type == ChannelType::SYSTEM {
        Ok(())
    } else {
        Err(EditRefusal::NotAuthorized)
    }
}

/// An `ADMIN` delete comes from an administrator: an OWNER or an AGENT, as
/// the participant the sender is on their channel, neither LEFT nor BANNED
/// (RFC §10.3 validation rule 3, gap G-062). A sender who names no one at
/// all cannot be told apart; a channel's own output holds no participant's
/// authority, whatever participant it names.
fn check_administrator(event: &RoomEvent, participants: &[Participant]) -> Result<(), EditRefusal> {
    let source = &event.source;
    if source.direction == EventDirection::Outbound {
        return Err(EditRefusal::NotAuthorized);
    }
    let Some(sender_id) = &source.participant_id else {
        return Err(EditRefusal::IdentityRequired);
    };
    let administrator =
        named_participant(sender_id, &source.channel_id, participants).is_some_and(|participant| {
            matches!(
                participant.role,
                ParticipantRole::Owner | ParticipantRole::Agent
            ) && !matches!(
                participant.status,
                ParticipantStatus::Left | ParticipantStatus::Banned
            )
        });
    if administrator {
        Ok(())
    } else {
        Err(EditRefusal::NotAuthorized)
    }
}

/// A `SENDER` change comes from the target's author (RFC §10.3 validation
/// rule 2): both must name a sender, and it must be the same.
fn check_author(
    event: &RoomEvent,
    target: &RoomEvent,
    participants: &[Participant],
) -> Result<(), EditRefusal> {
    let (Some(sender), Some(original)) =
        (author(event, participants), author(target, participants))
    else {
        return Err(EditRefusal::IdentityRequired);
    };
    if sender == original {
        Ok(())
    } else {
        Err(EditRefusal::NotOriginalAuthor)
    }
}

/// Who wrote `event`, in a room holding `participants` (RFC §10.3, §5.5):
///
/// - for a channel's own output (an event that travelled outbound, as an
///   agent's response does), the channel, whatever participant the channel
///   wrote on it: a channel never speaks as a participant (gap G-062);
/// - else the participant its sender id names, when the room reached that
///   participant through the event's channel (`connected_via`): an id one
///   channel gives says nothing of another's;
/// - else the sender id on its channel;
/// - else nobody: an inbound event that names no sender.
///
/// Ids compare as written: the pipeline writes a sender in its channel's
/// form before anything reads it (RFC §10.1 step 1).
pub(crate) fn author(event: &RoomEvent, participants: &[Participant]) -> Option<Author> {
    let source = &event.source;
    if source.direction == EventDirection::Outbound {
        return Some(Author::Channel(source.channel_id.clone()));
    }
    let sender_id = source.participant_id.as_ref()?;
    Some(
        match named_participant(sender_id, &source.channel_id, participants) {
            Some(participant) => Author::Participant(participant.id.clone()),
            None => Author::Sender {
                channel_id: source.channel_id.clone(),
                sender_id: sender_id.clone(),
            },
        },
    )
}

/// The participant `sender_id` names on `channel_id`: the room's participant
/// of that id, reached through the channel (RFC §5.5 "Who a sender is").
fn named_participant<'room>(
    sender_id: &ParticipantId,
    channel_id: &ChannelId,
    participants: &'room [Participant],
) -> Option<&'room Participant> {
    participants.iter().find(|participant| {
        &participant.id == sender_id && participant.connected_via.contains(channel_id)
    })
}

/// Whether `event` was deleted: an accepted delete set `metadata.deleted`
/// on it (RFC §10.3).
pub fn is_deleted(event: &RoomEvent) -> bool {
    event.metadata.get(DELETED) == Some(&Value::Bool(true))
}

/// Whether `event` was edited: an accepted edit set `metadata.edited` on it
/// (RFC §10.3). A host's direct update sets no such mark.
pub fn is_edited(event: &RoomEvent) -> bool {
    event.metadata.get(EDITED) == Some(&Value::Bool(true))
}

/// What the commit writes on `target` for an accepted `mutation` (RFC §10.3
/// state updates): an edit's new content and `metadata.edited = true`, a
/// delete's `metadata.deleted = true`. The target's other metadata is kept.
pub(crate) fn target_updates(mutation: &Mutation<'_>, target: &RoomEvent) -> EventUpdates {
    let mut metadata = target.metadata.clone();
    match mutation {
        Mutation::Edit(edit) => {
            metadata.insert(EDITED.to_owned(), Value::Bool(true));
            EventUpdates {
                content: Some((*edit.new_content).clone()),
                metadata: Some(metadata),
                ..EventUpdates::default()
            }
        }
        Mutation::Delete(_) => {
            metadata.insert(DELETED.to_owned(), Value::Bool(true));
            EventUpdates {
                metadata: Some(metadata),
                ..EventUpdates::default()
            }
        }
    }
}

/// The visibility an accepted edit or delete is committed with: its own,
/// narrowed to its target's, so that the change reaches no one who could
/// not see what it changes (gap G-062). A correction of a message an
/// advisor alone saw is never delivered to the customer.
pub(crate) fn visibility_within(
    visibility: &Visibility,
    target: &RoomEvent,
    category_of: impl Fn(&ChannelId) -> Option<ChannelCategory>,
) -> Visibility {
    visibility.intersect(&target.visibility, category_of)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::model::{EventSource, RoomId, TextContent, Timestamp};

    const ALICE: &str = "+15550000001";
    const BOB: &str = "+15550000002";

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn participant_id(id: &str) -> ParticipantId {
        ParticipantId::new(id).unwrap()
    }

    /// A message of `room_1` from `sender` on the WhatsApp channel `wa`.
    fn message_from(sender: Option<&str>) -> RoomEvent {
        let mut source = EventSource::new(
            channel("wa"),
            ChannelType::WHATSAPP,
            EventDirection::Inbound,
        );
        source.participant_id = sender.map(participant_id);
        let mut event = RoomEvent::new(
            EventId::new("evt_3").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            source,
            TextContent::new("I need 5000$").into(),
            Timestamp::UNIX_EPOCH,
        );
        event.status = EventStatus::Delivered;
        event
    }

    fn target() -> RoomEvent {
        message_from(Some(ALICE))
    }

    fn edit_content(source: Option<EditSource>) -> EventContent {
        EventContent::Edit(EditContent {
            target_event_id: EventId::new("evt_3").unwrap(),
            new_content: Box::new(TextContent::new("I need 50000$").into()),
            edit_source: source,
        })
    }

    fn delete_content(delete_type: DeleteType) -> EventContent {
        EventContent::Delete(DeleteContent {
            target_event_id: EventId::new("evt_3").unwrap(),
            delete_type,
            reason: None,
        })
    }

    /// An event of `event_type` with `content`, from `sender` on `wa`.
    fn change(event_type: EventType, content: EventContent, sender: Option<&str>) -> RoomEvent {
        let mut event = message_from(sender);
        event.id = EventId::new("evt_7").unwrap();
        event.event_type = event_type;
        event.content = content;
        event.status = EventStatus::Pending;
        event
    }

    fn edit_from(sender: &str) -> RoomEvent {
        change(
            EventType::Edit,
            edit_content(Some(EditSource::Sender)),
            Some(sender),
        )
    }

    fn delete_from(sender: &str, delete_type: DeleteType) -> RoomEvent {
        change(EventType::Delete, delete_content(delete_type), Some(sender))
    }

    fn checked(
        event: &RoomEvent,
        target: Option<&RoomEvent>,
        origin: Origin,
        participants: &[Participant],
    ) -> Result<(), EditRefusal> {
        let mutation = mutation(event).unwrap().unwrap();
        validate(event, &mutation, target, origin, participants)
    }

    fn by_channel(event: &RoomEvent) -> Result<(), EditRefusal> {
        checked(event, Some(&target()), Origin::Channel, &[])
    }

    fn member(id: &str, channel_id: &str, role: ParticipantRole) -> Participant {
        let mut participant = Participant::new(
            participant_id(id),
            RoomId::new("room_1").unwrap(),
            channel(channel_id),
            Timestamp::UNIX_EPOCH,
        );
        participant.role = role;
        participant
    }

    /// RFC §10.1 step 8a: the four reasons, as the RFC writes them.
    mod reasons {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn are_written_as_the_rfc_names_them() {
            assert_eq!(
                [
                    EditRefusal::TargetNotFound,
                    EditRefusal::IdentityRequired,
                    EditRefusal::NotAuthorized,
                    EditRefusal::NotOriginalAuthor
                ]
                .map(EditRefusal::reason),
                [
                    "target_event_not_found",
                    "identity_required_for_edit",
                    "not_authorized",
                    "not_original_author"
                ]
            );
        }
    }

    /// Gap G-062: an edit or a delete is told by its type and its content
    /// together.
    mod the_mutation {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn of_an_edit_targets_its_content_s_event() {
            let edit = edit_from(ALICE);
            let found = mutation(&edit)
                .unwrap()
                .map(|found| found.target_event_id().clone());
            assert_eq!(found, Some(EventId::new("evt_3").unwrap()));
        }

        #[test]
        fn of_a_message_is_none() {
            assert_eq!(mutation(&target()), Ok(None));
        }

        #[test]
        fn of_an_edit_type_without_its_content_names_no_target() {
            let bare = change(
                EventType::Edit,
                TextContent::new("Oops").into(),
                Some(ALICE),
            );
            assert_eq!(mutation(&bare), Err(EditRefusal::TargetNotFound));
        }

        #[test]
        fn of_an_edit_content_under_another_type_names_no_target() {
            let smuggled = change(EventType::Message, edit_content(None), Some(ALICE));
            assert_eq!(mutation(&smuggled), Err(EditRefusal::TargetNotFound));
        }

        #[test]
        fn of_a_delete_content_under_the_edit_type_names_no_target() {
            let crossed = change(
                EventType::Edit,
                delete_content(DeleteType::Sender),
                Some(ALICE),
            );
            assert_eq!(mutation(&crossed), Err(EditRefusal::TargetNotFound));
        }

        #[test]
        fn of_an_edit_whose_new_content_is_a_delete_names_no_target() {
            let nested = change(
                EventType::Edit,
                EventContent::Edit(EditContent {
                    target_event_id: EventId::new("evt_3").unwrap(),
                    new_content: Box::new(delete_content(DeleteType::Sender)),
                    edit_source: Some(EditSource::Sender),
                }),
                Some(ALICE),
            );
            assert_eq!(mutation(&nested), Err(EditRefusal::TargetNotFound));
        }

        #[test]
        fn of_a_message_whose_composite_holds_an_edit_names_no_target() {
            let parts = vec![TextContent::new("Hi").into(), edit_content(None)];
            let smuggled = change(
                EventType::Message,
                EventContent::Composite(crate::model::CompositeContent::new(parts).unwrap()),
                Some(ALICE),
            );
            assert_eq!(mutation(&smuggled), Err(EditRefusal::TargetNotFound));
        }

        #[test]
        fn of_a_message_whose_composite_holds_no_change_is_none() {
            let parts = vec![
                TextContent::new("Hi").into(),
                TextContent::new("there").into(),
            ];
            let composite = change(
                EventType::Message,
                EventContent::Composite(crate::model::CompositeContent::new(parts).unwrap()),
                Some(ALICE),
            );
            assert_eq!(mutation(&composite), Ok(None));
        }

        /// Gap G-062: a null `edit_source` is the sender's.
        #[test]
        fn of_an_edit_with_no_source_claims_the_author_s_authority() {
            let edit = change(EventType::Edit, edit_content(None), Some(ALICE));
            assert_eq!(
                mutation(&edit).unwrap().map(|found| found.authority()),
                Some(Authority::Author)
            );
        }

        #[test]
        fn of_a_system_edit_claims_the_system_s_authority() {
            let edit = change(
                EventType::Edit,
                edit_content(Some(EditSource::System)),
                None,
            );
            assert_eq!(
                mutation(&edit).unwrap().map(|found| found.authority()),
                Some(Authority::System)
            );
        }

        #[test]
        fn of_each_delete_type_claims_its_authority() {
            let claimed = [DeleteType::Sender, DeleteType::Admin, DeleteType::System].map(|kind| {
                let delete = delete_from(ALICE, kind);
                mutation(&delete).unwrap().map(|found| found.authority())
            });
            assert_eq!(
                claimed,
                [
                    Some(Authority::Author),
                    Some(Authority::Admin),
                    Some(Authority::System)
                ]
            );
        }
    }

    /// RFC §10.3 validation rule 1: the target is an event of the room.
    mod the_target {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn that_exists_in_the_room_is_found() {
            assert_eq!(by_channel(&edit_from(ALICE)), Ok(()));
        }

        #[test]
        fn that_does_not_exist_is_not_found() {
            assert_eq!(
                checked(&edit_from(ALICE), None, Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }

        #[test]
        fn of_another_room_is_not_found() {
            let mut elsewhere = target();
            elsewhere.room_id = RoomId::new("room_2").unwrap();
            assert_eq!(
                checked(&edit_from(ALICE), Some(&elsewhere), Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }

        /// Gap G-062: a refused event was never received.
        #[test]
        fn stored_blocked_is_not_found() {
            let mut refused = target();
            refused.status = EventStatus::Blocked;
            assert_eq!(
                checked(&edit_from(ALICE), Some(&refused), Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }

        #[test]
        fn deleted_already_is_not_found() {
            let mut deleted = target();
            deleted.metadata.insert(DELETED.to_owned(), json!(true));
            assert_eq!(
                checked(&edit_from(ALICE), Some(&deleted), Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }

        #[test]
        fn edited_already_is_found() {
            let mut edited = target();
            edited.metadata.insert(EDITED.to_owned(), json!(true));
            assert_eq!(
                checked(&edit_from(ALICE), Some(&edited), Origin::Channel, &[]),
                Ok(())
            );
        }

        #[test]
        fn that_is_itself_an_edit_is_not_found() {
            let earlier = edit_from(ALICE);
            assert_eq!(
                checked(&edit_from(ALICE), Some(&earlier), Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }

        #[test]
        fn that_is_itself_a_delete_is_not_found() {
            let earlier = delete_from(ALICE, DeleteType::Sender);
            assert_eq!(
                checked(
                    &delete_from(ALICE, DeleteType::Sender),
                    Some(&earlier),
                    Origin::Channel,
                    &[]
                ),
                Err(EditRefusal::TargetNotFound)
            );
        }

        /// The target is checked before who may change it.
        #[test]
        fn not_found_is_reported_before_the_authorship() {
            assert_eq!(
                checked(&edit_from(BOB), None, Origin::Channel, &[]),
                Err(EditRefusal::TargetNotFound)
            );
        }
    }

    /// RFC §10.3 validation rule 2: a sender's change comes from the
    /// target's author.
    mod the_author {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn the_same_sender_on_the_same_channel_may_edit() {
            assert_eq!(by_channel(&edit_from(ALICE)), Ok(()));
        }

        #[test]
        fn the_same_sender_on_the_same_channel_may_delete() {
            assert_eq!(by_channel(&delete_from(ALICE, DeleteType::Sender)), Ok(()));
        }

        #[test]
        fn another_sender_is_not_the_original_author() {
            assert_eq!(
                by_channel(&edit_from(BOB)),
                Err(EditRefusal::NotOriginalAuthor)
            );
        }

        #[test]
        fn another_sender_may_not_delete_as_the_sender() {
            assert_eq!(
                by_channel(&delete_from(BOB, DeleteType::Sender)),
                Err(EditRefusal::NotOriginalAuthor)
            );
        }

        /// RFC §5.5: an id one channel gives says nothing of another's.
        #[test]
        fn the_same_id_on_another_channel_is_not_the_original_author() {
            let mut edit = edit_from(ALICE);
            edit.source.channel_id = channel("sms");
            assert_eq!(by_channel(&edit), Err(EditRefusal::NotOriginalAuthor));
        }

        /// RFC §5.5: one participant reached through two channels is one
        /// author.
        #[test]
        fn the_same_participant_on_another_of_their_channels_may_edit() {
            let mut jean = member(ALICE, "wa", ParticipantRole::Member);
            jean.connected_via.push(channel("sms"));
            let mut edit = edit_from(ALICE);
            edit.source.channel_id = channel("sms");
            assert_eq!(
                checked(&edit, Some(&target()), Origin::Channel, &[jean]),
                Ok(())
            );
        }

        #[test]
        fn a_sender_naming_no_one_needs_an_identity() {
            let edit = change(
                EventType::Edit,
                edit_content(Some(EditSource::Sender)),
                None,
            );
            assert_eq!(by_channel(&edit), Err(EditRefusal::IdentityRequired));
        }

        #[test]
        fn a_target_naming_no_sender_needs_an_identity() {
            let anonymous = message_from(None);
            assert_eq!(
                checked(&edit_from(ALICE), Some(&anonymous), Origin::Channel, &[]),
                Err(EditRefusal::IdentityRequired)
            );
        }

        /// Gap G-062: a null `edit_source` is checked as the sender's.
        #[test]
        fn an_edit_with_no_source_from_another_sender_is_not_the_original_author() {
            let edit = change(EventType::Edit, edit_content(None), Some(BOB));
            assert_eq!(by_channel(&edit), Err(EditRefusal::NotOriginalAuthor));
        }

        /// Gap G-062: an agent's response is its channel's own.
        #[test]
        fn an_agent_may_edit_its_own_response() {
            let mut response = target();
            response.source =
                EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            let mut edit = edit_from(ALICE);
            edit.source = response.source.clone();
            assert_eq!(
                checked(&edit, Some(&response), Origin::Channel, &[]),
                Ok(())
            );
        }

        /// Gap G-062: a channel never speaks as a participant, whatever
        /// participant it writes on its output.
        #[test]
        fn an_agent_naming_the_author_on_its_response_is_not_the_original_author() {
            let mut jean = member(ALICE, "wa", ParticipantRole::Member);
            jean.connected_via.push(channel("ai"));
            let mut edit = edit_from(ALICE);
            edit.source =
                EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            edit.source.participant_id = Some(participant_id(ALICE));
            assert_eq!(
                checked(&edit, Some(&target()), Origin::Channel, &[jean]),
                Err(EditRefusal::NotOriginalAuthor)
            );
        }

        #[test]
        fn an_agent_may_not_edit_another_channel_s_message() {
            let mut edit = edit_from(ALICE);
            edit.source =
                EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            assert_eq!(by_channel(&edit), Err(EditRefusal::NotOriginalAuthor));
        }

        #[test]
        fn a_sender_may_not_edit_an_agent_s_response() {
            let mut response = target();
            response.source =
                EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            assert_eq!(
                checked(&edit_from(ALICE), Some(&response), Origin::Channel, &[]),
                Err(EditRefusal::NotOriginalAuthor)
            );
        }
    }

    /// RFC §10.3 validation rule 3, gap G-062: an administrator's delete.
    mod an_administrator {
        use super::*;
        use pretty_assertions::assert_eq;

        fn admin_delete(participants: &[Participant]) -> Result<(), EditRefusal> {
            checked(
                &delete_from(BOB, DeleteType::Admin),
                Some(&target()),
                Origin::Channel,
                participants,
            )
        }

        #[test]
        fn an_owner_on_their_channel_may_delete_anyone_s_message() {
            assert_eq!(
                admin_delete(&[member(BOB, "wa", ParticipantRole::Owner)]),
                Ok(())
            );
        }

        #[test]
        fn an_agent_on_their_channel_may_delete_anyone_s_message() {
            assert_eq!(
                admin_delete(&[member(BOB, "wa", ParticipantRole::Agent)]),
                Ok(())
            );
        }

        #[test]
        fn a_member_is_not_authorized() {
            assert_eq!(
                admin_delete(&[member(BOB, "wa", ParticipantRole::Member)]),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn a_bot_is_not_authorized() {
            assert_eq!(
                admin_delete(&[member(BOB, "wa", ParticipantRole::Bot)]),
                Err(EditRefusal::NotAuthorized)
            );
        }

        /// RFC §10.3: "an id that names an owner on another channel grants
        /// none".
        #[test]
        fn an_owner_on_another_channel_grants_no_authority() {
            assert_eq!(
                admin_delete(&[member(BOB, "sms", ParticipantRole::Owner)]),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn a_sender_who_is_no_participant_is_not_authorized() {
            assert_eq!(admin_delete(&[]), Err(EditRefusal::NotAuthorized));
        }

        #[test]
        fn an_owner_who_left_is_not_authorized() {
            let mut left = member(BOB, "wa", ParticipantRole::Owner);
            left.status = ParticipantStatus::Left;
            assert_eq!(admin_delete(&[left]), Err(EditRefusal::NotAuthorized));
        }

        #[test]
        fn an_agent_who_left_is_not_authorized() {
            let mut left = member(BOB, "wa", ParticipantRole::Agent);
            left.status = ParticipantStatus::Left;
            assert_eq!(admin_delete(&[left]), Err(EditRefusal::NotAuthorized));
        }

        /// Gap G-062: a channel's output holds no participant's authority.
        #[test]
        fn a_channel_naming_an_agent_on_its_output_is_not_authorized() {
            let mut marie = member(BOB, "ai", ParticipantRole::Agent);
            marie.connected_via.push(channel("wa"));
            let mut delete = delete_from(BOB, DeleteType::Admin);
            delete.source =
                EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            delete.source.participant_id = Some(participant_id(BOB));
            assert_eq!(
                checked(&delete, Some(&target()), Origin::Channel, &[marie]),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn a_banned_owner_is_not_authorized() {
            let mut banned = member(BOB, "wa", ParticipantRole::Owner);
            banned.status = ParticipantStatus::Banned;
            assert_eq!(admin_delete(&[banned]), Err(EditRefusal::NotAuthorized));
        }

        #[test]
        fn a_sender_naming_no_one_needs_an_identity() {
            let delete = change(EventType::Delete, delete_content(DeleteType::Admin), None);
            assert_eq!(by_channel(&delete), Err(EditRefusal::IdentityRequired));
        }
    }

    /// RFC §10.3 validation rule 4: a system change comes from a SYSTEM
    /// channel or a hook.
    mod the_system {
        use super::*;
        use pretty_assertions::assert_eq;

        fn from_the_system_channel(content: EventContent, event_type: EventType) -> RoomEvent {
            let mut event = change(event_type, content, None);
            event.source = EventSource::new(
                ChannelId::system(),
                ChannelType::SYSTEM,
                EventDirection::Inbound,
            );
            event
        }

        #[test]
        fn a_system_channel_may_delete() {
            let delete =
                from_the_system_channel(delete_content(DeleteType::System), EventType::Delete);
            assert_eq!(by_channel(&delete), Ok(()));
        }

        #[test]
        fn a_system_channel_may_edit() {
            let edit =
                from_the_system_channel(edit_content(Some(EditSource::System)), EventType::Edit);
            assert_eq!(by_channel(&edit), Ok(()));
        }

        #[test]
        fn another_channel_is_not_authorized_to_delete_as_the_system() {
            assert_eq!(
                by_channel(&delete_from(ALICE, DeleteType::System)),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn another_channel_is_not_authorized_to_edit_as_the_system() {
            let edit = change(
                EventType::Edit,
                edit_content(Some(EditSource::System)),
                Some(ALICE),
            );
            assert_eq!(by_channel(&edit), Err(EditRefusal::NotAuthorized));
        }
    }

    /// Gap G-031: an injected event is the hook's.
    mod a_hook {
        use super::*;
        use pretty_assertions::assert_eq;

        fn injected(event: &RoomEvent) -> Result<(), EditRefusal> {
            checked(event, Some(&target()), Origin::Hook, &[])
        }

        #[test]
        fn may_delete_as_the_system() {
            assert_eq!(injected(&delete_from(ALICE, DeleteType::System)), Ok(()));
        }

        #[test]
        fn may_edit_as_the_system() {
            let edit = change(
                EventType::Edit,
                edit_content(Some(EditSource::System)),
                None,
            );
            assert_eq!(injected(&edit), Ok(()));
        }

        #[test]
        fn is_not_authorized_to_edit_in_the_author_s_name() {
            assert_eq!(injected(&edit_from(ALICE)), Err(EditRefusal::NotAuthorized));
        }

        #[test]
        fn is_not_authorized_to_delete_in_the_author_s_name() {
            assert_eq!(
                injected(&delete_from(ALICE, DeleteType::Sender)),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn is_not_authorized_to_delete_as_an_administrator() {
            assert_eq!(
                checked(
                    &delete_from(BOB, DeleteType::Admin),
                    Some(&target()),
                    Origin::Hook,
                    &[member(BOB, "wa", ParticipantRole::Owner)]
                ),
                Err(EditRefusal::NotAuthorized)
            );
        }

        #[test]
        fn still_needs_its_target() {
            assert_eq!(
                checked(
                    &delete_from(ALICE, DeleteType::System),
                    None,
                    Origin::Hook,
                    &[]
                ),
                Err(EditRefusal::TargetNotFound)
            );
        }
    }

    /// Gap G-062: a change reaches no one who could not see its target.
    mod the_visibility {
        use super::*;
        use pretty_assertions::assert_eq;

        fn within(visibility: &str, target_visibility: &str) -> String {
            let mut restricted = target();
            restricted.visibility = target_visibility.parse().unwrap();
            visibility_within(&visibility.parse().unwrap(), &restricted, |_| {
                Some(ChannelCategory::Transport)
            })
            .to_string()
        }

        #[test]
        fn of_a_change_to_a_message_all_saw_is_its_own() {
            assert_eq!(within("ws_advisor,sms", "all"), "sms,ws_advisor");
        }

        #[test]
        fn of_a_change_to_a_message_one_channel_saw_is_that_channel() {
            assert_eq!(within("all", "ws_advisor"), "ws_advisor");
        }

        #[test]
        fn of_a_change_to_a_message_no_channel_saw_is_none() {
            assert_eq!(within("all", "none"), "none");
        }
    }

    /// RFC §10.3 state updates: what the commit writes on the target.
    mod the_target_s_update {
        use super::*;
        use pretty_assertions::assert_eq;

        fn applied(event: &RoomEvent) -> RoomEvent {
            let mut updated = target();
            updated.metadata.insert("crm".to_owned(), json!("c_1"));
            let update = target_updates(&mutation(event).unwrap().unwrap(), &updated);
            update.apply(&mut updated);
            updated
        }

        #[test]
        fn of_an_edit_replaces_the_content_and_marks_it_edited() {
            let edited = applied(&edit_from(ALICE));
            assert!(is_edited(&edited) && !is_deleted(&edited));
            assert_eq!(
                (edited.content, Value::Object(edited.metadata)),
                (
                    TextContent::new("I need 50000$").into(),
                    json!({ "crm": "c_1", "edited": true })
                )
            );
        }

        #[test]
        fn of_a_delete_keeps_the_content_and_marks_it_deleted() {
            let deleted = applied(&delete_from(ALICE, DeleteType::Sender));
            assert_eq!(
                (
                    is_deleted(&deleted),
                    deleted.content,
                    Value::Object(deleted.metadata)
                ),
                (
                    true,
                    TextContent::new("I need 5000$").into(),
                    json!({ "crm": "c_1", "deleted": true })
                )
            );
        }
    }
}
