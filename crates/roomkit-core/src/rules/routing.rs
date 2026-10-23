//! Inbound room routing (RFC §10.4): the default strategy, and what step 2
//! records of a routed message's sender (RFC §10.1 step 2, §5.7).
//!
//! A message that names no room is routed by the address it came from: its
//! sender's, or on a chat channel the chat it was posted in (RFC §5.7, gap
//! G-057). The sender is known by that address, compared in the channel's
//! form, and by the identity the store resolves it to (RFC §5.6, §14.1). The
//! default strategy reads the rooms the channel reaches, each a
//! [`Candidate`], and returns:
//!
//! - step 1: the newest ACTIVE or PAUSED room whose binding of the channel
//!   names the sender, else the newest where a participant the sender is
//!   was reached through the channel ([`conversation`]): a conversation its
//!   timer paused is the sender's still, and their message resumes it (gap
//!   G-045);
//! - step 3: else the one room bound to the channel, ACTIVE or PAUSED with
//!   its binding declared `group` (gap G-045), when it admits the sender
//!   ([`admits`]): its binding is declared `group`, or it names no one
//!   else, delivers to no one else, and the room has neither a member nor
//!   a message of anyone else on the channel;
//! - step 4: else none: the framework creates a room.
//!
//! The strategy never guesses: two rooms bound to the channel give none at
//! step 3 (RFC §10.4 "A router MUST NOT guess"). Its answer depends on the
//! rooms read, never on the order they were read in: [`newest`] tells rooms
//! created at one instant apart by their id.
//!
//! Once a room is chosen, step 2 records the sender on its binding of the
//! channel, under the room lock ([`recording`]). A binding found naming
//! another sender by then sends the message to be routed once more (gap
//! G-055), and so does a room found PAUSED that neither is the sender's
//! conversation by step 1 nor binds the channel as a group, or found CLOSED
//! or ARCHIVED: the lock tells it by [`kept_by_status`], which applies
//! [`paused_room_takes`], the tests of the strategy's steps 1 and 3.

use std::cmp::Ordering;

use serde_json::Value;

use super::addresses::{AddressForm, Addressing, SYSTEM_SENDER};
use super::organizations;
use crate::model::{
    BindingUpdate, ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventStatus,
    IdentityId, Metadata, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId,
    RoomStatus,
};

/// Who a routed message is from, as routing names them (RFC §10.4).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sender {
    /// The address the message is routed by, in the channel's form: the
    /// sender's, or the chat's on a chat channel ([`routing_address`]).
    /// `None` for a message with no address, and for the framework's own
    /// sender `system`.
    pub address: Option<String>,
    /// The identity the store resolves the address to (RFC §14.1
    /// `resolve_identity`), in the caller's organization.
    pub identity_id: Option<IdentityId>,
}

impl Sender {
    /// Whether `id`, an address or an id written on the channel, names this
    /// sender: it is their address once in the channel's form, or their
    /// identity's id (RFC §10.4).
    pub fn is(&self, id: &str, form: &AddressForm) -> bool {
        let by_address = self
            .address
            .as_deref()
            .is_some_and(|address| form.normalize(id) == address);
        let by_identity = self
            .identity_id
            .as_ref()
            .is_some_and(|identity| identity.as_str() == id);
        by_address || by_identity
    }

    /// Whether `participant` is this sender: its id names them, or it was
    /// resolved to their identity (RFC §10.4).
    pub fn is_participant(&self, participant: &Participant, form: &AddressForm) -> bool {
        let resolved = self
            .identity_id
            .as_ref()
            .is_some_and(|identity| participant.identity_id.as_ref() == Some(identity));
        resolved || self.is(participant.id.as_str(), form)
    }

    /// Whether `id`, which a binding or an event of `channel_id` names in a
    /// room holding `participants`, names this sender: it is their address
    /// or their identity's id, or the id of a participant reached through
    /// the channel who is them (RFC §10.4: "a participant or an event named
    /// by either is the sender's own", §5.5).
    pub fn named_by(
        &self,
        id: &str,
        form: &AddressForm,
        channel_id: &ChannelId,
        participants: &[Participant],
    ) -> bool {
        self.is(id, form)
            || participants.iter().any(|participant| {
                reached_through(participant, channel_id)
                    && form.normalize(participant.id.as_str()) == form.normalize(id)
                    && self.is_participant(participant, form)
            })
    }
}

/// What a router is asked: the RFC's `route(channel_id, channel_type,
/// sender_id, metadata)`, with the organization the caller acts for, the
/// sender as routing names them, and the form the channel's addresses are
/// compared in (RFC §10.4, gap G-056).
#[derive(Clone, Debug, PartialEq)]
pub struct RouteQuery {
    /// The channel the message arrived on.
    pub channel_id: ChannelId,
    /// Its type.
    pub channel_type: ChannelType,
    /// The sender, in the channel's form (RFC §10.1 step 1).
    pub sender_id: String,
    /// The message's metadata.
    pub metadata: Metadata,
    /// The organization the caller acts for (RFC §17.2); `None` reads rooms
    /// unscoped (gap G-051).
    pub organization_id: Option<String>,
    /// Who the message is from, as routing names them.
    pub sender: Sender,
    /// The form the channel's addresses are compared in.
    pub form: AddressForm,
}

impl RouteQuery {
    /// Whether the message is the framework's own, under the sender
    /// `system`, which names no correspondent (RFC §10.4).
    pub fn is_system(&self) -> bool {
        self.sender_id == SYSTEM_SENDER
    }
}

/// A room the channel reaches, as the default strategy reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// The room.
    pub room: Room,
    /// Its bindings.
    pub bindings: Vec<ChannelBinding>,
    /// Its participants.
    pub participants: Vec<Participant>,
    /// Who the room has heard on the channel: the participants the room's
    /// received events of the channel name ([`heard`]). Step 3 reads it, for
    /// the one room bound to the channel it considers only.
    pub heard: Vec<ParticipantId>,
}

impl Candidate {
    /// The room `context` holds, with its bindings and participants, having
    /// heard no one yet.
    pub fn new(context: RoomContext) -> Self {
        Self {
            room: context.room,
            bindings: context.bindings,
            participants: context.participants,
            heard: Vec::new(),
        }
    }

    /// The room's binding of `channel_id`.
    pub fn binding(&self, channel_id: &ChannelId) -> Option<&ChannelBinding> {
        self.bindings
            .iter()
            .find(|binding| &binding.channel_id == channel_id)
    }
}

/// Why step 3 does not admit a sender into the one room bound to the channel
/// (RFC §10.4 "Step 3 admits one conversation").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The room does not bind the channel.
    Unbound,
    /// The framework's own sender `system`, on a binding not declared
    /// `group`.
    System,
    /// The binding names another participant.
    NamesAnother,
    /// The binding delivers to someone else: it is their conversation.
    AddressedToAnother,
    /// A participant other than the sender joined the room through the
    /// channel.
    MemberJoined,
    /// The room received a message on the channel from another sender.
    HeardAnother,
}

/// What step 2 does with a routed message's sender on the binding it finds
/// under the room lock (RFC §10.1 step 2, §5.7, gap G-061).
#[derive(Clone, Debug, PartialEq)]
pub enum Recording {
    /// Nothing to write: the binding is declared `group`, names the sender
    /// and their recipient already, or the message names no one to record.
    Kept,
    /// The write that records the sender, through rule 6: their address as
    /// the correspondent where the binding names none, and on a transport as
    /// its recipient where it has none.
    Write(BindingUpdate),
    /// The binding names another sender: the message is routed once more
    /// (gap G-055).
    Taken,
}

/// How a room is a sender's conversation on a channel by step 1 (RFC §10.4
/// step 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Through {
    /// The room's binding of the channel names the sender as its
    /// correspondent.
    Binding,
    /// A participant the sender is was reached through the channel.
    Participant,
}

/// The address a message is routed by (RFC §5.7, §10.4, gap G-057): the
/// chat it was posted in, read under the channel's chat key; else, on a
/// channel whose replies go to the sender, the sender's address. `None` for
/// a chat message without its chat, for an empty sender, and for the
/// framework's own sender `system`, which names no correspondent.
pub fn routing_address(
    addressing: Addressing,
    sender_id: &str,
    metadata: &Metadata,
    chat_key: Option<&str>,
) -> Option<String> {
    if sender_id == SYSTEM_SENDER {
        return None;
    }
    if let Some(chat) = chat_key.and_then(|key| chat_of(metadata, key)) {
        return Some(chat);
    }
    let addressed = addressing.replies_to_sender() && !sender_id.trim().is_empty();
    addressed.then(|| sender_id.to_owned())
}

/// The chat `metadata` names under `key`, as written: a non-empty string, or
/// a number (a Telegram chat id is one).
fn chat_of(metadata: &Metadata, key: &str) -> Option<String> {
    match metadata.get(key)? {
        Value::String(chat) if !chat.trim().is_empty() => Some(chat.clone()),
        Value::Number(chat) => Some(chat.to_string()),
        Value::String(_) | Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            None
        }
    }
}

/// The room the default strategy routes `query`'s message to, among the
/// rooms `candidates` hold; `None` when the framework must create one (RFC
/// §10.4). Step 1 reads the rooms [`step_one_considers`], step 3 those
/// [`step_three_considers`].
pub fn route(query: &RouteQuery, candidates: &[Candidate]) -> Option<RoomId> {
    let considered = |considers: fn(&RouteQuery, &Candidate) -> bool| -> Vec<&Candidate> {
        candidates
            .iter()
            .filter(|candidate| considers(query, candidate))
            .collect()
    };
    conversation(query, &considered(step_one_considers))
        .or_else(|| admitted(query, &considered(step_three_considers)))
        .map(|candidate| candidate.room.id.clone())
}

/// Whether step 1 considers `candidate`'s room for `query`: an ACTIVE or a
/// PAUSED room the caller reaches (RFC §10.4 step 1, §17.2, gap G-045). A
/// conversation its timer paused is the sender's still, and their message
/// resumes it; a CLOSED or ARCHIVED room is not. A room's status is read as
/// stored; a timer that fell due is applied by the room's next locked
/// section, which keeps the room or routes the message once more (gap
/// G-055).
pub fn step_one_considers(query: &RouteQuery, candidate: &Candidate) -> bool {
    let room = &candidate.room;
    matches!(room.status, RoomStatus::Active | RoomStatus::Paused)
        && organizations::admits(query.organization_id.as_deref(), room)
}

/// Whether step 3 considers `candidate`'s room for `query`: a room the
/// caller reaches that is ACTIVE, or PAUSED with its binding of the channel
/// declared `group` (RFC §10.4 step 3, §17.2, gap G-045). A group binding
/// says the channel is one conversation, which its next message resumes;
/// any other PAUSED room is set aside, and does not count among the rooms
/// bound to the channel. The two kinds count alike: one ACTIVE room and one
/// PAUSED group room bound to the channel are two, which give none (RFC
/// §10.4 "A router MUST NOT guess"). A room's status is read as stored, as
/// at step 1.
pub fn step_three_considers(query: &RouteQuery, candidate: &Candidate) -> bool {
    let room = &candidate.room;
    let open = match room.status {
        RoomStatus::Active => true,
        RoomStatus::Paused => declared_group(candidate.binding(&query.channel_id)),
        RoomStatus::Closed | RoomStatus::Archived => false,
    };
    open && organizations::admits(query.organization_id.as_deref(), room)
}

/// Step 1: the sender's conversation, the newest room whose binding of the
/// channel names the sender, else the newest where a participant the sender
/// is was reached through the channel (RFC §10.4 step 1, gap G-058), each
/// told by [`conversation_through`].
pub fn conversation<'room>(
    query: &RouteQuery,
    candidates: &[&'room Candidate],
) -> Option<&'room Candidate> {
    let through = |candidate: &Candidate| {
        conversation_through(
            &query.sender,
            &query.form,
            &query.channel_id,
            candidate.binding(&query.channel_id),
            &candidate.participants,
        )
    };
    let found = |wanted: Through| {
        let matching = candidates
            .iter()
            .copied()
            .filter(move |candidate| through(candidate) == Some(wanted));
        newest(matching)
    };
    found(Through::Binding).or_else(|| found(Through::Participant))
}

/// How a room is `sender`'s conversation on `channel_id` by step 1,
/// addresses compared in `form` (RFC §10.4 step 1, §5.5, §5.7, gap G-058):
/// through `binding`, the room's binding of the channel, when its
/// correspondent names the sender; else through one of the room's
/// `participants` reached through the channel who is the sender; `None`
/// when the room is not their conversation. A binding speaks for its own
/// channel only, and so does a participant.
///
/// The strategy's step 1 asks it of every room it reads ([`conversation`]),
/// and the room lock of a routed room it finds PAUSED
/// ([`paused_room_takes`]): the two tell a conversation alike.
pub fn conversation_through(
    sender: &Sender,
    form: &AddressForm,
    channel_id: &ChannelId,
    binding: Option<&ChannelBinding>,
    participants: &[Participant],
) -> Option<Through> {
    let named = binding
        .and_then(|binding| correspondent(binding, form))
        .is_some_and(|named| sender.named_by(&named, form, channel_id, participants));
    if named {
        return Some(Through::Binding);
    }
    joined(sender, form, channel_id, participants).then_some(Through::Participant)
}

/// Whether a routed message stays in a room the lock finds PAUSED, whose
/// binding of `channel_id` is `binding` and which holds `participants` (RFC
/// §10.4, gaps G-045, G-055): the room is `sender`'s conversation by step 1
/// ([`conversation_through`]), or its binding is declared `group`, which
/// step 3 considers paused ([`step_three_considers`]). Any other PAUSED
/// room would take the sender in by step 3's admission, which it does not
/// give a PAUSED room, whatever strategy chose it.
pub fn paused_room_takes(
    sender: &Sender,
    form: &AddressForm,
    channel_id: &ChannelId,
    binding: Option<&ChannelBinding>,
    participants: &[Participant],
) -> bool {
    declared_group(binding)
        || conversation_through(sender, form, channel_id, binding, participants).is_some()
}

/// Whether a room in `status`, as its lock finds it, stays the routed
/// message's from `sender`, its binding of `channel_id` being `binding` and
/// its participants `participants` (RFC §5.1, §10.4, gaps G-045, G-055). An
/// ACTIVE room does. A PAUSED room does when it takes the sender
/// ([`paused_room_takes`]), the message's commit then resuming it; any other
/// PAUSED room is no longer the message's, whatever strategy chose it. A
/// CLOSED or ARCHIVED room never is.
///
/// A room the default strategy read ACTIVE is found PAUSED by the lock when
/// its gate applied a pause that fell due meanwhile, and is kept by the
/// same test.
pub fn kept_by_status(
    status: RoomStatus,
    sender: &Sender,
    form: &AddressForm,
    channel_id: &ChannelId,
    binding: Option<&ChannelBinding>,
    participants: &[Participant],
) -> bool {
    match status {
        RoomStatus::Active => true,
        RoomStatus::Paused => paused_room_takes(sender, form, channel_id, binding, participants),
        RoomStatus::Closed | RoomStatus::Archived => false,
    }
}

/// Steps 3 and 4: the one room bound to the channel, when it admits the
/// sender; `None` when no room or several are bound, or when the one room
/// refuses (RFC §10.4).
pub fn admitted<'room>(
    query: &RouteQuery,
    candidates: &[&'room Candidate],
) -> Option<&'room Candidate> {
    let room = sole_bound(query, candidates)?;
    admits(query, room).is_ok().then_some(room)
}

/// The one room of `candidates` bound to the channel, which step 3 reads;
/// `None` when none or several are (RFC §10.4 "A router MUST NOT guess").
pub fn sole_bound<'room>(
    query: &RouteQuery,
    candidates: &[&'room Candidate],
) -> Option<&'room Candidate> {
    let mut bound = candidates
        .iter()
        .copied()
        .filter(|candidate| candidate.binding(&query.channel_id).is_some());
    let room = bound.next()?;
    bound.next().is_none().then_some(room)
}

/// Whether the room `candidate` holds admits `query`'s sender at step 3
/// (RFC §10.4 "Step 3 admits one conversation"): its binding of the channel
/// is declared `group`, or all of these hold:
///
/// - the binding names no participant, or names the sender;
/// - the binding delivers to no one, or to the sender;
/// - no participant other than the sender was reached through the channel;
/// - the room has heard no one else on the channel.
///
/// The framework's own sender `system` is admitted by a `group` binding
/// only. A message with no address is held to the same conditions, so it is
/// admitted only where no one is named, joined or heard.
///
/// # Errors
///
/// Returns the first condition that does not hold, as a [`Refusal`].
pub fn admits(query: &RouteQuery, candidate: &Candidate) -> Result<(), Refusal> {
    let binding = candidate
        .binding(&query.channel_id)
        .ok_or(Refusal::Unbound)?;
    if binding.group {
        return Ok(());
    }
    if query.is_system() {
        return Err(Refusal::System);
    }
    admission(
        &query.sender,
        &query.form,
        binding,
        &candidate.participants,
        &candidate.heard,
    )
}

/// Whether a room read under its lock still admits a routed `sender` whose
/// record is about to name them on `binding`, the room's binding of the
/// channel, holding `participants` and having heard `heard` on the channel
/// (RFC §10.4 step 3, §10.1 step 2, gap G-055).
///
/// A record names the sender as the correspondent of a binding that names
/// none. The room was then the sender's by step 3, or by a custom
/// strategy, as the router read it; by the lock, someone else may have
/// been heard on the channel or have joined through it, and the record
/// would send their answers to the sender. The room is held to step 3's
/// conditions once more, unless the binding is declared `group`, it names
/// a correspondent ([`recording`] judges that one), or the sender is a
/// participant reached through the channel, whose conversation the room is
/// by step 1 whoever else was heard there.
///
/// # Errors
///
/// Returns the first of step 3's conditions that no longer holds, as a
/// [`Refusal`].
pub fn still_admits(
    binding: &ChannelBinding,
    sender: &Sender,
    form: &AddressForm,
    participants: &[Participant],
    heard: &[ParticipantId],
) -> Result<(), Refusal> {
    if binding.group
        || correspondent(binding, form).is_some()
        || joined(sender, form, &binding.channel_id, participants)
    {
        return Ok(());
    }
    admission(sender, form, binding, participants, heard)
}

/// Step 3's conditions on a binding not declared `group` ([`admits`]).
fn admission(
    sender: &Sender,
    form: &AddressForm,
    binding: &ChannelBinding,
    participants: &[Participant],
    heard: &[ParticipantId],
) -> Result<(), Refusal> {
    let channel_id = &binding.channel_id;
    let named_by = |id: &str| sender.named_by(id, form, channel_id, participants);
    if let Some(named) = &binding.participant_id
        && !named_by(named.as_str())
    {
        return Err(Refusal::NamesAnother);
    }
    if let Some(recipient) = form.recipient(binding)
        && !sender.is(&recipient, form)
    {
        return Err(Refusal::AddressedToAnother);
    }
    let member = participants.iter().any(|participant| {
        reached_through(participant, channel_id) && !sender.is_participant(participant, form)
    });
    if member {
        return Err(Refusal::MemberJoined);
    }
    if heard.iter().any(|author| !named_by(author.as_str())) {
        return Err(Refusal::HeardAnother);
    }
    Ok(())
}

/// The newest of `candidates`: the room created last, and among rooms
/// created at one instant the one whose id sorts last, so that the answer
/// never depends on the order the rooms were read in (RFC §10.4 "Routing
/// MUST be deterministic").
pub fn newest<'room>(
    candidates: impl IntoIterator<Item = &'room Candidate>,
) -> Option<&'room Candidate> {
    candidates
        .into_iter()
        .max_by(|left, right| recency(&left.room, &right.room))
}

/// How `left` compares with `right` by recency: its creation, then its id.
/// A store's routing queries order rooms by it (RFC §14.1
/// `find_room_id_by_binding`).
pub fn recency(left: &Room, right: &Room) -> Ordering {
    (left.created_at, &left.id).cmp(&(right.created_at, &right.id))
}

/// Who the room has heard on `channel_id` among `events`: the participant
/// each event of the channel names, a `BLOCKED` one excepted (RFC §10.4).
pub fn heard(events: &[RoomEvent], channel_id: &ChannelId) -> Vec<ParticipantId> {
    events
        .iter()
        .filter(|event| &event.source.channel_id == channel_id)
        .filter(|event| event.status != EventStatus::Blocked)
        .filter_map(|event| event.source.participant_id.clone())
        .collect()
}

/// The correspondent `binding` names, in the channel's form: its
/// participant, or else its recipient, since a binding given a recipient
/// and no correspondent is that recipient's conversation (RFC §5.7).
pub fn correspondent(binding: &ChannelBinding, form: &AddressForm) -> Option<String> {
    match &binding.participant_id {
        Some(participant) => Some(form.normalize(participant.as_str())),
        None => form.recipient(binding),
    }
}

/// What step 2 writes on `binding` for a routed message from `sender`, in a
/// room holding `participants` (RFC §10.1 step 2, §5.7, gap G-061): nothing
/// on a binding declared `group`; [`Recording::Taken`] when the binding
/// names someone else; otherwise the sender's address as its correspondent
/// where it names none, and on a transport as its recipient where it has
/// none. The framework's own sender, and a message with no address, record
/// no one. A binding naming a participant who is the sender names them.
pub fn recording(
    binding: &ChannelBinding,
    sender: &Sender,
    form: &AddressForm,
    participants: &[Participant],
) -> Recording {
    if binding.group {
        return Recording::Kept;
    }
    if let Some(named) = correspondent(binding, form)
        && !sender.named_by(&named, form, &binding.channel_id, participants)
    {
        return Recording::Taken;
    }
    let Some(address) = &sender.address else {
        return Recording::Kept;
    };
    let mut update = BindingUpdate::default();
    if binding.participant_id.is_none() {
        update.participant_id = Some(ParticipantId::new(address.clone()).ok());
    }
    let addressed = binding.category == ChannelCategory::Transport;
    if let (true, Some(key), None) = (addressed, &form.recipient_key, form.recipient(binding)) {
        update
            .metadata
            .insert(key.clone(), Value::String(address.clone()));
    }
    if update.is_empty() {
        Recording::Kept
    } else {
        Recording::Write(update)
    }
}

/// Whether one of `participants` is `sender` and was reached through
/// `channel_id` (RFC §10.4 step 1, §5.5): only then is their room the
/// sender's conversation on this channel.
fn joined(
    sender: &Sender,
    form: &AddressForm,
    channel_id: &ChannelId,
    participants: &[Participant],
) -> bool {
    participants.iter().any(|participant| {
        reached_through(participant, channel_id) && sender.is_participant(participant, form)
    })
}

/// Whether `binding` is declared `group`: the channel is one conversation,
/// whoever writes on it (RFC §5.7).
fn declared_group(binding: Option<&ChannelBinding>) -> bool {
    binding.is_some_and(|binding| binding.group)
}

/// Whether the room reached `participant` through `channel_id` (RFC §5.5
/// `connected_via`), whatever its status (gap G-058).
fn reached_through(participant: &Participant, channel_id: &ChannelId) -> bool {
    participant.connected_via.contains(channel_id)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::model::{
        ChannelDirection, EventContent, EventDirection, EventId, EventSource, EventType, Timestamp,
    };
    use crate::rules::addresses::{CountryCode, PHONE_NUMBER};

    const ALICE: &str = "+15550000001";
    const BOB: &str = "+15550000002";

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn sms_form() -> AddressForm {
        AddressForm {
            addressing: Addressing::PhoneNumber,
            country_code: Some(CountryCode::new("1").unwrap()),
            recipient_key: Some(PHONE_NUMBER.to_owned()),
        }
    }

    /// A message from `sender` on the SMS channel `sms`, for a caller naming
    /// no organization.
    fn query(sender: &str) -> RouteQuery {
        query_on("sms", sender)
    }

    fn query_on(channel_id: &str, sender: &str) -> RouteQuery {
        RouteQuery {
            channel_id: channel(channel_id),
            channel_type: ChannelType::Sms,
            sender_id: sender.to_owned(),
            metadata: Metadata::new(),
            organization_id: None,
            sender: Sender {
                address: routing_address(Addressing::PhoneNumber, sender, &Metadata::new(), None),
                identity_id: None,
            },
            form: sms_form(),
        }
    }

    fn with_identity(mut query: RouteQuery, identity: &str) -> RouteQuery {
        query.sender.identity_id = Some(IdentityId::new(identity).unwrap());
        query
    }

    /// An ACTIVE room `id` created `seconds` after the epoch, with nothing.
    fn room(id: &str, seconds: i64) -> Candidate {
        Candidate {
            room: Room::new(
                RoomId::new(id).unwrap(),
                Timestamp::from_unix_millis(seconds * 1000).unwrap(),
            ),
            bindings: Vec::new(),
            participants: Vec::new(),
            heard: Vec::new(),
        }
    }

    fn sms_binding(room_id: &RoomId, channel_id: &str) -> ChannelBinding {
        ChannelBinding::new(
            channel(channel_id),
            room_id.clone(),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    /// `candidate` with the channel `sms` bound, naming no one.
    fn bound(candidate: Candidate) -> Candidate {
        bound_on(candidate, "sms", |_| {})
    }

    fn bound_on(
        mut candidate: Candidate,
        channel_id: &str,
        configure: impl FnOnce(&mut ChannelBinding),
    ) -> Candidate {
        let mut binding = sms_binding(&candidate.room.id, channel_id);
        configure(&mut binding);
        candidate.bindings.push(binding);
        candidate
    }

    /// `candidate` with `sms` bound naming `participant`.
    fn naming(candidate: Candidate, participant: &str) -> Candidate {
        bound_on(candidate, "sms", |binding| {
            binding.participant_id = Some(ParticipantId::new(participant).unwrap());
        })
    }

    /// `candidate` with `sms` bound delivering to `number`, naming no one.
    fn addressed_to(candidate: Candidate, number: &str) -> Candidate {
        bound_on(candidate, "sms", |binding| {
            binding
                .metadata
                .insert(PHONE_NUMBER.to_owned(), json!(number));
        })
    }

    fn grouped(candidate: Candidate) -> Candidate {
        bound_on(candidate, "sms", |binding| binding.group = true)
    }

    fn member(id: &str, room: &Candidate, channel_id: &str) -> Participant {
        Participant::new(
            ParticipantId::new(id).unwrap(),
            room.room.id.clone(),
            channel(channel_id),
            Timestamp::UNIX_EPOCH,
        )
    }

    /// `candidate` with the participant `id` reached through `channel_id`.
    fn with_member(mut candidate: Candidate, id: &str, channel_id: &str) -> Candidate {
        let participant = member(id, &candidate, channel_id);
        candidate.participants.push(participant);
        candidate
    }

    fn having_heard(mut candidate: Candidate, author: &str) -> Candidate {
        candidate.heard.push(ParticipantId::new(author).unwrap());
        candidate
    }

    /// `candidate` in `status`.
    fn in_status(mut candidate: Candidate, status: RoomStatus) -> Candidate {
        candidate.room.status = status;
        candidate
    }

    fn paused(candidate: Candidate) -> Candidate {
        in_status(candidate, RoomStatus::Paused)
    }

    /// `candidate` with the member `p_jean`, resolved to `id_jean`, reached
    /// through `channel_id`.
    fn with_jean(mut candidate: Candidate, channel_id: &str) -> Candidate {
        let mut jean = member("p_jean", &candidate, channel_id);
        jean.identity_id = Some(IdentityId::new("id_jean").unwrap());
        candidate.participants.push(jean);
        candidate
    }

    /// A message from jean's number, which resolves to `id_jean`.
    fn from_jean() -> RouteQuery {
        with_identity(query(ALICE), "id_jean")
    }

    fn routed(query: &RouteQuery, candidates: &[Candidate]) -> Option<String> {
        route(query, candidates).map(String::from)
    }

    /// RFC §10.4 step 1: the sender's conversation.
    mod step_one {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn a_binding_naming_the_sender_finds_its_room() {
            let rooms = [naming(room("room_1", 0), ALICE)];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// Gap G-058: a correspondent the integrator wrote in another form
        /// is compared in the channel's.
        #[test]
        fn a_binding_naming_the_sender_in_another_form_finds_its_room() {
            let rooms = [naming(room("room_1", 0), "+1 (555) 000-0001")];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// RFC §5.7: a binding given a recipient and no correspondent names
        /// the recipient.
        #[test]
        fn a_binding_addressed_to_the_sender_without_a_correspondent_finds_its_room() {
            let rooms = [addressed_to(room("room_1", 0), ALICE)];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        #[test]
        fn a_binding_naming_the_sender_s_identity_finds_its_room() {
            let rooms = [naming(room("room_1", 0), "id_alice")];
            let query = with_identity(query(ALICE), "id_alice");
            assert_eq!(routed(&query, &rooms), Some("room_1".to_owned()));
        }

        /// RFC §10.4: a participant named by the sender's identity is the
        /// sender's own, and so is a binding naming them.
        #[test]
        fn a_binding_naming_a_member_who_is_the_sender_finds_its_room() {
            let rooms = [
                with_jean(naming(room("room_1", 10), "p_jean"), "sms"),
                with_member(room("room_2", 20), ALICE, "sms"),
            ];
            assert_eq!(routed(&from_jean(), &rooms), Some("room_1".to_owned()));
        }

        #[test]
        fn the_newest_room_naming_the_sender_is_found() {
            let rooms = [
                naming(room("room_1", 10), ALICE),
                naming(room("room_2", 20), ALICE),
            ];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_2".to_owned()));
        }

        #[test]
        fn rooms_created_at_one_instant_are_told_apart_by_their_id() {
            let rooms = [
                naming(room("room_b", 10), ALICE),
                naming(room("room_a", 10), ALICE),
            ];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_b".to_owned()));
        }

        #[test]
        fn a_participant_reached_through_the_channel_finds_its_room() {
            let rooms = [with_member(bound(room("room_1", 0)), ALICE, "sms")];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// Gap G-058: a participant id the host wrote in another form is
        /// compared in the channel's.
        #[test]
        fn a_participant_named_in_another_form_finds_its_room() {
            let rooms = [with_member(room("room_1", 0), "+1 (555) 000-0001", "sms")];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// RFC §10.4: a member added under their identity is found by an
        /// address linked to it.
        #[test]
        fn a_participant_resolved_to_the_sender_s_identity_finds_its_room() {
            let mut jean = bound(room("room_1", 0));
            let mut participant = member("p_jean", &jean, "sms");
            participant.identity_id = Some(IdentityId::new("id_jean").unwrap());
            jean.participants.push(participant);
            let query = with_identity(query(ALICE), "id_jean");
            assert_eq!(routed(&query, &[jean]), Some("room_1".to_owned()));
        }

        #[test]
        fn a_participant_whose_id_is_the_sender_s_identity_finds_its_room() {
            let rooms = [with_member(room("room_1", 0), "id_alice", "sms")];
            let query = with_identity(query(ALICE), "id_alice");
            assert_eq!(routed(&query, &rooms), Some("room_1".to_owned()));
        }

        /// RFC §10.4 step 1: a message is not carried into a room of another
        /// channel.
        #[test]
        fn a_participant_reached_through_another_channel_finds_nothing() {
            let rooms = [with_member(room("room_1", 0), ALICE, "whatsapp")];
            assert_eq!(routed(&query(ALICE), &rooms), None);
        }

        #[test]
        fn a_binding_is_looked_up_before_a_participant() {
            let rooms = [
                naming(room("room_1", 10), ALICE),
                with_member(room("room_2", 20), ALICE, "sms"),
            ];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// Gap G-045: a conversation its timer paused is the sender's still.
        #[test]
        fn a_paused_room_whose_binding_names_the_sender_is_found() {
            let rooms = [paused(naming(room("room_1", 0), ALICE))];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        #[test]
        fn a_paused_room_the_sender_joined_through_the_channel_is_found() {
            let rooms = [paused(with_member(bound(room("room_1", 0)), ALICE, "sms"))];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// The newest wins, whichever of the two is paused.
        #[test]
        fn the_newest_of_an_active_and_a_paused_room_naming_the_sender_is_found() {
            let newer_paused = [
                naming(room("room_1", 10), ALICE),
                paused(naming(room("room_2", 20), ALICE)),
            ];
            let newer_active = [
                paused(naming(room("room_1", 10), ALICE)),
                naming(room("room_2", 20), ALICE),
            ];
            assert_eq!(
                [
                    routed(&query(ALICE), &newer_paused),
                    routed(&query(ALICE), &newer_active)
                ],
                [Some("room_2".to_owned()), Some("room_2".to_owned())]
            );
        }

        #[test]
        fn a_closed_or_archived_room_is_not_found() {
            let found = [RoomStatus::Closed, RoomStatus::Archived].map(|status| {
                let named = in_status(naming(room("room_1", 0), ALICE), status);
                let joined = in_status(with_member(room("room_2", 0), ALICE, "sms"), status);
                [
                    routed(&query(ALICE), &[named]),
                    routed(&query(ALICE), &[joined]),
                ]
            });
            assert_eq!(found, [[None, None], [None, None]]);
        }

        /// RFC §17.2.
        #[test]
        fn a_room_of_another_organization_is_not_found() {
            let mut clinic = naming(room("room_1", 0), ALICE);
            clinic.room.organization_id = Some("clinic".to_owned());
            let query = RouteQuery {
                organization_id: Some("bank".to_owned()),
                ..query(ALICE)
            };
            assert_eq!(routed(&query, &[clinic]), None);
        }

        /// Gap G-051: a caller naming no organization reads every room.
        #[test]
        fn a_caller_naming_no_organization_finds_a_room_of_any() {
            let mut clinic = naming(room("room_1", 0), ALICE);
            clinic.room.organization_id = Some("clinic".to_owned());
            assert_eq!(routed(&query(ALICE), &[clinic]), Some("room_1".to_owned()));
        }
    }

    /// RFC §10.4 step 3, "Step 3 admits one conversation", condition by
    /// condition.
    mod step_three {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn the_one_room_bound_to_the_channel_admits_a_first_sender() {
            assert_eq!(admits(&query(ALICE), &bound(room("room_1", 0))), Ok(()));
        }

        /// RFC §10.4 "A router MUST NOT guess".
        #[test]
        fn two_rooms_bound_to_the_channel_give_none() {
            let rooms = [bound(room("room_1", 0)), bound(room("room_2", 0))];
            assert_eq!(routed(&query(ALICE), &rooms), None);
        }

        #[test]
        fn a_room_the_channel_does_not_bind_is_not_step_three_s() {
            let rooms = [bound_on(room("room_1", 0), "whatsapp", |_| {})];
            assert_eq!(routed(&query(ALICE), &rooms), None);
        }

        /// Gap G-045: step 3 admits a sender into a paused room only through
        /// a `group` binding.
        #[test]
        fn a_paused_room_whose_binding_is_not_group_is_not_step_three_s() {
            let rooms = [paused(bound(room("room_1", 0)))];
            assert_eq!(routed(&query(ALICE), &rooms), None);
        }

        #[test]
        fn a_closed_or_archived_room_is_not_step_three_s_even_bound_as_a_group() {
            let found = [RoomStatus::Closed, RoomStatus::Archived].map(|status| {
                let rooms = [in_status(grouped(room("room_1", 0)), status)];
                routed(&query(ALICE), &rooms)
            });
            assert_eq!(found, [None, None]);
        }

        /// Gap G-045: a paused room bound to the channel leaves the one
        /// ACTIVE room bound to it to step 3.
        #[test]
        fn a_paused_room_beside_the_active_one_bound_to_the_channel_leaves_it_to_step_three() {
            let rooms = [bound(room("room_1", 10)), paused(bound(room("room_2", 20)))];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// Gap G-045: a group binding says the channel is one conversation,
        /// which its next message resumes.
        #[test]
        fn a_paused_room_bound_as_a_group_is_step_three_s() {
            let rooms = [having_heard(paused(grouped(room("room_1", 0))), BOB)];
            assert_eq!(routed(&query(ALICE), &rooms), Some("room_1".to_owned()));
        }

        /// RFC §10.4 "A router MUST NOT guess": a paused group room counts
        /// among the rooms bound to the channel.
        #[test]
        fn an_active_room_and_a_paused_group_room_bound_to_the_channel_give_none() {
            let rooms = [
                bound(room("room_1", 10)),
                paused(grouped(room("room_2", 20))),
            ];
            assert_eq!(routed(&query(ALICE), &rooms), None);
        }

        #[test]
        fn a_group_binding_admits_any_sender() {
            let group = having_heard(grouped(room("room_1", 0)), BOB);
            assert_eq!(admits(&query(ALICE), &group), Ok(()));
        }

        #[test]
        fn a_group_binding_admits_the_framework_s_own_sender() {
            assert_eq!(
                admits(&query(SYSTEM_SENDER), &grouped(room("room_1", 0))),
                Ok(())
            );
        }

        #[test]
        fn the_framework_s_own_sender_is_never_admitted_by_a_binding_not_declared_group() {
            assert_eq!(
                admits(&query(SYSTEM_SENDER), &bound(room("room_1", 0))),
                Err(Refusal::System)
            );
        }

        #[test]
        fn a_binding_naming_another_participant_refuses_the_sender() {
            assert_eq!(
                admits(&query(BOB), &naming(room("room_1", 0), ALICE)),
                Err(Refusal::NamesAnother)
            );
        }

        #[test]
        fn a_binding_naming_the_sender_admits_them() {
            assert_eq!(
                admits(&query(ALICE), &naming(room("room_1", 0), ALICE)),
                Ok(())
            );
        }

        /// Gap G-058: compared in the channel's form, the binding names the
        /// sender.
        #[test]
        fn a_binding_naming_the_sender_in_another_form_admits_them() {
            assert_eq!(
                admits(
                    &query(ALICE),
                    &naming(room("room_1", 0), "+1 (555) 000-0001")
                ),
                Ok(())
            );
        }

        #[test]
        fn a_message_the_sender_wrote_in_another_form_admits_them() {
            let room = having_heard(bound(room("room_1", 0)), "15550000001");
            assert_eq!(admits(&query(ALICE), &room), Ok(()));
        }

        #[test]
        fn a_binding_naming_a_member_who_is_the_sender_admits_them() {
            let room = with_jean(naming(room("room_1", 0), "p_jean"), "sms");
            assert_eq!(admits(&from_jean(), &room), Ok(()));
        }

        #[test]
        fn a_message_a_member_who_is_the_sender_wrote_admits_them() {
            let room = having_heard(with_jean(bound(room("room_1", 0)), "sms"), "p_jean");
            assert_eq!(admits(&from_jean(), &room), Ok(()));
        }

        #[test]
        fn a_binding_delivering_to_another_address_refuses_the_sender() {
            assert_eq!(
                admits(&query(BOB), &addressed_to(room("room_1", 0), ALICE)),
                Err(Refusal::AddressedToAnother)
            );
        }

        #[test]
        fn a_binding_delivering_to_the_sender_admits_them() {
            assert_eq!(
                admits(&query(ALICE), &addressed_to(room("room_1", 0), ALICE)),
                Ok(())
            );
        }

        #[test]
        fn a_member_reached_through_the_channel_refuses_another_sender() {
            let room = with_member(bound(room("room_1", 0)), ALICE, "sms");
            assert_eq!(admits(&query(BOB), &room), Err(Refusal::MemberJoined));
        }

        #[test]
        fn the_sender_s_own_membership_admits_them() {
            let room = with_member(bound(room("room_1", 0)), ALICE, "sms");
            assert_eq!(admits(&query(ALICE), &room), Ok(()));
        }

        #[test]
        fn a_member_reached_through_another_channel_admits_the_sender() {
            let room = with_member(bound(room("room_1", 0)), ALICE, "email");
            assert_eq!(admits(&query(BOB), &room), Ok(()));
        }

        #[test]
        fn a_message_from_another_sender_on_the_channel_refuses_the_sender() {
            let room = having_heard(bound(room("room_1", 0)), ALICE);
            assert_eq!(admits(&query(BOB), &room), Err(Refusal::HeardAnother));
        }

        #[test]
        fn a_message_the_sender_wrote_under_their_identity_admits_them() {
            let room = having_heard(bound(room("room_1", 0)), "id_alice");
            let query = with_identity(query(ALICE), "id_alice");
            assert_eq!(admits(&query, &room), Ok(()));
        }

        #[test]
        fn a_message_with_no_address_is_admitted_where_no_one_is_named_joined_or_heard() {
            assert_eq!(admits(&query(""), &bound(room("room_1", 0))), Ok(()));
        }

        #[test]
        fn a_message_with_no_address_is_refused_where_someone_was_heard() {
            let room = having_heard(bound(room("room_1", 0)), ALICE);
            assert_eq!(admits(&query(""), &room), Err(Refusal::HeardAnother));
        }
    }

    /// RFC §10.4 "A delivery status follows its message" and the timeline
    /// step 3 reads.
    mod heard_on_the_channel {
        use super::*;
        use pretty_assertions::assert_eq;

        fn event(channel_id: &str, author: Option<&str>, status: EventStatus) -> RoomEvent {
            let mut source = EventSource::new(
                channel(channel_id),
                ChannelType::Sms,
                EventDirection::Inbound,
            );
            source.participant_id = author.map(|author| ParticipantId::new(author).unwrap());
            let mut event = RoomEvent::new(
                EventId::new("evt_1").unwrap(),
                RoomId::new("room_1").unwrap(),
                EventType::Message,
                source,
                EventContent::text("Hi"),
                Timestamp::UNIX_EPOCH,
            );
            event.status = status;
            event
        }

        #[test]
        fn names_the_authors_of_the_channel_s_received_messages_only() {
            let events = [
                event("sms", Some(ALICE), EventStatus::Delivered),
                event("sms", Some(BOB), EventStatus::Blocked),
                event("ws", Some("carol"), EventStatus::Delivered),
                event("sms", None, EventStatus::Delivered),
            ];
            assert_eq!(
                heard(&events, &channel("sms")),
                vec![ParticipantId::new(ALICE).unwrap()]
            );
        }
    }

    /// RFC §10.4's example, line by line, on one SMS number.
    mod alice_and_bob {
        use super::*;
        use pretty_assertions::assert_eq;

        /// The room created for alice at step 2: her address recorded on a
        /// fresh binding.
        fn alices_room() -> Candidate {
            let mut alice = bound(room("room_1", 10));
            let Recording::Write(update) =
                recording(&alice.bindings[0], &query(ALICE).sender, &sms_form(), &[])
            else {
                panic!("a fresh binding records its first sender");
            };
            alice.bindings[0] = update.apply(&alice.bindings[0]);
            alice
        }

        #[test]
        fn alice_writes_first_and_finds_no_room() {
            assert_eq!(routed(&query(ALICE), &[]), None);
        }

        #[test]
        fn the_room_created_for_alice_has_a_binding_naming_her() {
            let binding = &alices_room().bindings[0];
            assert_eq!(
                (
                    binding.participant_id.as_ref().map(ParticipantId::as_str),
                    binding.metadata.get(PHONE_NUMBER)
                ),
                (Some(ALICE), Some(&json!(ALICE)))
            );
        }

        #[test]
        fn bob_finds_no_room_of_his_and_alice_s_binding_refuses_him() {
            let query = query(BOB);
            let rooms = [alices_room()];
            let considered: Vec<&Candidate> = rooms.iter().collect();
            assert_eq!(
                (
                    conversation(&query, &considered).is_none(),
                    admits(&query, &rooms[0]),
                    routed(&query, &rooms)
                ),
                (true, Err(Refusal::NamesAnother), None)
            );
        }

        #[test]
        fn alice_s_next_message_is_found_through_her_binding() {
            let bobs = naming(room("room_2", 20), BOB);
            assert_eq!(
                routed(&query(ALICE), &[alices_room(), bobs]),
                Some("room_1".to_owned())
            );
        }

        #[test]
        fn a_room_opened_addressed_to_alice_refuses_bob_before_she_writes() {
            let opened = [addressed_to(room("room_1", 0), ALICE)];
            assert_eq!(routed(&query(BOB), &opened), None);
        }

        #[test]
        fn a_room_opened_addressed_to_alice_finds_her_first_message() {
            let opened = [addressed_to(room("room_1", 0), ALICE)];
            assert_eq!(routed(&query(ALICE), &opened), Some("room_1".to_owned()));
        }

        #[test]
        fn a_room_opened_with_alice_as_a_member_refuses_bob_before_she_writes() {
            let opened = [with_member(bound(room("room_1", 0)), ALICE, "sms")];
            assert_eq!(routed(&query(BOB), &opened), None);
        }

        #[test]
        fn a_room_opened_with_alice_as_a_member_finds_her_first_message() {
            let opened = [with_member(bound(room("room_1", 0)), ALICE, "sms")];
            assert_eq!(routed(&query(ALICE), &opened), Some("room_1".to_owned()));
        }

        #[test]
        fn a_room_opened_naming_no_one_admits_the_first_sender() {
            let opened = [bound(room("room_1", 0))];
            assert_eq!(routed(&query(ALICE), &opened), Some("room_1".to_owned()));
        }

        #[test]
        fn a_room_opened_naming_no_one_refuses_the_second_sender_once_the_first_is_recorded() {
            let mut opened = bound(room("room_1", 0));
            if let Recording::Write(update) =
                recording(&opened.bindings[0], &query(ALICE).sender, &sms_form(), &[])
            {
                opened.bindings[0] = update.apply(&opened.bindings[0]);
            }
            assert_eq!(routed(&query(BOB), &[opened]), None);
        }
    }

    /// RFC §10.4 "A binding speaks for its own channel": a bank and a
    /// clinic, each on its own SMS number.
    mod bank_and_clinic {
        use super::*;
        use pretty_assertions::assert_eq;

        fn banks_room() -> Candidate {
            bound_on(room("bank_room", 10), "sms_bank", |binding| {
                binding.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            })
        }

        #[test]
        fn a_customer_the_bank_recorded_writing_to_the_clinic_lands_in_the_clinic_s_conversation() {
            let clinics = bound_on(room("clinic_room", 20), "sms_clinic", |_| {});
            assert_eq!(
                routed(&query_on("sms_clinic", ALICE), &[banks_room(), clinics]),
                Some("clinic_room".to_owned())
            );
        }

        #[test]
        fn a_customer_the_bank_recorded_writing_to_the_clinic_is_not_taken_to_the_bank_s_room() {
            assert_eq!(
                routed(&query_on("sms_clinic", ALICE), &[banks_room()]),
                None
            );
        }

        /// Gap G-059: §10.4 step 1 finds a member's room only through the
        /// channel they were reached on, whatever the bank-and-clinic
        /// paragraph says of members.
        #[test]
        fn a_member_of_the_bank_s_room_writing_to_the_clinic_is_not_carried_into_it() {
            let bank = with_member(
                bound_on(room("bank_room", 10), "sms_bank", |_| {}),
                ALICE,
                "sms_bank",
            );
            assert_eq!(routed(&query_on("sms_clinic", ALICE), &[bank]), None);
        }
    }

    /// RFC §10.4 "Routing MUST be deterministic".
    mod determinism {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Every order of `items`.
        fn orders<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
            if items.len() <= 1 {
                return vec![items.to_vec()];
            }
            let mut all = Vec::new();
            for (at, item) in items.iter().enumerate() {
                let mut rest = items.to_vec();
                rest.remove(at);
                for mut order in orders(&rest) {
                    order.insert(0, item.clone());
                    all.push(order);
                }
            }
            all
        }

        #[test]
        fn the_same_rooms_read_in_any_order_give_the_same_room() {
            let rooms = [
                naming(room("room_1", 10), ALICE),
                naming(room("room_3", 20), ALICE),
                naming(room("room_2", 20), ALICE),
                with_member(room("room_4", 30), ALICE, "sms"),
            ];
            let answers: Vec<Option<String>> = orders(&rooms)
                .iter()
                .map(|order| routed(&query(ALICE), order))
                .collect();
            assert_eq!(answers, vec![Some("room_3".to_owned()); 24]);
        }

        #[test]
        fn the_same_bound_rooms_read_in_any_order_give_none() {
            let rooms = [
                bound(room("room_1", 10)),
                bound(room("room_2", 20)),
                naming(room("room_3", 30), BOB),
            ];
            let answers: Vec<Option<String>> = orders(&rooms)
                .iter()
                .map(|order| routed(&query(ALICE), order))
                .collect();
            assert_eq!(answers, vec![None; 6]);
        }
    }

    /// RFC §5.1, §10.4, gaps G-045, G-055: whether a routed room's status,
    /// as the lock finds it, keeps the message.
    mod a_routed_room_s_status {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn keeps_the_message_active_or_paused_as_its_conversation_and_never_closed() {
            let room_id = RoomId::new("room_1").unwrap();
            let mut alices = sms_binding(&room_id, "sms");
            alices.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            let nobodys = sms_binding(&room_id, "sms");
            let kept = |status, binding: &ChannelBinding| {
                let sender = query(ALICE).sender;
                kept_by_status(
                    status,
                    &sender,
                    &sms_form(),
                    &channel("sms"),
                    Some(binding),
                    &[],
                )
            };
            assert_eq!(
                [
                    kept(RoomStatus::Active, &nobodys),
                    kept(RoomStatus::Paused, &alices),
                    kept(RoomStatus::Paused, &nobodys),
                    kept(RoomStatus::Closed, &alices),
                    kept(RoomStatus::Archived, &alices),
                ],
                [true, true, false, false, false]
            );
        }
    }

    /// RFC §10.1 step 2, §5.7: what step 2 records of a routed sender.
    mod recording_the_sender {
        use super::*;
        use pretty_assertions::assert_eq;

        fn binding() -> ChannelBinding {
            sms_binding(&RoomId::new("room_1").unwrap(), "sms")
        }

        fn sender(address: &str) -> Sender {
            query(address).sender
        }

        #[test]
        fn a_binding_naming_no_one_records_the_sender_and_their_recipient() {
            let Recording::Write(update) = recording(&binding(), &sender(ALICE), &sms_form(), &[])
            else {
                panic!("the sender is recorded");
            };
            let recorded = update.apply(&binding());
            assert_eq!(
                (
                    recorded.participant_id.as_ref().map(ParticipantId::as_str),
                    Value::Object(recorded.metadata)
                ),
                (Some(ALICE), json!({ "phone_number": ALICE }))
            );
        }

        #[test]
        fn a_binding_naming_another_sender_is_taken() {
            let mut alices = binding();
            alices.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            assert_eq!(
                recording(&alices, &sender(BOB), &sms_form(), &[]),
                Recording::Taken
            );
        }

        #[test]
        fn a_binding_delivering_to_another_is_taken() {
            let mut alices = binding();
            alices
                .metadata
                .insert(PHONE_NUMBER.to_owned(), json!(ALICE));
            assert_eq!(
                recording(&alices, &sender(BOB), &sms_form(), &[]),
                Recording::Taken
            );
        }

        #[test]
        fn a_group_binding_records_no_one() {
            let group = ChannelBinding {
                group: true,
                ..binding()
            };
            assert_eq!(
                recording(&group, &sender(ALICE), &sms_form(), &[]),
                Recording::Kept
            );
        }

        #[test]
        fn the_framework_s_own_sender_is_never_recorded() {
            assert_eq!(
                recording(&binding(), &sender(SYSTEM_SENDER), &sms_form(), &[]),
                Recording::Kept
            );
        }

        /// RFC §5.7: a binding found naming the sender without a recipient
        /// gets their address as its recipient.
        #[test]
        fn a_binding_naming_the_sender_without_a_recipient_gets_one() {
            let mut alices = binding();
            alices.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            let Recording::Write(update) = recording(&alices, &sender(ALICE), &sms_form(), &[])
            else {
                panic!("the recipient is written");
            };
            assert_eq!(
                (update.participant_id, Value::Object(update.metadata)),
                (None, json!({ "phone_number": ALICE }))
            );
        }

        #[test]
        fn a_binding_naming_the_sender_and_their_recipient_writes_nothing() {
            let mut alices = binding();
            alices.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            alices
                .metadata
                .insert(PHONE_NUMBER.to_owned(), json!(ALICE));
            assert_eq!(
                recording(&alices, &sender(ALICE), &sms_form(), &[]),
                Recording::Kept
            );
        }

        #[test]
        fn a_binding_naming_the_sender_s_identity_keeps_it_and_gets_their_address() {
            let mut jeans = binding();
            jeans.participant_id = Some(ParticipantId::new("id_jean").unwrap());
            let sender = Sender {
                identity_id: Some(IdentityId::new("id_jean").unwrap()),
                ..sender(ALICE)
            };
            let Recording::Write(update) = recording(&jeans, &sender, &sms_form(), &[]) else {
                panic!("the recipient is written");
            };
            assert_eq!(
                update.apply(&jeans).participant_id,
                Some(ParticipantId::new("id_jean").unwrap())
            );
        }

        /// RFC §10.4: the binding names a participant who is the sender, so
        /// it is theirs, and gets their address as its recipient.
        #[test]
        fn a_binding_naming_a_member_who_is_the_sender_is_theirs() {
            let mut jeans = binding();
            jeans.participant_id = Some(ParticipantId::new("p_jean").unwrap());
            let room = with_jean(room("room_1", 0), "sms");
            let Recording::Write(update) =
                recording(&jeans, &from_jean().sender, &sms_form(), &room.participants)
            else {
                panic!("the recipient is written");
            };
            assert_eq!(
                (update.participant_id, Value::Object(update.metadata)),
                (None, json!({ "phone_number": ALICE }))
            );
        }

        /// RFC §5.5: a participant id names its participant only on a
        /// channel they were reached through.
        #[test]
        fn a_binding_naming_a_member_reached_through_another_channel_is_taken() {
            let mut jeans = binding();
            jeans.participant_id = Some(ParticipantId::new("p_jean").unwrap());
            let room = with_jean(room("room_1", 0), "email");
            assert_eq!(
                recording(&jeans, &from_jean().sender, &sms_form(), &room.participants),
                Recording::Taken
            );
        }

        /// The binding names the sender: its recipient, someone the
        /// integrator chose, is kept.
        #[test]
        fn a_binding_naming_the_sender_and_delivering_elsewhere_is_kept() {
            let mut alices = binding();
            alices.participant_id = Some(ParticipantId::new(ALICE).unwrap());
            alices.metadata.insert(PHONE_NUMBER.to_owned(), json!(BOB));
            assert_eq!(
                recording(&alices, &sender(ALICE), &sms_form(), &[]),
                Recording::Kept
            );
        }

        #[test]
        fn an_intelligence_channel_s_binding_takes_no_recipient() {
            let agent = ChannelBinding {
                category: ChannelCategory::Intelligence,
                ..binding()
            };
            let Recording::Write(update) = recording(&agent, &sender(ALICE), &sms_form(), &[])
            else {
                panic!("the sender is recorded");
            };
            assert!(update.metadata.is_empty());
        }
    }

    /// RFC §10.4 step 3, gap G-055: a room read under its lock, held to step
    /// 3's conditions once more before a record names the sender.
    mod still_admitted_under_the_lock {
        use super::*;
        use pretty_assertions::assert_eq;

        fn binding() -> ChannelBinding {
            sms_binding(&RoomId::new("room_1").unwrap(), "sms")
        }

        fn alice() -> Sender {
            query(ALICE).sender
        }

        fn participant(id: &str) -> Participant {
            Participant::new(
                ParticipantId::new(id).unwrap(),
                RoomId::new("room_1").unwrap(),
                channel("sms"),
                Timestamp::UNIX_EPOCH,
            )
        }

        fn heard(author: &str) -> Vec<ParticipantId> {
            vec![ParticipantId::new(author).unwrap()]
        }

        #[test]
        fn a_room_that_heard_no_one_else_admits_the_sender() {
            assert_eq!(
                still_admits(&binding(), &alice(), &sms_form(), &[], &heard(ALICE)),
                Ok(())
            );
        }

        #[test]
        fn a_room_that_heard_another_sender_refuses_them() {
            assert_eq!(
                still_admits(&binding(), &alice(), &sms_form(), &[], &heard(BOB)),
                Err(Refusal::HeardAnother)
            );
        }

        #[test]
        fn a_room_another_member_joined_through_the_channel_refuses_them() {
            assert_eq!(
                still_admits(&binding(), &alice(), &sms_form(), &[participant(BOB)], &[]),
                Err(Refusal::MemberJoined)
            );
        }

        /// RFC §10.4 step 1: the room is the sender's conversation.
        #[test]
        fn a_room_the_sender_joined_through_the_channel_admits_them_whoever_was_heard() {
            assert_eq!(
                still_admits(
                    &binding(),
                    &alice(),
                    &sms_form(),
                    &[participant(ALICE)],
                    &heard(BOB)
                ),
                Ok(())
            );
        }

        /// A correspondent is [`recording`]'s to judge.
        #[test]
        fn a_binding_naming_the_sender_admits_them_whoever_was_heard() {
            let alices = ChannelBinding {
                participant_id: Some(ParticipantId::new(ALICE).unwrap()),
                ..binding()
            };
            assert_eq!(
                still_admits(&alices, &alice(), &sms_form(), &[], &heard(BOB)),
                Ok(())
            );
        }

        #[test]
        fn a_group_binding_admits_them_whoever_was_heard() {
            let group = ChannelBinding {
                group: true,
                ..binding()
            };
            assert_eq!(
                still_admits(&group, &alice(), &sms_form(), &[], &heard(BOB)),
                Ok(())
            );
        }
    }

    /// RFC §5.7, gap G-057: the address a message is routed by.
    mod the_routing_address {
        use super::*;
        use pretty_assertions::assert_eq;

        fn metadata(value: Value) -> Metadata {
            match value {
                Value::Object(metadata) => metadata,
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_) => Metadata::new(),
            }
        }

        #[test]
        fn of_a_phone_message_is_its_sender() {
            assert_eq!(
                routing_address(Addressing::PhoneNumber, ALICE, &Metadata::new(), None),
                Some(ALICE.to_owned())
            );
        }

        #[test]
        fn of_a_chat_message_is_its_chat() {
            let chat = metadata(json!({ "chat_id": "-100777" }));
            assert_eq!(
                routing_address(Addressing::Chat, "4242", &chat, Some("chat_id")),
                Some("-100777".to_owned())
            );
        }

        #[test]
        fn of_a_chat_message_reads_a_numeric_chat_as_written() {
            let chat = metadata(json!({ "chat_id": -100_777 }));
            assert_eq!(
                routing_address(Addressing::Chat, "4242", &chat, Some("chat_id")),
                Some("-100777".to_owned())
            );
        }

        #[test]
        fn of_a_chat_message_without_its_chat_is_none() {
            assert_eq!(
                routing_address(Addressing::Chat, "4242", &Metadata::new(), Some("chat_id")),
                None
            );
        }

        /// RFC §5.7: WhatsApp Personal takes a group message's group as its
        /// conversation, and a private message's sender.
        #[test]
        fn of_a_group_message_on_a_channel_declaring_a_chat_key_is_its_group() {
            let group = metadata(json!({ "group_jid": "120363@g.us" }));
            let routed = [
                routing_address(Addressing::PhoneNumber, ALICE, &group, Some("group_jid")),
                routing_address(
                    Addressing::PhoneNumber,
                    ALICE,
                    &Metadata::new(),
                    Some("group_jid"),
                ),
            ];
            assert_eq!(
                routed,
                [Some("120363@g.us".to_owned()), Some(ALICE.to_owned())]
            );
        }

        #[test]
        fn of_the_framework_s_own_sender_is_none() {
            assert_eq!(
                routing_address(
                    Addressing::PhoneNumber,
                    SYSTEM_SENDER,
                    &Metadata::new(),
                    None
                ),
                None
            );
        }

        #[test]
        fn of_an_empty_sender_is_none() {
            assert_eq!(
                routing_address(Addressing::AsWritten, " ", &Metadata::new(), None),
                None
            );
        }
    }
}
