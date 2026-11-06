//! The changes step 2 makes under a room's lock (RFC §10.1 step 2, §5.7,
//! §7.5 rule 7, §10.4): the framework's own attach for a message arriving on
//! a channel the room does not bind, and the record of a routed message's
//! sender on the room's binding of the channel. Each runs through the
//! lifecycle's locked section, and refuses nothing: what does not apply
//! writes nothing, or sends the message to be routed once more.

use std::collections::VecDeque;
use std::convert::Infallible;

use crate::model::{ChannelBinding, ErrorRecord, ParticipantId, RoomContext};
use crate::ops::Cx;
use crate::ops::lifecycle::change::{self, Attached, Decision, SectionChange, Updated};
use crate::ops::lifecycle::report::Reported;
use crate::ops::protocol::{ChannelRequest, Outcome, Request, StoreRequest};
use crate::rules::addresses::{AddressForm, SYSTEM_SENDER};
use crate::rules::attachment;
use crate::rules::routing::{self, Recording, Sender};

/// A change step 2 makes under the room's lock.
#[derive(Clone, Debug)]
pub(super) enum ArrivalChange {
    /// The framework's own attach for a message arriving on the channel,
    /// made only where rule 7 allows it (gap G-053).
    AttachAutomatically(Box<ChannelBinding>),
    /// The record of a routed message's sender on the room's binding of the
    /// channel (gap G-061).
    RecordSender(Box<SenderRecord>),
}

/// What the channel answered, under the lock, of who the room's binding
/// delivers to (Python's `routed_recipient`, RFC §5.7, RMK-1129).
#[derive(Clone, Debug, Default)]
pub(super) struct RecipientRead {
    /// Whether the channel was asked.
    asked: bool,
    /// What it answered.
    answered: Option<String>,
}

impl RecipientRead {
    /// Asks the channel of `binding`, unless it answered: it is asked once.
    fn request(&self, binding: &ChannelBinding) -> Option<Request> {
        (!self.asked).then(|| {
            ChannelRequest::RoutedRecipient {
                binding: binding.clone(),
            }
            .into()
        })
    }

    /// Takes the channel's answer.
    fn take(&mut self, outcome: Outcome) -> Result<(), ErrorRecord> {
        self.answered = outcome.into_recipient()?;
        self.asked = true;
        Ok(())
    }
}

/// What recording a routed sender needs (RFC §10.1 step 2, §5.7).
#[derive(Clone, Debug)]
pub(super) struct SenderRecord {
    /// The binding attached when the room does not bind the channel: the
    /// channel's defaults, naming the sender.
    pub(super) binding: ChannelBinding,
    /// The sender, as routing names them.
    pub(super) sender: Sender,
    /// The form the channel's addresses are compared in.
    pub(super) form: AddressForm,
    /// Who the room heard on the channel, as far as the record read it.
    pub(super) heard: Heard,
    /// Who the room's binding delivers to, once the channel is asked.
    pub(super) recipient: RecipientRead,
    /// Whether step 2 created the room for the message, its creation hooks
    /// over: a room they closed or left unbound takes the attach as any room
    /// step 2 creates (RFC §10.1 step 2, RMK-1396).
    pub(super) created: bool,
}

impl SenderRecord {
    /// The form with the channel's answer, if it was asked.
    fn answered_form(&self) -> AddressForm {
        answered_form(self.form.clone(), &self.recipient)
    }

    /// The channel's read of the binding `context` holds, when routing still
    /// needs it: under the lock, before anything reads who the binding names.
    fn recipient_request(&self, context: &RoomContext) -> Option<Request> {
        let current = context.get_binding(&self.binding.channel_id)?;
        routing::reads_recipient(current, &self.form)
            .then(|| self.recipient.request(current))
            .flatten()
    }
}

/// `form`, with what the channel answered of the binding it reads.
fn answered_form(mut form: AddressForm, recipient: &RecipientRead) -> AddressForm {
    form.answered_recipient.clone_from(&recipient.answered);
    form
}

/// What the record of a routed sender read of who the room heard on the
/// channel under the lock, a page at a time (RFC §10.4 step 3, RMK-1127).
#[derive(Clone, Debug, Default)]
pub(super) struct Heard {
    /// The participants its events name, up to the first other sender.
    authors: Vec<ParticipantId>,
    /// How many events it read.
    offset: u64,
    /// Whether it read all it needs.
    over: bool,
}

/// What a change of step 2 did.
#[derive(Clone, Debug)]
pub(super) enum Recorded {
    /// The channel was attached, its binding naming the sender when step 2
    /// created the room or recorded a routed sender.
    Attached(Attached),
    /// The routed sender was recorded on the binding (gap G-061).
    Updated(Updated),
    /// Nothing was written: the binding stands as it is.
    Kept(Box<RoomContext>),
    /// The room is no longer the routed message's: it is CLOSED or
    /// ARCHIVED, or PAUSED, neither bound to the channel as a group nor the
    /// sender's conversation by step 1; its binding names another sender by
    /// now; or it no longer admits the sender a record would name on a
    /// binding naming no one. Nothing was written, and the message is routed
    /// once more (gaps G-045, G-055).
    Rerouted(Box<RoomContext>),
}

impl Recorded {
    /// The room as the change left it.
    pub(super) fn into_context(self) -> Box<RoomContext> {
        match self {
            Self::Attached(Attached { context, .. }) | Self::Updated(Updated { context, .. }) => {
                context
            }
            Self::Kept(context) | Self::Rerouted(context) => context,
        }
    }
}

impl SectionChange for ArrivalChange {
    type Done = Recorded;
    type Refusal = Infallible;

    /// Step 3 goes on with the room this section read (`named_room`), which
    /// holds at least the floor of its latest events.
    fn reads_history(&self) -> bool {
        true
    }

    fn decide(
        &self,
        context: Box<RoomContext>,
        _cx: &Cx<'_>,
    ) -> Result<Decision<Recorded>, Infallible> {
        Ok(match self {
            Self::AttachAutomatically(binding) => attach_automatically(binding, context),
            Self::RecordSender(record) => record_sender(record, context),
        })
    }

    /// A record whose check depends on who the room heard reads the channel's
    /// received events a page at a time, as Python's `_heard_from_other`
    /// scans them under the lock, up to the first other sender: a room that
    /// heard another on the channel, however long ago, is not this sender's
    /// (RFC §10.4 step 3, RMK-1071, RMK-1127). Who the room's binding delivers
    /// to comes first, as the channel answers it (RFC §5.7, RMK-1129): all that
    /// follows reads it.
    fn next_read(&self, context: &RoomContext) -> Option<Request> {
        let Self::RecordSender(record) = self else {
            return None;
        };
        if let Some(request) = record.recipient_request(context) {
            return Some(request);
        }
        if record.heard.over || !reads_heard(record, context) {
            return None;
        }
        let request = StoreRequest::ListEventsPage {
            room_id: context.room.id.clone(),
            channel_id: record.binding.channel_id.clone(),
            offset: record.heard.offset,
            limit: routing::HISTORY_PAGE,
        };
        Some(request.into())
    }

    fn take_read(&mut self, context: &RoomContext, outcome: Outcome) -> Result<(), ErrorRecord> {
        let Self::RecordSender(record) = self else {
            return Ok(());
        };
        if record.recipient_request(context).is_some() {
            return record.recipient.take(outcome);
        }
        let page = outcome.into_events()?;
        let channel_id = &record.binding.channel_id;
        let form = record.answered_form();
        let other = |author: &ParticipantId| {
            !record
                .sender
                .named_by(author.as_str(), &form, channel_id, &context.participants)
        };
        record.heard.over =
            routing::read_heard_page(&page, channel_id, other, &mut record.heard.authors);
        record.heard.offset = record
            .heard
            .offset
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// An attach reports `ON_CHANNEL_ATTACHED` and `room_channel_attached`;
    /// the record itself reports nothing (RFC §10.1 step 2).
    fn reported(done: &Recorded) -> Option<(Reported<'_>, &RoomContext)> {
        match done {
            Recorded::Attached(attached) => Some(attached.reported()),
            Recorded::Updated(updated) => updated.reported(),
            Recorded::Kept(_) | Recorded::Rerouted(_) => None,
        }
    }

    /// An attach tells its channel; the message's pass goes on in the room
    /// as the channel left it.
    fn told_context(done: &mut Recorded) -> Option<&mut Box<RoomContext>> {
        match done {
            Recorded::Attached(attached) => Some(&mut attached.context),
            Recorded::Updated(_) | Recorded::Kept(_) | Recorded::Rerouted(_) => None,
        }
    }
}

/// Stores the binding, then, once its channel is told, its lifecycle record
/// (gap G-046), unless the room binds the channel, revoked it or refuses
/// the message the attach is for (RFC §7.5 rule 7, gap G-053): the
/// framework never replaces a binding, and never undoes a detach.
fn attach_automatically(
    binding: &ChannelBinding,
    mut context: Box<RoomContext>,
) -> Decision<Recorded> {
    if !attachment::may_attach_automatically(&context, &binding.channel_id) {
        return Decision::unwritten(Recorded::Kept(context));
    }
    context.bindings.push(binding.clone());
    let writes = VecDeque::from([StoreRequest::StoreBinding {
        binding: binding.clone(),
    }]);
    Decision {
        writes,
        done: Recorded::Attached(Attached {
            binding: Box::new(binding.clone()),
            context,
        }),
        telling: Some(change::told_of_attach(binding, None)),
    }
}

/// Records a routed message's sender on the room as the lock finds it (RFC
/// §10.1 step 2, §10.4, gap G-061). A room whose status no longer keeps it
/// the message's ([`routing::kept_by_status`]), whose binding names another
/// sender by now, or which no longer admits the sender a record would name
/// on a binding naming no one, is no longer the message's: the message is
/// routed once more (gap G-055). A room that does not bind the channel has
/// it attached naming the sender, unless rule 7 refuses; one that binds it
/// has the sender recorded through rule 6, unless the binding is declared
/// `group` or has them already.
fn record_sender(record: &SenderRecord, context: Box<RoomContext>) -> Decision<Recorded> {
    let form = record.answered_form();
    let current = context.get_binding(&record.binding.channel_id).cloned();
    // A room step 2 created that its hooks closed, or left without a binding
    // of the channel, is the message's: attached unless its status refuses
    // events, step 6 refusing the message then (RFC §10.1 step 2).
    if record.created && (current.is_none() || !context.room.status.accepts_events()) {
        return attach_automatically(&record.binding, context);
    }
    let kept = routing::kept_by_status(
        context.room.status,
        &record.sender,
        &form,
        &record.binding.channel_id,
        current.as_ref(),
        &context.participants,
    );
    if !kept {
        return Decision::unwritten(Recorded::Rerouted(context));
    }
    let Some(current) = current else {
        return attach_automatically(&record.binding, context);
    };
    if record.created && record.sender.address.is_none() {
        return created_with_no_address(&current, context, &form);
    }
    let recording = routing::recording(&current, &record.sender, &form, &context.participants);
    match recording {
        Recording::Kept if still_admitted(record, &current, &context) => {
            Decision::unwritten(Recorded::Kept(context))
        }
        Recording::Write(update) if still_admitted(record, &current, &context) => {
            let updated = update.apply(&current);
            change::store_updated(&current, updated, context).map(Recorded::Updated)
        }
        Recording::NotARecipient(kind) => {
            not_a_recipient(&current, &form, kind);
            Decision::unwritten(Recorded::Rerouted(context))
        }
        Recording::Taken | Recording::Kept | Recording::Write(_) => {
            Decision::unwritten(Recorded::Rerouted(context))
        }
    }
}

/// What a message with no address does with `current`, the binding made
/// while step 2 created its room (RFC §5.7, §10.4, RMK-1396): a `group`
/// binding takes it; one naming no one and holding no recipient, which left
/// unnamed would pass step 3 for the next sender, takes the name no sender
/// is; one naming anyone else is someone's conversation, and the message is
/// routed once more.
fn created_with_no_address(
    current: &ChannelBinding,
    context: Box<RoomContext>,
    form: &AddressForm,
) -> Decision<Recorded> {
    if current.group {
        return Decision::unwritten(Recorded::Kept(context));
    }
    if current.participant_id.is_some() || routing::correspondent(current, form).is_some() {
        return Decision::unwritten(Recorded::Rerouted(context));
    }
    let mut closed = current.clone();
    closed.participant_id = ParticipantId::new(SYSTEM_SENDER).ok();
    change::store_updated(current, closed, context).map(Recorded::Updated)
}

/// Whether the room as `context` holds it, kept by its status, tells by who
/// it heard on the channel whether it still admits the sender: the case
/// whose check reads the room's history (RFC §10.4). A message with no
/// address is refused without it.
fn reads_heard(record: &SenderRecord, context: &RoomContext) -> bool {
    let Some(current) = context.get_binding(&record.binding.channel_id) else {
        return false;
    };
    let form = record.answered_form();
    let kept = routing::kept_by_status(
        context.room.status,
        &record.sender,
        &form,
        &record.binding.channel_id,
        Some(current),
        &context.participants,
    );
    kept && routing::reads_heard(current, &record.sender, &form, &context.participants)
}

/// Logs that the record found a value under `binding`'s recipient key, in
/// `form`, that is not a recipient, of JSON type `kind` (RFC §5.7 SHOULD):
/// the binding and the type, never the value, which may be a person's
/// address. The room is no longer the routed message's.
fn not_a_recipient(binding: &ChannelBinding, form: &AddressForm, kind: &str) {
    tracing::warn!(
        target: "roomkit::core::inbound",
        room_id = %binding.room_id,
        channel_id = %binding.channel_id,
        recipient_key = %form.recipient_key.as_deref().unwrap_or_default(),
        held = %kind,
        "the binding holds a value under its recipient key that is not a recipient: \
         routing reads it as someone else's address, so no sender the binding does not \
         name is admitted through it, and deliveries to it are refused no_recipient"
    );
}

/// Whether the room, as its lock finds it, still admits the sender a record
/// is about to name on `current` (RFC §10.4 step 3, gap G-055): between the
/// router's read and the lock, a message from someone else may have been
/// committed on the channel, through a room its caller named (gap G-053),
/// or a member may have joined through it.
///
/// Who the room heard on the channel was read from its received events
/// under the lock, refused events set aside, as Python's `_heard_from_other`
/// reads them (RMK-1071, RMK-1127): a sender another wrote before, however
/// long ago, is not admitted. A message with no address is refused, unless
/// the binding is declared `group` (RFC §10.4).
fn still_admitted(record: &SenderRecord, current: &ChannelBinding, context: &RoomContext) -> bool {
    let admitted = routing::still_admits(
        current,
        &record.sender,
        &record.answered_form(),
        &context.participants,
        &record.heard.authors,
    );
    if let Err(refusal) = admitted {
        tracing::debug!(
            target: "roomkit::core::inbound",
            room_id = %context.room.id,
            channel_id = %current.channel_id,
            refusal = ?refusal,
            "the routed room no longer admits the sender by its lock"
        );
    }
    admitted.is_ok()
}
