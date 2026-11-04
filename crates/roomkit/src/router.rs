//! Inbound room routing (RFC §10.4): which room a message that names none
//! belongs to.
//!
//! The kit asks its router, after the core has read the identity the
//! sender's address resolves to, with the Python implementation's
//! parameters: the channel, its type, and [`RouteOptions`].
//! [`DefaultInboundRoomRouter`] applies the RFC's default strategy
//! ([`rules::routing`](roomkit_core::rules::routing)) to the rooms the
//! message's channel reaches, read from the store; an integrator gives the
//! kit a strategy of its own in
//! [`RoomKitOptions::inbound_router`](crate::RoomKitOptions::inbound_router).

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{
    ChannelId, ChannelType, Identity, Metadata, ParticipantId, RoomContext, RoomEvent, RoomId,
    RoomStatus,
};
use roomkit_core::rules::addresses::{AddressForm, Addressing};
use roomkit_core::rules::routing::{self, Candidate, Refusal, RouteQuery, Sender};

use crate::registered::RegisteredChannel;
use crate::store::{
    ConversationStore, EventFilter, FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, ListEventsOptions, RoomFilter, StoreError,
};

/// What a router failed with. The message fails with it, before anything is
/// written (RFC §10.4).
pub type RouterError = Box<dyn Error + Send + Sync>;

/// What a router's [`route`](InboundRoomRouter::route) returns: a boxed
/// future, so that the kit holds its router as a trait object.
pub type RouterFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<RoomId>, RouterError>> + Send + 'a>>;

/// The registered channel by its id, as the kit holds it: what a router
/// reads of a channel, its recipient key and the address a binding delivers
/// to, in its form (RFC §5.7). The Python implementation's `channel_of`.
pub type ChannelOf = Arc<dyn Fn(&ChannelId) -> Option<Arc<dyn RegisteredChannel>> + Send + Sync>;

/// The sender name the framework's own messages carry, which names no
/// correspondent (RFC §10.4).
const SYSTEM_SENDER: &str = "system";

/// A routing strategy (RFC §10.4 `InboundRoomRouter`): the room a message
/// that names none belongs to, or `None` for a new room.
///
/// The router answers with a room's id, which step 2 treats as a room id in
/// hand (RFC §10.1 step 2): a room of another organization than the
/// caller's is not found, and a missing room is created with the id. Unlike
/// a room the caller names, the framework then records the sender on the
/// room's binding of the channel under the room lock; a room whose binding
/// names someone else by then, that is CLOSED or ARCHIVED by then, or PAUSED
/// but neither the sender's conversation by step 1 nor bound to the channel
/// as a group (gap G-045), or that step 3 no longer admits the sender to
/// (someone else heard there meanwhile), is not the message's, which is
/// routed once more, then given a new room (gap G-055): step 2 never
/// refuses it `room_closed`. A room closed once step 2's lock is released
/// refuses the message at step 6, as it refuses any event. A PAUSED room
/// that is the sender's conversation, or bound to the channel as a group,
/// takes the message, which resumes it. A strategy that sends several
/// senders to one room declares that room's binding `group`.
///
/// A strategy MUST NOT guess among several rooms, and MUST answer the same
/// for the same stored state (RFC §10.4).
///
/// ```
/// use roomkit::model::{ChannelId, ChannelType, RoomId};
/// use roomkit::{InboundRoomRouter, RouteOptions, RouterFuture};
///
/// /// Routes a message to the room its `ticket` metadata names; any other
/// /// message gets a new room.
/// struct ByTicket;
///
/// impl InboundRoomRouter for ByTicket {
///     fn route<'a>(
///         &'a self,
///         _channel_id: &'a ChannelId,
///         _channel_type: &'a ChannelType,
///         options: RouteOptions,
///     ) -> RouterFuture<'a> {
///         Box::pin(async move {
///             let data = options.channel_data.unwrap_or_default();
///             let Some(ticket) = data.get("ticket").and_then(|ticket| ticket.as_str()) else {
///                 return Ok(None);
///             };
///             Ok(Some(RoomId::new(ticket)?))
///         })
///     }
/// }
/// ```
pub trait InboundRoomRouter: Send + Sync + 'static {
    /// The room a message on `channel_id`, of `channel_type`, belongs to,
    /// `None` for a new room.
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a>;
}

/// What a router is told of a message beside its channel, the Python
/// implementation's `route` keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RouteOptions {
    /// The address naming the message's conversation, in the channel's
    /// form: the sender's, or on a chat channel the chat it was posted in
    /// (RFC §5.7); `system` for the framework's own sender, and `None` for a
    /// message with no address.
    pub participant_id: Option<String>,
    /// The message's metadata.
    pub channel_data: Option<Metadata>,
    /// The organization the caller acts for (RFC §17.2), `None` when the
    /// call names none.
    pub organization_id: Option<String>,
    /// The identity the store resolves `participant_id` to within that
    /// organization, read by the kit once per message before the first
    /// route (RFC §10.4, §14.1).
    pub identity: Option<Identity>,
}

/// The RFC's default routing strategy over a store (RFC §10.4): the
/// sender's newest conversation on the channel, ACTIVE or PAUSED, else the
/// one room bound to the channel, ACTIVE or PAUSED with a `group` binding,
/// when it admits the sender, else a new room (gap G-045).
///
/// It reads the store as the Python implementation's router does: step 1
/// through `find_room_id_by_binding` (the channel's recipient key naming a
/// binding's recipient), then `find_room_id_by_participant`, among the
/// ACTIVE and PAUSED rooms of the caller's organization; step 3 through
/// `find_room_ids_by_channel`, the ACTIVE rooms bound to the channel and
/// the PAUSED ones bound as a `group`. The one room step 3 reads is read
/// whole, with who it heard on the channel, and admits the sender as the
/// rules say ([`rules::routing`](roomkit_core::rules::routing)), comparing
/// addresses in the form of the channel `channel_of` gives; without one,
/// addresses compare as written and no binding's recipient names anyone. A strategy wrapping
/// the default calls its [`route`](InboundRoomRouter::route) for the rooms
/// it does not decide itself. A room step 3 refuses because its binding
/// holds a value under the recipient key that is not a recipient is logged
/// as a warning naming the binding and the value's type (RFC §5.7).
///
/// The kit's own default router reads its store and its registered
/// channels ([`RoomKit::get_channel`](crate::RoomKit::get_channel)).
pub struct DefaultInboundRoomRouter<S> {
    store: Arc<S>,
    channel_of: Option<ChannelOf>,
}

/// What [`DefaultInboundRoomRouter::new`] takes beside the store.
#[derive(Clone, Default)]
pub struct DefaultInboundRoomRouterOptions {
    /// The registered channel by its id; `None` reads none.
    pub channel_of: Option<ChannelOf>,
}

impl fmt::Debug for DefaultInboundRoomRouterOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DefaultInboundRoomRouterOptions")
            .field("channel_of", &self.channel_of.is_some())
            .finish()
    }
}

impl<S> fmt::Debug for DefaultInboundRoomRouter<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DefaultInboundRoomRouter")
            .field("channel_of", &self.channel_of.is_some())
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> DefaultInboundRoomRouter<S> {
    /// The default strategy over `store`.
    pub fn new(store: Arc<S>, options: DefaultInboundRoomRouterOptions) -> Self {
        Self {
            store,
            channel_of: options.channel_of,
        }
    }

    /// The query the rules read: the sender as routing names them, and the
    /// form of the channel's addresses.
    fn query(
        &self,
        channel_id: &ChannelId,
        channel_type: &ChannelType,
        options: RouteOptions,
    ) -> RouteQuery {
        let form = self
            .channel_of
            .as_ref()
            .and_then(|channel_of| channel_of(channel_id))
            .map_or_else(as_written, |channel| channel.form());
        let sender_id = options.participant_id.unwrap_or_default();
        let address =
            (!sender_id.is_empty() && sender_id != SYSTEM_SENDER).then(|| sender_id.clone());
        let identity_id = options
            .identity
            .as_ref()
            .map(|identity| identity.id.clone());
        RouteQuery {
            channel_id: channel_id.clone(),
            channel_type: channel_type.clone(),
            sender_id,
            metadata: options.channel_data.unwrap_or_default(),
            organization_id: options.organization_id,
            sender: Sender {
                address,
                identity_id,
                ..Sender::default()
            },
            form,
            identity: options.identity,
        }
    }
}

impl<S: ConversationStore> InboundRoomRouter for DefaultInboundRoomRouter<S> {
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        let query = self.query(channel_id, channel_type, options);
        Box::pin(async move {
            let store = &*self.store;
            if let Some(room_id) = senders_room(store, &query).await? {
                return Ok(Some(room_id));
            }
            let bound = step_three_rooms(store, &query).await?;
            let [sole] = bound.as_slice() else {
                if bound.len() > 1 {
                    tracing::debug!(
                        target: "roomkit::router",
                        channel_id = %query.channel_id,
                        "the channel is bound to several active or paused group rooms: the \
                         router does not guess which one the message belongs to"
                    );
                }
                return Ok(None);
            };
            let Some(candidate) = candidate(store, sole, &query).await? else {
                return Ok(None);
            };
            let routed = routing::routed(&query, &[candidate]);
            if let Some((room_id, Refusal::NotARecipient(kind))) = &routed.refused {
                not_a_recipient(room_id, &query, kind);
            }
            Ok(routed.room_id)
        })
    }
}

/// The form of a channel nobody describes: addresses as written, no
/// recipient key.
fn as_written() -> AddressForm {
    AddressForm {
        addressing: Addressing::AsWritten,
        country_code: None,
        recipient_key: None,
    }
}

/// What the kit asks its router for the message `query` describes: Python's
/// `route(channel_id, channel_type, participant_id, channel_data, *,
/// organization_id, identity)`.
pub(crate) fn options_of(query: &RouteQuery) -> RouteOptions {
    let participant_id = if query.is_system() {
        Some(SYSTEM_SENDER.to_owned())
    } else {
        query.sender.address.clone()
    };
    RouteOptions {
        participant_id,
        channel_data: Some(query.metadata.clone()),
        organization_id: query.organization_id.clone(),
        identity: query.identity.clone(),
    }
}

/// Logs that step 3 refused `room_id` for `query`'s message because its
/// binding holds a value under the channel's recipient key that is not a
/// recipient, of JSON type `kind` (RFC §5.7 SHOULD): the binding and the
/// type, never the value, which may be a person's address.
fn not_a_recipient(room_id: &RoomId, query: &RouteQuery, kind: &str) {
    tracing::warn!(
        target: "roomkit::router",
        room_id = %room_id,
        channel_id = %query.channel_id,
        recipient_key = %query.form.recipient_key.as_deref().unwrap_or_default(),
        held = %kind,
        "the binding holds a value under its recipient key that is not a recipient: \
         routing reads it as someone else's address, so no sender the binding does not \
         name is admitted through it, and deliveries to it are refused no_recipient"
    );
}

/// Step 1 (RFC §10.4): the newest ACTIVE or PAUSED room of the caller's
/// organization that is the sender's conversation on the channel, as
/// Python's `_senders_room` reads it: the room whose binding of the
/// channel names the sender, as its correspondent or, naming none, as its
/// recipient under the channel's recipient key (RFC §5.7), else the room
/// holding a participant the sender is, reached through the channel (RFC
/// §5.5), the sender named by their address or the identity resolved for
/// it; `None` for a message with no address.
async fn senders_room<S: ConversationStore>(
    store: &S,
    query: &RouteQuery,
) -> Result<Option<RoomId>, StoreError> {
    let Some(address) = query.sender.address.as_deref() else {
        return Ok(None);
    };
    let step_one = RoomFilter {
        statuses: Some(vec![RoomStatus::Active, RoomStatus::Paused]),
        organization_id: query.organization_id.clone(),
    };
    let by_binding = FindRoomIdByBindingOptions {
        identity_id: query.sender.identity_id.clone(),
        room_filter: Some(step_one.clone()),
        recipient_key: query.form.recipient_key.clone(),
    };
    let found = store
        .find_room_id_by_binding(&query.channel_id, address, by_binding)
        .await?;
    if found.is_some() {
        return Ok(found);
    }
    let by_participant = FindRoomIdByParticipantOptions {
        identity_id: query.sender.identity_id.clone(),
        room_filter: Some(step_one),
    };
    store
        .find_room_id_by_participant(&query.channel_id, address, by_participant)
        .await
}

/// The rooms step 3 reads (RFC §10.4), as Python's `_step_three_rooms`:
/// up to two, the ACTIVE rooms bound to the channel, then the PAUSED ones
/// whose binding of it is `group`, a group's conversation being the
/// channel's; any other PAUSED room is no one's to be admitted into.
async fn step_three_rooms<S: ConversationStore>(
    store: &S,
    query: &RouteQuery,
) -> Result<Vec<RoomId>, StoreError> {
    let of = |status| RoomFilter {
        statuses: Some(vec![status]),
        organization_id: query.organization_id.clone(),
    };
    let active = FindRoomIdsByChannelOptions {
        room_filter: Some(of(RoomStatus::Active)),
        ..FindRoomIdsByChannelOptions::default()
    };
    let mut rooms = store
        .find_room_ids_by_channel(&query.channel_id, active)
        .await?;
    if rooms.len() > 1 {
        return Ok(rooms);
    }
    let paused_groups = FindRoomIdsByChannelOptions {
        room_filter: Some(of(RoomStatus::Paused)),
        group: Some(true),
        ..FindRoomIdsByChannelOptions::default()
    };
    rooms.extend(
        store
            .find_room_ids_by_channel(&query.channel_id, paused_groups)
            .await?,
    );
    rooms.truncate(2);
    Ok(rooms)
}

/// The one room step 3 reads, as the rules read it: its bindings, its
/// participants, and who it heard on the channel; `None` once the store
/// no longer holds it.
async fn candidate<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    query: &RouteQuery,
) -> Result<Option<Candidate>, StoreError> {
    let (room, bindings, participants) = store.load_room_context(room_id).await?;
    let Some(room) = room else {
        return Ok(None);
    };
    // A `group` binding admits whoever writes: its history is not read, as
    // Python's `admits_sender` returns before it scans.
    let group = bindings
        .iter()
        .any(|binding| binding.channel_id == query.channel_id && binding.group);
    let heard = if group {
        Vec::new()
    } else {
        let other = |author: &ParticipantId| {
            !query.sender.named_by(
                author.as_str(),
                &query.form,
                &query.channel_id,
                &participants,
            )
        };
        let page = |offset| {
            let page = received_page(&query.channel_id, offset, routing::HISTORY_PAGE);
            store.list_events(room_id, page)
        };
        heard_until_another(page, &query.channel_id, other).await?
    };
    let mut candidate = Candidate::new(RoomContext {
        bindings,
        participants,
        ..RoomContext::new(room)
    });
    candidate.heard = heard;
    Ok(Some(candidate))
}

/// The page of at most `limit` of a room's received events of `channel_id`
/// at `offset`.
pub(crate) fn received_page(channel_id: &ChannelId, offset: u64, limit: u64) -> ListEventsOptions {
    ListEventsOptions {
        offset,
        limit,
        event_filter: Some(EventFilter {
            source_channel_id: Some(channel_id.clone()),
            ..EventFilter::default()
        }),
        ..ListEventsOptions::default()
    }
}

/// Who the room heard on `channel_id`, its received events read a page at
/// a time from the oldest (`page`, by offset), up to the first participant
/// who is `other` than the sender, then no further, as Python's
/// `_heard_from_other` reads them (RMK-1100): the rule reads that one
/// other author as it reads them all.
async fn heard_until_another<P, Page>(
    mut page: P,
    channel_id: &ChannelId,
    other: impl Fn(&ParticipantId) -> bool,
) -> Result<Vec<ParticipantId>, StoreError>
where
    P: FnMut(u64) -> Page,
    Page: Future<Output = Result<Vec<RoomEvent>, StoreError>>,
{
    let mut heard = Vec::new();
    let mut offset = 0;
    loop {
        let events = page(offset).await?;
        if routing::read_heard_page(&events, channel_id, &other, &mut heard) {
            return Ok(heard);
        }
        offset += u64::try_from(events.len()).unwrap_or(u64::MAX);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::ready;

    use pretty_assertions::assert_eq;
    use roomkit_core::model::{EventSource, TextContent};

    use super::*;

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    /// `count` messages on `sms` from `author`.
    fn from(author: &str, count: usize) -> Vec<RoomEvent> {
        let mut source = EventSource::new(channel("sms"), ChannelType::SMS);
        source.participant_id = ParticipantId::new(author).ok();
        let room = RoomId::new("r1").unwrap();
        (0..count)
            .map(|_| RoomEvent::new(room.clone(), source.clone(), TextContent::new("hi")))
            .collect()
    }

    /// RMK-1100, as Python's `_heard_from_other`: a page holding another
    /// author ends the scan, the later pages unread.
    #[tokio::test]
    async fn the_history_scan_stops_at_the_first_other_author() {
        let pages = [
            [from("alice", 150), from("bob", 1), from("alice", 49)].concat(),
            from("alice", 200),
            from("alice", 10),
        ];
        let reads = Cell::new(0_usize);
        let page = |offset: u64| {
            reads.set(reads.get() + 1);
            let index = usize::try_from(offset / routing::HISTORY_PAGE).unwrap();
            ready(Ok(pages[index].clone()))
        };
        let sms = channel("sms");
        let heard = heard_until_another(page, &sms, |author| author.as_str() != "alice");
        let heard = heard.await.unwrap();
        assert_eq!(
            (
                reads.get(),
                heard.len(),
                heard.last().map(ParticipantId::as_str)
            ),
            (1, 151, Some("bob"))
        );
    }
}
