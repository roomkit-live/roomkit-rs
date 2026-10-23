//! Inbound room routing (RFC §10.4): which room a message that names none
//! belongs to.
//!
//! The kit asks its router through the core's `Route` request, as it runs a
//! hook, after the core has read the identity the sender's address resolves
//! to. [`DefaultRouter`] applies the RFC's default strategy
//! ([`rules::routing`](roomkit_core::rules::routing)) to the rooms the
//! message's channel reaches, read from the store; an integrator gives the
//! kit a strategy of its own with
//! [`RoomKit::set_router`](crate::RoomKit::set_router).

use std::error::Error;
use std::future::Future;

use roomkit_core::model::{RoomId, RoomStatus};
use roomkit_core::rules::routing::{self, Candidate, RouteQuery};

use crate::channel::BoxFuture;
use crate::store::{ConversationStore, RoomFilter, StoreError};

/// What a router failed with. The message fails with it, before anything is
/// written (RFC §10.4).
pub type RouterError = Box<dyn Error + Send + Sync>;

/// A routing strategy (RFC §10.4 `InboundRoomRouter`): the room a message
/// that names none belongs to, or `None` for a new room.
///
/// Implement it with `async fn`, for any store, or for the store of your
/// kit. The router answers with a room's id, which step 2 treats as a room
/// id in hand (RFC §10.1 step 2): a room of another organization than the
/// caller's fails the message, and a missing room is created with the id.
/// Unlike a room the caller names, the framework then records the sender on
/// the room's binding of the channel under the room lock; a room whose
/// binding names someone else by then, that is no longer ACTIVE, or that step
/// 3 no longer admits the sender to (someone else heard there meanwhile), is not
/// the message's, which is routed once more, then given a new room (gap
/// G-055), never refused `room_closed`. A strategy that sends several
/// senders to one room declares that room's binding `group`.
///
/// A strategy MUST NOT guess among several rooms, and MUST answer the same
/// for the same stored state (RFC §10.4).
///
/// ```
/// use roomkit::model::RoomId;
/// use roomkit::rules::routing::RouteQuery;
/// use roomkit::{ConversationStore, InboundRoomRouter, RouterError};
///
/// /// Routes a message to the room its `ticket` metadata names, when the
/// /// room exists; any other message gets a new room.
/// struct ByTicket;
///
/// impl<S: ConversationStore> InboundRoomRouter<S> for ByTicket {
///     async fn route(&self, store: &S, query: &RouteQuery) -> Result<Option<RoomId>, RouterError> {
///         let Some(ticket) = query.metadata.get("ticket").and_then(|ticket| ticket.as_str()) else {
///             return Ok(None);
///         };
///         let room = store.get_room(&RoomId::new(ticket)?).await?;
///         Ok(room.map(|room| room.id))
///     }
/// }
/// ```
pub trait InboundRoomRouter<S: ConversationStore>: Send + Sync + 'static {
    /// The room `query`'s message belongs to, `None` for a new room.
    fn route(
        &self,
        store: &S,
        query: &RouteQuery,
    ) -> impl Future<Output = Result<Option<RoomId>, RouterError>> + Send;
}

/// An [`InboundRoomRouter`] behind a trait object, so the kit holds a
/// router of any type.
pub(crate) trait DynRouter<S>: Send + Sync {
    fn route<'call>(
        &'call self,
        store: &'call S,
        query: &'call RouteQuery,
    ) -> BoxFuture<'call, Result<Option<RoomId>, RouterError>>;
}

impl<S: ConversationStore, R: InboundRoomRouter<S>> DynRouter<S> for R {
    fn route<'call>(
        &'call self,
        store: &'call S,
        query: &'call RouteQuery,
    ) -> BoxFuture<'call, Result<Option<RoomId>, RouterError>> {
        Box::pin(InboundRoomRouter::route(self, store, query))
    }
}

/// The RFC's default routing strategy over the kit's store (RFC §10.4): the
/// sender's newest conversation on the channel, else the one ACTIVE room
/// bound to the channel when it admits the sender, else a new room.
///
/// It reads every ACTIVE room of the caller's organization the channel
/// reaches, with its bindings and participants, and what the room has heard
/// on the channel when it is the one room bound to it; the rules of
/// [`rules::routing`](roomkit_core::rules::routing) decide on what it read,
/// comparing addresses in the channel's form. A message is routed in a
/// number of store reads that grows with the rooms its channel reaches.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRouter;

impl<S: ConversationStore> InboundRoomRouter<S> for DefaultRouter {
    async fn route(&self, store: &S, query: &RouteQuery) -> Result<Option<RoomId>, RouterError> {
        let candidates = Self::candidates(store, query).await?;
        Ok(routing::route(query, &candidates))
    }
}

impl DefaultRouter {
    /// The rooms the default strategy reads for `query`: every ACTIVE room
    /// of the caller's organization the channel reaches, binding it or
    /// holding a participant reached through it, with what the room heard on
    /// the channel when it is the one room bound to it.
    ///
    /// # Errors
    ///
    /// Returns the store's error when a read fails.
    pub async fn candidates<S: ConversationStore>(
        store: &S,
        query: &RouteQuery,
    ) -> Result<Vec<Candidate>, StoreError> {
        let filter = RoomFilter {
            organization_id: query.organization_id.clone(),
            status: Some(RoomStatus::Active),
            channel_id: Some(query.channel_id.clone()),
            ..RoomFilter::default()
        };
        let mut candidates = Vec::new();
        for room in store.list_rooms(&filter).await? {
            if let Some(context) = store.load_context(&room.id, 0).await? {
                candidates.push(Candidate::new(context));
            }
        }
        let considered: Vec<&Candidate> = candidates
            .iter()
            .filter(|candidate| routing::considered(query, &candidate.room))
            .collect();
        let Some(sole) = routing::sole_bound(query, &considered).map(|sole| sole.room.id.clone())
        else {
            return Ok(candidates);
        };
        let events = store.list_events(&sole, false).await?;
        if let Some(candidate) = candidates
            .iter_mut()
            .find(|candidate| candidate.room.id == sole)
        {
            candidate.heard = routing::heard(&events, &query.channel_id);
        }
        Ok(candidates)
    }
}
