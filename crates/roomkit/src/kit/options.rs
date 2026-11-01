//! What a kit is built with, the Python implementation's `RoomKit`
//! constructor keywords for every feature this crate has (step 5,
//! decision 2), and what each of its calls takes beside its required
//! arguments: one `<Call>Options` per call, Python's defaulted parameters
//! with their names and defaults (decision 1).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{
    Access, AgentResponsePolicy, ChannelBinding, ChannelCategory, ChannelDirection, ChannelId,
    DirectEvent, EventContent, EventId, EventSource, EventType, Metadata, ParticipantId, Post,
    RateLimit, RetryPolicy, RoomId, RoomTimers, Timestamp, Visibility,
};

use crate::router::InboundRoomRouter;
use crate::store::{EventFilter, InMemoryStore, ListEventsOptions};

/// Python's `max_chain_depth=5` (RFC §8.3).
const MAX_CHAIN_DEPTH: u32 = 5;

/// Python's `process_timeout=30.0` (RFC §13.6).
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);

/// What [`RoomKit::new`](crate::RoomKit::new) builds a kit with, Python's
/// `RoomKit(store=None, inbound_router=None, max_chain_depth=5,
/// agent_response_policy=AGENT_CHAIN, process_timeout=30.0)`.
///
/// The in-memory store, with one keyword changed:
///
/// ```
/// use roomkit::{RoomKit, RoomKitOptions};
///
/// let kit = RoomKit::new(RoomKitOptions { max_chain_depth: 3, ..Default::default() });
/// # let _ = kit;
/// ```
///
/// A struct update cannot change the store's type, so a store of another
/// type comes through [`with_store`](Self::with_store):
/// `RoomKit::new(RoomKitOptions { process_timeout, ..RoomKitOptions::with_store(store) })`.
pub struct RoomKitOptions<S = InMemoryStore> {
    /// Python `store=None`: where rooms and their timelines live (RFC §14),
    /// an in-memory store by default. The kit shares it, as Python's object
    /// is shared: a router built over it reads what the kit writes
    /// ([`DefaultInboundRoomRouter::new`](crate::DefaultInboundRoomRouter::new)).
    pub store: Arc<S>,
    /// Python `inbound_router=None`: the strategy that finds the room of a
    /// message that names none (RFC §10.4); `None` is the default strategy
    /// over the kit's store and its registered channels.
    pub inbound_router: Option<Arc<dyn InboundRoomRouter>>,
    /// Python `max_chain_depth=5`: the chain-depth limit (RFC §8.3).
    pub max_chain_depth: u32,
    /// Python `agent_response_policy=AgentResponsePolicy.AGENT_CHAIN`: what
    /// an agent's own output solicits in a room created without a policy
    /// of its own (RFC §19.3.1).
    pub agent_response_policy: AgentResponsePolicy,
    /// Python `process_timeout=30.0`: how long an inbound call may take from
    /// its start to its decision (RFC §13.6). Past it, the call writes
    /// nothing of its event, lets go of what it holds and returns `blocked`
    /// with the reason `process_timeout`, after the `process_timeout`
    /// framework event. The commit and what follows it, the event's delivery
    /// and its responses, are never bounded. The bound counts from the
    /// call's start; the waits for the room's admission ticket and its lock
    /// count against it, the wait for the event's turn in the room's
    /// delivery lane, after the commit, does not (gap G-068).
    pub process_timeout: Duration,
    /// Python `realtime=None`: the backend of the kit's ephemeral events
    /// (RFC §8.4), an [`InMemoryRealtime`](crate::realtime::InMemoryRealtime)
    /// by default; the kit's close closes it.
    pub realtime: Option<Arc<dyn crate::realtime::RealtimeBackend>>,
}

/// Python's defaults, over a new in-memory store. Defined for the in-memory
/// store alone, so that `..Default::default()` names the kit's store type.
impl Default for RoomKitOptions {
    fn default() -> Self {
        Self::with_store(InMemoryStore::new())
    }
}

impl<S> RoomKitOptions<S> {
    /// Python's defaults over `store`, for a store of another type than the
    /// in-memory one.
    pub fn with_store(store: S) -> Self {
        Self {
            store: Arc::new(store),
            inbound_router: None,
            max_chain_depth: MAX_CHAIN_DEPTH,
            agent_response_policy: AgentResponsePolicy::AgentChain,
            process_timeout: PROCESS_TIMEOUT,
            realtime: None,
        }
    }
}

impl<S> fmt::Debug for RoomKitOptions<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomKitOptions")
            .field("inbound_router", &self.inbound_router.is_some())
            .field("max_chain_depth", &self.max_chain_depth)
            .field("agent_response_policy", &self.agent_response_policy)
            .field("process_timeout", &self.process_timeout)
            .field("realtime", &self.realtime.is_some())
            .finish_non_exhaustive()
    }
}

/// Writes the options struct of each call whose one defaulted Python
/// parameter is `organization_id` (step 5, convention 2).
macro_rules! scoped_options {
    ($($(#[$doc:meta])* $name:ident;)+) => {$(
        $(#[$doc])*
        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct $name {
            /// Python `organization_id=None`: the organization the call
            /// acts for (RFC §17.2). A room of another organization, or of
            /// none, is not found; `None` reads rooms unscoped.
            pub organization_id: Option<String>,
        }
    )+};
}

scoped_options! {
    /// What [`RoomKit::get_room`](crate::RoomKit::get_room) takes beside the
    /// room: Python's `get_room(room_id, *, organization_id=None)`.
    GetRoomOptions;
    /// What [`RoomKit::close_room`](crate::RoomKit::close_room) takes beside
    /// the room.
    CloseRoomOptions;
    /// What [`RoomKit::archive_room`](crate::RoomKit::archive_room) takes
    /// beside the room.
    ArchiveRoomOptions;
    /// What [`RoomKit::pause_room`](crate::RoomKit::pause_room) takes beside
    /// the room.
    PauseRoomOptions;
    /// What [`RoomKit::resume_room`](crate::RoomKit::resume_room) takes
    /// beside the room.
    ResumeRoomOptions;
    /// What [`RoomKit::reopen_room`](crate::RoomKit::reopen_room) takes
    /// beside the room.
    ReopenRoomOptions;
    /// What [`RoomKit::check_all_timers`](crate::RoomKit::check_all_timers)
    /// takes: the organization whose rooms it checks.
    CheckAllTimersOptions;
    /// What [`RoomKit::check_room_timers`](crate::RoomKit::check_room_timers)
    /// takes beside the room.
    CheckRoomTimersOptions;
    /// What [`RoomKit::detach_channel`](crate::RoomKit::detach_channel)
    /// takes beside the room and the channel.
    DetachChannelOptions;
    /// What [`RoomKit::mute`](crate::RoomKit::mute) takes beside the room and
    /// the channel.
    MuteOptions;
    /// What [`RoomKit::unmute`](crate::RoomKit::unmute) takes beside the room
    /// and the channel.
    UnmuteOptions;
    /// What [`RoomKit::set_access`](crate::RoomKit::set_access) takes beside
    /// the room, the channel and the access.
    SetAccessOptions;
    /// What [`RoomKit::set_visibility`](crate::RoomKit::set_visibility) takes
    /// beside the room, the channel and the visibility.
    SetVisibilityOptions;
    /// What [`RoomKit::update_binding_metadata`](crate::RoomKit::update_binding_metadata)
    /// takes beside the room, the channel and the metadata.
    UpdateBindingMetadataOptions;
    /// What [`RoomKit::get_binding`](crate::RoomKit::get_binding) takes
    /// beside the room and the channel.
    GetBindingOptions;
    /// What [`RoomKit::list_bindings`](crate::RoomKit::list_bindings) takes
    /// beside the room.
    ListBindingsOptions;
    /// What [`RoomKit::commit_event`](crate::RoomKit::commit_event) takes
    /// beside the room and the record.
    CommitEventOptions;
}

/// What [`RoomKit::create_room`](crate::RoomKit::create_room) takes, Python's
/// `create_room(room_id=None, metadata=None, ..., organization_id=None,
/// timers=None, agent_response_policy=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateRoomOptions {
    /// The room's id; `None`, a generated one.
    pub room_id: Option<RoomId>,
    /// Integrator-defined data. A key only the framework writes, under
    /// `roomkit:` or of the room's private state, is dropped with a warning
    /// (RFC §5.1).
    pub metadata: Metadata,
    /// The organization the room belongs to (RFC §17.2); `None`, none.
    pub organization_id: Option<String>,
    /// The room's automatic transitions (RFC §5.1); `None`, none.
    pub timers: Option<RoomTimers>,
    /// What an agent's own output solicits in the room (RFC §19.3.1);
    /// `None`, the kit's ([`RoomKitOptions::agent_response_policy`]).
    pub agent_response_policy: Option<AgentResponsePolicy>,
}

/// What [`RoomKit::attach_channel`](crate::RoomKit::attach_channel) takes
/// beside the room and the channel, Python's `attach_channel(room_id,
/// channel_id, category=None, access=READ_WRITE, visibility=ALL,
/// organization_id=None, **kwargs)`, its `**kwargs` the binding's fields
/// they reach. The binding's capabilities are the channel's.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AttachChannelOptions {
    /// How the channel takes part; `None`, the channel's own category.
    pub category: Option<ChannelCategory>,
    /// Whether the channel reads, writes, both or neither (RFC §7.1):
    /// read-write.
    pub access: Access,
    /// Who sees what the channel writes (RFC §7.3): all. `internal` is the
    /// framework's only, and refuses the attach (gap G-041).
    pub visibility: Visibility,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
    /// Whether the channel is delivered to (RFC §4.2); `None`, the
    /// channel's own direction.
    pub direction: Option<ChannelDirection>,
    /// Temporarily silenced: its response events are not broadcast (RFC
    /// §7.2).
    pub muted: bool,
    /// Speech output muted: events still arrive, nothing is spoken.
    pub output_muted: bool,
    /// The binding's correspondent (RFC §10.4).
    pub participant_id: Option<ParticipantId>,
    /// Several senders share the binding's conversation (RFC §10.4).
    pub group: bool,
    /// The channel's read horizon.
    pub last_read_index: Option<u64>,
    /// When the channel was attached; `None`, now.
    pub attached_at: Option<Timestamp>,
    /// The binding's rate limit.
    pub rate_limit: Option<RateLimit>,
    /// The binding's retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Binding-specific data: a transport's recipient, under the key the
    /// channel's declaration names (`phone_number`, `email_address`, ...),
    /// a persona.
    pub metadata: Metadata,
}

impl AttachChannelOptions {
    /// `binding`, the channel's default binding to the room, with these
    /// options: its channel, room, type and capabilities stay `binding`'s.
    pub(crate) fn apply(self, binding: ChannelBinding) -> ChannelBinding {
        ChannelBinding {
            category: self.category.unwrap_or(binding.category),
            direction: self.direction.unwrap_or(binding.direction),
            access: self.access,
            muted: self.muted,
            output_muted: self.output_muted,
            visibility: self.visibility,
            participant_id: self.participant_id,
            group: self.group,
            last_read_index: self.last_read_index,
            attached_at: self.attached_at.unwrap_or(binding.attached_at),
            rate_limit: self.rate_limit,
            retry_policy: self.retry_policy,
            metadata: self.metadata,
            ..binding
        }
    }
}

/// What [`RoomKit::list_members`](crate::RoomKit::list_members) takes beside
/// the room.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListMembersOptions {
    /// Python `include_left=False`: the participants who left or were
    /// removed too, not the ACTIVE ones alone.
    pub include_left: bool,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
}

/// What [`RoomKit::process_inbound`](crate::RoomKit::process_inbound) takes
/// beside the message, Python's `process_inbound(message, *, room_id=None,
/// defer_delivery=False, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessInboundOptions {
    /// The room the message goes into; `None`, the room the kit's router
    /// finds for it (RFC §10.4).
    pub room_id: Option<RoomId>,
    /// Return at the commit instead of once the event's delivery set ran
    /// (RFC §10.1 step 18, detached completion): the blocking decisions, a
    /// hook's refusal among them, are still made before the call returns,
    /// and the result's [`delivery`](crate::InboundResult::delivery) waits
    /// for the rest, its delivery results included. `false` by default.
    pub defer_delivery: bool,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2): a room it creates belongs to it.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::send_event`](crate::RoomKit::send_event) takes beside the
/// room, the channel and the content, Python's `send_event` keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SendEventOptions {
    /// Its type: `message`, `system`, `instruction` (RFC §10.1.1), `edit` or
    /// `delete` (RFC §10.3), ...; `message` by default.
    pub event_type: EventType,
    /// The chain it continues (RFC §8.3), 0 opening one.
    pub chain_depth: u32,
    /// The participant behind it, when there is one.
    pub participant_id: Option<ParticipantId>,
    /// Extra data.
    pub metadata: Metadata,
    /// Who may see it (RFC §7.3): all. `internal` is the framework's alone
    /// (gap G-041).
    pub visibility: Visibility,
    /// The provider or backend behind it, written on its source (RFC §5.2);
    /// `None`, the channel's own.
    pub provider: Option<String>,
    /// Where its answers may go (RFC §5.12, gap G-073); `None` restricts
    /// nothing. Never `internal` (gap G-041).
    pub response_visibility: Option<Visibility>,
    /// When it was created; `None`, now.
    pub created_at: Option<Timestamp>,
    /// The event it replies to, in its thread; the pipeline stores its
    /// thread's root (RFC §10.1 step 8).
    pub parent_event_id: Option<EventId>,
    /// The key that prevents a duplicate, per room and channel (RFC §13.4,
    /// gap G-023).
    pub idempotency_key: Option<String>,
    /// The intelligence channels asked to act; `None` for any eligible one
    /// (RFC §19.3).
    pub addressed_to: Option<Vec<ChannelId>>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
    /// For an instruction only: its turn reads nothing of the room (RFC
    /// §10.1.1 rule 7).
    pub standalone: bool,
}

impl SendEventOptions {
    /// The direct event these options make of `content` from `channel_id`.
    pub(crate) fn event(self, channel_id: ChannelId, content: EventContent) -> DirectEvent {
        DirectEvent {
            event_type: self.event_type,
            chain_depth: self.chain_depth,
            participant_id: self.participant_id,
            metadata: self.metadata,
            visibility: self.visibility,
            provider: self.provider,
            response_visibility: self.response_visibility,
            created_at: self.created_at,
            parent_event_id: self.parent_event_id,
            idempotency_key: self.idempotency_key,
            addressed_to: self.addressed_to,
            standalone: self.standalone,
            ..DirectEvent::new(channel_id, content)
        }
    }
}

/// What [`RoomKit::post`](crate::RoomKit::post) takes beside the room and
/// the content, Python's `post` keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostOptions {
    /// Who speaks: a participant of the room the host channel reaches, by
    /// their id or their identity's (RFC §10.5, §5.5); `None` for the host
    /// itself.
    pub participant_id: Option<ParticipantId>,
    /// The intelligence channels asked to answer; none by default: the
    /// host's words solicit no agent unless they name one (RFC §19.3).
    pub addressed_to: Vec<ChannelId>,
    /// Where it comes from: a workflow, its step, a job.
    pub metadata: Metadata,
    /// The registered host channel it speaks from; `None` for the kit's
    /// own, registered on first use under the id `host`.
    pub channel_id: Option<ChannelId>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
    /// The key that makes a retry a replay, per room and host channel (RFC
    /// §13.4); a blank key is no key.
    pub idempotency_key: Option<String>,
    /// The chain it continues (RFC §8.3), 0 opening one.
    pub chain_depth: u32,
}

impl PostOptions {
    /// The post these options make of `content`.
    pub(crate) fn post(self, content: EventContent) -> Post {
        Post {
            participant_id: self.participant_id,
            addressed_to: self.addressed_to,
            metadata: self.metadata,
            channel_id: self.channel_id,
            idempotency_key: self.idempotency_key,
            chain_depth: self.chain_depth,
            ..Post::new(content)
        }
    }
}

/// What [`RoomKit::update_event`](crate::RoomKit::update_event) takes beside
/// the room and the event: what it writes, each field whole.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateEventOptions {
    /// The event's new content.
    pub content: Option<EventContent>,
    /// The event's new source.
    pub source: Option<EventSource>,
    /// The event's new metadata, replacing the stored map whole; `None`
    /// leaves it as it is.
    pub metadata: Option<Metadata>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
}

/// What [`RoomKit::delete_event`](crate::RoomKit::delete_event) takes beside
/// the room and the event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteEventOptions {
    /// Python `cascade_replies=True`: the replies one level deep go too.
    pub cascade_replies: bool,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
}

impl Default for DeleteEventOptions {
    fn default() -> Self {
        Self {
            cascade_replies: true,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::get_timeline`](crate::RoomKit::get_timeline) reads,
/// Python's `get_timeline` keywords and defaults: the oldest 50 events the
/// room received.
///
/// ```
/// use roomkit::GetTimelineOptions;
///
/// // A reconnect snapshot: the 50 newest events, oldest first.
/// let snapshot = GetTimelineOptions { newest_first: true, ..Default::default() };
/// # let _ = snapshot;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTimelineOptions {
    /// In offset mode, how many events the window skips, counted from the
    /// newest end with `newest_first`.
    pub offset: u64,
    /// The most events the page holds: 50 by default.
    pub limit: u64,
    /// Only the events of this exact visibility.
    pub visibility_filter: Option<Visibility>,
    /// The page's events have an index above this one.
    pub after_index: Option<u64>,
    /// The page's events have an index below this one.
    pub before_index: Option<u64>,
    /// In offset mode, the newest `limit` events rather than the oldest.
    pub newest_first: bool,
    /// Serve the events the room refused too (status `BLOCKED`), for an
    /// audit.
    pub include_blocked: bool,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2).
    pub organization_id: Option<String>,
}

impl Default for GetTimelineOptions {
    /// The oldest 50 events the room received.
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 50,
            visibility_filter: None,
            after_index: None,
            before_index: None,
            newest_first: false,
            include_blocked: false,
            organization_id: None,
        }
    }
}

impl GetTimelineOptions {
    /// The store read it stands for.
    pub(crate) fn query(&self) -> ListEventsOptions {
        ListEventsOptions {
            offset: self.offset,
            limit: self.limit,
            visibility_filter: self.visibility_filter.clone(),
            event_filter: self.include_blocked.then(|| EventFilter {
                include_blocked: true,
                ..EventFilter::default()
            }),
            after_index: self.after_index,
            before_index: self.before_index,
            newest_first: self.newest_first,
        }
    }
}
