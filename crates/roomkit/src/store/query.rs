//! Which events a timeline read or a count stands for (RFC §14.1, "Reading
//! a timeline"), in the Python implementation's terms.

use roomkit_core::model::{
    ChannelId, ChannelType, EventId, EventStatus, EventType, ParticipantId, RoomEvent, Timestamp,
    Visibility,
};

use super::StoreError;

/// The page of a room's timeline [`ConversationStore::list_events`] returns,
/// its arguments as the Python implementation's `list_events` names them
/// (RFC §14.1, "Reading a timeline").
///
/// - **Offset**: the oldest `limit` events after `offset` of them, or with
///   `newest_first` the newest `limit`, `offset` then counted back from
///   the newest end.
/// - **Cursor**: the oldest `limit` events with an index above
///   `after_index`, or the newest `limit` below `before_index`. The two
///   cursors are exclusive, and under one `offset` and `newest_first` are
///   ignored.
///
/// Whichever events the window selects, the page is in ascending index
/// order: its last event is its newest. The events are those
/// `event_filter` admits, only the ones the room received with none, and
/// of the `visibility_filter` visibility when the filter names none; the
/// criteria apply before the page is cut, so a page of `limit` events is
/// full whatever they left out around it.
///
/// ```
/// use roomkit::store::ListEventsOptions;
///
/// // A reconnecting reader's snapshot: the 50 newest events, oldest first.
/// let latest = ListEventsOptions { newest_first: true, ..ListEventsOptions::default() };
/// // The 50 before the oldest it holds, index 120.
/// let earlier = ListEventsOptions { before_index: Some(120), ..ListEventsOptions::default() };
/// # let _ = (latest, earlier);
/// ```
///
/// It is not `#[non_exhaustive]`, on purpose, as [`EventFilter`] is not: a
/// store reads every field, so a new field is a breaking change for stores,
/// while a caller that fills the rest with `..ListEventsOptions::default()` keeps
/// compiling.
///
/// [`ConversationStore::list_events`]: super::ConversationStore::list_events
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListEventsOptions {
    /// In offset mode, how many events the window skips, from the oldest
    /// end, or from the newest with `newest_first`.
    pub offset: u64,
    /// The most events the page holds: 50 by default.
    pub limit: u64,
    /// Only the events of this exact visibility, when `event_filter` names
    /// none: a value compared as it is, not what a channel may see
    /// ([`visible_events`](crate::visible_events) answers that).
    pub visibility_filter: Option<Visibility>,
    /// Which events the page may hold; `None` for every one the room
    /// received.
    pub event_filter: Option<EventFilter>,
    /// The page's events have an index above this one.
    pub after_index: Option<u64>,
    /// The page's events have an index below this one.
    pub before_index: Option<u64>,
    /// In offset mode, the window is the newest events rather than the
    /// oldest.
    pub newest_first: bool,
}

impl Default for ListEventsOptions {
    /// The oldest 50 events the room received.
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 50,
            visibility_filter: None,
            event_filter: None,
            after_index: None,
            before_index: None,
            newest_first: false,
        }
    }
}

impl ListEventsOptions {
    /// Refuses a query the read cannot serve, as the Python implementation
    /// raises: both cursors, which exclude each other, or a filter
    /// [`EventFilter::validate`] refuses. A store calls it first thing in
    /// `list_events`: outside the public API (decision 16).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Value`] saying what is wrong.
    #[doc(hidden)]
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.after_index.is_some() && self.before_index.is_some() {
            return Err(StoreError::Value(
                "after_index and before_index are mutually exclusive".to_owned(),
            ));
        }
        self.event_filter
            .as_ref()
            .map_or(Ok(()), EventFilter::validate)
    }

    /// Whether the page may hold `event`: the filter admits it, and its
    /// visibility is the one asked for when the filter names none.
    pub(crate) fn admits(&self, event: &RoomEvent) -> bool {
        let visibility = self
            .event_filter
            .as_ref()
            .and_then(|filter| filter.visibility.as_ref())
            .or(self.visibility_filter.as_ref());
        visibility.is_none_or(|visibility| &event.visibility == visibility)
            && self
                .event_filter
                .as_ref()
                .map_or(event.status != EventStatus::Blocked, |filter| {
                    filter.matches(event)
                })
    }
}

/// Which of a room's events a read or a count stands for (RFC §14.1
/// `EventFilter`), its criteria the Python implementation's: every one it
/// names must hold. A count with a filter counts exactly the rows
/// [`ConversationStore::list_events`] serves under it, with no page.
///
/// It is not `#[non_exhaustive]`, on purpose: a store reads every field,
/// so a new field is a breaking change on purpose. A store that
/// destructures it fails to compile until it reads the new field, rather
/// than silently reading too wide (a field narrowing a read to an
/// organization, say).
///
/// [`ConversationStore::list_events`]: super::ConversationStore::list_events
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// Only the events of these types.
    pub event_types: Option<Vec<EventType>>,
    /// None of the events of these types, whatever `event_types` says.
    pub exclude_types: Option<Vec<EventType>>,
    /// Only the events of this exact visibility.
    pub visibility: Option<Visibility>,
    /// Only the events from this channel.
    pub source_channel_id: Option<ChannelId>,
    /// Only the events from channels of this type.
    pub source_channel_type: Option<ChannelType>,
    /// Only the events of this correlation group (one AI response, say).
    pub correlation_id: Option<String>,
    /// Only the events of this participant, as their source names them.
    pub participant_id: Option<ParticipantId>,
    /// Only the answers to this event, those whose `responds_to` names it
    /// (RFC §8.5).
    pub responds_to: Option<EventId>,
    /// Only the replies of this thread root.
    pub parent_event_id: Option<EventId>,
    /// Only the events outside any thread: thread roots and events with no
    /// parent. It excludes `parent_event_id`.
    pub top_level_only: bool,
    /// Only the events created after this instant.
    pub after_time: Option<Timestamp>,
    /// Only the events created before this instant.
    pub before_time: Option<Timestamp>,
    /// Whether the events stored `BLOCKED` count; a timeline read serves
    /// what the room received by default.
    pub include_blocked: bool,
}

impl EventFilter {
    /// Refuses a filter that can admit nothing by construction, as the
    /// Python implementation's model refuses it: `after_time` not before
    /// `before_time`, or `top_level_only` with a `parent_event_id`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Value`] saying what is wrong.
    #[doc(hidden)]
    pub fn validate(&self) -> Result<(), StoreError> {
        if let (Some(after), Some(before)) = (self.after_time, self.before_time)
            && after >= before
        {
            return Err(StoreError::Value(
                "after_time must be before before_time".to_owned(),
            ));
        }
        if self.top_level_only && self.parent_event_id.is_some() {
            return Err(StoreError::Value(
                "top_level_only and parent_event_id are mutually exclusive".to_owned(),
            ));
        }
        Ok(())
    }

    /// Whether `event` passes the filter. A store reads it: outside the
    /// public API (decision 16).
    #[doc(hidden)]
    pub fn matches(&self, event: &RoomEvent) -> bool {
        let Self {
            event_types,
            exclude_types,
            visibility,
            source_channel_id,
            source_channel_type,
            correlation_id,
            participant_id,
            responds_to,
            parent_event_id,
            top_level_only,
            after_time,
            before_time,
            include_blocked,
        } = self;
        let source = &event.source;
        event_types
            .as_ref()
            .is_none_or(|types| types.contains(&event.r#type))
            && exclude_types
                .as_ref()
                .is_none_or(|types| !types.contains(&event.r#type))
            && visibility
                .as_ref()
                .is_none_or(|visibility| &event.visibility == visibility)
            && source_channel_id
                .as_ref()
                .is_none_or(|channel| &source.channel_id == channel)
            && source_channel_type
                .as_ref()
                .is_none_or(|kind| &source.channel_type == kind)
            && correlation_id
                .as_ref()
                .is_none_or(|id| event.correlation_id.as_ref() == Some(id))
            && participant_id
                .as_ref()
                .is_none_or(|id| source.participant_id.as_ref() == Some(id))
            && responds_to
                .as_ref()
                .is_none_or(|id| event.responds_to.as_ref() == Some(id))
            && parent_event_id
                .as_ref()
                .is_none_or(|id| event.parent_event_id.as_ref() == Some(id))
            && (!top_level_only || event.parent_event_id.is_none())
            && after_time.is_none_or(|after| event.created_at > after)
            && before_time.is_none_or(|before| event.created_at < before)
            && (*include_blocked || event.status != EventStatus::Blocked)
    }
}
