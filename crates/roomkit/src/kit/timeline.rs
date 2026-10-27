//! The kit's timeline read (RFC §14.1, "Reading a timeline"), the Python
//! implementation's `get_timeline`.

use roomkit_core::model::{RoomEvent, RoomId, Visibility};

use super::RoomKit;
use crate::error::KitError;
use crate::store::{ConversationStore, EventFilter, EventQuery};

/// What [`RoomKit::get_timeline`] reads, its arguments as the Python
/// implementation's `get_timeline` names them, with its defaults: the
/// oldest 50 events the room received. The modes and the order of the
/// page are [`EventQuery`]'s.
///
/// ```
/// use roomkit::TimelineQuery;
///
/// // A reconnect snapshot: the 50 newest events, oldest first.
/// let snapshot = TimelineQuery { newest_first: true, ..TimelineQuery::default() };
/// # let _ = snapshot;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineQuery {
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
}

impl Default for TimelineQuery {
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
        }
    }
}

impl TimelineQuery {
    /// The store read it stands for.
    fn to_store(&self) -> EventQuery {
        let Self {
            offset,
            limit,
            visibility_filter,
            after_index,
            before_index,
            newest_first,
            include_blocked,
        } = self.clone();
        EventQuery {
            offset,
            limit,
            visibility_filter,
            event_filter: include_blocked.then(|| EventFilter {
                include_blocked: true,
                ..EventFilter::default()
            }),
            after_index,
            before_index,
            newest_first,
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// A page of the room `room_id`'s timeline, in ascending index order
    /// (RFC §14.1, "Reading a timeline"), as `query` asks: the Python
    /// implementation's `get_timeline`. A read for an organization goes
    /// through [`OrganizationKit::get_timeline`](crate::OrganizationKit::get_timeline).
    ///
    /// What a channel may see of the page is [`visible_events`](crate::visible_events)'s
    /// answer, given the room's bindings.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] with [`StoreError::RoomNotFound`] when the
    /// room does not exist, with [`StoreError::InvalidArgument`] when `query`
    /// names both cursors, and when the store fails the read.
    ///
    /// [`StoreError::RoomNotFound`]: crate::StoreError::RoomNotFound
    /// [`StoreError::InvalidArgument`]: crate::StoreError::InvalidArgument
    pub async fn get_timeline(
        &self,
        room_id: &RoomId,
        query: &TimelineQuery,
    ) -> Result<Vec<RoomEvent>, KitError> {
        Ok(self.store().list_events(room_id, &query.to_store()).await?)
    }
}
