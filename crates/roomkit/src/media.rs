//! The application's readers of an author's media (RFC §6.4): Python's
//! `MediaResolver` and `AttachedMedia`, handed to the kit
//! (`RoomKitOptions::media_resolver`, `RoomKitOptions::attached_media`),
//! which serve every AI channel registered on it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{MediaContent, RoomEvent, RoomId};

use crate::ChannelError;

/// What an [`AttachedMedia`] reader calls: the media an event names.
type Reading = dyn Fn(&RoomEvent) -> Result<Vec<MediaContent>, ChannelError> + Send + Sync;

/// What a [`MediaResolver`]'s call is: a future of the bytes, or of none.
type Resolving = Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, ChannelError>> + Send>>;

/// An application's resolver (RFC §6.4), Python's `MediaResolver`:
/// `(media, room_id)` to the bytes of the media a reference names, or
/// `None` when it has none to give. Handed to the kit
/// ([`RoomKitOptions::media_resolver`](crate::RoomKitOptions::media_resolver)),
/// it serves every AI channel registered on it, asked afresh as each turn
/// is built: the store and every broadcast carry the reference alone. Any
/// participant can write a reference: answer only for a media the room
/// `room_id` may show, `None` for any other. A resolver that fails, Python's
/// that raises, gives none: the channel logs it and reads an image
/// unavailable, never a failed turn.
///
/// ```
/// use roomkit::MediaResolver;
///
/// let resolver = MediaResolver::new(|media, _room_id| async move {
///     Ok((media.url == "app-media://photo-1").then(|| vec![1, 2, 3, 4]))
/// });
/// # let _ = resolver;
/// ```
#[derive(Clone)]
pub struct MediaResolver(Arc<dyn Fn(MediaContent, RoomId) -> Resolving + Send + Sync>);

impl MediaResolver {
    /// The resolver calling `resolve` with each media and its room.
    pub fn new<F, Fut>(resolve: F) -> Self
    where
        F: Fn(MediaContent, RoomId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Vec<u8>>, ChannelError>> + Send + 'static,
    {
        Self(Arc::new(move |media, room_id| {
            Box::pin(resolve(media, room_id))
        }))
    }

    /// The bytes of `media` for the room `room_id`: Python's
    /// `await resolver(media, room_id)`.
    pub fn call(
        &self,
        media: MediaContent,
        room_id: RoomId,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, ChannelError>> + Send + 'static {
        (self.0)(media, room_id)
    }
}

impl std::fmt::Debug for MediaResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MediaResolver")
    }
}

/// An application's reader of the media an event carries outside its
/// content (RFC §6.4), Python's `AttachedMedia`: `event` to the media it
/// names, the same ones in the same order every time it is asked, since an
/// image is known by its event and its place. Handed to the kit
/// ([`RoomKitOptions::attached_media`](crate::RoomKitOptions::attached_media)),
/// it serves every AI channel registered on it: each reads the events of a
/// turn with the media named after their own content, the store and the
/// broadcasts keeping the events as stored. A reader that fails, Python's
/// that raises, names none for that event: the channel logs it and reads
/// the event as it is, never a failed turn.
///
/// ```
/// use roomkit::AttachedMedia;
/// use roomkit::model::MediaContent;
///
/// let reader = AttachedMedia::new(|event| {
///     let Some(url) = event.metadata.get("photo").and_then(|url| url.as_str()) else {
///         return Ok(Vec::new());
///     };
///     Ok(vec![MediaContent::new(url, "image/png")?])
/// });
/// # let _ = reader;
/// ```
#[derive(Clone)]
pub struct AttachedMedia(Arc<Reading>);

impl AttachedMedia {
    /// The reader calling `read` with each event.
    pub fn new<F>(read: F) -> Self
    where
        F: Fn(&RoomEvent) -> Result<Vec<MediaContent>, ChannelError> + Send + Sync + 'static,
    {
        Self(Arc::new(read))
    }

    /// The media `event` carries outside its content: Python's
    /// `reader(event)`.
    ///
    /// # Errors
    ///
    /// The reader's own failure, which the channel logs, naming none.
    pub fn call(&self, event: &RoomEvent) -> Result<Vec<MediaContent>, ChannelError> {
        (self.0)(event)
    }
}

impl std::fmt::Debug for AttachedMedia {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AttachedMedia")
    }
}

/// The kit's readers of an author's media, which its AI channels' turns
/// read (RFC §6.4).
#[derive(Clone, Debug, Default)]
pub(crate) struct KitMedia {
    pub(crate) media_resolver: Option<MediaResolver>,
    pub(crate) attached_media: Option<AttachedMedia>,
}
