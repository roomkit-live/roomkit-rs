//! What an image an author sent becomes in a model's input (RFC §6.4):
//! Python's `channels/_author_images.py`.
//!
//! A media an event brings (alone, or a part of a composite) reaches a
//! model that sees as an image after its caption, with the words of the
//! event. Its URL is a data URI, a link handed to the provider as it is, or
//! a reference the host's [`ImageResolver`] turns into an image as the turn
//! is built: the store and every broadcast carry the reference alone, and
//! nothing is kept from one turn to the next.
//!
//! Per turn the newest `max_author_images` images go, history and input
//! counted together, of a type every vision provider takes. Anything else
//! takes the image's place as a text mention, never a failed turn: a media
//! that is no such image reads as its label (RFC §6.6), an image for a
//! model that does not see as an image that model cannot see, an image past
//! the bound as an earlier image not shown, a reference that does not
//! resolve as an image unavailable, an image a provider refused as
//! refused. A request that carried author images and failed on a provider
//! error not retried marks them refused in the room ([`RefusedImages`]):
//! one the provider refused would fail every later turn while it stays in
//! the window.
//!
//! An application that keeps a message's media beside its content names
//! them through the kit's [`AttachedMedia`] reader, and the channel reads
//! the event with them as parts ([`with_attached_media`]); a reference
//! among an event's media resolves through the agent's [`ImageResolver`],
//! else the kit's [`MediaResolver`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{CompositeContent, EventContent, EventType, MediaContent, RoomEvent, RoomId};
use roomkit::{AttachedMedia, MediaResolver};
use roomkit_core::rules::edits::is_deleted;
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::transcript::{Image, Part, media_type, quoted};

use crate::error::AiError;
use crate::provider::ProviderError;
use crate::stream::join_all;

/// Host-owned, room-authorized resolution of an opaque image reference (RFC §6.4).
/// Return a resolved data/HTTP image, or `None` for missing/inaccessible content.
/// Neither the resolver nor returned image bytes are persisted by the runtime.
pub trait ImageResolver: Send + Sync + 'static {
    /// Resolve only references authorized for `room`. Called afresh each turn;
    /// the runtime never reads a local path or caches the result across turns.
    fn resolve<'a>(
        &'a self,
        image: &'a Image,
        room: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Option<Image>> + Send + 'a>>;
}

/// The newest author images a turn sends, history and input together:
/// Python's `DEFAULT_MAX_AUTHOR_IMAGES`.
pub(crate) const DEFAULT_MAX_AUTHOR_IMAGES: usize = 4;

/// The image types every vision provider takes: Python's
/// `VISION_IMAGE_TYPES`.
const VISION_IMAGE_TYPES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

const EARLIER: &str = "Earlier image not shown";
const NOT_SEEN: &str = "Image not shown, this model does not see images";
const UNAVAILABLE: &str = "Image unavailable";
const REFUSED: &str = "Image refused by the model's provider";
/// How many characters of an image's name a mention quotes.
const NAME_CHARS: usize = 120;
/// The rooms whose refused images a channel keeps, the oldest dropped
/// past it: Python's `_MAX_ROOMS`.
const MAX_ROOMS: usize = 100;
/// The statuses of an error that is not about a request's content: an
/// authentication, a payment, a permission, a missing model, a rate
/// limit. Python's `_NOT_CONTENT_STATUSES`.
const NOT_CONTENT_STATUSES: [u16; 5] = [401, 402, 403, 404, 429];

/// An author image: the id of the event that brought it, and its part's
/// index (0 for a lone media).
pub(crate) type ImageKey = (String, usize);

/// What an author image becomes: the image sent, or the mention in its
/// place.
#[derive(Clone, Debug)]
enum Decided {
    Image(Image),
    Mention(String),
}

/// What each author image of one turn becomes: the image it is sent as,
/// or the mention that takes its place. A media it does not name reads as
/// its label (RFC §6.6). Python's `AuthorImages`.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthorImages {
    decided: HashMap<ImageKey, Decided>,
}

impl AuthorImages {
    /// The images the turn sends as images: Python's `AuthorImages.sent`.
    pub(crate) fn sent(&self) -> HashSet<ImageKey> {
        self.decided
            .iter()
            .filter(|(_, decided)| matches!(decided, Decided::Image(_)))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Whether the part `index` of the event `event_id` is an image this
    /// turn decided.
    pub(crate) fn decides(&self, event_id: &str, index: usize) -> bool {
        self.decided.contains_key(&(event_id.to_owned(), index))
    }

    /// The parts `media`, the part `index` of the event `event_id`, reads
    /// as: its caption then its image, else its mention, else its label.
    /// Python's `AuthorImages.parts`.
    pub(crate) fn parts(&self, event_id: &str, index: usize, media: &EventContent) -> Vec<Part> {
        let EventContent::Media(content) = media else {
            return vec![Part::Text(text_of(media))];
        };
        match self.decided.get(&(event_id.to_owned(), index)) {
            Some(Decided::Image(image)) => content
                .caption
                .iter()
                .filter(|caption| !caption.is_empty())
                .map(|caption| Part::Text(caption.clone()))
                .chain([Part::Image(image.clone())])
                .collect(),
            Some(Decided::Mention(mention)) => vec![Part::Text(mention.clone())],
            None => vec![Part::Text(text_of(media))],
        }
    }
}

/// What each image `events` carry becomes for a model that does not see: a
/// mention that it cannot see it, so it says so rather than answer as if it
/// had (RFC §6.4). Nothing is resolved, and the bound does not apply.
/// Python's `unseen_images`.
pub(crate) fn unseen_images(events: &[&RoomEvent]) -> AuthorImages {
    let decided = events
        .iter()
        .flat_map(|event| {
            media_of(&event.content)
                .into_iter()
                .filter(|(_, media)| is_vision_image(media))
                .map(|(index, media)| {
                    let mention = Decided::Mention(mention(NOT_SEEN, media));
                    ((event.id.to_string(), index), mention)
                })
        })
        .collect();
    AuthorImages { decided }
}

/// What resolves a turn's image references: the agent's own
/// [`ImageResolver`], else the kit's [`MediaResolver`].
#[derive(Clone, Copy)]
pub(crate) enum Resolver<'a> {
    /// The agent's (`AiChannelOptions::image_resolver`).
    Image(&'a dyn ImageResolver),
    /// The kit's (`RoomKitOptions::media_resolver`): Python's.
    Media(&'a MediaResolver),
}

impl<'a> Resolver<'a> {
    /// What resolves a turn's references: the agent's `own` resolver, else
    /// the `kit`'s; none without either.
    pub(crate) fn of(
        own: Option<&'a dyn ImageResolver>,
        kit: Option<&'a MediaResolver>,
    ) -> Option<Self> {
        own.map(Self::Image).or_else(|| kit.map(Self::Media))
    }
}

/// Decides what each image `events` carry becomes, the newest first, the
/// newest `limit` sent, those `refused` in the room read as refused, and
/// resolves the references among those sent through `resolver`, for
/// `room`, all together as Python's `asyncio.gather` does: a turn waits
/// for its slowest reference, not for their sum. Python's `author_images`.
pub(crate) async fn author_images(
    events: &[&RoomEvent],
    room: &RoomId,
    resolver: Option<Resolver<'_>>,
    limit: Option<usize>,
    refused: &HashSet<ImageKey>,
) -> AuthorImages {
    let (mut decided, references) = decide(events, limit, refused);
    let resolving = references
        .iter()
        .map(|(_, media)| resolved(media, room, resolver))
        .collect();
    let images = join_all(resolving).await;
    let keys = references.into_iter().map(|(key, _)| key);
    decided.extend(keys.zip(images));
    AuthorImages { decided }
}

/// The image or mention each image of `events` gets, and the references
/// still to resolve among the images kept: the newest `limit` of them, an
/// image `refused` taking no place among them. Python's `_decide`.
fn decide<'a>(
    events: &[&'a RoomEvent],
    limit: Option<usize>,
    refused: &HashSet<ImageKey>,
) -> (
    HashMap<ImageKey, Decided>,
    Vec<(ImageKey, &'a MediaContent)>,
) {
    let mut decided = HashMap::new();
    let mut references: Vec<(ImageKey, &MediaContent)> = Vec::new();
    let mut kept = 0;
    for event in events.iter().rev() {
        for (index, media) in media_of(&event.content).into_iter().rev() {
            let key = (event.id.to_string(), index);
            let seen = decided.contains_key(&key) || references.iter().any(|(at, _)| at == &key);
            if seen || !is_vision_image(media) {
                continue;
            }
            if refused.contains(&key) {
                decided.insert(key, Decided::Mention(mention(REFUSED, media)));
            } else if limit.is_some_and(|limit| kept >= limit) {
                decided.insert(key, Decided::Mention(mention(EARLIER, media)));
            } else if is_reference(&media.url) {
                kept += 1;
                references.push((key, media));
            } else {
                kept += 1;
                decided.insert(key, inline(media));
            }
        }
    }
    (decided, references)
}

/// A data URI or a link as the image sent, its type folded as it was
/// read; one with no data, past the size, or of a form the image refuses,
/// a mention. Python's `_inline`.
fn inline(media: &MediaContent) -> Decided {
    match Image::new(&media.url, &vision_image_type(media)) {
        Some(image) => Decided::Image(image),
        None => Decided::Mention(mention(UNAVAILABLE, media)),
    }
}

/// The image a reference names, as the host's `resolver` gives it for
/// `room`, or the mention that takes its place when it gives none to send.
/// Python's `_resolved`.
async fn resolved(media: &MediaContent, room: &RoomId, resolver: Option<Resolver<'_>>) -> Decided {
    let kind = vision_image_type(media);
    let image = match resolver {
        None => None,
        Some(Resolver::Image(resolver)) => match Image::new(&media.url, &kind) {
            Some(reference) => resolver
                .resolve(&reference, room)
                .await
                .filter(Image::is_resolved),
            None => None,
        },
        Some(Resolver::Media(resolver)) => match resolver.call(media.clone(), room.clone()).await {
            Ok(data) => data.and_then(|data| Image::from_bytes(&data, &kind)),
            Err(error) => {
                tracing::warn!(target: "roomkit::ai", room_id = %room, %error, "the media resolver failed on an image");
                None
            }
        },
    };
    image.map_or_else(
        || Decided::Mention(mention(UNAVAILABLE, media)),
        Decided::Image,
    )
}

/// `event` as an AI channel reads it (RFC §6.4): its content, then the
/// media the application's `reader` names for it, as the parts of one
/// composite. The event itself when the reader names none, an
/// instruction, the application's own direction, as it is, and a deleted
/// event with its content withheld, its media with it (§10.3). Python's
/// `with_attached_media`.
pub(crate) fn with_attached_media(event: &RoomEvent, reader: Option<&AttachedMedia>) -> RoomEvent {
    let Some(reader) = reader else {
        return event.clone();
    };
    if event.r#type == EventType::Instruction || is_deleted(event) {
        return event.clone();
    }
    let attached = match reader.call(event) {
        Ok(attached) if !attached.is_empty() => attached,
        Ok(_) => return event.clone(),
        Err(error) => {
            tracing::warn!(target: "roomkit::ai", event_id = %event.id, %error, "the attached-media reader failed");
            return event.clone();
        }
    };
    match followed_by(&event.content, attached) {
        Some(content) => RoomEvent {
            content,
            ..event.clone()
        },
        None => event.clone(),
    }
}

/// `content`'s parts then `attached`; a text without words gives them its
/// place. `None` when the parts make no composite. Python's
/// `_followed_by`.
fn followed_by(content: &EventContent, attached: Vec<MediaContent>) -> Option<EventContent> {
    let mut parts: Vec<EventContent> = if let EventContent::Composite(composite) = content {
        composite.parts().to_vec()
    } else if matches!(content, EventContent::Text(text) if text.body.trim().is_empty()) {
        Vec::new()
    } else {
        vec![content.clone()]
    };
    parts.extend(attached.into_iter().map(EventContent::Media));
    if parts.len() == 1 {
        return parts.pop();
    }
    CompositeContent::new(parts)
        .ok()
        .map(EventContent::Composite)
}

/// The author images a provider refused, per room: later turns send a
/// mention in their place. Python's `RefusedImages`.
#[derive(Debug, Default)]
pub(crate) struct RefusedImages {
    /// The rooms' refused images, the room refused in last at the back.
    by_room: Mutex<VecDeque<(RoomId, HashSet<ImageKey>)>>,
}

impl RefusedImages {
    /// The images refused in `room`.
    pub(crate) fn of(&self, room: &RoomId) -> HashSet<ImageKey> {
        let by_room = self.by_room.lock().unwrap_or_else(PoisonError::into_inner);
        by_room
            .iter()
            .find(|(at, _)| at == room)
            .map(|(_, keys)| keys.clone())
            .unwrap_or_default()
    }

    /// Drops what `room`'s provider refused (RFC §10.6.3). Python's
    /// `forget()`.
    pub(crate) fn forget(&self, room: &RoomId) {
        let mut by_room = self.by_room.lock().unwrap_or_else(PoisonError::into_inner);
        by_room.retain(|(at, _)| at != room);
    }

    /// Records `keys` as refused in `room`, the oldest room dropped past
    /// [`MAX_ROOMS`].
    pub(crate) fn add(&self, room: &RoomId, keys: &HashSet<ImageKey>) {
        let mut by_room = self.by_room.lock().unwrap_or_else(PoisonError::into_inner);
        let mut refused = by_room
            .iter()
            .position(|(at, _)| at == room)
            .and_then(|at| by_room.remove(at))
            .map(|(_, refused)| refused)
            .unwrap_or_default();
        refused.extend(keys.iter().cloned());
        by_room.push_back((room.clone(), refused));
        while by_room.len() > MAX_ROOMS {
            by_room.pop_front();
        }
    }
}

/// The author images a turn's requests send in its room, which a
/// provider's refusal of the turn's first request marks refused there
/// (RFC §6.4).
#[derive(Clone, Debug)]
pub(crate) struct SentImages {
    pub(crate) refused: Arc<RefusedImages>,
    pub(crate) room: RoomId,
    pub(crate) keys: HashSet<ImageKey>,
}

impl SentImages {
    /// Sends no more the images the turn's request carried, its provider
    /// having refused `error` on it, an error that may be about its
    /// content: they read as refused in the room's later turns. Python's
    /// `_forget_sent_images`.
    pub(crate) fn refused_by(&self, error: &ProviderError) {
        if refuses_content(error) && !self.keys.is_empty() {
            self.refused.add(&self.room, &self.keys);
        }
    }
}

/// Whether `error` may be the provider refusing what a request carried:
/// not retried, and no authentication, payment, permission, missing model
/// or rate limit (RFC §6.4). Only the turn's first request, before
/// anything streamed, can say so: a later round, or a stream under way,
/// shows the content was taken. Python's `refuses_content`.
///
/// A refusal before any request says nothing of the content either: the
/// provider sent nothing ([`ProviderError::before_request`], HITL fb46b635,
/// d91b8137). Nor does a response schema outside the portable subset, which
/// a provider refuses before any request: Python refuses it where it is
/// set, in the hook or the context that writes it, and no provider sees it
/// (HITL 17bfa822).
pub(crate) fn refuses_content(error: &ProviderError) -> bool {
    !error.retryable
        && !error
            .status_code
            .is_some_and(|status| NOT_CONTENT_STATUSES.contains(&status))
        && !refused_before_request(error)
        && !outside_the_subset(error)
}

/// Whether `error`, or a provider error it was raised from, is a refusal
/// before any request, however deep a provider wrapped it.
fn refused_before_request(error: &ProviderError) -> bool {
    error.before_request
        || std::iter::successors(error.source(), |&source| source.source()).any(|source| {
            source
                .downcast_ref::<ProviderError>()
                .is_some_and(|inner| inner.before_request)
        })
}

/// Whether `error` is a provider's refusal of a response schema outside the
/// portable subset (RFC §6.7), however deep a provider wrapped it.
fn outside_the_subset(error: &ProviderError) -> bool {
    std::iter::successors(error.source(), |&source| source.source()).any(|source| {
        matches!(
            source.downcast_ref::<AiError>(),
            Some(AiError::ResponseSchema { .. })
        )
    })
}

/// The text that takes an image's place, naming it as its author did: its
/// caption, else its file name, else its type, quoted. Python's `_mention`.
fn mention(kind: &str, media: &MediaContent) -> String {
    let name = [media.caption.as_deref(), media.filename.as_deref()]
        .into_iter()
        .flatten()
        .find(|name| !name.is_empty())
        .unwrap_or(&media.mime_type);
    format!("[{kind}: {}]", quoted(name, NAME_CHARS))
}

/// The media `content` carries, each with its part's index (0 for a lone
/// media). Python's `media_of`.
fn media_of(content: &EventContent) -> Vec<(usize, &MediaContent)> {
    if let EventContent::Media(media) = content {
        return vec![(0, media)];
    }
    let EventContent::Composite(composite) = content else {
        return Vec::new();
    };
    composite
        .parts()
        .iter()
        .enumerate()
        .filter_map(|(index, part)| {
            if let EventContent::Media(media) = part {
                Some((index, media))
            } else {
                None
            }
        })
        .collect()
}

/// Whether `media` is an image of a type every vision provider takes.
/// Python's `is_vision_image`.
pub(crate) fn is_vision_image(media: &MediaContent) -> bool {
    is_vision_type(&vision_image_type(media))
}

/// Whether `kind`, a folded media type, is one every vision provider
/// takes: Python's `kind in VISION_IMAGE_TYPES`.
pub(crate) fn is_vision_type(kind: &str) -> bool {
    VISION_IMAGE_TYPES.contains(&kind)
}

/// The type `media` is read and sent as: its `data:` URI's, else its own,
/// folded (`image/jpg` and `IMAGE/JPEG` are `image/jpeg`). Python's
/// `vision_image_type`.
fn vision_image_type(media: &MediaContent) -> String {
    let declared = media
        .url
        .get(..5)
        .filter(|scheme| scheme.eq_ignore_ascii_case("data:"))
        .and_then(|_| media.url.get(5..))
        .map(|rest| rest.split(',').next().unwrap_or(rest));
    // A header that declares no type reads as the media's own.
    declared
        .map(media_type)
        .filter(|kind| !kind.is_empty())
        .unwrap_or_else(|| media_type(&media.mime_type))
}

/// Whether `url` names a media the application holds and resolves: neither
/// a data URI nor a link. Python's `is_reference`.
fn is_reference(url: &str) -> bool {
    let starts = |prefix: &str| {
        url.get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    };
    !(starts("data:") || starts("http://") || starts("https://"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roomkit::model::{ChannelId, ChannelType, EventSource, EventType};
    use roomkit_core::rules::transcript::to_data_uri;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    struct Resolver(AtomicUsize);
    impl ImageResolver for Resolver {
        fn resolve<'a>(
            &'a self,
            image: &'a Image,
            room: &'a RoomId,
        ) -> Pin<Box<dyn Future<Output = Option<Image>> + Send + 'a>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                if room == &RoomId::new("allowed").unwrap() && image.url() == "attachment://present"
                {
                    Image::new("data:image/png;base64,AQIDBA==", "image/png")
                } else {
                    None
                }
            })
        }
    }

    fn photo(id: &str, url: &str, filename: &str) -> RoomEvent {
        let mut media = MediaContent::new(url, "image/png").unwrap();
        media.filename = Some(filename.to_owned());
        let source = EventSource::new(ChannelId::new("web").unwrap(), ChannelType::WEBSOCKET);
        RoomEvent {
            id: id.parse().unwrap(),
            r#type: EventType::Message,
            ..RoomEvent::new(RoomId::new("allowed").unwrap(), source, media)
        }
    }

    fn said(images: &AuthorImages, event: &RoomEvent) -> Vec<Part> {
        images.parts(event.id.as_str(), 0, &event.content)
    }

    fn text(said: &str) -> Vec<Part> {
        vec![Part::Text(said.to_owned())]
    }

    // Python: tests/test_author_images.py::TestReference::test_a_reference_reaches_the_model_as_the_hosts_image
    #[tokio::test]
    async fn a_reference_reaches_the_model_as_the_hosts_image() {
        let resolver = Resolver(AtomicUsize::new(0));
        let present = photo("e1", "attachment://present", "lamp.png");
        let missing = photo("e2", "attachment://missing", "gone.png");
        let room = RoomId::new("allowed").unwrap();
        let images = author_images(
            &[&present, &missing],
            &room,
            Some(super::Resolver::Image(&resolver)),
            Some(4),
            &HashSet::new(),
        )
        .await;
        let sent = Image::new("data:image/png;base64,AQIDBA==", "image/png").unwrap();
        assert_eq!(said(&images, &present), [Part::Image(sent)]);
        assert_eq!(
            said(&images, &missing),
            text("[Image unavailable: “gone.png”]")
        );
        assert_eq!(resolver.0.load(Ordering::SeqCst), 2);
        // Another room's turn is given none of them.
        let foreign = RoomId::new("foreign").unwrap();
        let images = author_images(
            &[&present],
            &foreign,
            Some(super::Resolver::Image(&resolver)),
            Some(4),
            &HashSet::new(),
        )
        .await;
        assert_eq!(
            said(&images, &present),
            text("[Image unavailable: “lamp.png”]")
        );
    }

    // Python: tests/test_author_images.py::TestBound::test_the_bound_is_the_channels_to_change
    #[tokio::test]
    async fn the_bound_is_the_channels_to_change() {
        let events: Vec<RoomEvent> = (0..5)
            .map(|i| {
                let url = format!("https://cdn.example.com/p{i}.png");
                photo(&format!("e{i}"), &url, &format!("p{i}.png"))
            })
            .collect();
        let refs: Vec<&RoomEvent> = events.iter().collect();
        let room = RoomId::new("allowed").unwrap();
        for (bound, sent) in [(Some(1), 1), (Some(0), 0), (None, 5)] {
            let images = author_images(&refs, &room, None, bound, &HashSet::new()).await;
            let images_sent = events
                .iter()
                .flat_map(|event| said(&images, event))
                .filter(|part| matches!(part, Part::Image(_)))
                .count();
            assert_eq!(images_sent, sent, "bound {bound:?}");
        }
    }

    // Python: tests/test_author_images.py::TestConstraints::test_an_empty_data_uri_reads_as_unavailable
    #[test]
    fn an_empty_data_uri_reads_as_unavailable() {
        let empty = photo("e1", "data:image/jpeg;base64,", "lamp.png");
        let (decided, _) = decide(&[&empty], Some(4), &HashSet::new());
        let images = AuthorImages { decided };
        assert_eq!(
            said(&images, &empty),
            text("[Image unavailable: “lamp.png”]")
        );
    }

    /// The kit's reader in the Python tests' shape: each file the event's
    /// `files` metadata names, as a png.
    fn reader() -> AttachedMedia {
        AttachedMedia::new(|event| {
            let files = event
                .metadata
                .get("files")
                .and_then(|files| files.as_array());
            Ok(files
                .into_iter()
                .flatten()
                .filter_map(|name| name.as_str())
                .map(|name| MediaContent::new(format!("app-media://{name}"), "image/png").unwrap())
                .collect())
        })
    }

    fn message(body: &str, files: &[&str]) -> RoomEvent {
        let mut event = photo("e1", "app-media://unused", "unused.png");
        event.content = EventContent::Text(roomkit::model::TextContent::new(body));
        event
            .metadata
            .insert("files".to_owned(), serde_json::json!(files));
        event
    }

    // Python: tests/test_attached_media.py::TestNotSeen::test_an_instruction_is_read_as_it_is
    #[test]
    fn an_instruction_is_read_as_it_is() {
        let instruction = RoomEvent {
            r#type: EventType::Instruction,
            ..message("summarize the room", &["photo-1"])
        };
        assert_eq!(
            with_attached_media(&instruction, Some(&reader())),
            instruction
        );
    }

    // Python: tests/test_attached_media.py::TestNotSeen::test_a_deleted_message_s_media_stays_withheld
    #[test]
    fn a_deleted_message_s_media_stays_withheld() {
        let mut deleted = message("here is my lamp", &["photo-1"]);
        deleted
            .metadata
            .insert("deleted".to_owned(), serde_json::Value::Bool(true));
        assert_eq!(with_attached_media(&deleted, Some(&reader())), deleted);
    }

    // Python: tests/test_attached_media.py::TestWithoutWords::test_a_model_that_sees_gets_the_image_alone
    #[test]
    fn a_model_that_sees_gets_the_image_alone() {
        let read = with_attached_media(&message(" ", &["photo-1"]), Some(&reader()));
        let photo = MediaContent::new("app-media://photo-1", "image/png").unwrap();
        assert_eq!(read.content, EventContent::Media(photo));
    }

    // Python: tests/test_attached_media.py::TestAttached::test_they_follow_the_events_own_media
    #[test]
    fn they_follow_the_events_own_media() {
        let mut event = message("both", &["photo-1"]);
        let own = MediaContent::new("data:image/png;base64,AQIDBA==", "image/png").unwrap();
        let words = EventContent::Text(roomkit::model::TextContent::new("both"));
        let parts = vec![words.clone(), EventContent::Media(own.clone())];
        event.content = EventContent::Composite(CompositeContent::new(parts).unwrap());
        let read = with_attached_media(&event, Some(&reader()));
        let attached = MediaContent::new("app-media://photo-1", "image/png").unwrap();
        let EventContent::Composite(read) = read.content else {
            panic!("not a composite: {read:?}");
        };
        let expected = [
            words,
            EventContent::Media(own),
            EventContent::Media(attached),
        ];
        assert_eq!(read.parts(), expected);
    }

    // Python: tests/test_author_images.py::TestReference::test_a_reference_reaches_the_model_as_the_hosts_image
    #[tokio::test]
    async fn a_reference_reaches_the_model_as_the_kit_s_image() {
        let resolver = MediaResolver::new(|media, room| async move {
            let bytes: &[u8] = match media.url.as_str() {
                "app-media://one" => b"a",
                "app-media://two" => b"ab",
                "app-media://three" => b"abc",
                _ => return Ok(None),
            };
            Ok((room.as_str() == "allowed").then(|| bytes.to_vec()))
        });
        let room = RoomId::new("allowed").unwrap();
        let events = [
            photo("e1", "app-media://one", "one.png"),
            photo("e2", "app-media://two", "two.png"),
            photo("e3", "app-media://three", "three.png"),
            photo("e4", "app-media://none", "none.png"),
        ];
        let refs: Vec<&RoomEvent> = events.iter().collect();
        let images = author_images(
            &refs,
            &room,
            Some(super::Resolver::Media(&resolver)),
            None,
            &HashSet::new(),
        )
        .await;
        let sent = |data: &str| {
            let url = format!("data:image/png;base64,{data}");
            vec![Part::Image(Image::new(&url, "image/png").unwrap())]
        };
        assert_eq!(said(&images, &events[0]), sent("YQ=="));
        assert_eq!(said(&images, &events[1]), sent("YWI="));
        assert_eq!(said(&images, &events[2]), sent("YWJj"));
        assert_eq!(
            said(&images, &events[3]),
            text("[Image unavailable: “none.png”]")
        );
    }

    // Python: tests/test_author_images.py::TestRefusal::test_an_error_that_is_not_about_the_content_refuses_nothing
    #[test]
    fn an_error_that_is_not_about_the_content_refuses_nothing() {
        let room = RoomId::new("allowed").unwrap();
        let error = |status: Option<u16>, retryable: bool| {
            let options = crate::provider::ProviderErrorOptions {
                status_code: status,
                retryable,
                ..Default::default()
            };
            ProviderError::new("refused", options)
        };
        for status in [401, 402, 403, 404, 429] {
            assert!(!refuses_content(&error(Some(status), false)), "{status}");
        }
        assert!(!refuses_content(&error(Some(400), true)));
        assert!(refuses_content(&error(Some(400), false)));
        assert!(refuses_content(&error(None, false)));
        // The refusal marks the images the request sent, in its room alone.
        let refused = Arc::new(RefusedImages::default());
        let sent = SentImages {
            refused: Arc::clone(&refused),
            room: room.clone(),
            keys: HashSet::from([("e1".to_owned(), 0)]),
        };
        sent.refused_by(&error(Some(429), false));
        assert!(refused.of(&room).is_empty());
        sent.refused_by(&error(Some(400), false));
        assert_eq!(refused.of(&room), sent.keys);
        assert!(refused.of(&RoomId::new("other").unwrap()).is_empty());
    }

    /// A response schema outside the portable subset, refused before any
    /// request, is no refusal of the content, however deep a provider
    /// wrapped the refusal (HITL 17bfa822).
    #[test]
    fn a_schema_outside_the_subset_is_no_content_refusal() {
        let refused =
            crate::check_portable_schema(&serde_json::json!({ "type": "string" })).unwrap_err();
        let options = crate::provider::ProviderErrorOptions::default;
        let direct = ProviderError::new(refused.to_string(), options()).with_source(refused);
        let wrapped = ProviderError::new("an integrator's provider", options()).with_source(direct);
        assert!(!refuses_content(&wrapped));
    }

    // Python: tests/test_author_images.py::TestRefusal::test_a_refusal_before_any_request_refuses_nothing
    // (a provider's own refusals, flagged where each is raised: the
    // schema's below, the model's and a tool name's in roomkit-openai)
    #[test]
    fn a_refusal_before_any_request_is_no_content_refusal() {
        let options = crate::provider::ProviderErrorOptions::default;
        let before = || {
            ProviderError::new(
                "refused",
                crate::provider::ProviderErrorOptions {
                    before_request: true,
                    ..options()
                },
            )
        };
        let unsupported = ProviderError::from(crate::ResponseSchemaError::new(
            "mock does not support a response schema",
            crate::ResponseSchemaFailure::Unsupported,
            crate::ResponseSchemaErrorOptions::default(),
        ));
        let wrapped =
            ProviderError::new("an integrator's provider", options()).with_source(before());
        let invalid = ProviderError::from(crate::ResponseSchemaError::new(
            "the answer is not a JSON document",
            crate::ResponseSchemaFailure::InvalidJson,
            crate::ResponseSchemaErrorOptions::default(),
        ));
        assert_eq!(
            [&before(), &unsupported, &wrapped, &invalid].map(refuses_content),
            [false, false, false, true]
        );
    }

    // Python: tests/test_attached_media.py::TestNotSeen::test_a_reader_that_fails_names_none
    #[test]
    fn a_reader_that_fails_names_none() {
        let broken = AttachedMedia::new(|_| Err("record unreadable".into()));
        let event = message("look", &["photo-1"]);
        assert_eq!(with_attached_media(&event, Some(&broken)), event);
    }

    // Python: tests/test_author_images.py::TestMention::test_an_image_that_does_not_resolve_leaves_the_turn_a_mention
    #[tokio::test]
    async fn an_image_that_does_not_resolve_leaves_the_turn_a_mention() {
        const MAX_AUTHOR_IMAGE_BYTES: usize = 5 * 1024 * 1024;
        let resolver = MediaResolver::new(|media, _room| async move {
            match media.url.as_str() {
                "app-media://fails" => Err("storage unreachable".into()),
                "app-media://empty" => Ok(Some(Vec::new())),
                "app-media://large" => Ok(Some(vec![0; MAX_AUTHOR_IMAGE_BYTES + 1])),
                "app-media://max" => Ok(Some(vec![0; MAX_AUTHOR_IMAGE_BYTES])),
                _ => Ok(None),
            }
        });
        let room = RoomId::new("allowed").unwrap();
        // RMK-1197: 5 MiB of bytes resolve, though their base64 reads a
        // byte more off its length.
        let max = Image::from_bytes(&vec![0; MAX_AUTHOR_IMAGE_BYTES], "image/png").unwrap();
        for url in ["fails", "empty", "large", "none", "max"] {
            let event = photo("e1", &format!("app-media://{url}"), "lamp.png");
            let resolver = Some(super::Resolver::Media(&resolver));
            let images = author_images(&[&event], &room, resolver, None, &HashSet::new()).await;
            let expected = match url {
                "max" => vec![Part::Image(max.clone())],
                _ => text("[Image unavailable: “lamp.png”]"),
            };
            assert_eq!(said(&images, &event), expected, "{url}");
        }
    }

    /// Rust only: an agent's own `ImageResolver` (approved 2026-10-30)
    /// comes before the kit's `MediaResolver`.
    #[test]
    fn the_agent_s_own_resolver_comes_first() {
        let own = Resolver(AtomicUsize::new(0));
        let kit = MediaResolver::new(|_, _| async { Ok(None) });
        let chosen = super::Resolver::of(Some(&own), Some(&kit));
        assert!(matches!(chosen, Some(super::Resolver::Image(_))));
        let chosen = super::Resolver::of(None, Some(&kit));
        assert!(matches!(chosen, Some(super::Resolver::Media(_))));
        assert!(super::Resolver::of(None, None).is_none());
    }

    /// Python's `RefusedImages` (`_MAX_ROOMS = 100`): the newest hundred
    /// rooms' refusals are kept, a room refused in again counting as the
    /// newest, and the 101st room drops the oldest.
    #[test]
    fn refusals_are_kept_for_the_newest_hundred_rooms() {
        let refused = RefusedImages::default();
        let keys = HashSet::from([("e1".to_owned(), 0)]);
        let room = |at: usize| RoomId::new(format!("room-{at}")).unwrap();
        for at in 0..100 {
            refused.add(&room(at), &keys);
        }
        assert_eq!(refused.of(&room(0)), keys, "a hundred rooms kept");
        refused.add(&room(0), &keys);
        refused.add(&room(100), &keys);
        assert!(
            refused.of(&room(1)).is_empty(),
            "the 101st drops the oldest"
        );
        assert_eq!(refused.of(&room(0)), keys, "refused in again, the newest");
        assert_eq!(refused.of(&room(2)), keys);
        assert_eq!(refused.of(&room(100)), keys);
    }

    // Python: channels/_author_images.py::author_images resolves the
    // references together (`asyncio.gather`): four slow ones take the
    // slowest one's time, each event given its own image though they end
    // in the order opposite to the one they are asked in (the newest
    // first).
    #[tokio::test(start_paused = true)]
    async fn references_resolve_together() {
        let resolver = MediaResolver::new(|media, _room| async move {
            let at: u8 = media.url.trim_start_matches("app-media://").parse()?;
            sleep(Duration::from_millis(40 + 20 * u64::from(at))).await;
            Ok(Some(vec![at]))
        });
        let events: Vec<RoomEvent> = (0..4)
            .map(|at| {
                let url = format!("app-media://{at}");
                photo(&format!("e{at}"), &url, &format!("p{at}.png"))
            })
            .collect();
        let refs: Vec<&RoomEvent> = events.iter().collect();
        let room = RoomId::new("allowed").unwrap();
        let started = Instant::now();
        let resolver = Some(super::Resolver::Media(&resolver));
        let images = author_images(&refs, &room, resolver, None, &HashSet::new()).await;
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(150),
            "resolved one after another: {took:?}"
        );
        for (at, event) in (0u8..).zip(&events) {
            let url = to_data_uri(&[at], "image/png");
            let sent = Image::new(&url, "image/png").unwrap();
            assert_eq!(said(&images, event), [Part::Image(sent)], "e{at}");
        }
    }
}
