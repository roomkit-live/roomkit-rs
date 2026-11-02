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
//! resolve as an image unavailable.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use roomkit::model::{EventContent, MediaContent, RoomEvent, RoomId};
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::transcript::{Image, Part, quoted};

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
/// How many characters of an image's name a mention quotes.
const NAME_CHARS: usize = 120;

/// An author image: the id of the event that brought it, and its part's
/// index (0 for a lone media).
type ImageKey = (String, usize);

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

/// Decides what each image `events` carry becomes, the newest first, the
/// newest `limit` sent, and resolves the references among those sent
/// through `resolver`, for `room`. Python's `author_images`.
pub(crate) async fn author_images(
    events: &[&RoomEvent],
    room: &RoomId,
    resolver: Option<&dyn ImageResolver>,
    limit: Option<usize>,
) -> AuthorImages {
    let (mut decided, references) = decide(events, limit);
    for (key, media) in references {
        let image = resolved(media, room, resolver).await;
        decided.insert(key, image);
    }
    AuthorImages { decided }
}

/// The image or mention each image of `events` gets, and the references
/// still to resolve among the images kept: the newest `limit` of them.
/// Python's `_decide`.
fn decide<'a>(
    events: &[&'a RoomEvent],
    limit: Option<usize>,
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
            if limit.is_some_and(|limit| kept >= limit) {
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
async fn resolved(
    media: &MediaContent,
    room: &RoomId,
    resolver: Option<&dyn ImageResolver>,
) -> Decided {
    let unavailable = || Decided::Mention(mention(UNAVAILABLE, media));
    let (Some(resolver), Some(reference)) =
        (resolver, Image::new(&media.url, &vision_image_type(media)))
    else {
        return unavailable();
    };
    match resolver
        .resolve(&reference, room)
        .await
        .filter(Image::is_resolved)
    {
        Some(image) => Decided::Image(image),
        None => unavailable(),
    }
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
    VISION_IMAGE_TYPES.contains(&vision_image_type(media).as_str())
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
    media_type(declared.unwrap_or(&media.mime_type))
}

/// `mime_type` as RoomKit compares it: lowercased, its parameters aside,
/// its aliases folded. Python's `media_type`.
fn media_type(mime_type: &str) -> String {
    let bare = mime_type
        .split(';')
        .next()
        .unwrap_or(mime_type)
        .trim()
        .to_lowercase();
    match bare.as_str() {
        "image/jpg" | "image/pjpeg" => "image/jpeg".to_owned(),
        _ => bare,
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        let images = author_images(&[&present, &missing], &room, Some(&resolver), Some(4)).await;
        let sent = Image::new("data:image/png;base64,AQIDBA==", "image/png").unwrap();
        assert_eq!(said(&images, &present), [Part::Image(sent)]);
        assert_eq!(
            said(&images, &missing),
            text("[Image unavailable: “gone.png”]")
        );
        assert_eq!(resolver.0.load(Ordering::SeqCst), 2);
        // Another room's turn is given none of them.
        let foreign = RoomId::new("foreign").unwrap();
        let images = author_images(&[&present], &foreign, Some(&resolver), Some(4)).await;
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
            let images = author_images(&refs, &room, None, bound).await;
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
        let (decided, _) = decide(&[&empty], Some(4));
        let images = AuthorImages { decided };
        assert_eq!(
            said(&images, &empty),
            text("[Image unavailable: “lamp.png”]")
        );
    }
}
