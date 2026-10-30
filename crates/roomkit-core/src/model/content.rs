//! What an event says (RFC §5.3).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::media_url::{media_url, playable_url};
use super::{EventId, Metadata, ModelError};

/// How deep composite contents may nest (RFC §5.3). The outermost composite
/// is level 1 (gap G-006). Hidden: the Python implementation keeps it
/// private, and roomkit reads it across crates.
#[doc(hidden)]
pub const MAX_COMPOSITE_DEPTH: usize = 5;

/// An event's payload: exactly one content type, tagged by `type` with the
/// names of RFC §5.3.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum EventContent {
    /// Plain text.
    Text(TextContent),
    /// Formatted text with interactive elements.
    Rich(RichContent),
    /// A file, an image or a document.
    Media(MediaContent),
    /// Geographic coordinates.
    Location(LocationContent),
    /// An audio message or voice note.
    Audio(AudioContent),
    /// A video message.
    Video(VideoContent),
    /// Several parts in order.
    Composite(CompositeContent),
    /// A framework-generated message.
    System(SystemContent),
    /// A pre-approved template.
    Template(TemplateContent),
    /// An edit of a previous message.
    Edit(EditContent),
    /// A deletion of a previous message.
    Delete(DeleteContent),
    /// A tool call, on `TOOL_CALL_START` and `TOOL_CALL_END` events.
    ToolCall(ToolCallContent),
}

impl EventContent {
    /// Refuses a content its constructor or its deserialization would have
    /// refused, a media URL no reader may take, a negative duration (RFC
    /// §6.4), and writes what it keeps as they write it, a link's or a
    /// `data:` URI's scheme in lower case, through a composite's parts and
    /// an edit's new content. A struct literal runs no check, so every door
    /// that writes an event runs this one: the kit's calls, the hook chain, a
    /// channel's answer and the stores. Outside the public API.
    ///
    /// # Errors
    ///
    /// Returns what the content's constructor would have returned, the
    /// content left as it was.
    #[doc(hidden)]
    pub fn check(&mut self) -> Result<(), ModelError> {
        match self {
            Self::Media(media) => {
                media.url = media_url(media.url.clone())?;
                Ok(())
            }
            Self::Audio(audio) => {
                playable_url(audio.url.clone())?;
                duration(audio.duration_seconds).map(drop)
            }
            Self::Video(video) => {
                playable_url(video.url.clone())?;
                video.thumbnail_url.clone().map(playable_url).transpose()?;
                duration(video.duration_seconds).map(drop)
            }
            Self::Composite(composite) => composite.parts.iter_mut().try_for_each(Self::check),
            Self::Edit(edit) => edit.new_content.check(),
            Self::Text(_)
            | Self::Rich(_)
            | Self::Location(_)
            | Self::System(_)
            | Self::Template(_)
            | Self::Delete(_)
            | Self::ToolCall(_) => Ok(()),
        }
    }

    /// How many composite levels this content holds, counting through the
    /// new content of an edit (gap G-006): 0 for any other content.
    pub(crate) fn composite_depth(&self) -> usize {
        match self {
            Self::Composite(composite) => composite.depth(),
            Self::Edit(edit) => edit.new_content.composite_depth(),
            Self::Text(_)
            | Self::Rich(_)
            | Self::Media(_)
            | Self::Location(_)
            | Self::Audio(_)
            | Self::Video(_)
            | Self::System(_)
            | Self::Template(_)
            | Self::Delete(_)
            | Self::ToolCall(_) => 0,
        }
    }
}

/// Each content is an [`EventContent`], as a Python content is a member of
/// the union a parameter takes: a parameter that takes a content takes
/// `impl Into<EventContent>`.
macro_rules! into_event_content {
    ($($variant:ident($content:ident)),* $(,)?) => {$(
        impl From<$content> for EventContent {
            fn from(content: $content) -> Self {
                Self::$variant(content)
            }
        }
    )*};
}

into_event_content!(
    Text(TextContent),
    Rich(RichContent),
    Media(MediaContent),
    Location(LocationContent),
    Audio(AudioContent),
    Video(VideoContent),
    Composite(CompositeContent),
    System(SystemContent),
    Template(TemplateContent),
    Edit(EditContent),
    Delete(DeleteContent),
    ToolCall(ToolCallContent),
);

/// Plain text (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextContent {
    /// The message text.
    pub body: String,
    /// An ISO 639-1 language code.
    #[serde(default)]
    pub language: Option<String>,
}

impl TextContent {
    /// Text with no declared language.
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            language: None,
        }
    }
}

/// The markup of a rich content's body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RichFormat {
    /// HTML.
    Html,
    /// Markdown, the default.
    #[default]
    Markdown,
}

/// Formatted text with interactive elements (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RichContent {
    /// The primary text, in `format`.
    pub body: String,
    /// The markup of `body`.
    #[serde(default)]
    pub format: RichFormat,
    /// The plain-text fallback.
    #[serde(default)]
    pub plain_text: Option<String>,
    /// Interactive buttons.
    #[serde(default)]
    pub buttons: Vec<Metadata>,
    /// Structured card elements.
    #[serde(default)]
    pub cards: Vec<Metadata>,
    /// Suggested quick responses.
    #[serde(default)]
    pub quick_replies: Vec<String>,
}

impl RichContent {
    /// Formatted text in the default format, with no plain-text fallback
    /// and no interactive element.
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            format: RichFormat::default(),
            plain_text: None,
            buttons: Vec::new(),
            cards: Vec::new(),
            quick_replies: Vec::new(),
        }
    }
}

/// A file, an image or a document (RFC §5.3).
///
/// `url` is a link (`http`, `https`), a `data:` URI, or a reference
/// `<scheme>://…` in a scheme the application chooses, naming a media it
/// holds and resolves (RFC §6.4). [`MediaContent::new`] and deserialization
/// refuse any other, as the Python implementation's validator does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MediaContentWire")]
pub struct MediaContent {
    /// A link, a `data:` URI, or a reference the application resolves.
    pub url: String,
    /// The MIME type.
    pub mime_type: String,
    /// The original file name.
    #[serde(default)]
    pub filename: Option<String>,
    /// A text caption.
    #[serde(default)]
    pub caption: Option<String>,
    /// The file size in bytes.
    #[serde(default)]
    pub size_bytes: Option<u64>,
}

impl MediaContent {
    /// A file at `url` of type `mime_type`, with no name, caption or size; a
    /// link's or a `data:` URI's scheme lower-cased.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidMediaUrl`] for a URL that is neither a
    /// link, a `data:` URI nor a reference: a scheme of one character, or
    /// `file`, `ftp`, `javascript` or `vbscript`, a `..` segment or a
    /// backslash (RFC §6.4).
    pub fn new(url: impl Into<String>, mime_type: impl Into<String>) -> Result<Self, ModelError> {
        Ok(Self {
            url: media_url(url.into())?,
            mime_type: mime_type.into(),
            filename: None,
            caption: None,
            size_bytes: None,
        })
    }
}

#[derive(Deserialize)]
struct MediaContentWire {
    url: String,
    mime_type: String,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    size_bytes: Option<u64>,
}

impl TryFrom<MediaContentWire> for MediaContent {
    type Error = ModelError;

    fn try_from(wire: MediaContentWire) -> Result<Self, Self::Error> {
        Ok(Self {
            filename: wire.filename,
            caption: wire.caption,
            size_bytes: wire.size_bytes,
            ..Self::new(wire.url, wire.mime_type)?
        })
    }
}

/// Geographic coordinates (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationContent {
    /// The latitude.
    pub latitude: Latitude,
    /// The longitude.
    pub longitude: Longitude,
    /// The location's name.
    #[serde(default)]
    pub label: Option<String>,
    /// The street address.
    #[serde(default)]
    pub address: Option<String>,
}

impl LocationContent {
    /// The point at `latitude` and `longitude`, with no label or address.
    pub fn new(latitude: Latitude, longitude: Longitude) -> Self {
        Self {
            latitude,
            longitude,
            label: None,
            address: None,
        }
    }
}

/// A latitude in degrees, from -90 to 90.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Latitude(f64);

impl Latitude {
    /// Builds a latitude.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::OutOfRange`] outside -90..=90, `NaN` included.
    pub fn new(degrees: f64) -> Result<Self, ModelError> {
        in_range("latitude", degrees, 90.0).map(Self)
    }

    /// The latitude in degrees.
    pub fn degrees(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Latitude {
    type Error = ModelError;

    fn try_from(degrees: f64) -> Result<Self, Self::Error> {
        Self::new(degrees)
    }
}

impl From<Latitude> for f64 {
    fn from(latitude: Latitude) -> Self {
        latitude.0
    }
}

/// A longitude in degrees, from -180 to 180.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Longitude(f64);

impl Longitude {
    /// Builds a longitude.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::OutOfRange`] outside -180..=180, `NaN` included.
    pub fn new(degrees: f64) -> Result<Self, ModelError> {
        in_range("longitude", degrees, 180.0).map(Self)
    }

    /// The longitude in degrees.
    pub fn degrees(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Longitude {
    type Error = ModelError;

    fn try_from(degrees: f64) -> Result<Self, Self::Error> {
        Self::new(degrees)
    }
}

impl From<Longitude> for f64 {
    fn from(longitude: Longitude) -> Self {
        longitude.0
    }
}

fn in_range(name: &'static str, value: f64, bound: f64) -> Result<f64, ModelError> {
    if (-bound..=bound).contains(&value) {
        Ok(value)
    } else {
        Err(ModelError::OutOfRange {
            name,
            value,
            min: -bound,
            max: bound,
        })
    }
}

/// An audio message or voice note (RFC §5.3).
///
/// `url` starts with `http://`, `https://` or `data:`, as written, and
/// `duration_seconds` is not negative: [`AudioContent::new`] and
/// deserialization refuse any other, as the Python implementation's
/// validators do.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AudioContentWire")]
pub struct AudioContent {
    /// The audio URL, or a `data:` URI.
    pub url: String,
    /// The duration in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// The MIME type, `audio/ogg` by default.
    #[serde(default = "default_audio_mime_type")]
    pub mime_type: String,
    /// The speech-to-text transcript, when available.
    #[serde(default)]
    pub transcript: Option<String>,
}

impl AudioContent {
    /// The audio at `url`, `audio/ogg`, with no duration or transcript.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidMediaUrl`] for a URL that does not start
    /// with `http://`, `https://` or `data:`.
    pub fn new(url: impl Into<String>) -> Result<Self, ModelError> {
        Ok(Self {
            url: playable_url(url.into())?,
            duration_seconds: None,
            mime_type: default_audio_mime_type(),
            transcript: None,
        })
    }
}

fn default_audio_mime_type() -> String {
    "audio/ogg".to_owned()
}

#[derive(Deserialize)]
struct AudioContentWire {
    url: String,
    #[serde(default)]
    duration_seconds: Option<f64>,
    #[serde(default = "default_audio_mime_type")]
    mime_type: String,
    #[serde(default)]
    transcript: Option<String>,
}

impl TryFrom<AudioContentWire> for AudioContent {
    type Error = ModelError;

    fn try_from(wire: AudioContentWire) -> Result<Self, Self::Error> {
        Ok(Self {
            duration_seconds: duration(wire.duration_seconds)?,
            mime_type: wire.mime_type,
            transcript: wire.transcript,
            ..Self::new(wire.url)?
        })
    }
}

/// A duration a content declares: not negative, as Python's `ge=0.0`.
fn duration(seconds: Option<f64>) -> Result<Option<f64>, ModelError> {
    match seconds {
        Some(value) if value.is_nan() || value < 0.0 => Err(ModelError::OutOfRange {
            name: "duration_seconds",
            value,
            min: 0.0,
            max: f64::INFINITY,
        }),
        _ => Ok(seconds),
    }
}

/// A video message (RFC §5.3).
///
/// `url` and `thumbnail_url` follow [`AudioContent`]'s rule.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "VideoContentWire")]
pub struct VideoContent {
    /// The video URL, or a `data:` URI.
    pub url: String,
    /// The duration in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// The MIME type, `video/mp4` by default.
    #[serde(default = "default_video_mime_type")]
    pub mime_type: String,
    /// A preview image URL.
    #[serde(default)]
    pub thumbnail_url: Option<String>,
}

impl VideoContent {
    /// The video at `url`, `video/mp4`, with no duration or preview.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidMediaUrl`] for a URL that does not start
    /// with `http://`, `https://` or `data:`.
    pub fn new(url: impl Into<String>) -> Result<Self, ModelError> {
        Ok(Self {
            url: playable_url(url.into())?,
            duration_seconds: None,
            mime_type: default_video_mime_type(),
            thumbnail_url: None,
        })
    }
}

fn default_video_mime_type() -> String {
    "video/mp4".to_owned()
}

#[derive(Deserialize)]
struct VideoContentWire {
    url: String,
    #[serde(default)]
    duration_seconds: Option<f64>,
    #[serde(default = "default_video_mime_type")]
    mime_type: String,
    #[serde(default)]
    thumbnail_url: Option<String>,
}

impl TryFrom<VideoContentWire> for VideoContent {
    type Error = ModelError;

    fn try_from(wire: VideoContentWire) -> Result<Self, Self::Error> {
        Ok(Self {
            duration_seconds: duration(wire.duration_seconds)?,
            mime_type: wire.mime_type,
            thumbnail_url: wire.thumbnail_url.map(playable_url).transpose()?,
            ..Self::new(wire.url)?
        })
    }
}

/// Several parts in order (RFC §5.3): at least one, nesting at most
/// [`MAX_COMPOSITE_DEPTH`] levels.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawCompositeContent")]
pub struct CompositeContent {
    parts: Vec<EventContent>,
}

impl CompositeContent {
    /// Builds a composite of `parts`.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::EmptyComposite`] without a part, and
    /// [`ModelError::CompositeTooDeep`] when the composite would nest more
    /// than [`MAX_COMPOSITE_DEPTH`] levels.
    pub fn new(parts: Vec<EventContent>) -> Result<Self, ModelError> {
        let deepest_part = parts
            .iter()
            .map(EventContent::composite_depth)
            .max()
            .ok_or(ModelError::EmptyComposite)?;
        let depth = deepest_part + 1;
        if depth > MAX_COMPOSITE_DEPTH {
            return Err(ModelError::CompositeTooDeep {
                depth,
                max: MAX_COMPOSITE_DEPTH,
            });
        }
        Ok(Self { parts })
    }

    /// The parts, in order.
    pub fn parts(&self) -> &[EventContent] {
        &self.parts
    }

    /// The parts, taken out of the composite. Hidden: the Python
    /// implementation has no such helper, and roomkit reads it across crates.
    #[doc(hidden)]
    pub fn into_parts(self) -> Vec<EventContent> {
        self.parts
    }

    /// Rebuilds a composite from the parts of a valid one, each replaced by a
    /// part that nests no deeper (a transcoding, a truncation). The caller
    /// keeps the invariant, so it is only checked in debug builds.
    pub(crate) fn from_derived_parts(parts: Vec<EventContent>) -> Self {
        debug_assert!(
            Self::new(parts.clone()).is_ok(),
            "derived parts must keep the composite invariant"
        );
        Self { parts }
    }

    fn depth(&self) -> usize {
        1 + self
            .parts
            .iter()
            .map(EventContent::composite_depth)
            .max()
            .unwrap_or(0)
    }
}

#[derive(Deserialize)]
struct RawCompositeContent {
    parts: Vec<EventContent>,
}

impl TryFrom<RawCompositeContent> for CompositeContent {
    type Error = ModelError;

    fn try_from(raw: RawCompositeContent) -> Result<Self, Self::Error> {
        Self::new(raw.parts)
    }
}

/// A framework-generated message (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemContent {
    /// A human-readable description.
    pub body: String,
    /// A machine-readable code.
    #[serde(default)]
    pub code: Option<String>,
    /// A structured payload.
    #[serde(default)]
    pub data: Metadata,
}

impl SystemContent {
    /// A system message with no code and no data.
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            code: None,
            data: Metadata::new(),
        }
    }
}

/// A pre-approved template (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemplateContent {
    /// The template's id.
    pub template_id: String,
    /// The template's language, `en` by default.
    #[serde(default = "default_template_language")]
    pub language: String,
    /// The variable substitutions.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// The text for channels without template support.
    #[serde(default)]
    pub body: Option<String>,
}

impl TemplateContent {
    /// The template `template_id`, in English, with no substitution and no
    /// fallback text.
    pub fn new(template_id: impl Into<String>) -> Self {
        Self {
            template_id: template_id.into(),
            language: default_template_language(),
            parameters: BTreeMap::new(),
            body: None,
        }
    }
}

fn default_template_language() -> String {
    "en".to_owned()
}

/// Who made an edit (RFC §5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EditSource {
    /// The message's sender.
    Sender,
    /// The system, for example auto-moderation.
    System,
}

/// An edit of a previous message (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EditContent {
    /// The edited event.
    pub target_event_id: EventId,
    /// The replacement content.
    pub new_content: Box<EventContent>,
    /// Who made the edit.
    #[serde(default)]
    pub edit_source: Option<EditSource>,
}

impl EditContent {
    /// An edit of `target_event_id` to `new_content`, naming no source, as
    /// the RFC's `edit_source` defaults to null (RFC §5.3).
    pub fn new(target_event_id: EventId, new_content: impl Into<EventContent>) -> Self {
        Self {
            target_event_id,
            new_content: Box::new(new_content.into()),
            edit_source: None,
        }
    }
}

/// Who deleted a message (RFC §5.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeleteType {
    /// The message's author deleted it.
    #[default]
    Sender,
    /// An automated deletion: moderation, policy.
    System,
    /// An administrator or operator deleted it.
    Admin,
}

/// A deletion of a previous message (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteContent {
    /// The deleted event.
    pub target_event_id: EventId,
    /// Who deleted it.
    #[serde(default)]
    pub delete_type: DeleteType,
    /// Why.
    #[serde(default)]
    pub reason: Option<String>,
}

impl DeleteContent {
    /// A delete of `target_event_id` by its author, as the RFC's
    /// `delete_type` defaults to `SENDER` (RFC §5.3), with no reason.
    pub fn new(target_event_id: EventId) -> Self {
        Self {
            target_event_id,
            delete_type: DeleteType::default(),
            reason: None,
        }
    }
}

/// Where a tool call stands (RFC §5.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolCallStatus {
    /// On the start row.
    #[default]
    Pending,
    /// On an end row: the call completed.
    Completed,
    /// On an end row: the call failed.
    Failed,
}

/// How a tool call ended (RFC §5.3, §6.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolCallOutcome {
    /// A handler served the call.
    Served,
    /// The call was refused before execution.
    Refused,
    /// The call failed during execution.
    Failed,
    /// A hook blocked the call's result.
    Blocked,
    /// Nothing served the call.
    Unserved,
    /// The call was cut before its outcome.
    Cancelled,
}

/// A tool call (RFC §5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallContent {
    /// The tool called.
    pub tool_name: String,
    /// The call's id.
    pub tool_id: String,
    /// The call's arguments.
    #[serde(default)]
    pub arguments: Metadata,
    /// The call's result, on an end row; `null` otherwise.
    #[serde(default)]
    pub result: Value,
    /// Where the call stands.
    #[serde(default)]
    pub status: ToolCallStatus,
    /// The execution time, on an end row.
    #[serde(default)]
    pub duration_ms: Option<u64>,
    /// The error of a failed call.
    #[serde(default)]
    pub error: Option<String>,
    /// A structured copy of the result for UI surfaces.
    #[serde(default)]
    pub structured_content: Option<Metadata>,
    /// How the call ended, on an end row.
    #[serde(default)]
    pub outcome: Option<ToolCallOutcome>,
    /// The call ran although RoomKit refused it (RFC §9.3).
    #[serde(default)]
    pub refused_but_ran: bool,
    /// What the call does, in the sentence its model wrote for the person
    /// (RFC §6.4): written only when set, so a row of a call without one has
    /// no `intent` key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
}

impl ToolCallContent {
    /// The call `tool_id` of `tool_name`, pending, with no argument and no
    /// result yet.
    pub fn new(tool_name: impl Into<String>, tool_id: impl Into<String>) -> Self {
        Self {
            tool_name: tool_name.into(),
            tool_id: tool_id.into(),
            arguments: Metadata::new(),
            result: Value::Null,
            status: ToolCallStatus::default(),
            duration_ms: None,
            error: None,
            structured_content: None,
            outcome: None,
            refused_but_ran: false,
            intent: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn composite_of_depth(depth: usize) -> EventContent {
        let mut content = EventContent::from(TextContent::new("innermost"));
        for _ in 0..depth {
            content = EventContent::Composite(CompositeContent::new(vec![content]).unwrap());
        }
        content
    }

    fn tag_of(content: &EventContent) -> String {
        serde_json::to_value(content).unwrap()["type"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn each_content_is_tagged_with_its_rfc_name() {
        let id = EventId::new("evt_1").unwrap();
        let contents = [
            TextContent::new("hello").into(),
            EventContent::Rich(RichContent {
                body: "**hello**".to_owned(),
                format: RichFormat::Markdown,
                plain_text: None,
                buttons: Vec::new(),
                cards: Vec::new(),
                quick_replies: Vec::new(),
            }),
            EventContent::Media(MediaContent {
                url: "https://example.com/a.pdf".to_owned(),
                mime_type: "application/pdf".to_owned(),
                filename: None,
                caption: None,
                size_bytes: None,
            }),
            EventContent::Location(LocationContent {
                latitude: Latitude::new(45.5).unwrap(),
                longitude: Longitude::new(-73.6).unwrap(),
                label: None,
                address: None,
            }),
            EventContent::Audio(AudioContent {
                url: "https://example.com/a.ogg".to_owned(),
                duration_seconds: None,
                mime_type: default_audio_mime_type(),
                transcript: None,
            }),
            EventContent::Video(VideoContent {
                url: "https://example.com/a.mp4".to_owned(),
                duration_seconds: None,
                mime_type: "video/mp4".to_owned(),
                thumbnail_url: None,
            }),
            composite_of_depth(1),
            EventContent::System(SystemContent::new("room created")),
            EventContent::Template(TemplateContent {
                template_id: "welcome".to_owned(),
                language: default_template_language(),
                parameters: BTreeMap::new(),
                body: None,
            }),
            EventContent::Edit(EditContent {
                target_event_id: id.clone(),
                new_content: Box::new(TextContent::new("fixed").into()),
                edit_source: None,
            }),
            EventContent::Delete(DeleteContent {
                target_event_id: id,
                delete_type: DeleteType::Sender,
                reason: None,
            }),
            EventContent::ToolCall(ToolCallContent {
                tool_name: "lookup".to_owned(),
                tool_id: "call_1".to_owned(),
                arguments: Metadata::new(),
                result: Value::Null,
                status: ToolCallStatus::Pending,
                duration_ms: None,
                error: None,
                structured_content: None,
                outcome: None,
                refused_but_ran: false,
                intent: None,
            }),
        ];
        let tags: Vec<String> = contents.iter().map(tag_of).collect();
        assert_eq!(
            tags,
            [
                "text",
                "rich",
                "media",
                "location",
                "audio",
                "video",
                "composite",
                "system",
                "template",
                "edit",
                "delete",
                "tool_call"
            ]
        );
    }

    #[test]
    fn text_content_serializes_with_its_tag() {
        assert_eq!(
            serde_json::to_value(EventContent::from(TextContent::new("Bonjour"))).unwrap(),
            json!({ "type": "text", "body": "Bonjour", "language": null })
        );
    }

    #[test]
    fn a_composite_without_parts_is_refused() {
        assert_eq!(
            CompositeContent::new(Vec::new()),
            Err(ModelError::EmptyComposite)
        );
    }

    #[test]
    fn a_composite_may_nest_five_levels() {
        assert_eq!(composite_of_depth(5).composite_depth(), 5);
    }

    #[test]
    fn a_sixth_composite_level_is_refused() {
        let five_levels = composite_of_depth(5);
        assert_eq!(
            CompositeContent::new(vec![five_levels]),
            Err(ModelError::CompositeTooDeep { depth: 6, max: 5 })
        );
    }

    #[test]
    fn composite_depth_counts_through_an_edit() {
        let edit = EventContent::Edit(EditContent {
            target_event_id: EventId::new("evt_1").unwrap(),
            new_content: Box::new(composite_of_depth(5)),
            edit_source: Some(EditSource::Sender),
        });
        assert!(matches!(
            CompositeContent::new(vec![edit]),
            Err(ModelError::CompositeTooDeep { depth: 6, .. })
        ));
    }

    #[test]
    fn deserialization_enforces_the_composite_depth() {
        let mut json = json!({ "type": "text", "body": "innermost" });
        for _ in 0..6 {
            json = json!({ "type": "composite", "parts": [json] });
        }
        assert!(serde_json::from_value::<EventContent>(json).is_err());
    }

    #[test]
    fn a_composite_round_trips_through_json() {
        let content = composite_of_depth(3);
        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(
            serde_json::from_value::<EventContent>(json).unwrap(),
            content
        );
    }

    #[test]
    fn a_latitude_beyond_ninety_degrees_is_refused() {
        assert!(matches!(
            Latitude::new(90.5),
            Err(ModelError::OutOfRange {
                name: "latitude",
                ..
            })
        ));
    }

    #[test]
    fn a_nan_longitude_is_refused() {
        assert!(Longitude::new(f64::NAN).is_err());
    }

    #[test]
    fn deserialization_enforces_the_coordinate_ranges() {
        let json = json!({ "type": "location", "latitude": 0.0, "longitude": 200.0 });
        assert!(serde_json::from_value::<EventContent>(json).is_err());
    }

    #[test]
    fn audio_content_defaults_its_mime_type_to_ogg() {
        let parsed: EventContent =
            serde_json::from_value(json!({ "type": "audio", "url": "https://example.com/a" }))
                .unwrap();
        let EventContent::Audio(audio) = parsed else {
            panic!("expected audio content, got {parsed:?}");
        };
        assert_eq!(audio.mime_type, "audio/ogg");
    }

    /// RFC §6.4: a media URL is refused when the content is built, with the
    /// Python implementation's message (probe
    /// `tasks/probes/step5-lot2/media_urls.py`).
    #[test]
    fn a_media_url_no_reader_may_take_is_refused_when_built() {
        let refused = [
            MediaContent::new("javascript:alert(1)", "text/html"),
            MediaContent::new("file:///etc/passwd", "text/plain"),
            MediaContent::new("app://a/../b", "image/png"),
        ]
        .map(|built| built.unwrap_err());
        assert_eq!(
            refused,
            ["javascript:alert(1)", "file:///etc/passwd", "app://a/../b"].map(|value| {
                ModelError::InvalidMediaUrl {
                    value: value.to_owned(),
                    reason: "URL must be a link (http://, https://), a data: URI, or a \
                             reference <scheme>://... (two letters of scheme at least, not \
                             file, ftp, javascript or vbscript, no '..' segment and no \
                             backslash)",
                }
            })
        );
        assert_eq!(
            AudioContent::new("ftp://x"),
            Err(ModelError::InvalidMediaUrl {
                value: "ftp://x".to_owned(),
                reason: "URL must start with http://, https://, or data:",
            })
        );
    }

    #[test]
    fn deserialization_applies_the_media_url_rule() {
        let refused = [
            json!({ "type": "media", "url": "javascript:alert(1)", "mime_type": "text/html" }),
            json!({ "type": "audio", "url": "HTTPS://x/a.ogg" }),
            json!({ "type": "audio", "url": "https://x/a.ogg", "duration_seconds": -1.0 }),
            json!({ "type": "video", "url": "https://x/a.mp4", "thumbnail_url": "javascript:x" }),
        ]
        .map(|json| serde_json::from_value::<EventContent>(json).is_err());
        let media: EventContent = serde_json::from_value(
            json!({ "type": "media", "url": "HTTPS://A/b.png", "mime_type": "image/png" }),
        )
        .unwrap();
        assert_eq!(
            (refused, media),
            (
                [true; 4],
                MediaContent::new("https://A/b.png", "image/png")
                    .unwrap()
                    .into()
            )
        );
    }

    #[test]
    fn video_content_defaults_its_mime_type_to_mp4() {
        let parsed: EventContent =
            serde_json::from_value(json!({ "type": "video", "url": "https://example.com/a" }))
                .unwrap();
        assert_eq!(
            parsed,
            VideoContent::new("https://example.com/a").unwrap().into()
        );
    }

    #[test]
    fn template_content_defaults_its_language_to_english() {
        let parsed: EventContent =
            serde_json::from_value(json!({ "type": "template", "template_id": "welcome" }))
                .unwrap();
        let EventContent::Template(template) = parsed else {
            panic!("expected template content, got {parsed:?}");
        };
        assert_eq!(template.language, "en");
    }

    #[test]
    fn an_unknown_content_type_does_not_deserialize() {
        let json = json!({ "type": "hologram", "body": "hi" });
        assert!(serde_json::from_value::<EventContent>(json).is_err());
    }
}
