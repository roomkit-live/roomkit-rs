//! Knowledge sources a memory enriches a turn's context with (RFC §20.2):
//! Python's `roomkit.knowledge`.
//!
//! - [`KnowledgeSource`]: a retrieval backend (a vector store, a search
//!   engine, a document index) that answers a query with passages, and may
//!   index what the room says.
//! - [`KnowledgeResult`]: one passage, its relevance and where it comes
//!   from.

use std::any::type_name;
use std::future::{self, Future};
use std::pin::Pin;

use roomkit::ChannelError;
use roomkit::model::{Metadata, RoomId};

mod mock;
pub use self::mock::{MockKnowledgeSource, MockKnowledgeSourceOptions};

/// One result of a knowledge source's search: Python's
/// `KnowledgeResult(content, score=0.0, source="", metadata={})`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KnowledgeResult {
    /// The text retrieved.
    pub content: String,
    /// How relevant it is, the higher the better: results of several
    /// sources are ranked and told apart by it.
    pub score: f64,
    /// Where it comes from, for a reader (`faq-db`, `product-docs`).
    pub source: String,
    /// What else the source says of it (a document id, a chunk index).
    pub metadata: Metadata,
}

impl KnowledgeResult {
    /// The passage `content`, of score 0 and no source.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            ..Self::default()
        }
    }
}

/// What [`KnowledgeSource::search`] takes beside the query: Python's
/// keyword-only `room_id=None, limit=5`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchOptions {
    /// The room the results are scoped to, when the source scopes them.
    pub room_id: Option<RoomId>,
    /// The most results it answers.
    pub limit: usize,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            room_id: None,
            limit: 5,
        }
    }
}

/// What [`KnowledgeSource::index`] takes beside the content: Python's
/// `metadata=None`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IndexOptions {
    /// What the source keeps beside the content (the room, the event).
    pub metadata: Option<Metadata>,
}

/// A knowledge retrieval backend (RFC §20.2): Python's `KnowledgeSource`.
/// Only [`search`](Self::search) is required: indexing and closing do
/// nothing by default, for a source that is read only.
///
/// ```
/// use std::future::{Future, ready};
/// use std::pin::Pin;
///
/// use roomkit::ChannelError;
/// use roomkit_ai::knowledge::{KnowledgeResult, KnowledgeSource, SearchOptions};
///
/// /// One passage, whatever the question.
/// struct Handbook;
///
/// impl KnowledgeSource for Handbook {
///     fn search<'a>(
///         &'a self,
///         _query: &'a str,
///         _options: SearchOptions,
///     ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>>
///     {
///         let passage = KnowledgeResult {
///             score: 1.0,
///             source: "handbook".to_owned(),
///             ..KnowledgeResult::new("Refunds are accepted within 30 days.")
///         };
///         Box::pin(ready(Ok(vec![passage])))
///     }
/// }
/// ```
pub trait KnowledgeSource: Send + Sync + 'static {
    /// The source's name: Python's `name`, its type's by default.
    fn name(&self) -> String {
        let full = type_name::<Self>();
        let plain = full.split('<').next().unwrap_or(full);
        plain.rsplit("::").next().unwrap_or(plain).to_owned()
    }

    /// The passages relevant to `query`, the most relevant first, at most
    /// `options.limit`: Python's `search`.
    fn search<'a>(
        &'a self,
        query: &'a str,
        options: SearchOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>>;

    /// Indexes `content`, as the room says it, for later searches: Python's
    /// `index`, nothing by default.
    fn index<'a>(
        &'a self,
        _content: &'a str,
        _options: IndexOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    /// Releases what the source holds: Python's `close`, nothing by
    /// default.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(future::ready(()))
    }
}
