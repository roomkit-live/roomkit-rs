//! A knowledge source for tests: Python's `MockKnowledgeSource`.

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use roomkit::ChannelError;
use roomkit::model::{Metadata, RoomId};

use super::{IndexOptions, KnowledgeResult, KnowledgeSource, SearchOptions};

/// What [`MockKnowledgeSource::new`] takes: Python's
/// `MockKnowledgeSource(results=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MockKnowledgeSourceOptions {
    /// Python `results=None`: what every search answers, at most its limit.
    pub results: Vec<KnowledgeResult>,
}

/// What a mock source was asked.
#[derive(Debug, Default)]
pub(crate) struct Asked {
    pub(crate) searched: Vec<(String, Option<RoomId>, usize)>,
    pub(crate) indexed: Vec<(String, Option<Metadata>)>,
    pub(crate) closed: bool,
}

/// A source answering its results, at most a search's limit, and
/// recording what it was asked: Python's `MockKnowledgeSource`, for a
/// host's tests. What it recorded reads through methods of Python's
/// attributes' names.
///
/// ```
/// use roomkit_ai::knowledge::{
///     KnowledgeResult, KnowledgeSource, MockKnowledgeSource, MockKnowledgeSourceOptions,
///     SearchOptions,
/// };
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), roomkit::ChannelError> {
/// let source = MockKnowledgeSource::new(MockKnowledgeSourceOptions {
///     results: vec![KnowledgeResult::new("Refunds take five days.")],
/// });
/// let found = source.search("refund", SearchOptions::default()).await?;
/// assert_eq!(found.len(), 1);
/// assert_eq!(source.search_calls(), [("refund".to_owned(), None, 5)]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct MockKnowledgeSource {
    results: Vec<KnowledgeResult>,
    pub(crate) asked: Mutex<Asked>,
}

impl MockKnowledgeSource {
    /// The source answering `options`' results: Python's
    /// `MockKnowledgeSource(results=None)`.
    pub fn new(options: MockKnowledgeSourceOptions) -> Self {
        Self {
            results: options.results,
            asked: Mutex::default(),
        }
    }

    /// Each search it was asked, in order: the query, the room and the
    /// limit. Python's `search_calls`.
    pub fn search_calls(&self) -> Vec<(String, Option<RoomId>, usize)> {
        self.asked().searched.clone()
    }

    /// Each content it was asked to index, in order, with its metadata:
    /// Python's `index_calls`.
    pub fn index_calls(&self) -> Vec<(String, Option<Metadata>)> {
        self.asked().indexed.clone()
    }

    /// Whether it was closed: Python's `closed`.
    pub fn closed(&self) -> bool {
        self.asked().closed
    }

    pub(crate) fn asked(&self) -> std::sync::MutexGuard<'_, Asked> {
        self.asked.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl KnowledgeSource for MockKnowledgeSource {
    fn name(&self) -> String {
        "MockKnowledgeSource".to_owned()
    }

    fn search<'a>(
        &'a self,
        query: &'a str,
        options: SearchOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>> {
        self.asked()
            .searched
            .push((query.to_owned(), options.room_id, options.limit));
        let results = self.results.iter().take(options.limit).cloned().collect();
        Box::pin(future::ready(Ok(results)))
    }

    fn index<'a>(
        &'a self,
        content: &'a str,
        options: IndexOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.asked()
            .indexed
            .push((content.to_owned(), options.metadata));
        Box::pin(future::ready(Ok(())))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.asked().closed = true;
        Box::pin(future::ready(()))
    }
}
