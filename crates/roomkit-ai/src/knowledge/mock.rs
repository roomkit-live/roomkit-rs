//! A knowledge source for tests: Python's `MockKnowledgeSource`.

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use roomkit::ChannelError;
use roomkit::model::{Metadata, RoomId};

use super::{IndexOptions, KnowledgeResult, KnowledgeSource, SearchOptions};

/// What a mock source was asked.
#[derive(Debug, Default)]
pub(crate) struct Asked {
    pub(crate) searched: Vec<(String, Option<RoomId>, usize)>,
    pub(crate) indexed: Vec<(String, Option<Metadata>)>,
    pub(crate) closed: bool,
}

/// A source answering its results, at most a search's limit, and
/// recording what it was asked: Python's `MockKnowledgeSource`, for this
/// crate's tests until its recorded calls take their public form.
#[derive(Debug, Default)]
pub(crate) struct MockKnowledgeSource {
    results: Vec<KnowledgeResult>,
    pub(crate) asked: Mutex<Asked>,
}

impl MockKnowledgeSource {
    pub(crate) fn new(results: Vec<KnowledgeResult>) -> Self {
        Self {
            results,
            asked: Mutex::default(),
        }
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
