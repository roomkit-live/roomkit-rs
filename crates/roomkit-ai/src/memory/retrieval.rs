//! A memory that enriches a turn with knowledge passages (RFC §20.2):
//! Python's `RetrievalMemory`.

use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit::model::{EventContent, Metadata, RoomContext, RoomEvent, RoomId};
use roomkit_core::rules::transcript::fence;
use serde_json::Value;

use super::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::knowledge::{IndexOptions, KnowledgeResult, KnowledgeSource, SearchOptions};
use crate::stream::join_all;

/// What [`RetrievalMemory::new`] takes beside its sources and its inner
/// memory: Python's keyword-only `max_results=5, min_query_length=3`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetrievalMemoryOptions {
    /// The most passages a turn's note holds.
    pub max_results: usize,
    /// The shortest query searched, in characters.
    pub min_query_length: usize,
}

impl Default for RetrievalMemoryOptions {
    fn default() -> Self {
        Self {
            max_results: 5,
            min_query_length: 3,
        }
    }
}

/// Wraps a memory and enriches each turn with what its knowledge sources
/// hold (RFC §20.2): Python's `RetrievalMemory`.
///
/// A turn's query is the author's words in its event, a media's caption
/// included, never a file name or a type (RFC §6.6); a query shorter than
/// `min_query_length` searches nothing. Every source is searched at once,
/// one that fails logged and left out; the passages, one per content at
/// its best score, the best first and `max_results` at most, become one
/// note of the turn, each fenced as data: they ride the turn's input, not
/// the history a provider caches. Each event learnt is indexed in every
/// source too.
pub struct RetrievalMemory {
    sources: Vec<Arc<dyn KnowledgeSource>>,
    inner: Arc<dyn MemoryProvider>,
    options: RetrievalMemoryOptions,
}

impl RetrievalMemory {
    /// `inner`, each turn enriched from `sources`.
    pub fn new(
        sources: Vec<Arc<dyn KnowledgeSource>>,
        inner: Arc<dyn MemoryProvider>,
        options: RetrievalMemoryOptions,
    ) -> Self {
        Self {
            sources,
            inner,
            options,
        }
    }

    /// The passages the sources hold for `query` in the room `room_id`.
    async fn passages(&self, query: &str, room_id: &RoomId) -> Vec<KnowledgeResult> {
        let searches = self.sources.iter().map(|source| {
            let options = SearchOptions {
                room_id: Some(room_id.clone()),
                limit: self.options.max_results,
            };
            source.search(query, options)
        });
        let answers = join_all(searches.collect()).await;
        let mut found = Vec::new();
        for (source, answer) in self.sources.iter().zip(answers) {
            match answer {
                Ok(results) => found.extend(results),
                Err(error) => {
                    tracing::warn!(target: "roomkit::memory", source = %source.name(), %error, "a knowledge source failed");
                }
            }
        }
        best(found, self.options.max_results)
    }
}

/// `results`, one per content at its best score, the best first,
/// `limit` at most.
fn best(results: Vec<KnowledgeResult>, limit: usize) -> Vec<KnowledgeResult> {
    let mut kept: Vec<KnowledgeResult> = Vec::new();
    for result in results {
        match kept.iter_mut().find(|seen| seen.content == result.content) {
            Some(seen) if result.score > seen.score => *seen = result,
            Some(_) => {}
            None => kept.push(result),
        }
    }
    kept.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    kept.truncate(limit);
    kept
}

/// The passages as the turn's note, each set apart as data.
fn note(passages: &[KnowledgeResult]) -> String {
    let mut lines = vec!["Relevant context from knowledge sources:".to_owned()];
    for passage in passages {
        let prefix = if passage.source.is_empty() {
            String::new()
        } else {
            format!("[{}] ", passage.source)
        };
        lines.push(fence("knowledge", &format!("{prefix}{}", passage.content)));
    }
    lines.join("\n")
}

/// The author's words in `content` (RFC §6.6): its text, a media's
/// caption, a voice message's transcript, never a file name or a type; a
/// composite's parts' words, one per line. Python's `content_words`, an
/// empty text for a content of another kind.
fn words(content: &EventContent) -> String {
    match content {
        EventContent::Text(text) => text.body.clone(),
        EventContent::Rich(rich) => rich
            .plain_text
            .clone()
            .filter(|plain| !plain.is_empty())
            .unwrap_or_else(|| rich.body.clone()),
        EventContent::Media(media) => media.caption.clone().unwrap_or_default(),
        EventContent::Audio(audio) => audio.transcript.clone().unwrap_or_default(),
        EventContent::Composite(composite) => composite
            .parts()
            .iter()
            .map(words)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        EventContent::Video(_)
        | EventContent::Location(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => String::new(),
    }
}

impl MemoryProvider for RetrievalMemory {
    fn name(&self) -> String {
        format!("RetrievalMemory({})", self.inner.name())
    }

    fn recent_events_window(&self) -> usize {
        self.inner.recent_events_window()
    }

    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = self
                .inner
                .retrieve(room_id, current_event, context, options)
                .await?;
            let query = words(&current_event.content);
            if self.sources.is_empty()
                || query.is_empty()
                || query.chars().count() < self.options.min_query_length
            {
                return Ok(result);
            }
            let passages = self.passages(&query, room_id).await;
            if !passages.is_empty() {
                // For this turn alone: with the turn's notes, not ahead of
                // the history a provider caches (RFC §20.2).
                result.notes.push(note(&passages));
            }
            Ok(result)
        })
    }

    /// Hands `event` to the inner memory, then indexes its words in every
    /// source, a failing one logged.
    fn ingest<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: &'a RoomEvent,
        options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            self.inner.ingest(room_id, event, options).await?;
            let text = words(&event.content);
            if text.is_empty() {
                return Ok(());
            }
            let mut metadata = Metadata::new();
            metadata.insert("room_id".to_owned(), Value::from(room_id.as_str()));
            metadata.insert("event_id".to_owned(), Value::from(event.id.as_str()));
            let indexing = self.sources.iter().map(|source| {
                let options = IndexOptions {
                    metadata: Some(metadata.clone()),
                };
                source.index(&text, options)
            });
            let answers = join_all(indexing.collect()).await;
            for (source, answer) in self.sources.iter().zip(answers) {
                if let Err(error) = answer {
                    tracing::warn!(target: "roomkit::memory", source = %source.name(), %error, "a knowledge source failed to index");
                }
            }
            Ok(())
        })
    }

    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.clear(room_id)
    }

    /// Closes the inner memory, then every source.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.inner.close().await;
            join_all(self.sources.iter().map(|source| source.close()).collect()).await;
        })
    }
}

impl fmt::Debug for RetrievalMemory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sources: Vec<String> = self.sources.iter().map(|source| source.name()).collect();
        formatter
            .debug_struct("RetrievalMemory")
            .field("sources", &sources)
            .field("inner", &self.inner.name())
            .field("options", &self.options)
            .finish()
    }
}

#[cfg(test)]
mod tests;
