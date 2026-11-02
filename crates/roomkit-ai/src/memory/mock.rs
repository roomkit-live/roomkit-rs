//! A memory for tests: Python's `MockMemoryProvider`.

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use roomkit::ChannelError;
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};

use super::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::provider::AiMessage;

/// What [`MockMemoryProvider::new`] takes: Python's
/// `MockMemoryProvider(messages=None, events=None, notes=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MockMemoryProviderOptions {
    /// The messages every retrieval answers.
    pub(crate) messages: Vec<AiMessage>,
    /// The events every retrieval answers.
    pub(crate) events: Vec<RoomEvent>,
    /// The notes every retrieval answers.
    pub(crate) notes: Vec<String>,
}

/// What a mock memory was asked.
#[derive(Debug, Default)]
struct Asked {
    retrieved: Vec<(RoomId, RoomEvent, RoomContext, Option<ChannelId>)>,
    ingested: Vec<(RoomId, RoomEvent, Option<ChannelId>)>,
    cleared: Vec<RoomId>,
    closed: bool,
}

/// A memory that answers what it was given and records what it was asked:
/// Python's `MockMemoryProvider`, for this crate's tests until its
/// recorded calls take their public form.
#[derive(Debug, Default)]
pub(crate) struct MockMemoryProvider {
    answer: MockMemoryProviderOptions,
    asked: Mutex<Asked>,
}

impl MockMemoryProvider {
    /// The memory answering `options`' messages, events and notes.
    pub(crate) fn new(options: MockMemoryProviderOptions) -> Self {
        Self {
            answer: options,
            asked: Mutex::default(),
        }
    }

    /// Each retrieval it was asked, in order: the room, the turn's event,
    /// the context and the channel asking. Python's `retrieve_calls`.
    pub(crate) fn retrieve_calls(
        &self,
    ) -> Vec<(RoomId, RoomEvent, RoomContext, Option<ChannelId>)> {
        self.asked().retrieved.clone()
    }

    /// Each event it learnt, in order: the room, the event and the channel.
    /// Python's `ingest_calls`.
    pub(crate) fn ingest_calls(&self) -> Vec<(RoomId, RoomEvent, Option<ChannelId>)> {
        self.asked().ingested.clone()
    }

    /// The rooms it was asked to forget, in order: Python's `clear_calls`.
    pub(crate) fn clear_calls(&self) -> Vec<RoomId> {
        self.asked().cleared.clone()
    }

    /// Whether it was closed: Python's `closed`.
    pub(crate) fn closed(&self) -> bool {
        self.asked().closed
    }

    fn asked(&self) -> std::sync::MutexGuard<'_, Asked> {
        self.asked.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MemoryProvider for MockMemoryProvider {
    fn name(&self) -> String {
        "MockMemoryProvider".to_owned()
    }

    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        self.asked().retrieved.push((
            room_id.clone(),
            current_event.clone(),
            context.clone(),
            options.channel_id,
        ));
        Box::pin(future::ready(Ok(MemoryResult {
            messages: self.answer.messages.clone(),
            events: self.answer.events.clone(),
            notes: self.answer.notes.clone(),
            ..MemoryResult::default()
        })))
    }

    fn ingest<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: &'a RoomEvent,
        options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.asked()
            .ingested
            .push((room_id.clone(), event.clone(), options.channel_id));
        Box::pin(future::ready(Ok(())))
    }

    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.asked().cleared.push(room_id.clone());
        Box::pin(future::ready(Ok(())))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.asked().closed = true;
        Box::pin(future::ready(()))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit::model::{ChannelType, EventSource, Room, TextContent};

    use super::*;
    use crate::provider::AiRole;

    fn room() -> RoomId {
        RoomId::new("room1").unwrap()
    }

    fn event(text: &str) -> RoomEvent {
        RoomEvent::new(
            room(),
            EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
            roomkit::model::EventContent::Text(TextContent::new(text)),
        )
    }

    fn context() -> RoomContext {
        RoomContext::new(Room::new(room()))
    }

    // Python: tests/test_memory.py::TestMockMemoryProvider::test_returns_configured_messages
    // and test_returns_configured_events
    #[tokio::test]
    async fn it_answers_what_it_was_given() {
        let summary = AiMessage::new(AiRole::System, "Summary");
        let memory = MockMemoryProvider::new(MockMemoryProviderOptions {
            messages: vec![summary.clone()],
            events: vec![event("past")],
            notes: vec!["note".to_owned()],
        });
        let read = memory
            .retrieve(
                &room(),
                &event("now"),
                &context(),
                RetrieveOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            (read.messages, read.events.len(), read.notes),
            (vec![summary], 1, vec!["note".to_owned()])
        );
    }

    // Python: tests/test_memory.py::TestMockMemoryProvider::test_tracks_retrieve_calls,
    // test_tracks_ingest_calls, test_tracks_clear_calls, test_close and test_name
    #[tokio::test]
    async fn it_records_what_it_was_asked() {
        let memory = MockMemoryProvider::default();
        let agent = Some(ChannelId::new("ai1").unwrap());
        let options = RetrieveOptions {
            channel_id: agent.clone(),
        };
        memory
            .retrieve(&room(), &event("now"), &context(), options)
            .await
            .unwrap();
        let learnt = IngestOptions {
            channel_id: agent.clone(),
        };
        memory.ingest(&room(), &event("hi"), learnt).await.unwrap();
        memory.clear(&room()).await.unwrap();
        assert!(!memory.closed());
        memory.close().await;
        let retrieved = memory.retrieve_calls();
        let ingested = memory.ingest_calls();
        assert_eq!(
            (
                retrieved
                    .iter()
                    .map(|call| (&call.0, &call.3))
                    .collect::<Vec<_>>(),
                ingested
                    .iter()
                    .map(|call| (&call.0, &call.2))
                    .collect::<Vec<_>>(),
                memory.clear_calls(),
                memory.closed(),
                memory.name(),
            ),
            (
                vec![(&room(), &agent)],
                vec![(&room(), &agent)],
                vec![room()],
                true,
                "MockMemoryProvider".to_owned(),
            )
        );
    }
}
