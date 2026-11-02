//! What this module's tests share: a summarizer that answers from a list
//! and records what it was asked, and the events and contexts they read.

use std::future::{self, Future};
use std::sync::{Arc, Mutex};

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventId, EventSource, Room, RoomContext, RoomEvent,
    RoomId, TextContent,
};

use crate::provider::{AiContext, AiProvider, AiResponse, ProviderError};

/// What a [`Summarizer`] was asked.
#[derive(Debug, Default)]
pub(super) struct Asked {
    pub(super) prompts: Vec<String>,
    pub(super) closed: bool,
}

/// A summarizer answering its answers in turn, then failing; its clones
/// share what it was asked.
#[derive(Clone, Debug, Default)]
pub(super) struct Summarizer {
    answers: Vec<String>,
    pub(super) asked: Arc<Mutex<Asked>>,
}

impl Summarizer {
    pub(super) fn answering(answers: &[&str]) -> Self {
        Self {
            answers: answers.iter().map(|answer| (*answer).to_owned()).collect(),
            asked: Arc::default(),
        }
    }

    pub(super) fn prompts(&self) -> Vec<String> {
        self.asked.lock().unwrap().prompts.clone()
    }
}

impl AiProvider for Summarizer {
    fn model_name(&self) -> &'static str {
        "summarizer"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        let mut asked = self.asked.lock().unwrap();
        let at = asked.prompts.len();
        let prompt = context
            .messages
            .first()
            .map(|message| message.content.text());
        asked.prompts.push(prompt.unwrap_or_default());
        let answer = self.answers.get(at % self.answers.len().max(1));
        future::ready(answer.map_or_else(
            || Err(ProviderError::from("summarizer down".to_owned())),
            |answer| Ok(AiResponse::new(answer.clone())),
        ))
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        self.asked.lock().unwrap().closed = true;
        future::ready(())
    }
}

pub(super) fn room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// A text event of `id` from an SMS sender, or the agent's with `ai`.
pub(super) fn said(body: &str, id: &str, ai: bool) -> RoomEvent {
    let source = if ai {
        EventSource::new(ChannelId::new("ai1").unwrap(), ChannelType::AI)
    } else {
        EventSource::new(ChannelId::new("ch-1").unwrap(), ChannelType::SMS)
    };
    RoomEvent {
        id: EventId::new(id).unwrap(),
        ..RoomEvent::new(room(), source, EventContent::Text(TextContent::new(body)))
    }
}

pub(super) fn event(body: &str, id: &str) -> RoomEvent {
    said(body, id, false)
}

pub(super) fn context() -> RoomContext {
    RoomContext::new(Room::new(room()))
}

/// `count` events of `body` repeated 200 times, numbered.
pub(super) fn long_events(body: &str, count: usize) -> Vec<RoomEvent> {
    (0..count)
        .map(|n| event(&format!("{body} {n}").repeat(200), &format!("e{n}")))
        .collect()
}
