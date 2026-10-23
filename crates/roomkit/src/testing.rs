//! Channels for tests and examples: a transport that records what it
//! delivers, and an agent that echoes what it is asked.

use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventType, RoomContext, RoomEvent,
};
use roomkit_core::ops::ChannelDeclaration;
use roomkit_core::rules::transcoding::text_of;

use crate::channel::{Channel, ChannelError};

/// A delivery a [`RecordingTransport`] made.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    /// The event delivered, as transcoded for the transport.
    pub event: RoomEvent,
    /// Its text, as a person on the transport would read it.
    pub text: String,
}

/// A text transport that records every event delivered to it.
///
/// Cloning it shares the record, so a test keeps a clone to read what the
/// registered one delivered. Its destination is its record, so it needs no
/// recipient: its declaration names no recipient key, whatever its type
/// (RFC §10.2 step 3d). It addresses its senders as its type does: an SMS
/// recording transport writes them in E.164 (RFC §5.7).
#[derive(Clone, Debug)]
pub struct RecordingTransport {
    declaration: ChannelDeclaration,
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl RecordingTransport {
    /// A bidirectional text transport of `channel_type` named `channel_id`.
    pub fn new(channel_id: ChannelId, channel_type: ChannelType) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            channel_type,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration: ChannelDeclaration {
                provider_name: Some("RecordingTransport".to_owned()),
                recipient_key: None,
                ..declaration
            },
            deliveries: Arc::default(),
        }
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// What the transport delivered so far, in order.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The texts the transport delivered so far, in order.
    pub fn texts(&self) -> Vec<String> {
        self.deliveries()
            .into_iter()
            .map(|delivery| delivery.text)
            .collect()
    }
}

impl Channel for RecordingTransport {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let text = text_of(&event.content);
        self.deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Delivery { event, text });
        ready(Ok(ChannelOutput::default()))
    }
}

/// An agent that answers every event it is asked with `reply`, followed by
/// the event's text when `quote` is set.
#[derive(Clone, Debug)]
pub struct EchoAgent {
    declaration: ChannelDeclaration,
    reply: String,
    quote: bool,
}

impl EchoAgent {
    /// An agent named `channel_id` answering `reply`.
    pub fn new(channel_id: ChannelId, reply: impl Into<String>) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::Ai,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration: ChannelDeclaration {
                provider_name: Some("EchoAgent".to_owned()),
                ..declaration
            },
            reply: reply.into(),
            quote: false,
        }
    }

    /// The same agent, quoting the event it answers after its reply.
    #[must_use]
    pub fn quoting(mut self) -> Self {
        self.quote = true;
        self
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// The agent's answer to `event`.
    fn reply_to(&self, event: &RoomEvent) -> RoomEvent {
        let text = if self.quote {
            format!("{}{}", self.reply, text_of(&event.content))
        } else {
            self.reply.clone()
        };
        let source = EventSource::new(
            self.declaration.channel_id.clone(),
            ChannelType::Ai,
            EventDirection::Outbound,
        );
        // The pipeline gives every response a fresh id, its room and its
        // depth (gap G-024), and makes it answer the event that asked: the id
        // written here is a placeholder.
        RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text(text),
            event.created_at,
        )
    }
}

impl Channel for EchoAgent {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::respond(vec![self.reply_to(&event)])))
    }
}
