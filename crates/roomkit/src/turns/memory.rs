//! One turn as the in-memory backend carries it (RFC §10.6.1, §10.6.5): its
//! two pipes, the owner's execution contexts its work runs in, and its
//! request, the events as they are and the rest serialized.

use std::sync::{Arc, Mutex};

use roomkit_core::model::{
    ChannelBinding, ChannelId, Metadata, Participant, Room, RoomContext, RoomEvent, RoomId,
};
use serde::Serialize;

use super::lock;
use super::observe::{Carried, Observers};
use super::pipe::{MAX_MESSAGE_BYTES, Pipe};
use super::timeouts::TurnTimeouts;
use super::wire::{Control, Frame, OwnerTool, PreviousTurn};
use crate::context::ExecutionContext;

/// One turn in memory: what its two sides share.
#[derive(Debug)]
pub(crate) struct MemoryTurn {
    pub(crate) turn_id: Arc<str>,
    /// Executor to owner.
    pub(crate) frames: Pipe<Frame>,
    /// Owner to executor.
    pub(crate) controls: Pipe<Control>,
    /// The bounds both sides keep.
    pub(crate) timeouts: TurnTimeouts,
    /// The owner's execution contexts the turn's work runs in (RFC
    /// §10.6.5), the only thing the in-memory backend carries beside the
    /// messages; let go of once the turn is over.
    pub(super) contexts: Mutex<Contexts>,
}

/// The owner's execution contexts a turn's work runs in.
#[derive(Debug, Default)]
pub(super) struct Contexts {
    /// The context of the owner's call that submitted the turn, which its
    /// `on_event()` runs in.
    pub(super) turn: ExecutionContext,
    /// The context of the owner's call that first read the turn's stream,
    /// set as its first pull is sent, which the stream advances in.
    pub(super) stream: Option<ExecutionContext>,
}

impl MemoryTurn {
    /// The turn `turn_id` on `frames` and `controls`, keeping `timeouts`,
    /// its work to run in the context of the current call, the owner's that
    /// submits it.
    pub(super) fn new(
        turn_id: Arc<str>,
        frames: Pipe<Frame>,
        controls: Pipe<Control>,
        timeouts: TurnTimeouts,
    ) -> Self {
        Self {
            turn_id,
            frames,
            controls,
            timeouts,
            contexts: Mutex::new(Contexts {
                turn: ExecutionContext::current(),
                stream: None,
            }),
        }
    }

    /// The context the turn's `on_event()` runs in.
    pub(crate) fn turn_context(&self) -> ExecutionContext {
        lock(&self.contexts).turn.clone()
    }

    /// The context the turn's stream advances in: its first reader's, else
    /// the turn's own.
    pub(crate) fn stream_context(&self) -> ExecutionContext {
        let contexts = lock(&self.contexts);
        contexts
            .stream
            .clone()
            .unwrap_or_else(|| contexts.turn.clone())
    }

    /// Lends the stream the context of the call reading it, unless one
    /// already did.
    pub(crate) fn lend_stream_context(&self) {
        lock(&self.contexts)
            .stream
            .get_or_insert_with(ExecutionContext::current);
    }

    /// Stops both directions: their readers learn it, and what is put on
    /// them later is dropped. The owner's contexts are let go of: a turn
    /// over holds nothing of the run that submitted it.
    pub(crate) fn shut(&self) {
        self.frames.shut();
        self.controls.shut();
        *lock(&self.contexts) = Contexts::default();
    }
}

/// A turn's request as the in-memory backend carries it: its events as
/// they are, the rest serialized (RFC §10.6.1).
#[derive(Debug)]
pub(crate) struct CarriedRequest {
    pub(crate) rest: String,
    pub(crate) event: RoomEvent,
    pub(crate) history: Vec<RoomEvent>,
}

/// A turn's request, as the owner submits it.
pub(super) struct Request<'a> {
    pub(super) turn_id: &'a str,
    pub(super) binding: &'a ChannelBinding,
    pub(super) event: &'a RoomEvent,
    /// The room's context, its recent events given apart.
    pub(super) context: &'a RoomContext,
    pub(super) history: &'a [RoomEvent],
    pub(super) previous: Option<&'a PreviousTurn>,
}

impl Request<'_> {
    /// The request without its events, serialized within its bound; a tap
    /// is told it whole.
    ///
    /// # Errors
    ///
    /// What makes it unable to cross.
    pub(super) fn write(&self, observers: &Observers) -> Result<String, String> {
        let rest = self.rest(None);
        let written = serde_json::to_string(&rest).map_err(|error| error.to_string())?;
        if written.len() > MAX_MESSAGE_BYTES {
            return Err(format!(
                "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
                written.len()
            ));
        }
        if observers.tapped() {
            let whole = RequestOut {
                rest: &self.rest(Some(self.history)),
                event: self.event,
            };
            if let Ok(raw) = serde_json::to_string(&whole) {
                observers.tell(Carried::Request, &raw);
            }
        }
        Ok(written)
    }

    /// The request's fields, its context's recent events `history` when
    /// written whole.
    fn rest<'a>(&'a self, history: Option<&'a [RoomEvent]>) -> RestOut<'a> {
        RestOut {
            turn_id: self.turn_id,
            room_id: &self.event.room_id,
            channel_id: &self.binding.channel_id,
            binding: self.binding,
            context: ContextOut {
                room: &self.context.room,
                bindings: &self.context.bindings,
                participants: &self.context.participants,
                recent_events: history,
                author_register: self.context.author_register.as_ref(),
            },
            strategy_turn: false,
            notes: [],
            in_place: false,
            owner_tools: [],
            room_hooks: [],
            previous_turn: self.previous,
            trace: None,
        }
    }
}

/// A request without its events, as it is written.
#[derive(Serialize)]
struct RestOut<'a> {
    turn_id: &'a str,
    room_id: &'a RoomId,
    channel_id: &'a ChannelId,
    binding: &'a ChannelBinding,
    context: ContextOut<'a>,
    strategy_turn: bool,
    notes: [String; 0],
    in_place: bool,
    owner_tools: [OwnerTool; 0],
    room_hooks: [String; 0],
    previous_turn: Option<&'a PreviousTurn>,
    trace: Option<()>,
}

/// A room's context, its recent events given apart.
#[derive(Serialize)]
struct ContextOut<'a> {
    room: &'a Room,
    bindings: &'a [ChannelBinding],
    participants: &'a [Participant],
    #[serde(skip_serializing_if = "Option::is_none")]
    recent_events: Option<&'a [RoomEvent]>,
    author_register: Option<&'a Metadata>,
}

/// A whole request, as a tap reads it.
#[derive(Serialize)]
struct RequestOut<'a> {
    #[serde(flatten)]
    rest: &'a RestOut<'a>,
    event: &'a RoomEvent,
}
