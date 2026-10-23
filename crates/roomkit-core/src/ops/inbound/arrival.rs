//! A message's way to the room's lock (RFC §10.1 steps 1 to 4): its channel
//! and its sender in the channel's form, its room, the room's context, and
//! the event its channel parses.
//!
//! Step 1 is pure and runs before anything is asked of the driver; steps 2
//! to 4 are a sub-machine of their own, [`Arrival`], which hands the
//! cascade the event of its first pass.

use std::mem;

use super::InboundError;
use super::named_room::{Named, NamedRoom};
use super::stamp;
use crate::model::{EventId, InboundMessage, RoomContext, RoomEvent, RoomId};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{ChannelDeclaration, Cx, KitConfig, Progress};
use crate::rules::{addresses, permissions};

/// Where an inbound message goes (RFC §10.1 step 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboundTarget {
    /// The room the caller names: used when it exists, created with the id
    /// when it does not.
    Room(RoomId),
}

impl InboundTarget {
    /// The room the message goes to.
    pub(super) fn room_id(&self) -> &RoomId {
        match self {
            Self::Room(room_id) => room_id,
        }
    }
}

/// Step 1: the registered channel, and the message with its sender written
/// in the channel's form, before anything reads it (RFC §5.7, §10.1 step 1,
/// gap G-052). The raw payload keeps the provider's own spelling.
///
/// # Errors
///
/// Returns [`InboundError::UnknownChannel`] when no channel has the
/// message's channel id.
pub(super) fn resolve(
    config: &KitConfig,
    mut message: InboundMessage,
) -> Result<(ChannelDeclaration, InboundMessage), InboundError> {
    let channel = config
        .channel(&message.channel_id)
        .cloned()
        .ok_or_else(|| InboundError::UnknownChannel(message.channel_id.clone()))?;
    message.sender_id = addresses::normalize(
        &message.sender_id,
        channel.addressing(),
        channel.country_code.as_ref(),
    );
    Ok((channel, message))
}

/// Steps 2 to 4 for one message.
#[derive(Debug)]
pub(super) struct Arrival {
    room_id: RoomId,
    message: Box<InboundMessage>,
    channel: Box<ChannelDeclaration>,
    state: State,
}

#[derive(Debug)]
enum State {
    /// Step 2, and its read for step 3.
    Naming(Box<NamedRoom>),
    /// Step 4.
    Parsing {
        effect: EffectId,
        id: EventId,
    },
    Done,
}

impl Arrival {
    /// The arrival of `message`, resolved on `channel` (step 1), into the
    /// room `target` names, for a caller acting for `organization_id`.
    pub(super) fn new(
        target: &InboundTarget,
        organization_id: Option<String>,
        channel: ChannelDeclaration,
        message: InboundMessage,
    ) -> Self {
        let room_id = target.room_id().clone();
        let channel = Box::new(channel);
        let named = NamedRoom::new(
            room_id.clone(),
            organization_id,
            channel.clone(),
            message.sender_id.clone(),
        );
        Self {
            room_id,
            message: Box::new(message),
            channel,
            state: State::Naming(Box::new(named)),
        }
    }

    /// Starts step 2.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if let State::Naming(named) = &mut self.state {
            named.start(cx);
        }
    }

    /// Takes the driver's answer to the arrival's outstanding effect; done
    /// with the stamped event of the message, or why it fails.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<RoomEvent, InboundError>> {
        match mem::replace(&mut self.state, State::Done) {
            State::Naming(mut named) => match named.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Naming(named);
                    Progress::Waiting
                }
                Progress::Done(context) => self.on_named(cx, context),
            },
            State::Parsing {
                effect: expected,
                id,
            } if expected == effect => Progress::Done(self.on_parsed(id, outcome)),
            state @ (State::Parsing { .. } | State::Done) => {
                debug_assert!(false, "the arrival received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// Hands the room's context to the channel's `handle_inbound()` (RFC
    /// §10.1 steps 3 and 4).
    fn on_named(
        &mut self,
        cx: &mut Cx<'_>,
        named: Named,
    ) -> Progress<Result<RoomEvent, InboundError>> {
        let context = match named {
            Ok(context) => context,
            Err(error) => return Progress::Done(Err(error)),
        };
        let id = cx.env.new_event_id();
        let effect = cx.io.issue(Request::HandleInbound {
            channel_id: self.message.channel_id.clone(),
            message: (*self.message).clone(),
            context: self.receiver_context(*context),
            id: id.clone(),
        });
        self.state = State::Parsing { effect, id };
        Progress::Waiting
    }

    /// The context the receiving channel parses with: only what it may see
    /// (RFC §7.5 rule 8), and no event when the room does not bind it (RFC
    /// §10.1 step 3).
    fn receiver_context(&self, context: RoomContext) -> Box<RoomContext> {
        let receiver = context
            .bindings
            .iter()
            .find(|binding| binding.channel_id == self.message.channel_id);
        Box::new(match receiver {
            Some(receiver) => permissions::context_for(receiver, &context),
            None => RoomContext {
                recent_events: Vec::new(),
                ..context
            },
        })
    }

    /// Stamps the event the channel parsed (RFC §10.1 step 4).
    fn on_parsed(&self, id: EventId, outcome: Outcome) -> Result<RoomEvent, InboundError> {
        let parsed = outcome
            .into_parsed()
            .map_err(|error| InboundError::ChannelFailed {
                channel_id: self.message.channel_id.clone(),
                error,
            })?;
        let mut event = stamp::stamp_trigger(*parsed, &self.message, &self.channel, id);
        event.room_id = self.room_id.clone();
        Ok(event)
    }
}
