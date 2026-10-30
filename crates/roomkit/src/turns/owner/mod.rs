//! The owner's side of one turn (RFC §10.6.4, §10.6.5, §10.6.8).
//!
//! - [`OwnerTurn`]: what the turn's reader holds: the output, then the
//!   stream, each read pulling the executor a window of items further;
//!   dropped before its output, it cancels the turn, dropped with its
//!   stream unread, it closes it;
//! - the pump, a task of the kit's: it reads every frame as it comes,
//!   applies at once those that change what the owner holds or ask it to
//!   act (a call's arguments, a host verb, a framework event), queues the
//!   output, the items, the error and the end for the reader, beats every
//!   interval, and fails the turn once the executor fell silent too long,
//!   or gives it up once a cancelled turn did not end within its bound;
//! - `verbs`: the verbs a turn's host asks, run on the owner.
//!
//! This module holds what both share; `reader` is the reader's side,
//! `pump` the pump.

mod pump;
mod reader;
mod verbs;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use roomkit_core::model::{ChannelBinding, ChannelId, Metadata, Room, RoomEvent, RoomId};
use tokio::sync::{mpsc, watch};

use self::pump::Pump;
pub(crate) use self::reader::{OwnerTurn, RemoteItem};
pub(crate) use self::verbs::{answer_room_call, last_turn};
use super::error::TurnError;
use super::pipe::PipeReceiver;
use super::wire::{Control, Frame, Item, WireError, WireOutput};
use super::{MemoryTurn, Turns};
use crate::channel::BoxFuture;
use crate::context::ExecutionContext;
use crate::turn::TurnKit;

/// What the owner knows of a turn it submitted, which its verbs read.
#[derive(Clone, Debug)]
pub(crate) struct Facts {
    pub(crate) room_id: RoomId,
    pub(crate) channel_id: ChannelId,
    /// The depth of the turn's response (RFC §8.3): what the turn sends is
    /// at least that deep.
    pub(crate) response_depth: u32,
    /// The room's organization (RFC §17.2).
    pub(crate) organization_id: Option<String>,
}

impl Facts {
    pub(crate) fn of(binding: &ChannelBinding, event: &RoomEvent, room: &Room) -> Self {
        Self {
            room_id: event.room_id.clone(),
            channel_id: binding.channel_id.clone(),
            response_depth: roomkit_core::rules::chain_depth::response_depth(event),
            organization_id: room.organization_id.clone(),
        }
    }
}

/// What the pump hands the turn's reader, in order.
#[derive(Debug)]
pub(super) enum Main {
    Output(WireOutput, bool),
    /// The turn's record as it stands, sent before the item it was read
    /// with: the record a loop's end that cannot carry it whole ends on.
    Record(Metadata),
    Item(u64, Item),
    Error(WireError),
    End,
    Failed(TurnError),
}

/// What the pump and the reader share.
#[derive(Debug)]
pub(super) struct OwnerState {
    pub(super) turn: Arc<MemoryTurn>,
    pub(super) facts: Facts,
    /// The context of the owner's call last waiting on the turn: a verb the
    /// turn asks runs in a copy of it (RFC §10.6.5).
    pub(super) waiting: Mutex<ExecutionContext>,
    /// The arguments each call runs with once its gate fixed them, by its
    /// start's frame (`tool_args`).
    pub(super) ran_with: Mutex<HashMap<u64, Metadata>>,
    /// The items the reader granted, in all: an executor that sends more
    /// fails its turn.
    pub(super) granted: AtomicU64,
    /// The reason the owner cancelled the turn, once it did.
    pub(super) cancel: watch::Sender<Option<String>>,
    /// Whether the turn is over for the owner.
    pub(super) over: watch::Sender<bool>,
}

/// What cuts a turn from outside its reader: the run whose call the turn
/// is, when it gives the call up (RFC §10.1 "Explicit delivery
/// cancellation"), and what awaits its end.
#[derive(Clone, Debug)]
pub(crate) struct Canceller {
    state: Arc<OwnerState>,
}

impl Canceller {
    /// Cuts the turn: `cancel` crosses at once, before the call's task is
    /// even dropped, so its executor reads it before it goes on.
    pub(crate) fn cancel(&self, reason: &str) {
        cancel(&self.state, reason);
    }

    /// Returns once the turn is over for the owner.
    pub(crate) async fn ended(&self) {
        let mut over = self.state.over.subscribe();
        drop(over.wait_for(|over| *over).await);
    }
}

/// The owner's side of `turn`, and the pump to run as a task of the kit's.
pub(crate) fn open(
    turn: Arc<MemoryTurn>,
    frames: PipeReceiver<Frame>,
    facts: Facts,
    kit: Arc<dyn TurnKit>,
    turns: Arc<Turns>,
) -> (OwnerTurn, BoxFuture<'static, ()>) {
    let state = Arc::new(OwnerState {
        turn,
        facts,
        waiting: Mutex::new(ExecutionContext::default()),
        ran_with: Mutex::new(HashMap::new()),
        granted: AtomicU64::new(0),
        cancel: watch::Sender::new(None),
        over: watch::Sender::new(false),
    });
    let (main, received) = mpsc::unbounded_channel();
    let pump = Pump::new(Arc::clone(&state), frames, main, kit, turns);
    (OwnerTurn::new(state, received), Box::pin(pump.run()))
}

/// Sends `cancel` once, the pump then waiting for the end within the
/// cancel bound.
pub(super) fn cancel(state: &OwnerState, reason: &str) {
    if *state.over.borrow() || state.cancel.borrow().is_some() {
        return;
    }
    let control = Control::Cancel {
        reason: reason.to_owned(),
    };
    drop(state.turn.controls.put(&control));
    state.cancel.send_replace(Some(reason.to_owned()));
}
