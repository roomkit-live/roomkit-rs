//! A channel's turns in a room, one after the other (RFC §6.4 "A room's
//! turns one after the other"), and a message handed to the running one
//! ("A message for the running turn"): Python's `RoomTurns`.
//!
//! A channel that serializes its room turns has each turn take its place
//! in the channel's line in the room as its delivery set asks it, in the
//! room's delivery order. Its answer is a stream that starts nothing: read
//! once the set is over, outside the room's delivery lane, it waits until
//! the turns asked before it are over, then runs the turn on the room as it
//! is then, less the messages committed after it was asked
//! ([`history::turn_context_as_asked`]), and gives its place up once its
//! rows are stored, so the next turn reads that answer. A stream never read
//! (its delivery cancelled, its run over) gives its place up unread: the
//! turn never starts.
//!
//! A channel that steers its room turns hands a message whose room's line
//! holds its one running turn to that turn, as an `InjectMessage`, rather
//! than a turn of its own (RFC §21.3).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{ChannelOutcome, Outcome};
use roomkit_core::rules::transcript::steered_text;
use tokio::sync::watch;

use crate::history;
use crate::kit::Inner;
use crate::steering::{InjectMessage, SteeringDirective};
use crate::store::ConversationStore;
use crate::stream::{KeptStream, Stops};
use crate::telemetry::{get_current_span, within_kit};
use crate::turn::TurnKit;
use crate::turns::{Answer, SteerAddress};

type Key = (RoomId, ChannelId);

/// The lines of the channels that take a room's turns one at a time: one
/// per channel and room, kept while a turn of it waits or runs.
#[derive(Debug, Default)]
pub(crate) struct RoomTurns {
    lines: Mutex<HashMap<Key, VecDeque<Arc<Spot>>>>,
    next: AtomicU64,
}

/// One place of a line.
#[derive(Debug)]
struct Spot {
    id: u64,
    /// `true` once the turns asked before it are over.
    reached: watch::Sender<bool>,
}

/// A turn's place in its channel's line in a room: taken as the turn is
/// asked, reached once the turns asked before it are over, given up as it
/// drops.
#[derive(Debug)]
pub(crate) struct Place {
    turns: Arc<RoomTurns>,
    key: Key,
    spot: Arc<Spot>,
}

impl RoomTurns {
    /// A place in `channel_id`'s line in `room_id`, after the turns asked
    /// before it.
    pub(crate) fn join(self: &Arc<Self>, room_id: &RoomId, channel_id: &ChannelId) -> Place {
        let key = (room_id.clone(), channel_id.clone());
        let spot = Arc::new(Spot {
            id: self.next.fetch_add(1, Ordering::Relaxed),
            reached: watch::Sender::new(false),
        });
        let mut lines = lock(&self.lines);
        let line = lines.entry(key.clone()).or_default();
        if line.is_empty() {
            spot.reached.send_replace(true);
        }
        line.push_back(Arc::clone(&spot));
        drop(lines);
        Place {
            turns: Arc::clone(self),
            key,
            spot,
        }
    }

    /// Whether `channel_id`'s line in `room_id` holds one turn alone: the
    /// one running, no turn waiting behind it.
    pub(crate) fn holds_one(&self, room_id: &RoomId, channel_id: &ChannelId) -> bool {
        let key = (room_id.clone(), channel_id.clone());
        lock(&self.lines)
            .get(&key)
            .is_some_and(|line| line.len() == 1)
    }

    /// `spot` leaves its line; the next one is reached if it was first.
    fn leave(&self, key: &Key, spot: &Spot) {
        let mut lines = lock(&self.lines);
        let Some(line) = lines.get_mut(key) else {
            return;
        };
        let Some(position) = line.iter().position(|held| held.id == spot.id) else {
            return;
        };
        line.remove(position);
        match line.front() {
            None => {
                lines.remove(key);
            }
            Some(next) if position == 0 => {
                next.reached.send_replace(true);
            }
            Some(_) => {}
        }
    }
}

impl Place {
    /// Returns once the turns asked before it are over.
    async fn reached(&self) {
        let mut reached = self.spot.reached.subscribe();
        drop(reached.wait_for(|reached| *reached).await);
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        self.turns.leave(&self.key, &self.spot);
    }
}

/// A turn of a channel that serializes its room turns, asked and waiting
/// its place: its stream, until it is read.
#[derive(Debug)]
pub(crate) struct Queued {
    place: Place,
    binding: ChannelBinding,
    event: RoomEvent,
    /// The room as the turn was asked.
    asked: RoomContext,
    /// How many of the room's latest messages the turn reads.
    window: usize,
    /// The span of the broadcast that asked the turn, which it runs under
    /// whenever it starts, as Python's `restored_span`.
    span: Option<String>,
}

/// What a waiting turn's start came to.
#[derive(Debug)]
pub(crate) enum Started {
    /// The turn runs: its stream, and its place, held until it is read.
    Turn(KeptStream, Place),
    /// No stream to read: what answers the read.
    Answered(Outcome),
}

impl Queued {
    /// The turn of `binding`'s channel on `event`, asked in `asked`, at
    /// `place`.
    pub(crate) fn new(
        place: Place,
        binding: ChannelBinding,
        event: RoomEvent,
        asked: RoomContext,
        window: usize,
    ) -> Self {
        Self {
            place,
            binding,
            event,
            asked,
            window,
            span: get_current_span(),
        }
    }

    /// Waits until the turns asked before it are over, then starts the
    /// turn on the room as it is now, less what came after it was asked
    /// (RFC §6.4). A read stopped meanwhile starts nothing; a channel
    /// detached from the room meanwhile takes no turn.
    pub(crate) async fn start<S: ConversationStore>(
        self,
        inner: &Arc<Inner<S>>,
        stops: &mut Stops,
    ) -> Started {
        let Self {
            place,
            binding,
            event,
            asked,
            window,
            span,
        } = self;
        tokio::select! {
            biased;
            _ = stops.stopped() => return Started::Answered(nothing()),
            () = place.reached() => {}
        }
        let Some(context) = room_now(&*inner.store, asked, &binding).await else {
            return Started::Answered(nothing());
        };
        let context =
            history::turn_context_as_asked(&*inner.store, &binding, &event, context, window).await;
        let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
        let run = within_kit(
            &inner.telemetry,
            span,
            inner.turns.run(&kit, &binding, event, context, |_| {}),
        );
        let answered = tokio::select! {
            biased;
            _ = stops.stopped() => return Started::Answered(nothing()),
            answered = run => answered,
        };
        match answered {
            Ok(Answer::Streamed(output, turn)) if answers_nothing_beside(&output) => {
                Started::Turn(KeptStream { turn, then: None }, place)
            }
            Ok(Answer::Output(output)) if answers_nothing_beside(&output) => {
                Started::Answered(nothing())
            }
            Ok(Answer::Streamed(..) | Answer::Output(_)) => {
                Started::Answered(refused_beside(&binding.channel_id))
            }
            Err(failure) => Started::Answered(failure.into_outcome()),
        }
    }
}

/// The room as a waiting turn starts on: its record and bindings as the
/// store holds them now, the rest as the turn was asked; `None` once the
/// channel of `binding` left the room.
async fn room_now<S: ConversationStore>(
    store: &S,
    asked: RoomContext,
    binding: &ChannelBinding,
) -> Option<RoomContext> {
    let room_id = asked.room.id.clone();
    let bindings = store.list_bindings(&room_id).await.ok()?;
    if !bindings
        .iter()
        .any(|bound| bound.channel_id == binding.channel_id)
    {
        return None;
    }
    let room = match store.get_room(&room_id).await {
        Ok(Some(room)) => room,
        Ok(None) | Err(_) => asked.room.clone(),
    };
    Some(RoomContext {
        room,
        bindings,
        ..asked
    })
}

/// Whether a turn that waited its place answered through its stream alone:
/// what it returned beside it would reach nobody.
fn answers_nothing_beside(output: &ChannelOutput) -> bool {
    output.response_events.is_empty()
        && output.tasks.is_empty()
        && output.observations.is_empty()
        && output.metadata_updates.is_empty()
}

/// A turn that waited its place and answered beside its stream fails, as
/// Python's raises `TypeError`.
fn refused_beside(channel_id: &ChannelId) -> Outcome {
    let error = ErrorRecord::new(
        "TypeError",
        format!(
            "Channel '{channel_id}' takes a room's turns one at a time (serializes_room_turns) \
             and answered beside its response stream"
        ),
    );
    ChannelOutcome::Output(Box::new(ChannelOutput {
        error: Some(error),
        ..ChannelOutput::default()
    }))
    .into()
}

/// The read of a turn that never ran: nothing.
fn nothing() -> Outcome {
    ChannelOutcome::Output(Box::default()).into()
}

/// Whether `event`, for a room whose line of `binding`'s channel holds its
/// one running turn, was handed to that turn (RFC §6.4 "A message for the
/// running turn", §21.3): steered in as the text it reads as, under its
/// speaker's label when the room has several speakers, which the turn's
/// loop reached. A message that reads as no text, or that no loop takes
/// (not reachable yet, or reading its queue no more), waits a turn of its
/// own; so does an event that opens no turn (a notice, a change: RFC
/// §6.4), never handed to the running one.
pub(crate) async fn steered<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    binding: &ChannelBinding,
    event: &RoomEvent,
    context: &RoomContext,
) -> bool {
    let (room_id, channel_id) = (&event.room_id, &binding.channel_id);
    if !inner.room_turns.holds_one(room_id, channel_id) {
        return false;
    }
    let Some(text) = steered_text(event, context, channel_id) else {
        return false;
    };
    let directive = SteeringDirective::from(InjectMessage::new(text));
    let address = SteerAddress {
        channel: channel_id,
        room: Some(room_id),
        turn: None,
        organization: None,
    };
    inner.turns.steer(&directive, address).await > 0
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> (RoomId, ChannelId) {
        (RoomId::new("r").unwrap(), ChannelId::new("agent").unwrap())
    }

    // Python: tests/test_room_turns_in_order.py::test_a_waiting_turn_nobody_closes_gives_its_place_up_once_collected
    #[tokio::test]
    async fn a_waiting_turn_dropped_unread_gives_its_place_up() {
        let turns = Arc::new(RoomTurns::default());
        let (room, channel) = key();
        let running = turns.join(&room, &channel);
        let unread = turns.join(&room, &channel);
        let next = turns.join(&room, &channel);

        drop(unread);
        drop(running);

        tokio::time::timeout(std::time::Duration::from_secs(2), next.reached())
            .await
            .unwrap();
        drop(next);
        assert!(lock(&turns.lines).is_empty());
    }

    /// RFC §6.4: the line holds one turn alone only while no other waits.
    #[test]
    fn a_line_holds_one_while_no_turn_waits_behind_it() {
        let turns = Arc::new(RoomTurns::default());
        let (room, channel) = key();
        let running = turns.join(&room, &channel);
        let alone = turns.holds_one(&room, &channel);
        let waiting = turns.join(&room, &channel);
        let behind = turns.holds_one(&room, &channel);
        drop((running, waiting));
        assert_eq!(
            (alone, behind, turns.holds_one(&room, &channel)),
            (true, false, false)
        );
    }
}
