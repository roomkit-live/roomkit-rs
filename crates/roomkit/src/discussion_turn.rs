//! What an AI channel reads of a turn a discussion gives (RFC §19.7.5):
//! Python's `channels/_discussion_turn.py`.
//!
//! A discussion gives a turn as a rerun of the event the turn answers, the
//! event carrying a mark in its metadata. The channel adds the turn's notes
//! to the turn's input and reads the answered event at its place in the
//! history.
//!
//! The notes never travel in the event: event metadata can come from outside
//! (a transport, a host passing a webhook's fields through). The mark carries
//! only a random turn id, issued by [`issue_mark`] and retired once the turn
//! ends, and the notes are kept here under that id. A mark this process did
//! not issue, already retired, or of any other shape is no mark.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomEvent;
use roomkit_core::rules::discussion::TURN_KEY;
use serde_json::{Value, json};
use uuid::Uuid;

/// The notes of each turn this process issued and has not retired yet.
static ISSUED: LazyLock<Mutex<HashMap<String, Vec<String>>>> = LazyLock::new(Mutex::default);

fn issued() -> MutexGuard<'static, HashMap<String, Vec<String>>> {
    ISSUED.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A mark for one turn a discussion gives, its `notes` kept until the turn
/// ends ([`retire_mark`]), the empty ones left out.
#[must_use]
pub fn issue_mark(notes: Vec<String>) -> Value {
    let turn = Uuid::new_v4().simple().to_string();
    let notes = notes.into_iter().filter(|note| !note.is_empty()).collect();
    issued().insert(turn.clone(), notes);
    json!({ "turn": turn })
}

/// The turn `mark` was issued for has ended: the mark reads as no mark.
pub fn retire_mark(mark: &Value) {
    if let Some(turn) = mark.get("turn").and_then(Value::as_str) {
        issued().remove(turn);
    }
}

/// The notes of the turn `event`'s mark was issued for; `None` when no
/// discussion gave the turn: no mark, or one this process did not issue or
/// already retired.
fn notes_of(event: &RoomEvent) -> Option<Vec<String>> {
    let turn = event.metadata.get(TURN_KEY)?.get("turn")?.as_str()?;
    issued().get(turn).cloned()
}

/// Whether a discussion gave the turn on `event`.
#[must_use]
pub fn discussion_turn(event: &RoomEvent) -> bool {
    notes_of(event).is_some()
}

/// The notes a discussion's turn adds to the turn's input.
#[must_use]
pub fn discussion_notes(event: &RoomEvent) -> Vec<String> {
    notes_of(event).unwrap_or_default()
}

/// Retires its mark as the turn it was issued for ends, however it ends.
#[derive(Debug)]
pub(crate) struct IssuedMark(Value);

impl IssuedMark {
    /// A mark for a turn of `notes`.
    pub(crate) fn new(notes: Vec<String>) -> Self {
        Self(issue_mark(notes))
    }

    /// The mark, as an event's metadata carries it.
    pub(crate) fn mark(&self) -> &Value {
        &self.0
    }
}

impl Drop for IssuedMark {
    fn drop(&mut self) {
        retire_mark(&self.0);
    }
}
