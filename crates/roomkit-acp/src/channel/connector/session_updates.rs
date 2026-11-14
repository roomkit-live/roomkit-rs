//! Session notifications see current descriptors without retaining their connection.

use std::sync::{Mutex, PoisonError, Weak};

use serde_json::Value;

use super::State;
use crate::AcpConnection;
use crate::channel::{client::SessionOptions, config};

pub(super) struct SessionUpdates<C> {
    state: Weak<Mutex<State<C>>>,
}

impl<C> SessionUpdates<C> {
    pub(super) fn new(state: Weak<Mutex<State<C>>>) -> Self {
        Self { state }
    }
}

impl<C: AcpConnection> SessionOptions for SessionUpdates<C> {
    fn model(&self, session: &str) -> Option<Value> {
        let state = self.state.upgrade()?;
        let state = state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return None;
        }
        let (room, _) = state
            .sessions
            .iter()
            .find(|(_, id)| id.as_str() == session)?;
        config::values(state.options.get(room)?)
            .get("model")
            .cloned()
    }

    fn update(&self, session: &str, options: Vec<Value>) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return;
        }
        let room = state
            .sessions
            .iter()
            .find(|(_, id)| id.as_str() == session)
            .map(|(room, _)| room.clone());
        if let Some(room) = room {
            state.options.insert(room, options);
        }
    }
}
