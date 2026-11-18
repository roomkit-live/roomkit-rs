//! The two ends of an application-supplied turn transport (RFC §10.6).
use std::future::Future;
use std::pin::Pin;

use super::TurnContext;
use super::wire::{Control, Frame, TurnRequest};
use crate::RoomKitError;

/// A sendable asynchronous operation of a turn backend or one of its links.
pub type TurnFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The owner's end of one submitted turn (RFC §10.6.3 and §10.6.8).
pub trait TurnLink: Send + Sync + 'static {
    /// The request submitted through this link.
    fn request(&self) -> &TurnRequest;
    /// Whether an executor has taken this turn.
    fn taken(&self) -> bool;
    /// Read the next frame with its sequence number. A gap or lost peer fails the turn.
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>>;
    /// Send a control message or the answer to a call to the executor.
    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>>;
    /// Let go of the link. Later frames for its turn are discarded.
    fn release(&self);
    /// Whether an independent executor heartbeat arrived since the previous check.
    fn beat_heard(&self) -> bool {
        false
    }
}

/// The executor's end of one turn accepted by a backend (RFC §10.6.3 and §10.6.5).
pub trait ExecutorLink: Send + Sync + 'static {
    /// The request the backend delivered to the executor.
    fn request(&self) -> &TurnRequest;
    /// Read the next control from the owner, after validating the incoming sequence.
    fn receive(&self) -> TurnFuture<'_, Result<Control, RoomKitError>>;
    /// Send a frame to the owner and return its sequence number.
    fn send(&self, frame: Frame) -> TurnFuture<'_, Result<u64, RoomKitError>>;
    /// Context inherited at submission for a turn in the owner's process.
    fn turn_context(&self) -> TurnContext {
        TurnContext::default()
    }
    /// Called when the executor's task finishes, including when it is cancelled.
    fn turn_ended(&self) {}
    /// Context of the first read of a response stream in the owner's process.
    fn stream_context(&self) -> Option<TurnContext> {
        None
    }
}
