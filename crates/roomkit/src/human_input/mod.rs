//! Human input (RFC §9.3, `ON_USER_INPUT_REQUIRED`, where the other party is
//! a human; §10.6.5, Human input): a tool that asks a person, and the
//! requests it waits on. Python's `HumanInputHandler`, `PendingInput`,
//! `PendingInputEvent` and `PendingInputStatus`, which the Python
//! implementation exports from `roomkit`.
//!
//! - [`HumanInputHandler`]: the pending requests, created, waited for,
//!   answered, rejected, released. An `AiChannel` given a human-input tool
//!   handler (`roomkit_ai::HumanInputToolHandler`) serves its tools with
//!   one.
//! - [`PendingInput`]: a request as the handler records it;
//!   [`PendingInputEvent`]: what the `ON_USER_INPUT_REQUIRED` hooks are
//!   handed.
//!
//! A request made within an intelligence turn the kit runs is answerable on
//! the room's owner too: [`RoomKit::answer_input`](crate::RoomKit::answer_input)
//! and [`RoomKit::reject_input`](crate::RoomKit::reject_input), first come
//! first served with the handler's own answer.

mod handler;
mod model;
mod options;
#[cfg(test)]
mod tests;

pub use self::handler::HumanInputHandler;
pub use self::model::{PendingInput, PendingInputEvent, PendingInputStatus};
pub use self::options::{
    CloseOptions, CreateDetachedOptions, CreateOptions, HumanInputHandlerOptions, RejectOptions,
    WaitOptions,
};
