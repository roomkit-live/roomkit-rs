//! What a [`HumanInputHandler`](super::HumanInputHandler)'s calls take:
//! Python's keywords, with Python's defaults.

use std::time::Duration;

use roomkit_core::model::ChannelType;

/// What [`HumanInputHandler::new`](super::HumanInputHandler::new) takes: Python's `HumanInputHandler(*,
/// retention=128)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HumanInputHandlerOptions {
    /// Python `retention=128`: how many settled requests stay readable by
    /// [`wait`](super::HumanInputHandler::wait), the newest kept.
    pub retention: usize,
}

impl Default for HumanInputHandlerOptions {
    fn default() -> Self {
        Self { retention: 128 }
    }
}

/// What [`HumanInputHandler::create`](super::HumanInputHandler::create) takes beside the tool and its
/// arguments: Python's `create(tool_name, arguments, *, room_id='',
/// tool_call_id='', channel_id='', channel_type=ChannelType.AI,
/// actor_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOptions {
    /// Python `room_id=''`: the room the call was made in.
    pub room_id: String,
    /// Python `tool_call_id=''`: the model's id of the call.
    pub tool_call_id: String,
    /// Python `channel_id=''`: the channel whose model made the call, the
    /// owner of the request.
    pub channel_id: String,
    /// Python `channel_type=ChannelType.AI`: the door the call came through.
    pub channel_type: ChannelType,
    /// Python `actor_id=None`: the participant whose turn raised the
    /// request.
    pub actor_id: Option<String>,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            room_id: String::new(),
            tool_call_id: String::new(),
            channel_id: String::new(),
            channel_type: ChannelType::AI,
            actor_id: None,
        }
    }
}

/// What [`HumanInputHandler::create_detached`](super::HumanInputHandler::create_detached)
/// takes beside the tool and its arguments: Python's `create_detached`'s
/// keywords, those of [`create`](super::HumanInputHandler::create).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDetachedOptions {
    /// Python `room_id=''`: the room the call was made in.
    pub room_id: String,
    /// Python `tool_call_id=''`: the model's id of the call.
    pub tool_call_id: String,
    /// Python `channel_id=''`: the channel whose model made the call, the
    /// owner of the request.
    pub channel_id: String,
    /// Python `channel_type=ChannelType.AI`: the door the call came through.
    pub channel_type: ChannelType,
    /// Python `actor_id=None`: the participant whose turn raised the
    /// request.
    pub actor_id: Option<String>,
}

impl Default for CreateDetachedOptions {
    fn default() -> Self {
        CreateOptions::default().into()
    }
}

impl From<CreateOptions> for CreateDetachedOptions {
    fn from(options: CreateOptions) -> Self {
        let CreateOptions {
            room_id,
            tool_call_id,
            channel_id,
            channel_type,
            actor_id,
        } = options;
        Self {
            room_id,
            tool_call_id,
            channel_id,
            channel_type,
            actor_id,
        }
    }
}

impl From<CreateDetachedOptions> for CreateOptions {
    fn from(options: CreateDetachedOptions) -> Self {
        let CreateDetachedOptions {
            room_id,
            tool_call_id,
            channel_id,
            channel_type,
            actor_id,
        } = options;
        Self {
            room_id,
            tool_call_id,
            channel_id,
            channel_type,
            actor_id,
        }
    }
}

/// What [`HumanInputHandler::wait`](super::HumanInputHandler::wait) takes beside the request: Python's
/// `wait(pending_id, *, timeout=300)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WaitOptions {
    /// Python `timeout=300`: how long to wait for the outcome.
    pub timeout: Duration,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
        }
    }
}

/// What [`HumanInputHandler::close`](super::HumanInputHandler::close) takes: Python's `close(*,
/// channel_id=None, registration=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloseOptions {
    /// Python `channel_id=None`: the channel whose requests to settle;
    /// every request when `None`, the handler then taking no more.
    pub channel_id: Option<String>,
    /// Python `registration=None`: the token a channel's registration
    /// handed it; a close naming another channel object's token closes
    /// nothing.
    pub registration: Option<u64>,
}

/// What [`HumanInputHandler::reject`](super::HumanInputHandler::reject) takes beside the request: Python's
/// `reject(pending_id, reason='')`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RejectOptions {
    /// Python `reason=''`: why, which the waiting tool reads.
    pub reason: String,
}
