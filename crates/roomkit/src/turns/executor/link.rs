//! The executor's way to its owner for one turn: its frames, sent in
//! order, a call's end naming its start's frame and a call's fixed
//! arguments sent before what follows them (RFC §10.6.4); and the verbs of
//! the turn's host, each a `host_call` answered once while the turn runs,
//! a `room_call` after it (RFC §10.6.5); and the calls of the tools the
//! owner serves the turn, each a `tool_call` answered once while the turn
//! runs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use roomkit_core::model::{ChannelId, Metadata, RoomId, ToolCallContent};
use serde_json::Value;
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

use super::inputs::ExecutorInputs;
use crate::error::{RoomKitError, repr};
use crate::turn::{OwnerToolAnswer, TurnKit};
use crate::turns::codec::{encode_end, encode_end_item, encode_start};
use crate::turns::error::{TurnError, rebuild_kit_error};
use crate::turns::pipe::Uncarried;
use crate::turns::wire::{
    CallContext, Frame, Item, OwnerMessage, OwnerTool, ToolOutcome, WireError,
};
use crate::turns::{MemoryTurn, Turns, lock};

/// The frames of one turn, sent in order, its text deltas by burst: the
/// deltas the channel's stream yields without waiting between them cross
/// as one item, their text joined and their number told, sent before any
/// other frame of the turn and as soon as the stream waits, as the Python
/// implementation's `FrameSender` sends them (RFC §10.6.4).
#[derive(Debug)]
pub(crate) struct FrameSender {
    turn: Arc<MemoryTurn>,
    state: Mutex<Sent>,
}

/// What the sender keeps of what it sent.
#[derive(Debug, Default)]
struct Sent {
    /// The owner is lost: nothing more is sent (RFC §10.6.8 `owner_lost`).
    silenced: bool,
    /// The calls started and not ended: their start's frame, and their id.
    open: Vec<(u64, String)>,
    /// The fixed arguments sent for each open call's start, by its frame.
    fixed: HashMap<u64, Metadata>,
    /// The text deltas kept for the burst, in order.
    burst: Vec<String>,
}

impl FrameSender {
    pub(crate) fn new(turn: Arc<MemoryTurn>) -> Self {
        Self {
            turn,
            state: Mutex::new(Sent::default()),
        }
    }

    /// Sends `frame` after the text kept before it: its number. A sender
    /// silenced sends nothing.
    pub(crate) fn send(&self, frame: &Frame) -> Result<u64, Uncarried> {
        let mut sent = lock(&self.state);
        if sent.silenced {
            return Ok(0);
        }
        self.send_burst(&mut sent)?;
        self.turn.frames.put(frame)
    }

    /// Keeps the delta `text` for the burst, sent once the stream waits
    /// ([`flush`](Self::flush)) or before the next frame.
    pub(crate) fn add_text(&self, text: String) {
        lock(&self.state).burst.push(text);
    }

    /// Sends the burst kept, the stream about to wait: whether one went.
    pub(crate) fn flush(&self) -> Result<bool, Uncarried> {
        let mut sent = lock(&self.state);
        if sent.silenced {
            return Ok(false);
        }
        self.send_burst(&mut sent)
    }

    /// Sends the deltas kept as one item, their text joined, the item
    /// telling their number when it is more than one: whether it went.
    fn send_burst(&self, sent: &mut Sent) -> Result<bool, Uncarried> {
        if sent.burst.is_empty() {
            return Ok(false);
        }
        let texts = std::mem::take(&mut sent.burst);
        let deltas = u32::try_from(texts.len()).ok().filter(|&deltas| deltas > 1);
        let item = Item::Text {
            text: texts.concat(),
            deltas,
        };
        self.turn.frames.put(&Frame::Item { item })?;
        Ok(true)
    }

    /// Sends nothing more: the owner is lost.
    pub(crate) fn silence(&self) {
        lock(&self.state).silenced = true;
    }

    /// Sends a call's start, which its end and its fixed arguments name.
    pub(crate) fn send_start(&self, start: ToolCallContent) -> Result<(), Uncarried> {
        let tool_id = start.tool_id.clone();
        let item = encode_start(start);
        let mut sent = lock(&self.state);
        if sent.silenced {
            return Ok(());
        }
        self.send_burst(&mut sent)?;
        let seq = self.turn.frames.put(&Frame::Item { item })?;
        sent.open.push((seq, tool_id));
        Ok(())
    }

    /// Sends a call's end, naming the start it closes: the earliest open
    /// under its id (two calls under one id are two calls, RFC §6.4).
    pub(crate) fn send_end(&self, end: ToolCallContent) -> Result<(), Uncarried> {
        let mut sent = lock(&self.state);
        if sent.silenced {
            return Ok(());
        }
        self.send_burst(&mut sent)?;
        let start = take_open(&mut sent, &end.tool_id);
        let item = encode_end_item(end, start);
        self.turn.frames.put(&Frame::Item { item })?;
        Ok(())
    }

    /// Sends the end of a call read once its reader had closed the stream,
    /// as `tool_ended`: the owner closes the call with its own outcome
    /// (RFC §10.6.4).
    pub(crate) fn send_ended(&self, end: ToolCallContent) {
        let mut sent = lock(&self.state);
        if sent.silenced {
            return;
        }
        if self.send_burst(&mut sent).is_err() {
            return;
        }
        let Some(start) = take_open(&mut sent, &end.tool_id) else {
            return;
        };
        let frame = Frame::ToolEnded {
            start,
            end: encode_end(end),
        };
        drop(self.turn.frames.put(&frame));
    }

    /// Sends the arguments each open call runs with once its gate fixed
    /// them, `ran_with` by the call's id, when they were not sent yet.
    pub(crate) fn report_args(
        &self,
        ran_with: &HashMap<String, Metadata>,
    ) -> Result<(), Uncarried> {
        if ran_with.is_empty() {
            return Ok(());
        }
        let mut sent = lock(&self.state);
        if sent.silenced {
            return Ok(());
        }
        let unsent: Vec<(u64, Metadata)> = sent
            .open
            .iter()
            .filter_map(|(start, tool_id)| {
                let arguments = ran_with.get(tool_id)?;
                (sent.fixed.get(start) != Some(arguments)).then(|| (*start, arguments.clone()))
            })
            .collect();
        if unsent.is_empty() {
            return Ok(());
        }
        self.send_burst(&mut sent)?;
        for (start, arguments) in unsent {
            let frame = Frame::ToolArgs {
                start,
                arguments: arguments.clone(),
            };
            self.turn.frames.put(&frame)?;
            sent.fixed.insert(start, arguments);
        }
        Ok(())
    }
}

/// The start the end of `tool_id` closes, taken off the open calls.
fn take_open(sent: &mut Sent, tool_id: &str) -> Option<u64> {
    let position = sent.open.iter().position(|(_, id)| id == tool_id)?;
    let (start, _) = sent.open.remove(position);
    sent.fixed.remove(&start);
    Some(start)
}

/// What a turn's host knows of its turn, which its verbs name.
#[derive(Clone, Debug)]
pub(crate) struct TurnFacts {
    pub(crate) turn: String,
    pub(crate) room: RoomId,
    pub(crate) channel: ChannelId,
    pub(crate) executor: String,
}

/// What a call waits for: a verb's result, or the answer of a tool the
/// owner serves.
enum Answer {
    Verb(oneshot::Sender<Result<Value, RoomKitError>>),
    Tool(oneshot::Sender<OwnerToolAnswer>),
}

impl Answer {
    /// Answers the call with `error`: the owner will not.
    fn fail(self, error: &TurnError) {
        match self {
            Self::Verb(answer) => drop(answer.send(Err(error.clone().into_kit_error()))),
            Self::Tool(answer) => drop(answer.send(OwnerToolAnswer::Failed {
                kind: error.kind.clone(),
                message: error.message.clone(),
            })),
        }
    }
}

/// The way a turn's host reaches its owner (RFC §10.6.5).
pub(crate) struct HostLink {
    facts: TurnFacts,
    sender: Arc<FrameSender>,
    /// Whether the turn still runs: its verbs cross as `host_call`; after,
    /// as `room_call`.
    open: AtomicBool,
    pending: Mutex<HashMap<String, Answer>>,
    settled: Notify,
    turns: Weak<Turns>,
    kit: Arc<dyn TurnKit>,
    /// The tools the owner serves the turn, as its request lists them.
    owner_tools: Vec<OwnerTool>,
    /// The turn's open human-input requests (RFC §10.6.5).
    inputs: Arc<ExecutorInputs>,
}

impl std::fmt::Debug for HostLink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostLink")
            .field("facts", &self.facts)
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

impl HostLink {
    pub(crate) fn new(
        facts: TurnFacts,
        sender: Arc<FrameSender>,
        turns: &Arc<Turns>,
        kit: Arc<dyn TurnKit>,
        owner_tools: Vec<OwnerTool>,
    ) -> Self {
        Self {
            facts,
            inputs: Arc::new(ExecutorInputs::new(Arc::clone(&sender))),
            sender,
            open: AtomicBool::new(true),
            pending: Mutex::new(HashMap::new()),
            settled: Notify::new(),
            turns: Arc::downgrade(turns),
            kit,
            owner_tools,
        }
    }

    /// The id of the turn it serves (RFC §10.6.3).
    pub(crate) fn turn_id(&self) -> &str {
        &self.facts.turn
    }

    /// The definitions of the tools the owner serves the turn.
    pub(crate) fn owner_tools(&self) -> Vec<Value> {
        self.owner_tools
            .iter()
            .map(|owned| owned.tool.clone())
            .collect()
    }

    /// Runs the call `tool_call_id` of the owner's tool `name` with
    /// `arguments` on the owner, the turn's work at least `floor` deep
    /// (RFC §10.6.5): the handler's answer, or how it did not answer. A
    /// call once the turn ended is not sent.
    pub(crate) async fn call_tool(
        &self,
        name: &str,
        arguments: Metadata,
        tool_call_id: &str,
        floor: u32,
    ) -> OwnerToolAnswer {
        let call_id = Uuid::new_v4().simple().to_string();
        let (answer, answered) = oneshot::channel();
        {
            let mut pending = lock(&self.pending);
            if !self.open.load(Ordering::Acquire) {
                return OwnerToolAnswer::Failed {
                    kind: "RuntimeError".to_owned(),
                    message: format!(
                        "The turn has ended: tool {} is not sent to the room owner",
                        repr(name)
                    ),
                };
            }
            pending.insert(call_id.clone(), Answer::Tool(answer));
        }
        let frame = Frame::ToolCall {
            call_id: call_id.clone(),
            name: name.to_owned(),
            arguments,
            context: CallContext {
                tool_call_id: tool_call_id.to_owned(),
                room_id: self.facts.room.as_str().to_owned(),
                channel_id: self.facts.channel.as_str().to_owned(),
                chain_depth: floor,
                ..CallContext::default()
            },
        };
        if let Err(Uncarried(why)) = self.sender.send(&frame) {
            self.withdraw(&call_id, false);
            return OwnerToolAnswer::Failed {
                kind: "TypeError".to_owned(),
                message: format!(
                    "the arguments of tool {} cannot cross the turn backend: {why}",
                    repr(name)
                ),
            };
        }
        let withdrawn = Withdraw {
            link: self,
            call_id: Some(call_id),
        };
        let result = answered.await;
        withdrawn.disarm();
        result.unwrap_or_else(|_gone| OwnerToolAnswer::Failed {
            kind: "owner_lost".to_owned(),
            message: "the turn ended before the tool's call was answered".to_owned(),
        })
    }

    /// Whether the turn it serves still runs.
    pub(crate) fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Whether a verb can still reach the owner: during the turn, or after
    /// it as a `room_call` while the kit's turn backend lives.
    pub(crate) fn reaches_owner(&self) -> bool {
        self.is_open() || self.turns.strong_count() > 0
    }

    /// Runs `verb` with `arguments` on the owner, the turn's work at least
    /// `floor` deep: its result.
    ///
    /// # Errors
    ///
    /// What the verb raised on the owner, rebuilt; a link's failure, or a
    /// verb refused, as [`RoomKitError::RemoteTurn`] or
    /// [`RoomKitError::TurnLink`].
    pub(crate) async fn verb(
        &self,
        verb: &str,
        arguments: Metadata,
        floor: u32,
    ) -> Result<Value, RoomKitError> {
        let call_id = Uuid::new_v4().simple().to_string();
        let (answer, answered) = oneshot::channel();
        {
            // Checked under the lock its close takes: a verb is either
            // pending before the turn ends, which then waits for it, or made
            // after, outside the turn.
            let mut pending = lock(&self.pending);
            if !self.open.load(Ordering::Acquire) {
                drop(pending);
                return self.between_turns(verb, arguments).await;
            }
            pending.insert(call_id.clone(), Answer::Verb(answer));
        }
        let frame = Frame::HostCall {
            call_id: call_id.clone(),
            verb: verb.to_owned(),
            arguments,
            context: CallContext {
                room_id: self.facts.room.as_str().to_owned(),
                chain_depth: floor,
                ..CallContext::default()
            },
        };
        if let Err(Uncarried(why)) = self.sender.send(&frame) {
            self.withdraw(&call_id, false);
            return Err(TurnError::new(
                "TypeError",
                format!(
                    "the arguments of verb {} cannot cross the turn backend: {why}",
                    repr(verb)
                ),
            )
            .into_kit_error());
        }
        let withdrawn = Withdraw {
            link: self,
            call_id: Some(call_id),
        };
        let result = answered.await;
        withdrawn.disarm();
        result.unwrap_or_else(|_gone| {
            Err(
                TurnError::new("owner_lost", "the turn ended before the verb was answered")
                    .into_kit_error(),
            )
        })
    }

    /// Runs `verb` as work the turn began, after it ended (RFC §10.6.3
    /// `room_call`).
    async fn between_turns(&self, verb: &str, arguments: Metadata) -> Result<Value, RoomKitError> {
        let Some(turns) = self.turns.upgrade() else {
            return Err(RoomKitError::KitClosed);
        };
        let message = OwnerMessage::RoomCall {
            call_id: Uuid::new_v4().simple().to_string(),
            channel_id: self.facts.channel.clone(),
            room_id: self.facts.room.clone(),
            executor_id: self.facts.executor.clone(),
            turn_id: self.facts.turn.clone(),
            chain_depth: 0,
            verb: verb.to_owned(),
            arguments,
        };
        turns.room_call(Arc::clone(&self.kit), message).await
    }

    /// The owner answered the verb `call_id`.
    pub(crate) fn resolve(&self, call_id: &str, result: Value, error: Option<&WireError>) {
        let Some(answer) = lock(&self.pending).remove(call_id) else {
            return;
        };
        match answer {
            Answer::Verb(answer) => {
                let answered = match error {
                    Some(error) => Err(rebuild_kit_error(error)),
                    None => Ok(result),
                };
                drop(answer.send(answered));
            }
            Answer::Tool(answer) => drop(answer.send(OwnerToolAnswer::Failed {
                kind: "RuntimeError".to_owned(),
                message: "The room owner answered a tool call with a verb's result".to_owned(),
            })),
        }
        self.settled.notify_waiters();
    }

    /// The owner answered the tool call `call_id` `outcome`, with the
    /// handler's `result` when served, else `error`.
    pub(crate) fn resolve_tool(
        &self,
        call_id: &str,
        outcome: ToolOutcome,
        result: Option<Value>,
        error: Option<&WireError>,
    ) {
        let Some(answer) = lock(&self.pending).remove(call_id) else {
            return;
        };
        let answered = tool_answer(outcome, result, error);
        match answer {
            Answer::Tool(answer) => drop(answer.send(answered)),
            Answer::Verb(answer) => drop(answer.send(Err(RoomKitError::Internal(
                "The room owner answered a verb with a tool's result".to_owned(),
            )))),
        }
        self.settled.notify_waiters();
    }

    /// No call of the turn will be answered: each fails with `error`.
    pub(crate) fn fail_all(&self, error: &TurnError) {
        let pending: Vec<Answer> = lock(&self.pending)
            .drain()
            .map(|(_, answer)| answer)
            .collect();
        for answer in pending {
            answer.fail(error);
        }
        self.settled.notify_waiters();
    }

    /// The turn ended: its host's verbs cross as `room_call` from now on,
    /// and its human-input requests still open are withdrawn.
    pub(crate) fn close(&self) {
        {
            let _pending = lock(&self.pending);
            self.open.store(false, Ordering::Release);
        }
        self.inputs.close();
    }

    /// The turn's human-input requests.
    pub(crate) fn inputs(&self) -> Arc<ExecutorInputs> {
        Arc::clone(&self.inputs)
    }

    /// Whether every call the turn sent was answered.
    pub(crate) fn is_settled(&self) -> bool {
        lock(&self.pending).is_empty()
    }

    /// Returns once every call the turn sent was answered.
    pub(crate) async fn settled(&self) {
        loop {
            let mut settled = std::pin::pin!(self.settled.notified());
            settled.as_mut().enable();
            if self.is_settled() {
                return;
            }
            settled.await;
        }
    }

    /// Withdraws the call `call_id`, the owner told when `tell`.
    fn withdraw(&self, call_id: &str, tell: bool) {
        let withdrawn = lock(&self.pending).remove(call_id).is_some();
        if withdrawn && tell {
            drop(self.sender.send(&Frame::CallCancel {
                call_id: call_id.to_owned(),
            }));
        }
        self.settled.notify_waiters();
    }
}

/// The answer of an owner's tool as the model's call reads it: a served
/// call's result, its `value` as the codec writes it; a refusal's, a
/// failure's or an unserved call's error.
fn tool_answer(
    outcome: ToolOutcome,
    result: Option<Value>,
    error: Option<&WireError>,
) -> OwnerToolAnswer {
    let message = || error.map(|error| error.message.clone()).unwrap_or_default();
    match (outcome, result) {
        (ToolOutcome::Served, Some(result)) => {
            OwnerToolAnswer::Served(result.get("value").cloned().unwrap_or(result))
        }
        (ToolOutcome::Refused, _) => OwnerToolAnswer::Refused(message()),
        (ToolOutcome::Unserved, _) => OwnerToolAnswer::Unserved(message()),
        (ToolOutcome::Served | ToolOutcome::Failed, _) => match error {
            Some(error) => OwnerToolAnswer::Failed {
                kind: error.kind.clone(),
                message: error.message.clone(),
            },
            None => OwnerToolAnswer::Failed {
                kind: "RuntimeError".to_owned(),
                message: "The room owner answered a call without its result".to_owned(),
            },
        },
    }
}

/// A call withdrawn, the owner told, when its caller stops waiting before
/// its answer (its bound, its turn cut, RFC §10.6.5 `call_cancel`).
struct Withdraw<'a> {
    link: &'a HostLink,
    call_id: Option<String>,
}

impl Withdraw<'_> {
    fn disarm(mut self) {
        self.call_id = None;
    }
}

impl Drop for Withdraw<'_> {
    fn drop(&mut self) {
        if let Some(call_id) = self.call_id.take() {
            self.link.withdraw(&call_id, true);
        }
    }
}
