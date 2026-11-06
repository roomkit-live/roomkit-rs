//! The `delegate_task` tool's handler (RFC §23.4): Python's
//! `tasks.delegate.DelegateHandler`.
//!
//! A call delegates from the room of the call, its `task` as its model
//! wrote it, the label the parent room's records name the task by. What
//! the call passes as `context`, and the calling agent's previous tasks
//! when a cache holds any and no intelligence channel is shared, reach the
//! worker quoted as data, in its brief. A host that sets `max_conversation`
//! lets a call hand over the room's last messages too.
//!
//! `AiTool` is `roomkit-ai`'s, which this crate cannot name: the tool's
//! definition crosses as JSON, as the handoff's does, and
//! `roomkit_ai::tasks::setup_delegation` reads it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelCategory, ChannelId, Metadata, RoomId, TaskStatus};
use serde_json::{Value, json};
use tokio::sync::{Mutex as TaskMutex, OwnedMutexGuard};

use super::brief::{context_block, previous_tasks_block, task_brief};
use super::cache::{CompletedTaskCache, Kept};
use super::json::dumps_object;
use super::{DelegatedTask, Kind, WaitOptions};
use crate::error::RoomKitError;
use crate::kit::{DelegateOptions, GetRoomOptions, RoomKit, WeakRoomKit};
use crate::orchestration::StrategyAgent;
use crate::store::{ConversationStore, InMemoryStore};
use crate::turn::OwnerToolAnswer;
use crate::turns::served::{OwnerServe, OwnerToolEntry};

/// The tool's name: Python's `DELEGATE_TOOL_NAME`.
const DELEGATE_TOOL_NAME: &str = "delegate_task";

/// The argument a handler that allows it adds to the tool: Python's
/// `CONVERSATION_ARGUMENT`.
const CONVERSATION_ARGUMENT: &str = "conversation";

/// What the tool says of its `agent`.
const AGENT_DESCRIPTION: &str = "Target agent ID for the delegated task";

/// How long the handler watches a task's end for its cache: a task running
/// past it is not kept. Python's `task.wait(timeout=600.0)`.
const REMEMBER_WITHIN: Duration = Duration::from_secs(600);

/// How many of the calling agent's previous tasks its worker is handed:
/// Python's `recent_context(room_id, limit=3)`.
const PREVIOUS_TASKS: usize = 3;

/// What [`DelegateHandler::new`] takes beside the kit: Python's
/// `DelegateHandler(kit, *, notify=None, default_share_channels=None,
/// delivery_strategy=None, cache=None, serialize_per_room=False,
/// max_conversation=0)`.
pub struct DelegateHandlerOptions<S = InMemoryStore> {
    /// Python `notify=None`: the channel told of a task's end; the agent
    /// that called when `None`.
    pub notify: Option<ChannelId>,
    /// Python `default_share_channels=None`: the channels shared with a
    /// worker when a call names none.
    pub default_share_channels: Option<Vec<ChannelId>>,
    /// Python `delivery_strategy=None`: how results are delivered back,
    /// set on the kit.
    pub delivery_strategy: Option<Arc<dyn crate::DeliveryStrategy<S>>>,
    /// Python `cache=None`: answers a repeat of a completed task, and keeps
    /// the agent's previous tasks for its worker.
    pub cache: Option<CompletedTaskCache>,
    /// Python `serialize_per_room=False`: only one delegation per room
    /// starts at a time.
    pub serialize_per_room: bool,
    /// Python `max_conversation=0`: how many of the room's last messages a
    /// call may hand to its worker, those both the calling agent and the
    /// notified channel may read, quoted as data; none when an intelligence
    /// channel is shared into the child room, which reads them too. The tool
    /// gains a `conversation` argument when it is above 0, and a call is
    /// held to it whatever it asks. At 0: none, and no such argument: the
    /// worker may be another vendor's, and the model chooses it, so the host
    /// opens this.
    pub max_conversation: u64,
}

impl<S> Default for DelegateHandlerOptions<S> {
    fn default() -> Self {
        Self {
            notify: None,
            default_share_channels: None,
            delivery_strategy: None,
            cache: None,
            serialize_per_room: false,
            max_conversation: 0,
        }
    }
}

impl<S> Clone for DelegateHandlerOptions<S> {
    fn clone(&self) -> Self {
        Self {
            notify: self.notify.clone(),
            default_share_channels: self.default_share_channels.clone(),
            delivery_strategy: self.delivery_strategy.clone(),
            cache: self.cache.clone(),
            serialize_per_room: self.serialize_per_room,
            max_conversation: self.max_conversation,
        }
    }
}

impl<S> std::fmt::Debug for DelegateHandlerOptions<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DelegateHandlerOptions")
            .field("notify", &self.notify)
            .field("default_share_channels", &self.default_share_channels)
            .field("cache", &self.cache)
            .field("serialize_per_room", &self.serialize_per_room)
            .field("max_conversation", &self.max_conversation)
            .finish_non_exhaustive()
    }
}

/// Processes `delegate_task` calls by calling
/// [`RoomKit::delegate`](crate::RoomKit::delegate): Python's
/// `DelegateHandler`. Cloning it is cheap: the clones share its state. It
/// holds its kit without keeping it alive, the kit holding what it serves.
pub struct DelegateHandler<S = InMemoryStore> {
    inner: Arc<Inner<S>>,
}

struct Inner<S> {
    kit: WeakRoomKit<S>,
    notify: Option<ChannelId>,
    default_share_channels: Vec<ChannelId>,
    cache: Option<CompletedTaskCache>,
    serialize_per_room: bool,
    max_conversation: u64,
    room_turns: Arc<RoomTurns>,
}

impl<S> Clone for DelegateHandler<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> std::fmt::Debug for DelegateHandler<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DelegateHandler")
            .field("notify", &self.inner.notify)
            .field("max_conversation", &self.inner.max_conversation)
            .finish_non_exhaustive()
    }
}

/// A room's lock, and how many delegations hold it or wait for it: the
/// last one out drops it, so a room no longer delegating keeps nothing.
#[derive(Default)]
struct RoomTurns {
    rooms: Mutex<HashMap<RoomId, RoomLock>>,
}

/// A room's lock, and how many delegations hold it or wait for it.
type RoomLock = (Arc<TaskMutex<()>>, usize);

impl RoomTurns {
    /// The room's turn, held until the guard is dropped.
    async fn take(self: &Arc<Self>, room_id: &RoomId) -> RoomTurn {
        let lock = {
            let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
            let (lock, users) = rooms.entry(room_id.clone()).or_default();
            *users += 1;
            Arc::clone(lock)
        };
        // Named before it waits: a call cancelled there still leaves.
        let mut turn = RoomTurn {
            room_id: room_id.clone(),
            turns: Arc::clone(self),
            held: None,
        };
        turn.held = Some(lock.lock_owned().await);
        turn
    }

    #[cfg(test)]
    fn rooms_held(&self) -> usize {
        self.rooms
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// A room's turn: dropping it lets the next delegation start.
struct RoomTurn {
    room_id: RoomId,
    turns: Arc<RoomTurns>,
    held: Option<OwnedMutexGuard<()>>,
}

impl Drop for RoomTurn {
    fn drop(&mut self) {
        self.held = None;
        let mut rooms = self
            .turns
            .rooms
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((_, users)) = rooms.get_mut(&self.room_id) {
            *users -= 1;
            if *users == 0 {
                rooms.remove(&self.room_id);
            }
        }
    }
}

/// What a call asks for, read off its arguments.
struct Call {
    agent: ChannelId,
    task: String,
    passed: Metadata,
    count: u64,
    share_channels: Vec<ChannelId>,
}

impl<S: ConversationStore> DelegateHandler<S> {
    /// The handler serving `kit`, as `options` say.
    pub fn new(kit: &RoomKit<S>, options: DelegateHandlerOptions<S>) -> Self {
        let DelegateHandlerOptions {
            notify,
            default_share_channels,
            delivery_strategy,
            cache,
            serialize_per_room,
            max_conversation,
        } = options;
        if let Some(strategy) = delivery_strategy {
            *kit.inner
                .delivery_strategy
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Some(strategy);
        }
        Self {
            inner: Arc::new(Inner {
                kit: kit.downgrade(),
                notify,
                default_share_channels: default_share_channels.unwrap_or_default(),
                cache,
                serialize_per_room,
                max_conversation,
                room_turns: Arc::default(),
            }),
        }
    }

    /// Processes a `delegate_task` call made by `calling_agent_id` in
    /// `room_id`: the task's receipt, `status`, `task_id`, `child_room_id`
    /// and `agent_id`; a repeat answered from the cache, its `result` and
    /// `from_cache` too. Python's `handle`.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::delegate`]; [`RoomKitError::KitClosed`] once the kit
    /// is gone; [`RoomKitError::Value`] for a `task` that is no text or a
    /// `share_channels` that is no list; [`RoomKitError::ChannelNotRegistered`]
    /// for an `agent` that is no text.
    pub async fn handle(
        &self,
        room_id: &RoomId,
        calling_agent_id: &ChannelId,
        arguments: &Metadata,
    ) -> Result<Metadata, RoomKitError> {
        // A closed kit still answers a repeat from the cache: `delegate`
        // refuses it where Python's does.
        let kit = self.kit()?;
        let task = task_of(arguments)?;
        let named = agent_of(arguments)?;
        let share_channels = share_channels_of(arguments)?
            .unwrap_or_else(|| self.inner.default_share_channels.clone());
        let Ok(agent) = ChannelId::new(named.as_str()) else {
            // As the kit reads a delegation: open, then the room.
            kit.inner.open()?;
            kit.get_room(room_id, GetRoomOptions::default()).await?;
            return Err(unregistered(&named));
        };
        let call = Call {
            agent,
            task,
            passed: passed_context(arguments.get("context")),
            count: asked_conversation(arguments.get("conversation"), self.inner.max_conversation),
            share_channels,
        };
        // Gap 13: a call that hands the room's last messages over reads them
        // as they stand now.
        if let Some(cache) = self.inner.cache.as_ref().filter(|_| call.count == 0) {
            let named = Kept {
                room_id,
                agent_id: &call.agent,
                task: &call.task,
                context: Some(&call.passed),
            };
            if let Some(mut cached) = cache.answer_for(&named, calling_agent_id) {
                tracing::info!(
                    target: "roomkit::tasks",
                    agent = %call.agent,
                    "Returning a cached result for a repeated task",
                );
                cached.insert("from_cache".to_owned(), Value::Bool(true));
                return Ok(cached);
            }
        }
        // Gap 14: only one delegation per room starts at a time.
        let _turn = if self.inner.serialize_per_room {
            Some(self.inner.room_turns.take(room_id).await)
        } else {
            None
        };
        self.delegate(&kit, room_id, calling_agent_id, call).await
    }

    async fn delegate(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        calling: &ChannelId,
        call: Call,
    ) -> Result<Metadata, RoomKitError> {
        let notified = self.inner.notify.clone().unwrap_or_else(|| calling.clone());
        let mut recent = Vec::new();
        let mut conversation = None;
        // An intelligence channel shared into the child room reads the
        // worker's description: nothing the framework rebuilds goes there, as
        // it may hold what that channel may not read (RFC §7.5 rule 8). What
        // this call passed stays: the model wrote it, as it wrote the task
        // that channel reads too.
        let reads_rebuilt = call.count > 0 || self.inner.cache.is_some();
        if reads_rebuilt && !shares_an_agent(kit, room_id, &call.share_channels).await? {
            // Gap 15: the agent's own previous tasks, which it may read, not
            // another's, which may hold what it may not.
            if let Some(cache) = &self.inner.cache {
                recent = cache.recent_of(room_id, calling, PREVIOUS_TASKS);
            }
            // What both the agent that chose to hand it over and the channel
            // its result goes back to may read.
            if call.count > 0 {
                let mut readers = vec![calling.clone()];
                if notified != *calling {
                    readers.push(notified.clone());
                }
                conversation = kit.room_conversation(room_id, &readers, call.count).await?;
            }
        }
        let brief = task_brief(
            &call.task,
            [
                context_block(&call.passed),
                previous_tasks_block(&recent),
                conversation,
            ],
        );
        let mut context = call.passed.clone();
        if !recent.is_empty() {
            context.insert("previous_tasks".to_owned(), json!(recent));
        }
        let task = kit
            .delegate(
                room_id,
                &call.agent,
                call.task.clone(),
                DelegateOptions {
                    context: Some(context),
                    share_channels: Some(call.share_channels.clone()),
                    notify: Some(notified),
                    brief,
                    ..DelegateOptions::default()
                },
            )
            .await?;
        let receipt = Metadata::from_iter([
            ("status".to_owned(), json!("delegated")),
            ("task_id".to_owned(), json!(task.id)),
            ("child_room_id".to_owned(), json!(task.child_room_id)),
            ("agent_id".to_owned(), json!(call.agent)),
        ]);
        if let Some(cache) = &self.inner.cache {
            let watching = remember_when_completed(
                cache.clone(),
                task,
                receipt.clone(),
                call,
                calling.clone(),
            );
            // The kit's close cuts it, the task ending with the kit.
            kit.inner.tasks.spawn_alone(Kind::Observer, watching);
        }
        Ok(receipt)
    }

    fn kit(&self) -> Result<RoomKit<S>, RoomKitError> {
        self.inner.kit.upgrade().ok_or(RoomKitError::KitClosed)
    }

    /// Serves `delegate_task` on `agent`, its tool `definition`, the generic
    /// one when `None`, shaped by what the handler allows: Python's
    /// `setup_delegation`, for roomkit-ai's, which holds the `AiTool`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] when delegation is already set up on the
    /// agent; [`RoomKitError::ChannelNotRegistered`] for an id naming no
    /// channel of the kit; [`RoomKitError::ToolNameCollision`] for the name
    /// one of its own tools takes, nothing registered then;
    /// [`RoomKitError::Value`] for a tool whose list of agents is no list;
    /// [`RoomKitError::KitClosed`] once the kit is gone.
    #[doc(hidden)]
    pub fn serve_tool(
        &self,
        agent: &StrategyAgent,
        definition: Option<Value>,
    ) -> Result<(), RoomKitError> {
        let definition = served_tool(
            definition.unwrap_or_else(|| delegate_tool_definition(&[])),
            self.inner.max_conversation,
        );
        let serve = self.served_delegation(agent.channel_id().clone(), &definition)?;
        let kit = self.kit()?;
        kit.takes_handoff(agent)?;
        kit.refuse_own_tool_name(agent, DELEGATE_TOOL_NAME)?;
        kit.register_strategy_agents(std::slice::from_ref(agent))?;
        let channel = agent.channel_id();
        if kit.inner.owner_tools.serves(channel, DELEGATE_TOOL_NAME) {
            return Err(RoomKitError::Runtime(format!(
                "setup_delegation() already called for channel '{channel}'"
            )));
        }
        let entry = OwnerToolEntry {
            serve,
            definition,
            waits: true,
            owner: None,
        };
        kit.register_owner_tools(channel, vec![entry])
    }

    /// What serves a call of the tool `definition`: refused when it names a
    /// worker outside the list the tool declares, since a list a model can
    /// ignore holds nothing (RFC §23.4); a tool with no list takes any, and a
    /// list that is no list is refused, [`RoomKitError::Value`]. Python's
    /// `_served_delegation`.
    fn served_delegation(
        &self,
        calling: ChannelId,
        definition: &Value,
    ) -> Result<OwnerServe<S>, RoomKitError> {
        let listed: Option<Vec<Value>> =
            match definition.pointer("/parameters/properties/agent/enum") {
                None | Some(Value::Null) => None,
                Some(Value::Array(listed)) => Some(listed.clone()),
                Some(_) => {
                    return Err(RoomKitError::Value(
                        "the delegate tool's agent enum must be a list".to_owned(),
                    ));
                }
            };
        let handler = self.clone();
        Ok(Arc::new(move |_kit, call| {
            let (handler, listed, calling) = (handler.clone(), listed.clone(), calling.clone());
            Box::pin(async move {
                if let Some(listed) = &listed
                    && !call
                        .arguments
                        .get("agent")
                        .is_some_and(|agent| listed.contains(agent))
                {
                    let names: Vec<&str> = listed.iter().filter_map(Value::as_str).collect();
                    let refusal =
                        json!(format!("This tool delegates only to: {}", names.join(", ")));
                    return OwnerToolAnswer::Served(Value::String(dumps_object(
                        [("error", &refusal)],
                        true,
                    )));
                }
                match handler
                    .handle(&call.room_id, &calling, &call.arguments)
                    .await
                {
                    Ok(answer) => OwnerToolAnswer::Served(Value::String(dumps_object(
                        answer.iter().map(|(name, value)| (name.as_str(), value)),
                        true,
                    ))),
                    Err(error) => OwnerToolAnswer::Failed {
                        kind: error.class_name(),
                        message: error.to_string(),
                    },
                }
            })
        }))
    }
}

/// The tool's definition, `{name, description, parameters}`: its `agent`
/// held to `targets`, each an id and its description; the generic one for
/// none. Python's `DELEGATE_TOOL` and `build_delegate_tool(targets)`.
#[doc(hidden)]
#[must_use]
pub fn delegate_tool_definition(targets: &[(String, Option<String>)]) -> Value {
    let ids: Vec<&String> = targets.iter().map(|(id, _)| id).collect();
    let agent = if targets.is_empty() {
        json!({"type": "string", "description": AGENT_DESCRIPTION})
    } else {
        json!({"type": "string", "enum": ids, "description": AGENT_DESCRIPTION})
    };
    let description = if targets.is_empty() {
        "Delegate a task to a background agent. The agent works in its own room and you can \
         continue the current conversation. Use when: a task needs a specialist, the user wants \
         something done in the background, or you need to run work in parallel."
            .to_owned()
    } else {
        let lines: Vec<String> = targets
            .iter()
            .map(|(id, description)| match description {
                Some(description) if !description.is_empty() => format!("  - {id}: {description}"),
                _ => format!("  - {id}"),
            })
            .collect();
        format!(
            "Delegate a task to a background agent.\nAvailable agents:\n{}",
            lines.join("\n")
        )
    };
    json!({
        "name": DELEGATE_TOOL_NAME,
        "description": description,
        "parameters": {
            "type": "object",
            "properties": {
                "agent": agent,
                "task": {
                    "type": "string",
                    "description": "Clear description of what the agent should do",
                },
                "context": {
                    "type": "object",
                    "description": "Optional context to pass to the agent (e.g. a branch, a repo \
                        URL, what a tool returned): it reads it with the task, as data",
                },
                "share_channels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Channel IDs from the current room to share with the \
                        background agent",
                },
            },
            "required": ["agent", "task"],
        },
    })
}

/// `definition` as a handler allowing `max_conversation` messages serves
/// it: with a `conversation` argument up to that bound, without one when it
/// is 0, whatever `definition` declares. Python's `_served_tool`.
fn served_tool(mut definition: Value, max_conversation: u64) -> Value {
    let declared = definition
        .pointer("/parameters/properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| properties.contains_key(CONVERSATION_ARGUMENT));
    if max_conversation == 0 && !declared {
        return definition;
    }
    let Some(parameters) = definition
        .get_mut("parameters")
        .and_then(Value::as_object_mut)
    else {
        return definition;
    };
    let properties = parameters.entry("properties").or_insert_with(|| json!({}));
    if let Some(properties) = properties.as_object_mut() {
        properties.shift_remove(CONVERSATION_ARGUMENT);
        if max_conversation > 0 {
            properties.insert(
                CONVERSATION_ARGUMENT.to_owned(),
                json!({
                    "type": "integer",
                    "minimum": 0,
                    "maximum": max_conversation,
                    "description": format!(
                        "How many of the room's last messages to hand to the agent, quoted as \
                         data: at most {max_conversation}. Leave it out to hand over none."
                    ),
                }),
            );
        }
    }
    if let Some(required) = parameters.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|name| name != CONVERSATION_ARGUMENT);
    }
    definition
}

/// Caches what `task` returned once it completes, so a repeat of its call
/// gets the result, as `task_status` gives it, not the receipt of its
/// start. Only a completed task answers a repeat: a failed or cancelled one
/// is run again, as a strategy's dispatch is. One handed the room's last
/// messages answers none: what it returned holds for the room as it stood.
/// Python's `_remember_when_completed`.
async fn remember_when_completed(
    cache: CompletedTaskCache,
    task: DelegatedTask,
    mut receipt: Metadata,
    call: Call,
    caller: ChannelId,
) {
    let waited = task
        .wait(WaitOptions {
            timeout: Some(REMEMBER_WITHIN),
        })
        .await;
    let Ok(ended) = waited else {
        return;
    };
    if ended.status != TaskStatus::Completed {
        return;
    }
    receipt.insert("status".to_owned(), json!(ended.status));
    receipt.insert("result".to_owned(), json!(ended.output.unwrap_or_default()));
    let named = Kept {
        room_id: &task.parent_room_id,
        agent_id: &call.agent,
        task: &call.task,
        context: Some(&call.passed),
    };
    cache.keep(&named, receipt, Some(&caller), call.count == 0);
}

/// Whether one of `share_channels` is bound in the room as an intelligence
/// channel: one it does not bind is not shared (RFC §23.3 step 3).
async fn shares_an_agent<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    share_channels: &[ChannelId],
) -> Result<bool, RoomKitError> {
    for channel_id in share_channels {
        let binding = kit.store().get_binding(room_id, channel_id).await?;
        if binding.is_some_and(|binding| binding.category == ChannelCategory::Intelligence) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The worker a call names, `""` when it names none: only a text names one,
/// as the kit's channels are named; anything else is no channel of the kit.
fn agent_of(arguments: &Metadata) -> Result<String, RoomKitError> {
    match arguments.get("agent") {
        None => Ok(String::new()),
        Some(Value::String(agent)) => Ok(agent.clone()),
        Some(other) => Err(unregistered(&other.to_string())),
    }
}

/// The refusal of a worker no channel of the kit is: Python's
/// `ChannelNotRegisteredError`.
fn unregistered(named: &str) -> RoomKitError {
    RoomKitError::ChannelNotRegistered {
        // The id of no agent: what matters is the message, the id named.
        channel_id: ChannelId::system(),
        message: format!("Agent channel '{named}' not registered"),
    }
}

/// The task a call names; `""` when it names none, as Python reads it.
fn task_of(arguments: &Metadata) -> Result<String, RoomKitError> {
    match arguments.get("task") {
        None => Ok(String::new()),
        Some(Value::String(task)) => Ok(task.clone()),
        Some(_) => Err(RoomKitError::Value("task must be a string".to_owned())),
    }
}

/// What a call passed as `context`, as the object a task's metadata and its
/// brief read: a model may send a string where the tool says object, kept
/// under `context`. Null, an empty string and an empty list are no context;
/// 0 and false are values. Python's `_passed_context`.
pub(super) fn passed_context(passed: Option<&Value>) -> Metadata {
    match passed {
        None | Some(Value::Null) => Metadata::new(),
        Some(Value::Object(members)) => members.clone(),
        Some(Value::String(text)) if text.is_empty() => Metadata::new(),
        Some(Value::Array(items)) if items.is_empty() => Metadata::new(),
        Some(value) => Metadata::from_iter([("context".to_owned(), value.clone())]),
    }
}

/// How many messages a call hands over: what it asks, an integer as JSON
/// Schema reads one (`1.0` is, `"1"`, `1.5` and `true` are not), within
/// `maximum` and never below 0. Anything else asks for none. Python's
/// `_asked_conversation`.
pub(super) fn asked_conversation(asked: Option<&Value>, maximum: u64) -> u64 {
    let Some(Value::Number(asked)) = asked else {
        return 0;
    };
    if let Some(count) = asked.as_u64() {
        return count.min(maximum);
    }
    if asked.as_i64().is_some() {
        return 0;
    }
    match asked.as_f64() {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "an integral float above 0: one past u64's range saturates, which min holds"
        )]
        Some(float) if float.fract() == 0.0 && float > 0.0 => (float as u64).min(maximum),
        _ => 0,
    }
}

/// The channels a call names to share: `None`, the handler's default, when
/// it names none or an empty list; in a list, only a text names a channel.
/// Python's `_named_channels`.
///
/// # Errors
///
/// [`RoomKitError::Value`] for a value that is no list.
fn share_channels_of(arguments: &Metadata) -> Result<Option<Vec<ChannelId>>, RoomKitError> {
    match arguments.get("share_channels") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) if items.is_empty() => Ok(None),
        Some(Value::Array(items)) => Ok(Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .filter_map(|channel| ChannelId::new(channel).ok())
                .collect(),
        )),
        Some(_) => Err(RoomKitError::Value(
            "share_channels must be a list of channel ids".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/tasks/test_delegate_reach.py, the scenario
    // tasks/delegate-task-offers-the-conversation-on-the-host-s-option-alone
    #[test]
    fn a_call_is_held_to_the_maximum() {
        for (asked, read) in [
            (json!(2), 2),
            (json!(1_000_000), 5),
            (json!(1.0), 1),
            (json!(1e30), 5),
            (json!(-1), 0),
            (json!(-1.0), 0),
            (json!(1.5), 0),
            (json!("2"), 0),
            (json!(true), 0),
            (Value::Null, 0),
        ] {
            assert_eq!(asked_conversation(Some(&asked), 5), read, "{asked}");
        }
        assert_eq!(asked_conversation(None, 5), 0);
        assert_eq!(asked_conversation(Some(&json!(3)), 0), 0);
    }

    // Python: tests/tasks/test_delegate.py::TestSetupDelegation::test_a_custom_tool_declares_the_conversation_as_its_handler_allows
    #[test]
    fn a_custom_tool_declares_the_conversation_as_its_handler_allows() {
        let written = json!({
            "name": "delegate_task",
            "description": "Delegate.",
            "parameters": {
                "type": "object",
                "properties": {"agent": {"type": "string"}, "conversation": {"type": "integer"}},
                "required": ["agent", "conversation"],
            },
        });

        let closed = served_tool(written.clone(), 0);
        assert_eq!(
            closed["parameters"]["properties"]["conversation"],
            Value::Null
        );
        assert_eq!(closed["parameters"]["required"], json!(["agent"]));

        let open = served_tool(written, 3);
        assert_eq!(
            open["parameters"]["properties"]["conversation"]["maximum"],
            json!(3)
        );
        assert_eq!(open["parameters"]["required"], json!(["agent"]));

        // A tool with no such argument is left as it is at 0.
        let generic = delegate_tool_definition(&[]);
        assert_eq!(served_tool(generic.clone(), 0), generic);
    }

    #[test]
    fn a_context_is_an_object() {
        let object = |value: Value| value.as_object().cloned().expect("an object");
        for (passed, read) in [
            (json!({"a": 1}), json!({"a": 1})),
            (json!("text"), json!({"context": "text"})),
            (json!(["a"]), json!({"context": ["a"]})),
            (json!(0), json!({"context": 0})),
            (json!(false), json!({"context": false})),
            (json!(""), json!({})),
            (json!([]), json!({})),
            (json!({}), json!({})),
            (Value::Null, json!({})),
        ] {
            assert_eq!(passed_context(Some(&passed)), object(read), "{passed}");
        }
        assert_eq!(passed_context(None), Metadata::new());
    }

    // Python: tests/tasks/test_cache.py::TestDelegateHandlerSerialization::test_a_room_that_stopped_delegating_keeps_no_lock
    #[tokio::test]
    async fn a_room_that_stopped_delegating_keeps_no_lock() {
        let turns = Arc::new(RoomTurns::default());
        let room = RoomId::new("room-1").expect("a room id");
        let first = turns.take(&room).await;
        let waiting = {
            let (turns, room) = (Arc::clone(&turns), room.clone());
            tokio::spawn(async move { drop(turns.take(&room).await) })
        };
        tokio::task::yield_now().await;
        assert_eq!(turns.rooms_held(), 1);
        drop(first);
        waiting.await.expect("the second turn");
        assert_eq!(turns.rooms_held(), 0);
    }

    // A delegation cancelled while it waits for the room's turn leaves it too.
    #[tokio::test]
    async fn a_call_cut_while_it_waits_leaves_no_lock() {
        let turns = Arc::new(RoomTurns::default());
        let room = RoomId::new("room-1").expect("a room id");
        let first = turns.take(&room).await;
        let waiting = {
            let (turns, room) = (Arc::clone(&turns), room.clone());
            tokio::spawn(async move { drop(turns.take(&room).await) })
        };
        tokio::task::yield_now().await;
        waiting.abort();
        let _cut = waiting.await;
        drop(first);
        assert_eq!(turns.rooms_held(), 0);
    }
}
