//! A caller-described tool turn, scoped across asynchronous suspension.

mod catalogue;

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;

use roomkit::model::{Metadata, Room, RoomId};

use super::{AiTool, ToolCallContext};
use crate::AiError;
use crate::shutdown::ClosingContext;
use catalogue::Catalogue;

/// Python's `tool_turn_context` keyword arguments. All optional fields
/// default to `None`; `chain_depth` defaults to zero (RFC §21.4).
#[derive(Clone, Debug, Default)]
pub struct ToolTurnContextOptions {
    /// The turn's room id; may be omitted when `room` is supplied.
    pub room_id: Option<RoomId>,
    /// The immutable room snapshot. Readers retain this same `Arc`.
    pub room: Option<Arc<Room>>,
    /// Whose turn it is. This names a participant; it authenticates none.
    pub actor_id: Option<String>,
    /// The resolved catalogue. `None` is unresolved, `Some(vec![])` empty.
    pub tools: Option<Vec<AiTool>>,
    /// The response's depth (RFC §8.3). Rust accepts `0..=u32::MAX`;
    /// Python's helper also accepts signed, unbounded integers.
    pub chain_depth: u32,
    /// The call record; clones share its structured-content field.
    pub call: Option<ToolCallContext>,
    /// The conversation's label for who asked, not a participant id.
    pub requester: Option<String>,
}

/// Runs `work` as a tool of the turn described by `options` (RFC §21.4).
///
/// A test or host calling a handler directly supplies the context it would
/// otherwise lack. The response record is fresh. Nested scopes restore
/// their caller on return, error, panic and cancellation. `work` may borrow
/// values and need not be `Send`; its output, including a `Result`, is
/// returned unchanged inside the installer's `Result`.
///
/// Spawned tasks use [`roomkit::keep_context`]. They keep the readable
/// snapshot after this scope ends, but no longer lend its depth to calls
/// into a kit. A nested explicit scope replaces the outer tool depth;
/// runtime turn and hook depths still apply independently (RFC §8.3).
///
/// # Errors
///
/// Conflicting room arguments are rejected before `work` is polled.
///
/// ```
/// # async fn example() -> Result<(), roomkit_ai::AiError> {
/// use roomkit_ai::{ToolTurnContextOptions, current_tool_actor_id, tool_turn_context};
/// tool_turn_context(ToolTurnContextOptions {
///     actor_id: Some("alice".into()),
///     ..Default::default()
/// }, async {
///     assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
/// }).await?;
/// # Ok(())
/// # }
/// ```
pub fn tool_turn_context<F: Future>(
    options: ToolTurnContextOptions,
    work: F,
) -> impl Future<Output = Result<F::Output, AiError>> {
    // Box before constructing our future: otherwise each enclosing scope's
    // initial state embeds the full caller future, growing nested stacks.
    // This allocation requires neither Send nor 'static.
    let work = Box::pin(work);
    async move {
        check_one_room(&options)?;
        let depth = options.chain_depth;
        let scope = Scope::new(options);
        let work = roomkit::within_tool_call_scope(Arc::new(scope), work);
        let work = roomkit::TurnRecord::default().scope(work);
        Ok(roomkit::within_tool_turn_depth(depth, work).await)
    }
}

fn check_one_room(options: &ToolTurnContextOptions) -> Result<(), AiError> {
    if let (Some(room), Some(room_id)) = (&options.room, &options.room_id)
        && room.id != *room_id
    {
        return Err(AiError::ToolTurnRoomConflict {
            room: room.id.clone(),
            room_id: room_id.clone(),
        });
    }
    let room_id = options
        .room
        .as_ref()
        .map(|room| &room.id)
        .or(options.room_id.as_ref());
    if let Some(call) = &options.call
        && call.room_id.as_ref() != room_id
    {
        return Err(AiError::ToolTurnCallRoomConflict {
            call_room: call.room_id.clone(),
            turn_room: room_id.cloned(),
        });
    }
    Ok(())
}

/// The room id of the described tool turn, or `None` outside a scope.
pub fn current_tool_room_id() -> Option<RoomId> {
    current()?.turn.room_id.clone()
}

/// The turn's immutable room snapshot, retaining the supplied `Arc`.
/// A later store write does not change it. `None` when no room was supplied
/// or outside a scope. It names the room; it grants no authorization.
pub fn current_tool_room() -> Option<Arc<Room>> {
    current()?.turn.room.clone()
}

/// The conversation label of who asked (RFC §6.4, §19.7), or `None` when
/// no requester was supplied or outside a scope.
pub fn current_tool_requester() -> Option<String> {
    current()?.turn.requester.clone()
}

/// The captured response depth, even after a kept child outlives the
/// scope (RFC §21.4). Zero outside a scope. Reading this value does not
/// keep the scope's live depth in force for calls into a kit.
pub fn current_tool_chain_depth() -> u32 {
    current().map_or(0, |scope| scope.turn.chain_depth)
}

/// The resolved tool names. `None` outside a scope or for an unresolved
/// catalogue; an empty set for a resolved, empty catalogue.
/// A local AI turn includes names its policy admits before Tool Search
/// hides declarations, adds round declarations, and removes withdrawn tools.
pub fn current_tool_allowed_names() -> Option<HashSet<String>> {
    let scope = current()?;
    match &scope.turn.tools {
        Some(tools) => Some(tools.iter().map(|tool| tool.name.clone()).collect()),
        None => scope.catalogue.names(),
    }
}

/// The call this handler runs within (RFC §21.4). Clones share the
/// structured-content field. `None` outside a call; a task spawned with
/// [`roomkit::keep_context`] retains it.
pub fn current_tool_call() -> Option<ToolCallContext> {
    current()?.turn.call.clone()
}

/// The participant whose event woke the turn (RFC §5.5, §11), or the
/// explicit scope's actor. `None` when no participant woke it or outside
/// a scope. This names the turn; it does not authenticate it: a handler
/// accessing a person's data must require the participant's identification.
pub fn current_tool_actor_id() -> Option<String> {
    current()?.turn.actor_id.clone()
}

struct Scope {
    turn: ToolTurnContextOptions,
    room_metadata: Option<Arc<Metadata>>,
    catalogue: Catalogue,
    closing: ClosingContext,
}

impl Scope {
    fn new(mut turn: ToolTurnContextOptions) -> Self {
        let room_metadata = turn
            .room
            .as_ref()
            .map(|room| Arc::new(room.metadata.clone()));
        if let Some(room) = &turn.room {
            turn.room_id = Some(room.id.clone());
        }
        Self {
            turn,
            room_metadata,
            catalogue: Catalogue::default(),
            closing: closing_context(),
        }
    }

    async fn run<F: Future>(self: Arc<Self>, work: F) -> F::Output {
        let depth = self.turn.chain_depth;
        roomkit::within_tool_turn_depth(depth, roomkit::within_tool_call_scope(self, work)).await
    }
}

/// Preparation owns its own live depth, without replacing the turn record.
pub(crate) fn within_ai_turn<F: Future>(
    options: ToolTurnContextOptions,
    work: F,
) -> impl Future<Output = F::Output> {
    Arc::new(Scope::new(options)).run(Box::pin(work))
}

/// Capture preparation's readable snapshot now. Consumption starts a fresh
/// lifetime at its first poll, never reviving preparation's retained children.
pub(crate) fn continue_ai_turn<F: Future>(work: F) -> impl Future<Output = F::Output> {
    let scope = current().map(|parent| {
        Arc::new(Scope {
            turn: parent.turn.clone(),
            room_metadata: parent.room_metadata.clone(),
            catalogue: parent.catalogue.for_loop(),
            closing: parent.closing.clone(),
        })
    });
    let work = Box::pin(work);
    async move {
        match scope {
            Some(scope) => scope.run(work).await,
            None => work.await,
        }
    }
}

fn current() -> Option<Arc<Scope>> {
    roomkit::current_tool_call_scope()?.downcast::<Scope>().ok()
}

pub(crate) fn current_tool_room_metadata() -> Option<Arc<Metadata>> {
    current()?.room_metadata.clone()
}

pub(crate) fn resolve_tool_catalogue(names: HashSet<String>, withdrawn: &BTreeSet<String>) {
    if let Some(scope) = current() {
        scope.catalogue.resolve(names, withdrawn);
    }
}

pub(crate) fn record_tool_declaration(name: &str) {
    if let Some(scope) = current() {
        scope.catalogue.record(name);
    }
}

/// The existing handler boundary; installing a call does not replace
/// the response record its turn already holds.
pub(crate) async fn within_call<T>(
    call: ToolCallContext,
    actor_id: Option<String>,
    room_metadata: Option<Arc<Metadata>>,
    work: impl Future<Output = T>,
) -> T {
    let scope = match current() {
        Some(parent) => Scope {
            turn: ToolTurnContextOptions {
                call: Some(call),
                ..parent.turn.clone()
            },
            room_metadata: parent.room_metadata.clone(),
            catalogue: parent.catalogue.clone(),
            closing: parent.closing.clone(),
        },
        None => Scope {
            turn: ToolTurnContextOptions {
                room_id: call.room_id.clone(),
                call: Some(call),
                actor_id,
                ..Default::default()
            },
            room_metadata,
            catalogue: Catalogue::default(),
            closing: closing_context(),
        },
    };
    roomkit::within_tool_call_scope(Arc::new(scope), work).await
}

/// Lifecycle context carried by the same snapshot as tool calls and hooks.
pub(crate) fn closing_context() -> ClosingContext {
    let mut closing = ClosingContext::default();
    for scope in roomkit::current_tool_call_scopes() {
        if let Ok(scope) = scope.downcast::<Scope>() {
            closing.extend(&scope.closing);
        }
    }
    closing
}

pub(crate) async fn within_closing_context<F: Future>(
    closing: ClosingContext,
    work: F,
) -> F::Output {
    let mut scope = match current() {
        Some(parent) => Scope {
            turn: parent.turn.clone(),
            room_metadata: parent.room_metadata.clone(),
            catalogue: parent.catalogue.clone(),
            closing: parent.closing.clone(),
        },
        None => Scope::new(ToolTurnContextOptions::default()),
    };
    scope.closing = closing;
    roomkit::within_tool_call_scope(Arc::new(scope), work).await
}
