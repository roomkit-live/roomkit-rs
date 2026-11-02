//! The human-input requests a handler keeps (RFC §9.3, §10.6.5): Python's
//! `HumanInputHandler`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use roomkit_core::model::Metadata;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use uuid::Uuid;

use super::model::{PendingInput, PendingInputEvent, PendingInputStatus};
use super::options::{
    CloseOptions, CreateDetachedOptions, CreateOptions, HumanInputHandlerOptions, RejectOptions,
    WaitOptions,
};
use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::turn::{TurnHost, turn_in_scope};
use crate::turns::{ExecutorInputs, InputSettler};

/// The reason a request a hook blocked reads (RFC §9.3).
const DENIED: &str = "Denied by ON_USER_INPUT_REQUIRED hook";

/// The pending human-input requests of a tool that asks a person (RFC
/// §9.3, §10.6.5): Python's `HumanInputHandler`, created, waited for,
/// answered, rejected or released. Cloning it is cheap: the clones share
/// the requests.
///
/// - **The notification never gates the answer.**
///   [`create`](Self::create) arms the request and returns; its
///   `ON_USER_INPUT_REQUIRED` hooks run in the background, and a person
///   who answers while they still run answers a request already
///   listening. A block from them rejects the request, unless it was
///   answered first.
/// - **A recorded outcome stays readable.** A request that settled is kept
///   among the `retention` newest, and [`wait`](Self::wait) replays it once
///   the request left the active set; only an id never seen, or evicted,
///   is an error.
/// - **A request of a turn is answerable on the room's owner.** Made within
///   an intelligence turn the kit runs, it is told to the owner before its
///   hooks run, and
///   [`RoomKit::answer_input`](crate::RoomKit::answer_input) answers it;
///   one still open when its turn ends is withdrawn, but a detached one.
///
/// The hooks run for a request of a channel the handler serves (an
/// `AiChannel` given it as its human-input handler registers it): through
/// the turn it is made in, else through the kit the channel is registered
/// on, as from a host's webhook or job. A request of a channel no kit
/// registered, made outside a turn, is told to nobody.
#[derive(Clone)]
pub struct HumanInputHandler {
    shared: Arc<Shared>,
}

struct Shared {
    retention: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    pending: HashMap<String, Entry>,
    /// The settled requests, oldest first, and whether the handler gave
    /// each up.
    recent: VecDeque<(PendingInput, bool)>,
    registrations: HashMap<String, u64>,
    closed_channels: HashSet<String>,
    closed: bool,
    /// The notifications running, by their number, with their channel.
    notifying: HashMap<u64, (AbortHandle, String)>,
    next_notification: u64,
    /// The requests opened in a turn the kit runs: their owner keeps the
    /// record that answers them (RFC §10.6.5).
    turn_of: HashMap<String, Arc<ExecutorInputs>>,
    /// The kit's hooks of each channel a kit registered, for its requests
    /// made outside a turn: Python's `_on_input_required_by_channel`.
    on_input_required: HashMap<String, OnInputRequired>,
    /// In tests, what lands once a wait's bound passed, before the wait
    /// reads its request: the window another thread's answer can arrive in.
    #[cfg(test)]
    at_bound: Option<Box<dyn FnOnce() + Send>>,
}

struct Entry {
    record: PendingInput,
    /// Rejected by the handler itself (its close, a release), not by
    /// anyone's answer.
    withdrawn: bool,
    /// The request as it settled, and whether the handler gave it up:
    /// what its waiter reads, once.
    settled: watch::Sender<Option<(PendingInput, bool)>>,
}

/// A request that settled, which its turn's owner learns of once the
/// handler's lock is let go.
type Closing = Option<(Arc<ExecutorInputs>, String, PendingInputStatus)>;

/// The kit's `ON_USER_INPUT_REQUIRED` hooks on a request, and whether it
/// stands: Python's `OnInputRequiredCallback`, which the kit hands the
/// handler of a channel it registers.
pub(crate) type OnInputRequired =
    Arc<dyn Fn(PendingInputEvent) -> BoxFuture<'static, bool> + Send + Sync>;

/// What runs a request's `ON_USER_INPUT_REQUIRED` hooks.
enum Announcer {
    /// The turn the request is made in.
    Turn(TurnHost),
    /// The kit its channel is registered on, for a request made outside a
    /// turn.
    Kit(OnInputRequired),
}

impl Announcer {
    /// Runs the hooks on `event`: whether the request stands.
    async fn announce(self, event: PendingInputEvent) -> bool {
        match self {
            Self::Turn(host) => host.user_input_required(event).await,
            Self::Kit(on_input_required) => on_input_required(event).await,
        }
    }
}

impl std::fmt::Debug for HumanInputHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state();
        formatter
            .debug_struct("HumanInputHandler")
            .field("pending", &state.pending.len())
            .field("retention", &self.shared.retention)
            .finish_non_exhaustive()
    }
}

impl Default for HumanInputHandler {
    fn default() -> Self {
        Self::new(HumanInputHandlerOptions::default())
    }
}

impl HumanInputHandler {
    /// A handler keeping `options.retention` settled requests readable.
    pub fn new(options: HumanInputHandlerOptions) -> Self {
        Self {
            shared: Arc::new(Shared {
                retention: options.retention,
                state: Mutex::new(State::default()),
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The active requests, by their id: a snapshot.
    pub fn pending(&self) -> HashMap<String, PendingInput> {
        self.state()
            .pending
            .iter()
            .map(|(id, entry)| (id.clone(), entry.record.clone()))
            .collect()
    }

    /// Registers `channel_id` as the channel object serving this handler's
    /// requests: its requests fire the kit's `ON_USER_INPUT_REQUIRED`
    /// hooks, and a closed scope of its id opens again. The token, which
    /// the channel's close hands back. For an AI channel given the
    /// handler, Python's private `_set_on_input_required`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] once the handler closed.
    #[doc(hidden)]
    pub fn register(&self, channel_id: &str) -> Result<u64, RoomKitError> {
        let mut state = self.state();
        if state.closed {
            return Err(runtime("Human input handler is closed".to_owned()));
        }
        state.closed_channels.remove(channel_id);
        let registration = state
            .registrations
            .get(channel_id)
            .map_or(1, |token| token.saturating_add(1));
        state
            .registrations
            .insert(channel_id.to_owned(), registration);
        Ok(registration)
    }

    /// Runs `on_input_required`, the kit's `ON_USER_INPUT_REQUIRED` hooks,
    /// for the requests of `channel_id` made outside a turn, until the
    /// channel's scope closes: a kit hands it at the channel's
    /// registration, as Python's kit hands `_set_on_input_required` its
    /// callback.
    pub(crate) fn set_on_input_required(
        &self,
        channel_id: &str,
        on_input_required: OnInputRequired,
    ) {
        self.state()
            .on_input_required
            .insert(channel_id.to_owned(), on_input_required);
    }

    /// Settles the requests of `options.channel_id`, every request when
    /// `None`, and stops their notifications: each still pending is
    /// rejected, read by its waiter as given up (Python's `RuntimeError`).
    /// A channel's scope takes no request from then on, until it registers
    /// again; the whole handler's never again. A close naming a
    /// registration that is not the channel's current one closes nothing.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's method is a coroutine: the call is awaited"
    )]
    pub async fn close(&self, options: CloseOptions) {
        let CloseOptions {
            channel_id,
            registration,
        } = options;
        let (closing, stopped) = {
            let mut state = self.state();
            match &channel_id {
                None => {
                    state.closed = true;
                    state.registrations.clear();
                    state.on_input_required.clear();
                }
                Some(channel) => {
                    if registration
                        .is_some_and(|token| state.registrations.get(channel) != Some(&token))
                    {
                        return;
                    }
                    state.closed_channels.insert(channel.clone());
                    state.registrations.remove(channel);
                    state.on_input_required.remove(channel);
                }
            }
            let owned: Vec<String> = state
                .pending
                .iter()
                .filter(|(_, entry)| {
                    channel_id
                        .as_ref()
                        .is_none_or(|channel| &entry.record.channel_id == channel)
                })
                .map(|(id, _)| id.clone())
                .collect();
            let mut closing = Vec::new();
            for pending_id in owned {
                give_up(&mut state, &pending_id, "Human input handler closed");
                closing.push(self.retire(&mut state, &pending_id));
            }
            let stopped: Vec<u64> = state
                .notifying
                .iter()
                .filter(|(_, (_, channel))| channel_id.as_ref().is_none_or(|id| id == channel))
                .map(|(number, _)| *number)
                .collect();
            let stopped: Vec<AbortHandle> = stopped
                .into_iter()
                .filter_map(|number| state.notifying.remove(&number).map(|(task, _)| task))
                .collect();
            (closing, stopped)
        };
        for task in stopped {
            task.abort();
        }
        for closed in closing {
            tell_owner(closed);
        }
    }

    /// Opens a request of the tool `tool_name` with `arguments`, answerable
    /// at once, and starts its notification: the turn's owner told, then
    /// the kit's `ON_USER_INPUT_REQUIRED` hooks run, a block rejecting it.
    /// [`wait`](Self::wait) owns its cleanup; for a request no one waits
    /// on, [`create_detached`](Self::create_detached).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] once the handler, or the scope of
    /// `options.channel_id`, closed.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's method is a coroutine: the call is awaited"
    )]
    pub async fn create(
        &self,
        tool_name: impl Into<String>,
        arguments: Metadata,
        options: CreateOptions,
    ) -> Result<PendingInput, RoomKitError> {
        self.arm(tool_name.into(), arguments, options, false)
    }

    /// Opens a request no one will [`wait`](Self::wait) on, for a runtime
    /// whose tool loop is its own and whose answer travels back another
    /// way: nothing here retires it, so its creator [`release`]s it. A
    /// turn's end leaves it open.
    ///
    /// [`release`]: Self::release
    ///
    /// # Errors
    ///
    /// As [`create`](Self::create).
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's method is a coroutine: the call is awaited"
    )]
    pub async fn create_detached(
        &self,
        tool_name: impl Into<String>,
        arguments: Metadata,
        options: CreateDetachedOptions,
    ) -> Result<PendingInput, RoomKitError> {
        self.arm(tool_name.into(), arguments, options.into(), true)
    }

    /// Registers the request, then starts its notification.
    fn arm(
        &self,
        tool_name: String,
        arguments: Metadata,
        options: CreateOptions,
        detached: bool,
    ) -> Result<PendingInput, RoomKitError> {
        let turn = turn_in_scope();
        // Asked from work that outlived its turn: settled here alone.
        let inputs = turn.as_ref().map(TurnHost::inputs).filter(|i| i.is_open());
        let pending_id = Uuid::new_v4().simple().to_string();
        let mut record = PendingInput::new(
            pending_id.clone(),
            tool_name,
            arguments,
            options.room_id,
            options.tool_call_id,
            options.channel_id,
        );
        record.detached = detached;
        record.actor_id = options.actor_id;
        let notified = {
            let mut state = self.state();
            if state.closed {
                return Err(runtime("Human input handler is closed".to_owned()));
            }
            if state.closed_channels.contains(&record.channel_id) {
                return Err(runtime(format!(
                    "Human input handler is closed for channel {}",
                    record.channel_id
                )));
            }
            let (settled, _) = watch::channel(None);
            state.pending.insert(
                pending_id.clone(),
                Entry {
                    record: record.clone(),
                    withdrawn: false,
                    settled,
                },
            );
            if let Some(inputs) = &inputs {
                state.turn_of.insert(pending_id, Arc::clone(inputs));
            }
            // The hooks are run through the turn, else the kit, for a
            // channel this handler serves.
            announcer(&state, turn, &record.channel_id)
        };
        if notified.is_some() || inputs.is_some() {
            let mut event = PendingInputEvent::new(
                record.pending_id.clone(),
                record.tool_name.clone(),
                record.arguments.clone(),
                record.room_id.clone(),
                record.tool_call_id.clone(),
                record.channel_id.clone(),
                options.channel_type,
            );
            event.actor_id.clone_from(&record.actor_id);
            event.timestamp = record.created_at;
            self.notify(event, notified);
        }
        Ok(record)
    }

    /// Starts the notification of `event`, off the waiting path: the
    /// request's owner told first, then `announcer`'s hooks (RFC §10.6.5).
    fn notify(&self, event: PendingInputEvent, announcer: Option<Announcer>) {
        let weak = Arc::downgrade(&self.shared);
        let channel = event.channel_id.clone();
        let number = {
            let mut state = self.state();
            state.next_notification = state.next_notification.saturating_add(1);
            state.next_notification
        };
        let spawner = match &announcer {
            Some(Announcer::Turn(host)) => Some(host.clone()),
            Some(Announcer::Kit(_)) | None => None,
        };
        let task = crate::keep_context(notification(Weak::clone(&weak), event, announcer, number));
        let abort = match spawner {
            Some(host) => host.spawn(Box::pin(task)),
            None => Some(tokio::spawn(task).abort_handle()),
        };
        let Some(abort) = abort else {
            return;
        };
        if let Some(shared) = weak.upgrade() {
            let mut state = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            // Ended before it was kept: nothing to stop.
            if !abort.is_finished() {
                state.notifying.insert(number, (abort, channel));
            }
        }
    }

    /// Tells the owner of the request's turn that it is open, when it is
    /// still pending.
    fn open_on_owner(&self, pending_id: &str) {
        let opened = {
            let state = self.state();
            let Some(entry) = state.pending.get(pending_id) else {
                return;
            };
            if entry.record.status != PendingInputStatus::Pending {
                return;
            }
            state.turn_of.get(pending_id).map(|inputs| {
                (
                    Arc::clone(inputs),
                    entry.record.tool_name.clone(),
                    entry.record.arguments.clone(),
                )
            })
        };
        if let Some((inputs, tool_name, arguments)) = opened {
            let settle: Arc<dyn InputSettler> = Arc::new(self.clone());
            inputs.opened(pending_id, &tool_name, &arguments, settle);
        }
    }

    /// Waits until the request is answered, rejected or past
    /// `options.timeout`; an answer accepted as the bound passes is
    /// returned. An outcome already reached and consumed is
    /// replayed from the retention. Dropped before its outcome, the wait
    /// releases the request, so a late answer is not taken for it.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Timeout`] when the bound passes first, or a retained
    /// request had timed out; [`RoomKitError::HumanInputRejected`] for a
    /// request rejected, by a person or an `ON_USER_INPUT_REQUIRED` hook;
    /// [`RoomKitError::Runtime`] for one the handler gave up before an
    /// answer (its close, a release); [`RoomKitError::Value`] for an id
    /// never seen, or evicted from the retention.
    pub async fn wait(
        &self,
        pending_id: &str,
        options: WaitOptions,
    ) -> Result<String, RoomKitError> {
        let mut settled = {
            let state = self.state();
            match state.pending.get(pending_id) {
                Some(entry) => entry.settled.subscribe(),
                None => {
                    return match state
                        .recent
                        .iter()
                        .find(|(r, _)| r.pending_id == pending_id)
                    {
                        Some((record, withdrawn)) => outcome(record, *withdrawn),
                        None => Err(RoomKitError::Value(format!(
                            "No pending request with id {pending_id}"
                        ))),
                    };
                }
            }
        };
        let released = ReleaseOnDrop {
            handler: self,
            pending_id: Some(pending_id),
        };
        // Settled or past its bound, the request's outcome is read under
        // the lock, in `conclude`.
        drop(tokio::time::timeout(options.timeout, settled.wait_for(Option::is_some)).await);
        released.disarm();
        #[cfg(test)]
        self.bound_passed(&settled);
        self.conclude(pending_id, &settled)
    }

    /// Ends a wait on `pending_id`: a request still pending times out, one
    /// that settled keeps its outcome, an answer accepted as the bound
    /// passed included (RFC §9.3); the request retires, and its outcome is
    /// reported.
    fn conclude(
        &self,
        pending_id: &str,
        settled: &watch::Receiver<Option<(PendingInput, bool)>>,
    ) -> Result<String, RoomKitError> {
        let (closed, waited) = {
            let mut state = self.state();
            if let Some(entry) = state.pending.get_mut(pending_id)
                && entry.record.status == PendingInputStatus::Pending
            {
                entry.record.status = PendingInputStatus::TimedOut;
                let timed_out = Some((entry.record.clone(), entry.withdrawn));
                entry.settled.send_replace(timed_out);
            }
            let waited = settled.borrow().clone();
            (self.retire(&mut state, pending_id), waited)
        };
        tell_owner(closed);
        match waited {
            Some((record, withdrawn)) => outcome(&record, withdrawn),
            None => Err(RoomKitError::Timeout),
        }
    }

    /// Lands `landing` the next time a wait's bound passes, before the wait
    /// reads its request, as an answer from another thread can.
    #[cfg(test)]
    pub(crate) fn at_bound(&self, landing: impl FnOnce() + Send + 'static) {
        self.state().at_bound = Some(Box::new(landing));
    }

    /// Runs what a test lands once a wait's bound passed with no outcome.
    #[cfg(test)]
    fn bound_passed(&self, settled: &watch::Receiver<Option<(PendingInput, bool)>>) {
        let unsettled = settled.borrow().is_none();
        let landing = if unsettled {
            self.state().at_bound.take()
        } else {
            None
        };
        if let Some(landing) = landing {
            landing();
        }
    }

    /// Drops a request whose cleanup the caller owns, the counterpart of
    /// [`create_detached`](Self::create_detached): one still unanswered is
    /// rejected on the way out, its waiter reading it given up. Whether an
    /// active request was dropped.
    pub fn release(&self, pending_id: &str) -> bool {
        let closed = {
            let mut state = self.state();
            if !state.pending.contains_key(pending_id) {
                return false;
            }
            give_up(&mut state, pending_id, "Released before an answer arrived");
            self.retire(&mut state, pending_id)
        };
        tell_owner(closed);
        true
    }

    /// Answers the request `pending_id` with `result`: whether it was
    /// pending.
    pub fn resolve(&self, pending_id: &str, result: impl Into<String>) -> bool {
        let result = result.into();
        self.settle_with(pending_id, |record| {
            record.result = Some(result);
            record.status = PendingInputStatus::Resolved;
        })
    }

    /// Rejects the request `pending_id`, its waiter reading
    /// `options.reason`: whether it was pending.
    pub fn reject(&self, pending_id: &str, options: RejectOptions) -> bool {
        self.settle_with(pending_id, |record| {
            record.reject_reason = Some(options.reason);
            record.status = PendingInputStatus::Rejected;
        })
    }

    /// Settles the request with the outcome its turn's owner recorded: an
    /// answer (`value`) or a rejection (`reason`), when it is still open.
    pub fn settle(&self, pending_id: &str, value: Option<String>, reason: Option<String>) -> bool {
        self.state().turn_of.remove(pending_id);
        match value {
            Some(value) => self.resolve(pending_id, value),
            None => self.reject(
                pending_id,
                RejectOptions {
                    reason: reason.unwrap_or_default(),
                },
            ),
        }
    }

    /// Withdraws a request whose turn ended while it was open (RFC
    /// §10.6.5): its waiter reads it given up, as after a release. A
    /// detached request is its creator's to release: the turn only lets go
    /// of it.
    pub fn withdraw(&self, pending_id: &str) {
        let detached = {
            let mut state = self.state();
            state.turn_of.remove(pending_id);
            state
                .pending
                .get(pending_id)
                .is_some_and(|entry| entry.record.detached)
        };
        if !detached {
            self.release(pending_id);
        }
    }

    /// Settles the pending request `pending_id` as `change` writes it, and
    /// records the outcome: whether it was pending.
    fn settle_with(&self, pending_id: &str, change: impl FnOnce(&mut PendingInput)) -> bool {
        let closed = {
            let mut state = self.state();
            let Some(entry) = state.pending.get_mut(pending_id) else {
                return false;
            };
            if entry.record.status != PendingInputStatus::Pending {
                return false;
            }
            change(&mut entry.record);
            entry
                .settled
                .send_replace(Some((entry.record.clone(), entry.withdrawn)));
            self.retain(&mut state, pending_id)
        };
        tell_owner(closed);
        true
    }

    /// Moves a finished request out of the active set, recording it.
    fn retire(&self, state: &mut State, pending_id: &str) -> Closing {
        let closed = self.retain(state, pending_id);
        state.pending.remove(pending_id);
        closed
    }

    /// Records the settled outcome of `pending_id`, the oldest let go past
    /// the retention: a host that drops the request on `resolve()` cannot
    /// turn the answer into a failure for whoever asked. The request's
    /// turn, which its owner learns of once the lock is let go.
    fn retain(&self, state: &mut State, pending_id: &str) -> Closing {
        let entry = state.pending.get(pending_id)?;
        let kept = (entry.record.clone(), entry.withdrawn);
        let status = entry.record.status;
        let closed = state
            .turn_of
            .remove(pending_id)
            .map(|inputs| (inputs, pending_id.to_owned(), status));
        if self.shared.retention > 0 {
            match state
                .recent
                .iter_mut()
                .find(|(record, _)| record.pending_id == pending_id)
            {
                Some(slot) => *slot = kept,
                None => state.recent.push_back(kept),
            }
            while state.recent.len() > self.shared.retention {
                state.recent.pop_front();
            }
        }
        closed
    }
}

impl InputSettler for HumanInputHandler {
    fn settle(&self, pending_id: &str, value: Option<String>, reason: Option<String>) -> bool {
        Self::settle(self, pending_id, value, reason)
    }

    fn withdraw(&self, pending_id: &str) {
        Self::withdraw(self, pending_id);
    }
}

/// The notification `number` of `event`: the turn's owner told, then
/// `announcer`'s `ON_USER_INPUT_REQUIRED` hooks, a block rejecting the
/// request (RFC §9.3: they fail open).
async fn notification(
    shared: Weak<Shared>,
    event: PendingInputEvent,
    announcer: Option<Announcer>,
    number: u64,
) {
    let Some(handler) = shared.upgrade().map(|shared| HumanInputHandler { shared }) else {
        return;
    };
    let scope_open = {
        let state = handler.state();
        !state.closed && !state.closed_channels.contains(&event.channel_id)
    };
    if scope_open {
        handler.open_on_owner(&event.pending_id);
        if let Some(announcer) = announcer {
            let pending_id = event.pending_id.clone();
            if !announcer.announce(event).await {
                handler.reject(
                    &pending_id,
                    RejectOptions {
                        reason: DENIED.to_owned(),
                    },
                );
            }
        }
    }
    handler.state().notifying.remove(&number);
}

/// Whether the hooks of a request of `channel_id` run: a channel the
/// handler serves, or the one it serves for a request naming none.
fn registered(state: &State, channel_id: &str) -> bool {
    state.registrations.contains_key(channel_id)
        || (channel_id.is_empty() && state.registrations.len() == 1)
}

/// What runs the hooks of a request of `channel_id`, for a channel the
/// handler serves: the turn it is made in, else the kit that registered
/// the channel, the one kit for a request naming no channel (Python's
/// `_callback_for`).
fn announcer(state: &State, turn: Option<TurnHost>, channel_id: &str) -> Option<Announcer> {
    if !registered(state, channel_id) {
        return None;
    }
    if let Some(host) = turn {
        return Some(Announcer::Turn(host));
    }
    let kit = match state.on_input_required.get(channel_id) {
        Some(kit) => Some(kit),
        None if channel_id.is_empty() && state.on_input_required.len() == 1 => {
            state.on_input_required.values().next()
        }
        None => None,
    };
    kit.map(|kit| Announcer::Kit(Arc::clone(kit)))
}

/// The request `pending_id`, still pending, given up by the handler for
/// `reason`.
fn give_up(state: &mut State, pending_id: &str, reason: &str) {
    if let Some(entry) = state.pending.get_mut(pending_id)
        && entry.record.status == PendingInputStatus::Pending
    {
        entry.record.reject_reason = Some(reason.to_owned());
        entry.record.status = PendingInputStatus::Rejected;
        entry.withdrawn = true;
        entry
            .settled
            .send_replace(Some((entry.record.clone(), true)));
    }
}

/// Tells the owner of a request's turn that it settled where its tool
/// waits (RFC §10.6.5).
fn tell_owner(closed: Closing) {
    if let Some((inputs, pending_id, status)) = closed {
        inputs.closed(&pending_id, status.as_str());
    }
}

/// A terminal request as its result or its failure.
fn outcome(record: &PendingInput, withdrawn: bool) -> Result<String, RoomKitError> {
    match record.status {
        PendingInputStatus::Rejected => {
            let reason = record
                .reject_reason
                .clone()
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| "Request rejected".to_owned());
            if withdrawn {
                // The handler gave the request up: nobody's answer (RFC §9.3).
                Err(runtime(reason))
            } else {
                Err(RoomKitError::HumanInputRejected(reason))
            }
        }
        PendingInputStatus::Resolved => Ok(record.result.clone().unwrap_or_default()),
        PendingInputStatus::TimedOut => Err(RoomKitError::Timeout),
        PendingInputStatus::Pending => {
            Err(runtime("Unexpected pending status: pending".to_owned()))
        }
    }
}

/// Python's `RuntimeError`.
fn runtime(message: String) -> RoomKitError {
    RoomKitError::Runtime(message)
}

/// A wait dropped before its outcome releases its request: nobody waits for
/// the answer any more.
struct ReleaseOnDrop<'a> {
    handler: &'a HumanInputHandler,
    pending_id: Option<&'a str>,
}

impl ReleaseOnDrop<'_> {
    fn disarm(mut self) {
        self.pending_id = None;
    }
}

impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(pending_id) = self.pending_id.take() {
            self.handler.release(pending_id);
        }
    }
}
