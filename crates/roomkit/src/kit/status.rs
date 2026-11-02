//! The kit's status bus (RFC §19.8), a room's metadata update (RFC §5.1)
//! and a room's observations and feedback (RFC §5.11, §15.9), as the Python
//! implementation's `status_bus`, `update_room_metadata`,
//! `list_observations` and `submit_feedback`.
//!
//! The kit subscribes to its bus when it is built, or, when the bus does not
//! subscribe at once, at its first room activity, which awaits it: an entry
//! naming a room in its metadata's `room_id` fires that room's
//! `ON_STATUS_POSTED` hooks, and every entry is the framework event
//! `status_posted`.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Waker};

use roomkit_core::model::{
    ChannelId, ChannelType, Confidence, EventId, Metadata, Observation, Room, RoomContext,
    RoomEvent, RoomId, Task, TaskStatus,
};
use roomkit_core::ops::{Environment, UpdateRoomMetadata};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookSpec, HookSubject, HookTrigger};
use serde_json::Value;
use tokio::task::JoinSet;

use super::{Inner, RoomKit, read};
use crate::channel::BoxFuture;
use crate::context::ExecutionContext;
use crate::driver;
use crate::error::RoomKitError;
use crate::events::FrameworkEvent;
use crate::hooks::{self, HookHandler, Kept};
use crate::orchestration::backend::StatusCallback;
use crate::orchestration::{StatusBus, StatusEntry};
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// What [`RoomKit::update_room_metadata`] takes beside the room and the
/// keys: Python's `update_room_metadata(room_id, metadata, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateRoomMetadataOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization, or of none, is not
    /// found, nothing written.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::list_observations`] takes beside the room: Python's
/// `list_observations(room_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListObservationsOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::list_tasks`] takes beside the room: Python's
/// `list_tasks(room_id, status=None, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListTasksOptions {
    /// Python `status=None`: only the tasks of this status.
    pub status: Option<TaskStatus>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::submit_feedback`] takes beside the room and the rating:
/// Python's `submit_feedback(room_id, rating, *, event_id=None,
/// channel_id=None, comment='', dimension='overall', metadata=None,
/// organization_id=None)`.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmitFeedbackOptions {
    /// Python `event_id=None`: the response rated, when one is.
    pub event_id: Option<String>,
    /// Python `channel_id=None`: the channel rated.
    pub channel_id: Option<ChannelId>,
    /// Python `comment=''`: what the person said.
    pub comment: String,
    /// Python `dimension='overall'`: what is rated.
    pub dimension: String,
    /// Python `metadata=None`: what else the observation carries.
    pub metadata: Option<Metadata>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization is not found, nothing
    /// stored or fired.
    pub organization_id: Option<String>,
}

impl Default for SubmitFeedbackOptions {
    fn default() -> Self {
        Self {
            event_id: None,
            channel_id: None,
            comment: String::new(),
            dimension: "overall".to_owned(),
            metadata: None,
            organization_id: None,
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// The kit's status bus (RFC §19.8): the one
    /// [`RoomKitOptions::status_bus`](crate::RoomKitOptions::status_bus)
    /// named, or the in-memory one the kit made. Python's `status_bus`.
    pub fn status_bus(&self) -> &StatusBus {
        &self.inner.status_bus
    }

    /// Merges `metadata` into the room's metadata, under the room's lock,
    /// and returns the room as stored (RFC §5.1). A key only the framework
    /// writes, under `roomkit:` or of the room's private state
    /// (`author_register`), is dropped with a warning, the rest written, so
    /// a read-modify-write leaves the framework's keys as they are. The
    /// room's due timers are left as they are, as Python's.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::RoomNotFound`] for a room the store lacks
    /// or the organization `options` names does not reach, nothing written;
    /// the store's error when it fails.
    pub async fn update_room_metadata(
        &self,
        room_id: &RoomId,
        metadata: Metadata,
        options: UpdateRoomMetadataOptions,
    ) -> Result<Room, RoomKitError> {
        let update = UpdateRoomMetadata::new(
            self.config(),
            room_id.clone(),
            metadata,
            options.organization_id,
        );
        Ok(driver::run(&self.inner, "update_room_metadata", update).await??)
    }

    /// The room's observations (RFC §5.11), in the order they were stored.
    /// It answers during the kit's close and after it, while the store
    /// does.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the organization
    /// `options` names does not reach; the store's error when it fails.
    pub async fn list_observations(
        &self,
        room_id: &RoomId,
        options: ListObservationsOptions,
    ) -> Result<Vec<Observation>, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        self.store()
            .list_observations(room_id)
            .await
            .map_err(|error| self.read_failed(error))
    }

    /// The room's tasks (RFC §5.10), of the status `options` names, as the
    /// store lists them. It answers during the kit's close and after it,
    /// while the store does.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the organization
    /// `options` names does not reach; the store's error when it fails.
    pub async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> Result<Vec<Task>, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let filter = crate::store::ListTasksOptions {
            status: options.status,
        };
        self.store()
            .list_tasks(room_id, filter)
            .await
            .map_err(|error| self.read_failed(error))
    }

    /// Records a person's `rating` of the room, or of one of its responses,
    /// as an observation of the room (RFC §15.9): the rating clamped to
    /// 0.0..=1.0, its `category` `feedback:<dimension>`, its `confidence`
    /// the rating, its `content` a line naming the dimension, the rating
    /// and the comment, its metadata `type = "feedback"`, `dimension`,
    /// `rating`, `comment` and `event_id` beside `options.metadata`. Once it
    /// is stored, `ON_FEEDBACK` fires with it, and `feedback` is emitted once
    /// those hooks ended, the call not waiting for them. A feedback naming no
    /// channel is an observation of no channel, written empty.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::RoomNotFound`] for a room the organization
    /// `options` names does not reach, nothing stored or fired; the store's
    /// error when it fails.
    pub async fn submit_feedback(
        &self,
        room_id: &RoomId,
        rating: f64,
        options: SubmitFeedbackOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.inner.subscribed().await;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let channel_id = options.channel_id.clone();
        let observation = feedback(
            &self.inner.environment(),
            room_id,
            channel_id,
            rating,
            &options,
        );
        self.store().add_observation(observation.clone()).await?;
        let Some(context) = self.inner.observed_context(room_id).await else {
            return Ok(());
        };
        let reported = feedback_event(room_id, &observation, &options);
        let inner = Arc::clone(&self.inner);
        let then = async move {
            inner.emit_framework(reported).await;
        };
        let observers = self.inner.observers(HookTrigger::OnFeedback, room_id);
        if observers.is_empty() {
            then.await;
            return Ok(());
        }
        self.inner.observe(observers, observation, context, then);
        Ok(())
    }
}

/// The observation a feedback records (RFC §15.9), as Python's
/// `submit_feedback` writes it.
fn feedback(
    environment: &Arc<dyn Environment + Send + Sync>,
    room_id: &RoomId,
    channel_id: Option<ChannelId>,
    rating: f64,
    options: &SubmitFeedbackOptions,
) -> Observation {
    let rating = clamped(rating);
    let dimension = &options.dimension;
    let content = if options.comment.is_empty() {
        format!("[{dimension}] {rating:.2}")
    } else {
        format!("[{dimension}] {rating:.2}: {}", options.comment)
    };
    let mut metadata = Metadata::new();
    metadata.insert("type".to_owned(), Value::from("feedback"));
    metadata.insert("dimension".to_owned(), Value::from(dimension.as_str()));
    metadata.insert("rating".to_owned(), Value::from(rating));
    metadata.insert("comment".to_owned(), Value::from(options.comment.as_str()));
    metadata.insert(
        "event_id".to_owned(),
        options.event_id.clone().map_or(Value::Null, Value::from),
    );
    metadata.extend(options.metadata.clone().unwrap_or_default());
    Observation {
        id: environment.new_observation_id(),
        room_id: room_id.clone(),
        channel_id,
        content,
        category: Some(format!("feedback:{dimension}")),
        confidence: Confidence::new(rating).unwrap_or(Confidence::FULL),
        created_at: environment.now(),
        metadata,
    }
}

/// `rating` within 0.0..=1.0, as Python's `max(0.0, min(1.0, rating))`:
/// a NaN reads as 1.0 there.
fn clamped(rating: f64) -> f64 {
    if rating.is_nan() {
        return 1.0;
    }
    rating.clamp(0.0, 1.0)
}

/// The `feedback` framework event of `observation` (RFC §8.2): the room,
/// the channel, the event, the dimension and the rating.
fn feedback_event(
    room_id: &RoomId,
    observation: &Observation,
    options: &SubmitFeedbackOptions,
) -> FrameworkEvent {
    let mut data = Metadata::new();
    data.insert(
        "dimension".to_owned(),
        Value::from(options.dimension.as_str()),
    );
    data.insert(
        "rating".to_owned(),
        Value::from(observation.confidence.value()),
    );
    FrameworkEvent {
        r#type: "feedback".to_owned(),
        room_id: Some(room_id.clone()),
        channel_id: options.channel_id.clone(),
        event_id: options
            .event_id
            .as_deref()
            .and_then(|id| EventId::new(id).ok()),
        timestamp: observation.created_at,
        data,
    }
}

/// The handler of a hook whose payload is `P`, from what the kit keeps.
trait ObservedPayload: Clone + Send + Sync + 'static {
    fn handler(kept: &Kept) -> Option<Arc<dyn HookHandler<Self>>>;
}

impl ObservedPayload for StatusEntry {
    fn handler(kept: &Kept) -> Option<Arc<dyn HookHandler<Self>>> {
        match kept {
            Kept::StatusEntry(handler) => Some(Arc::clone(handler)),
            Kept::Event(_)
            | Kept::ToolCall(_)
            | Kept::Session(_)
            | Kept::Status(_)
            | Kept::Observation(_)
            | Kept::Input(_)
            | Kept::Turn(_) => None,
        }
    }
}

impl ObservedPayload for RoomEvent {
    fn handler(kept: &Kept) -> Option<Arc<dyn HookHandler<Self>>> {
        match kept {
            Kept::Event(handler) => Some(Arc::clone(handler)),
            Kept::Observation(_)
            | Kept::ToolCall(_)
            | Kept::Session(_)
            | Kept::Status(_)
            | Kept::StatusEntry(_)
            | Kept::Input(_)
            | Kept::Turn(_) => None,
        }
    }
}

impl ObservedPayload for Observation {
    fn handler(kept: &Kept) -> Option<Arc<dyn HookHandler<Self>>> {
        match kept {
            Kept::Observation(handler) => Some(Arc::clone(handler)),
            Kept::Event(_)
            | Kept::ToolCall(_)
            | Kept::Session(_)
            | Kept::Status(_)
            | Kept::StatusEntry(_)
            | Kept::Input(_)
            | Kept::Turn(_) => None,
        }
    }
}

/// The observers of a trigger whose payload is no event, each with its
/// registration.
type Observers = Vec<(HookSpec, Kept)>;

impl<S: ConversationStore> Inner<S> {
    /// Subscribes the kit to its status bus, at its construction: a
    /// backend whose subscription ends at once is subscribed now; any other
    /// at the kit's first room activity made in a runtime, which awaits it,
    /// as Python's kit subscribes at a room's first activity
    /// (`_ensure_status_bus_subscribed`).
    pub(super) fn subscribe_status_bus(self: &Arc<Self>) {
        let kit = Arc::downgrade(self);
        let callback: StatusCallback = Arc::new(move |entry| {
            let kit = Weak::clone(&kit);
            Box::pin(async move {
                if let Some(inner) = kit.upgrade() {
                    inner.on_status_posted(entry).await;
                }
            })
        });
        let bus = self.status_bus.clone();
        let subscriber: Subscriber = Arc::new(move || {
            let (bus, callback) = (bus.clone(), Arc::clone(&callback));
            Box::pin(async move { bus.subscribe(callback).await })
        });
        let mut subscribing = subscriber();
        if tokio::runtime::Handle::try_current().is_ok() {
            let mut waking = Context::from_waker(Waker::noop());
            if subscribing.as_mut().poll(&mut waking).is_ready() {
                return;
            }
        }
        // Outside a runtime, not polled: a backend that reaches a server
        // needs the runtime's reactor from its first poll.
        self.status_subscription.keep(subscriber, subscribing);
    }

    /// Makes the kit's subscription to its bus, when one waits, the first
    /// thing of a room activity (Python's `_ensure_status_bus_subscribed`
    /// in the context a call builds, `send_event` and `async with`), so
    /// that a status posted once the call is over reaches the room's hooks.
    /// The close's start ends the wait, the subscription left to the next
    /// activity: the call goes on under the close's rules, as one that did
    /// not wait.
    pub(crate) async fn subscribed(&self) {
        let mut closing = self.turns.cutting();
        tokio::select! {
            () = self.status_subscription.subscribe() => {}
            _ = closing.wait_for(|cut| *cut) => {}
        }
    }

    /// What the kit does with an entry posted on its bus (RFC §19.8): emit
    /// `status_posted`, then fire the `ON_STATUS_POSTED` hooks of the room
    /// its metadata names, none for a room the store lacks or once the kit
    /// is sealed: during its close too, as Python's, so that the end of a
    /// task the close cuts reaches them (RMK-1187).
    async fn on_status_posted(self: Arc<Self>, entry: StatusEntry) {
        let data = match serde_json::to_value(&entry) {
            Ok(Value::Object(data)) => data,
            _ => Metadata::new(),
        };
        let event = FrameworkEvent {
            r#type: "status_posted".to_owned(),
            room_id: None,
            channel_id: None,
            event_id: None,
            timestamp: self.environment.now(),
            data,
        };
        self.emit_framework(event).await;
        let Some(room_id) = entry
            .room_id()
            .and_then(|room_id| RoomId::new(room_id).ok())
        else {
            return;
        };
        if self.tasks.is_sealed() {
            return;
        }
        let Some(context) = self.observed_context(&room_id).await else {
            return;
        };
        let observers = self.observers(HookTrigger::OnStatusPosted, &room_id);
        self.observe(observers, entry, context, async {});
    }

    /// Fires `trigger`'s hooks on `event`, an event of its room no one
    /// stores (a delegation's `ON_TASK_DELEGATED`, `ON_TASK_COMPLETED`),
    /// as observers in the background, their filters read on its source
    /// (RFC §23.5).
    pub(super) async fn observe_event(self: &Arc<Self>, trigger: HookTrigger, event: RoomEvent) {
        let observers = self.event_observers(trigger, &event);
        if observers.is_empty() {
            return;
        }
        let Some(context) = self.observed_context(&event.room_id).await else {
            return;
        };
        self.observe(observers, event, context, async {});
    }

    /// Fires `trigger`'s hooks on `event` as [`observe_event`] does, its
    /// room's context read first whether a hook is registered or not, as
    /// Python's `_announce_task` builds it, its recent events only for a
    /// hook: a read that fails, or finds the room gone, is the caller's
    /// error, no hook fired.
    ///
    /// [`observe_event`]: Self::observe_event
    pub(super) async fn announce_event(
        self: &Arc<Self>,
        trigger: HookTrigger,
        event: RoomEvent,
    ) -> Result<(), RoomKitError> {
        let observers = self.event_observers(trigger, &event);
        let recent_events = if observers.is_empty() {
            self.config().recent_events()
        } else {
            self.config().recent_events().read_by_the_caller()
        };
        let context = driver::load_context(&*self.store, &event.room_id, recent_events).await?;
        let Some(context) = context else {
            return Err(RoomKitError::RoomNotFound(event.room_id));
        };
        if !observers.is_empty() {
            self.observe(observers, event, context, async {});
        }
        Ok(())
    }

    /// The hooks of `trigger` whose filters `event`, from its source,
    /// passes, the kit's and its room's.
    fn event_observers(&self, trigger: HookTrigger, event: &RoomEvent) -> Observers {
        let config = self.config();
        let handlers = read(&self.hooks);
        let subject = HookSubject::from(event).in_room(&event.room_id);
        config
            .hooks
            .observers(trigger, subject)
            .into_iter()
            .filter_map(|id| Some((config.hooks.get(id)?.clone(), handlers.get(&id)?.clone())))
            .collect()
    }

    /// Emits `event` to the kit's handlers and waits for them.
    async fn emit_framework(&self, event: FrameworkEvent) {
        if let Some(handled) = self.events.emit(&self.tasks, event) {
            handled.wait().await;
        }
    }

    /// The context the observers of a room's trigger read (RFC §10.1 step
    /// 3), as the Python implementation's `_build_context` builds it; `None`
    /// for a room the store lacks, or a store that fails the read.
    async fn observed_context(&self, room_id: &RoomId) -> Option<RoomContext> {
        let recent_events = self.config().recent_events().read_by_the_caller();
        match driver::load_context(&*self.store, room_id, recent_events).await {
            Ok(context) => context,
            Err(error) => {
                tracing::debug!(target: "roomkit::hooks", %room_id, %error, "no context for the room's observers");
                None
            }
        }
    }

    /// The hooks `trigger` runs as observers in `room_id`, the kit's and
    /// the room's, whatever their filter (Python's `skip_event_filter`).
    fn observers(&self, trigger: HookTrigger, room_id: &RoomId) -> Observers {
        let config = self.config();
        let handlers = read(&self.hooks);
        let system = ChannelId::system();
        let subject = HookSubject::of_channel(&system, &ChannelType::SYSTEM).in_room(room_id);
        config
            .hooks
            .observers(trigger, subject)
            .into_iter()
            .filter_map(|id| Some((config.hooks.get(id)?.clone(), handlers.get(&id)?.clone())))
            .collect()
    }

    /// Runs `observers` on `payload` in the background, together, each
    /// within its timeout, what it raises logged and a timeout reported as
    /// `hook_timeout` (RFC §9.3), at least as deep as the current work;
    /// `then` once they all ended.
    fn observe<P: ObservedPayload>(
        self: &Arc<Self>,
        observers: Observers,
        payload: P,
        context: RoomContext,
        then: impl Future<Output = ()> + Send + 'static,
    ) {
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        let inner = Arc::clone(self);
        let observing = async move {
            // Dropped with the task the kit's close cuts, the set aborts
            // the observers it still runs.
            let mut running = JoinSet::new();
            for (spec, kept) in observers {
                let Some(handler) = P::handler(&kept) else {
                    continue;
                };
                let body = hooks::run(
                    handler,
                    payload.clone(),
                    context.clone(),
                    spec.timeout,
                    |_| None,
                );
                let within = ExecutionContext::current().scope(body);
                running.spawn(async move { (spec, within.await) });
            }
            while let Some(ended) = running.join_next().await {
                match ended {
                    Ok((spec, outcome)) => inner.observed(&context.room.id, &spec, outcome).await,
                    Err(error) => {
                        tracing::warn!(target: "roomkit::hooks", %error, "an observer failed");
                    }
                }
            }
            then.await;
        };
        let task = ExecutionContext::default().at_least(floor).scope(observing);
        if self.tasks.spawn_alone(Kind::Observer, task).is_none() {
            tracing::debug!(target: "roomkit::hooks", "the kit is shut down: an observer is not run");
        }
    }

    /// Reports how the observer `spec` of `room_id` ended: a timeout as
    /// `hook_timeout`, an error logged.
    async fn observed<P>(&self, room_id: &RoomId, spec: &HookSpec, outcome: HookOutcome<P>) {
        match outcome {
            HookOutcome::TimedOut => {
                tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "an observer timed out");
                self.report_hook_timeout(room_id, spec).await;
            }
            HookOutcome::Raised(error) => {
                tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "an observer raised");
            }
            HookOutcome::Returned(_) | HookOutcome::Invalid { .. } => {}
        }
    }

    /// Emits `hook_timeout` for the observer `spec` of `room_id`, as the
    /// Python implementation's engine emits it.
    async fn report_hook_timeout(&self, room_id: &RoomId, spec: &HookSpec) {
        let mut data = Metadata::new();
        data.insert("hook_name".to_owned(), Value::from(spec.name.as_str()));
        data.insert(
            "trigger".to_owned(),
            serde_json::to_value(spec.trigger).unwrap_or_default(),
        );
        data.insert(
            "timeout".to_owned(),
            Value::from(spec.timeout.as_secs_f64()),
        );
        let event = FrameworkEvent {
            r#type: "hook_timeout".to_owned(),
            room_id: Some(room_id.clone()),
            channel_id: None,
            event_id: None,
            timestamp: self.environment.now(),
            data,
        };
        self.emit_framework(event).await;
    }
}

/// Waits on `future` once, with a waker that does nothing: whether it ended
/// at once.
#[cfg(test)]
fn ready_at_once(future: impl Future<Output = ()>) -> bool {
    let mut future = std::pin::pin!(future);
    let mut waking = Context::from_waker(Waker::noop());
    future.as_mut().poll(&mut waking).is_ready()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: core/mixins/helpers.py submit_feedback, `max(0.0, min(1.0,
    // rating))`.
    #[test]
    fn a_rating_is_clamped_as_python_clamps_it() {
        assert_eq!(
            [
                clamped(1.5),
                clamped(-0.2),
                clamped(0.42),
                clamped(f64::NAN)
            ],
            [1.0, 0.0, 0.42, 1.0]
        );
    }

    #[test]
    fn a_subscription_to_the_in_memory_bus_ends_at_once() {
        let bus = StatusBus::default();
        let callback: StatusCallback = Arc::new(|_entry| Box::pin(async {}));
        assert!(ready_at_once(async move { bus.subscribe(callback).await }));
    }
}

/// What subscribes the kit to its bus, made again for each try.
type Subscriber = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// The kit's subscription to its status bus, when it did not end at its
/// construction: the first room activity makes it and awaits it (Python's
/// `_ensure_status_bus_subscribed`, claimed before it subscribes, another
/// activity going on meanwhile). One that does not end, its call or its
/// runtime dropped first, is made again by the next activity, as Python
/// tries a failed one again.
#[derive(Default)]
pub(crate) struct PendingSubscription {
    waiting: Arc<AtomicBool>,
    subscriber: Mutex<Option<Subscriber>>,
    /// The subscription its construction started, polled once already.
    started: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl PendingSubscription {
    /// Keeps `subscriber`, and `started`, its first try, until a call is
    /// made in a runtime.
    fn keep(&self, subscriber: Subscriber, started: BoxFuture<'static, ()>) {
        *lock(&self.subscriber) = Some(subscriber);
        *lock(&self.started) = Some(started);
        self.waiting.store(true, Ordering::Release);
    }

    /// The subscription a call claims, once: its first try, else a new
    /// one; `None` when none waits.
    fn claim(&self) -> Option<Claimed> {
        if !self.waiting.swap(false, Ordering::AcqRel) {
            return None;
        }
        let subscribing = match lock(&self.started).take() {
            Some(started) => started,
            None => (lock(&self.subscriber).clone()?)(),
        };
        Some(Claimed {
            waiting: Arc::clone(&self.waiting),
            subscribing,
        })
    }

    /// Makes the subscription waiting and awaits it: nothing when none
    /// waits, or another activity claimed it.
    async fn subscribe(&self) {
        if let Some(claimed) = self.claim() {
            claimed.run().await;
        }
    }
}

/// A subscription a call claimed: waiting again unless it runs to its end.
struct Claimed {
    waiting: Arc<AtomicBool>,
    subscribing: BoxFuture<'static, ()>,
}

impl Claimed {
    /// Runs the subscription to its end; dropped before, it waits again
    /// for the next call.
    async fn run(self) {
        let mut again = Again(Some(self.waiting));
        self.subscribing.await;
        again.0 = None;
    }
}

/// Sets a subscription waiting again when dropped holding its flag.
struct Again(Option<Arc<AtomicBool>>);

impl Drop for Again {
    fn drop(&mut self) {
        if let Some(waiting) = self.0.take() {
            waiting.store(true, Ordering::Release);
        }
    }
}

/// `mutex`'s value, a poisoned one's included.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
