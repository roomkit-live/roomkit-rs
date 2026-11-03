//! Hook handlers: the host code behind a hook registration (RFC §9), what a
//! registration takes beside its trigger and its handler, and the payloads
//! a hook is handed.

use std::any::Any;
use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{
    ChannelId, ChannelType, DeliveryStatus, ErrorRecord, EventContent, EventDirection, EventType,
    HookAction, HookResult, Observation, RoomContext, RoomEvent, RoomId, Timestamp,
};
use roomkit_core::ops::{Environment, SystemEnvironment};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookFilter, HookSpec, HookTrigger};

use crate::orchestration::StatusEntry;
use crate::telemetry::{SpanKind, current_kit_telemetry, end_hook_span, end_observer_span};
use crate::tool_calls::ToolCallEvent;

/// What a hook's handler failed with. A SYNC hook that fails lets the event
/// through, unless the hook or its trigger fails closed (RFC §9.3).
pub type HookError = Box<dyn Error + Send + Sync>;

/// What a hook handler returns: Python's `HookResult | None`, or the error
/// it raised. `None` gives no result: a SYNC hook's chain reads it as an
/// unusable result, which lets the payload through unless the hook or its
/// trigger fails closed (RFC §9.3).
pub type HookReturn<P = RoomEvent> = Result<Option<HookResult<P>>, HookError>;

/// The future [`HookHandler::call`] returns: boxed and sendable, it gives
/// what the hook decided (`Box::pin(async move { ... })`).
pub type HookFuture<P = RoomEvent> = Pin<Box<dyn Future<Output = HookReturn<P>> + Send + 'static>>;

/// The host code of a hook: given the payload and the room, it decides.
///
/// A closure registers with [`RoomKit::hook`](crate::RoomKit::hook); a type
/// of its own implements this trait and registers with
/// [`RoomKit::hook_handler`](crate::RoomKit::hook_handler). What an ASYNC
/// hook returns is discarded.
pub trait HookHandler<P = RoomEvent>: Send + Sync + 'static {
    /// Runs the hook on `payload`.
    fn call(&self, payload: P, context: RoomContext) -> HookFuture<P>;
}

/// What a hook closure may return, turned into a [`HookReturn`]: a
/// `HookResult`, one or the error the hook raised, or `()`, Python's
/// `None`, which an observer returns.
pub trait IntoHookReturn<P>: Send + 'static {
    /// The return it stands for.
    ///
    /// # Errors
    ///
    /// The error the hook raised.
    fn into_hook_return(self) -> HookReturn<P>;
}

impl<P: Send + 'static> IntoHookReturn<P> for Result<HookResult<P>, HookError> {
    fn into_hook_return(self) -> HookReturn<P> {
        self.map(Some)
    }
}

impl<P: Send + 'static> IntoHookReturn<P> for Result<Option<HookResult<P>>, HookError> {
    fn into_hook_return(self) -> HookReturn<P> {
        self
    }
}

impl<P: Send + 'static> IntoHookReturn<P> for HookResult<P> {
    fn into_hook_return(self) -> HookReturn<P> {
        Ok(Some(self))
    }
}

impl<P: Send + 'static> IntoHookReturn<P> for () {
    fn into_hook_return(self) -> HookReturn<P> {
        Ok(None)
    }
}

/// A closure as a [`HookHandler`].
pub(crate) struct Closure<F>(pub(crate) F);

impl<P, F, Fut, R> HookHandler<P> for Closure<F>
where
    P: Send + 'static,
    F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoHookReturn<P>,
{
    fn call(&self, payload: P, context: RoomContext) -> HookFuture<P> {
        let running = (self.0)(payload, context);
        Box::pin(async move { running.await.into_hook_return() })
    }
}

/// What [`RoomKit::hook`](crate::RoomKit::hook) takes beside its trigger
/// and its handler: Python's `RoomKit.hook` keywords, with its defaults.
///
/// ```
/// use std::time::Duration;
/// use roomkit::{HookExecution, HookOptions};
///
/// // An approval that waits for a person needs more than 30 seconds.
/// let options = HookOptions {
///     name: "approve".to_owned(),
///     timeout: Duration::from_secs(300),
///     ..Default::default()
/// };
/// assert_eq!(options.execution, HookExecution::Sync);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookOptions {
    /// Python `execution=HookExecution.SYNC`: a SYNC hook decides in its
    /// trigger's chain, an ASYNC one observes after the fact.
    pub execution: HookExecution,
    /// Python `priority=0`: its place in its chain, lower first.
    pub priority: i32,
    /// Python `name=""`: unique among the kit's hooks; a hook given none is
    /// named `hook`, then `hook#2`, ...
    pub name: String,
    /// Python `timeout=30.0`: the longest it may run.
    pub timeout: Duration,
    /// Python `channel_types=None`: only payloads from these channel
    /// types.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Python `channel_ids=None`: only payloads from these channels.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Python `directions=None`: only events that travelled in these
    /// directions; any other payload passes.
    pub directions: Option<BTreeSet<EventDirection>>,
    /// Python `event_types=None`: only events of these types; any other
    /// payload passes.
    pub event_types: Option<BTreeSet<EventType>>,
    /// Python `fail_closed=False`: a SYNC hook whose outcome is unusable
    /// blocks instead of allowing (RFC §9.3).
    pub fail_closed: bool,
    /// Python `needs_lock=True`: `false` runs a SYNC `BEFORE_BROADCAST`
    /// hook off the room lock (RFC §9.5.1).
    pub needs_lock: bool,
}

impl Default for HookOptions {
    fn default() -> Self {
        let defaults = HookSpec::new("", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        Self {
            execution: defaults.execution,
            priority: defaults.priority,
            name: String::new(),
            timeout: defaults.timeout,
            channel_types: None,
            channel_ids: None,
            directions: None,
            event_types: None,
            fail_closed: defaults.fail_closed,
            needs_lock: defaults.needs_lock,
        }
    }
}

impl HookOptions {
    /// The registration of `r#fn` on `trigger` these options make.
    pub(crate) fn registration(self, trigger: HookTrigger, r#fn: Kept) -> HookRegistration {
        HookRegistration {
            priority: self.priority,
            name: self.name,
            timeout: self.timeout,
            channel_types: self.channel_types,
            channel_ids: self.channel_ids,
            directions: self.directions,
            event_types: self.event_types,
            fail_closed: self.fail_closed,
            needs_lock: self.needs_lock,
            ..HookRegistration::of(trigger, self.execution, r#fn)
        }
    }
}

/// A hook as the kit holds it (RFC §9.1): Python's
/// `HookRegistration(trigger, execution, fn, priority=0, name="",
/// timeout=30.0, channel_types=None, channel_ids=None, directions=None,
/// event_types=None, fail_closed=False, needs_lock=True)`, which
/// [`RoomKit::add_room_hook`](crate::RoomKit::add_room_hook) returns under
/// the name the kit gave it.
#[derive(Clone, Debug)]
pub struct HookRegistration {
    /// When it fires.
    pub trigger: HookTrigger,
    /// SYNC decides in its trigger's chain, ASYNC observes.
    pub execution: HookExecution,
    /// Its handler, by the payload its trigger hands it.
    pub r#fn: Kept,
    /// Python `priority=0`: its place in its chain, lower first.
    pub priority: i32,
    /// Python `name=""`: unique within its scope, the kit's hooks or one
    /// room's; a hook given none takes `hook`, then `hook#2`, ...
    pub name: String,
    /// Python `timeout=30.0`: the longest it may run.
    pub timeout: Duration,
    /// Python `channel_types=None`: only payloads from these channel types.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Python `channel_ids=None`: only payloads from these channels.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Python `directions=None`: only events that travelled in these
    /// directions.
    pub directions: Option<BTreeSet<EventDirection>>,
    /// Python `event_types=None`: only events of these types.
    pub event_types: Option<BTreeSet<EventType>>,
    /// Python `fail_closed=False`: a SYNC hook whose outcome is unusable
    /// blocks instead of allowing (RFC §9.3).
    pub fail_closed: bool,
    /// Python `needs_lock=True`: `false` runs a SYNC `BEFORE_BROADCAST`
    /// hook off the room lock (RFC §9.5.1).
    pub needs_lock: bool,
}

impl HookRegistration {
    /// The registration of the closure `r#fn` on `trigger`, run as
    /// `execution`, with Python's defaults for the rest; its payload, the
    /// type written on its argument, is its trigger's or the registration
    /// is refused (RFC §9.2), as [`RoomKit::hook`](crate::RoomKit::hook)'s.
    ///
    /// ```
    /// use roomkit::model::{RoomContext, RoomEvent};
    /// use roomkit::{HookExecution, HookRegistration, HookTrigger};
    ///
    /// let audit = HookRegistration {
    ///     name: "audit".to_owned(),
    ///     ..HookRegistration::new(
    ///         HookTrigger::AfterBroadcast,
    ///         HookExecution::Async,
    ///         |_event: RoomEvent, _context: RoomContext| async {},
    ///     )
    /// };
    /// assert_eq!(audit.priority, 0);
    /// ```
    pub fn new<P, F, Fut, R>(trigger: HookTrigger, execution: HookExecution, r#fn: F) -> Self
    where
        P: HookPayload,
        F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoHookReturn<P>,
    {
        let handler: Arc<dyn HookHandler<P>> = Arc::new(Closure(r#fn));
        Self::of(trigger, execution, P::kept(handler))
    }

    /// The registration of `r#fn` on `trigger`, run as `execution`, with
    /// Python's defaults for the rest.
    fn of(trigger: HookTrigger, execution: HookExecution, r#fn: Kept) -> Self {
        let defaults = HookSpec::new("", trigger, execution);
        Self {
            trigger,
            execution,
            r#fn,
            priority: defaults.priority,
            name: String::new(),
            timeout: defaults.timeout,
            channel_types: None,
            channel_ids: None,
            directions: None,
            event_types: None,
            fail_closed: defaults.fail_closed,
            needs_lock: defaults.needs_lock,
        }
    }

    /// What the kit's registry holds of the registration.
    pub(crate) fn spec(&self) -> HookSpec {
        let mut spec = HookSpec::new(self.name.clone(), self.trigger, self.execution);
        spec.priority = self.priority;
        spec.timeout = self.timeout;
        spec.fail_closed = self.fail_closed;
        spec.needs_lock = self.needs_lock;
        let mut filter = HookFilter::default();
        filter.channel_types.clone_from(&self.channel_types);
        filter.channel_ids.clone_from(&self.channel_ids);
        filter.directions.clone_from(&self.directions);
        filter.event_types.clone_from(&self.event_types);
        spec.filter = filter;
        spec
    }

    /// Whether its handler takes the payload its trigger hands (RFC §9.2).
    pub(crate) fn takes_its_payload(&self) -> bool {
        match self.r#fn {
            Kept::Event(_) => RoomEvent::accepts(self.trigger),
            Kept::ToolCall(_) => ToolCallEvent::accepts(self.trigger),
            Kept::Session(_) => SessionStartedEvent::accepts(self.trigger),
            Kept::Status(_) => DeliveryStatus::accepts(self.trigger),
            Kept::StatusEntry(_) => StatusEntry::accepts(self.trigger),
            Kept::Observation(_) => Observation::accepts(self.trigger),
            Kept::Input(_) => crate::human_input::PendingInputEvent::accepts(self.trigger),
            Kept::Turn(ref hook) => hook.trigger == self.trigger,
        }
    }

    /// The registration the kit holds as `spec`, of `r#fn`.
    pub(crate) fn held(spec: HookSpec, r#fn: Kept) -> Self {
        Self {
            trigger: spec.trigger,
            execution: spec.execution,
            r#fn,
            priority: spec.priority,
            name: spec.name,
            timeout: spec.timeout,
            channel_types: spec.filter.channel_types,
            channel_ids: spec.filter.channel_ids,
            directions: spec.filter.directions,
            event_types: spec.filter.event_types,
            fail_closed: spec.fail_closed,
            needs_lock: spec.needs_lock,
        }
    }
}

/// `ON_SESSION_STARTED`'s payload, Python's `SessionStartedEvent(room_id,
/// channel_id, channel_type, participant_id, session=None,
/// timestamp=<factory>)`: the inbound pipeline created a room for a first
/// message on a channel other than a voice one (RFC §10.1 step 2).
#[derive(Clone, Debug, PartialEq)]
pub struct SessionStartedEvent {
    /// The room created.
    pub room_id: RoomId,
    /// The channel the message came on.
    pub channel_id: ChannelId,
    /// Its type.
    pub channel_type: ChannelType,
    /// The message's sender.
    pub participant_id: String,
    /// When the session started.
    pub timestamp: Timestamp,
}

impl SessionStartedEvent {
    /// The session `participant_id` started on the channel `channel_id` of
    /// `channel_type` in `room_id`, now by the system clock, as Python's
    /// constructor defaults its timestamp.
    pub fn new(
        room_id: RoomId,
        channel_id: ChannelId,
        channel_type: ChannelType,
        participant_id: impl Into<String>,
    ) -> Self {
        Self {
            room_id,
            channel_id,
            channel_type,
            participant_id: participant_id.into(),
            timestamp: SystemEnvironment.now(),
        }
    }

    /// The session the framework's `session_started` payload reports, if it
    /// reports one.
    pub(crate) fn of(payload: &RoomEvent) -> Option<Self> {
        let EventContent::System(content) = &payload.content else {
            return None;
        };
        let text = |key: &str| content.data.get(key).and_then(serde_json::Value::as_str);
        Some(Self {
            room_id: payload.room_id.clone(),
            channel_id: ChannelId::new(text("channel_id")?).ok()?,
            channel_type: serde_json::from_value(content.data.get("channel_type")?.clone()).ok()?,
            participant_id: text("participant_id").unwrap_or_default().to_owned(),
            timestamp: payload.created_at,
        })
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for roomkit_core::model::RoomEvent {}
    impl Sealed for crate::tool_calls::ToolCallEvent {}
    impl Sealed for super::SessionStartedEvent {}
    impl Sealed for roomkit_core::model::DeliveryStatus {}
    impl Sealed for crate::orchestration::StatusEntry {}
    impl Sealed for roomkit_core::model::Observation {}
    impl Sealed for crate::human_input::PendingInputEvent {}
    impl<T: super::TurnPayload> Sealed for T {}
}

/// What a trigger hands its hooks (RFC §9.2): a [`RoomEvent`] for the
/// event and lifecycle triggers, a [`ToolCallEvent`] for `BEFORE_TOOL_USE`
/// and `ON_TOOL_CALL`, a [`SessionStartedEvent`] for `ON_SESSION_STARTED`,
/// a [`DeliveryStatus`] for `ON_DELIVERY_STATUS`, a [`StatusEntry`] for
/// `ON_STATUS_POSTED`, an [`Observation`] for `ON_FEEDBACK`, a
/// [`PendingInputEvent`](crate::human_input::PendingInputEvent) for
/// `ON_USER_INPUT_REQUIRED`. A
/// registration whose handler takes another payload than its trigger's is
/// refused.
pub trait HookPayload: sealed::Sealed + Clone + Send + Sync + 'static {
    /// Whether `trigger` hands this payload.
    #[doc(hidden)]
    fn accepts(trigger: HookTrigger) -> bool;

    /// `handler` as the kit keeps it.
    #[doc(hidden)]
    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept;
}

impl HookPayload for RoomEvent {
    fn accepts(trigger: HookTrigger) -> bool {
        !trigger.is_tool_trigger()
            && !matches!(
                trigger,
                HookTrigger::OnSessionStarted
                    | HookTrigger::OnDeliveryStatus
                    | HookTrigger::OnStatusPosted
                    | HookTrigger::OnFeedback
                    | HookTrigger::OnUserInputRequired
                    | HookTrigger::AfterToolRound
                    | HookTrigger::BeforeAiGeneration
                    | HookTrigger::OnAiResponse
            )
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Event(handler)
    }
}

impl HookPayload for ToolCallEvent {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger.is_tool_trigger()
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::ToolCall(handler)
    }
}

impl HookPayload for SessionStartedEvent {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == HookTrigger::OnSessionStarted
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Session(handler)
    }
}

impl HookPayload for DeliveryStatus {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == HookTrigger::OnDeliveryStatus
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Status(handler)
    }
}

impl HookPayload for StatusEntry {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == HookTrigger::OnStatusPosted
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::StatusEntry(handler)
    }
}

impl HookPayload for Observation {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == HookTrigger::OnFeedback
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Observation(handler)
    }
}

impl HookPayload for crate::human_input::PendingInputEvent {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == HookTrigger::OnUserInputRequired
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Input(handler)
    }
}

/// The payload of a trigger a turn fires whose type a crate beside the kit
/// defines: roomkit-ai's loop events, `AFTER_TOOL_ROUND`'s,
/// `BEFORE_AI_GENERATION`'s and `ON_AI_RESPONSE`'s (RFC §6.4). Its hooks
/// register as any hook's, and a turn runs them through its host.
#[doc(hidden)]
pub trait TurnPayload: Clone + Send + Sync + 'static {
    /// The trigger that hands it.
    const TRIGGER: HookTrigger;
}

impl<T: TurnPayload> HookPayload for T {
    fn accepts(trigger: HookTrigger) -> bool {
        trigger == T::TRIGGER
    }

    fn kept(handler: Arc<dyn HookHandler<Self>>) -> Kept {
        Kept::Turn(TurnHook {
            trigger: T::TRIGGER,
            handler: Arc::new(handler),
        })
    }
}

/// The hook of a [`TurnPayload`], its handler kept whatever its payload's
/// type, which the turn that runs it names again.
#[doc(hidden)]
#[derive(Clone)]
pub struct TurnHook {
    trigger: HookTrigger,
    /// An `Arc<dyn HookHandler<P>>` of the payload `P` of `trigger`.
    handler: Arc<dyn Any + Send + Sync>,
}

impl std::fmt::Debug for TurnHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnHook")
            .field("trigger", &self.trigger)
            .finish_non_exhaustive()
    }
}

impl TurnHook {
    /// Its handler, when its payload is `P`.
    pub(crate) fn handler<P: TurnPayload>(&self) -> Option<Arc<dyn HookHandler<P>>> {
        self.handler
            .downcast_ref::<Arc<dyn HookHandler<P>>>()
            .map(Arc::clone)
    }
}

/// A hook's handler as the kit keeps it, by its payload.
#[doc(hidden)]
#[derive(Clone)]
pub enum Kept {
    /// An event's or a lifecycle change's hook.
    Event(Arc<dyn HookHandler<RoomEvent>>),
    /// A tool call's hook.
    ToolCall(Arc<dyn HookHandler<ToolCallEvent>>),
    /// A session's start's hook.
    Session(Arc<dyn HookHandler<SessionStartedEvent>>),
    /// A delivery status's hook.
    Status(Arc<dyn HookHandler<DeliveryStatus>>),
    /// A status entry's hook.
    StatusEntry(Arc<dyn HookHandler<StatusEntry>>),
    /// A feedback's hook.
    Observation(Arc<dyn HookHandler<Observation>>),
    /// A human-input request's hook.
    Input(Arc<dyn HookHandler<crate::human_input::PendingInputEvent>>),
    /// The hook of a trigger of an AI channel's loop.
    Turn(TurnHook),
}

impl std::fmt::Debug for Kept {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Event(_) => "Kept::Event",
            Self::ToolCall(_) => "Kept::ToolCall",
            Self::Session(_) => "Kept::Session",
            Self::Status(_) => "Kept::Status",
            Self::StatusEntry(_) => "Kept::StatusEntry",
            Self::Observation(_) => "Kept::Observation",
            Self::Input(_) => "Kept::Input",
            Self::Turn(_) => "Kept::Turn",
        })
    }
}

/// The unusable result a hook that returned nothing gave, as Python
/// reports one that returned `None`.
const NO_RESULT: &str = "expected HookResult, got NoneType";

/// What a hook raised: a kit's error by its Python class (a loop its cascade's
/// cap stopped reads `ReentryLoopCapError`), as Python's `type(exc).__name__`;
/// any other `HookError`, a boxed error having no class name.
fn raised(error: &(dyn Error + Send + Sync + 'static)) -> ErrorRecord {
    let class = error
        .downcast_ref::<crate::RoomKitError>()
        .map_or_else(|| "HookError".to_owned(), crate::RoomKitError::class_name);
    ErrorRecord::new(class, error.to_string())
}

/// A sync hook's [`run`] under its span, Python's `hook.sync.<name>`, for a
/// hook a turn's host runs without the driver, in the kit's call.
pub(crate) async fn run_traced<P: Send + 'static>(
    spec: &HookSpec,
    handler: Arc<dyn HookHandler<P>>,
    payload: P,
    context: RoomContext,
    check: impl FnOnce(&mut HookResult<P>) -> Option<String>,
) -> HookOutcome<P> {
    let room_id = context.room.id.clone();
    let span = current_kit_telemetry().and_then(|telemetry| {
        telemetry.hook_span(SpanKind::HookSync, &spec.name, spec.trigger, &room_id)
    });
    let ran = run(handler, payload, context, spec.timeout, check).await;
    end_hook_span(span, &ran);
    ran
}

/// An observer's [`run`] under its span, Python's `hook.async.<name>`, for a
/// hook a turn's host or a task runs without the driver, in the kit's call.
pub(crate) async fn observe_traced<P: Send + 'static>(
    spec: HookSpec,
    handler: Arc<dyn HookHandler<P>>,
    payload: P,
    context: RoomContext,
) -> HookOutcome<P> {
    let room_id = context.room.id.clone();
    let span = current_kit_telemetry().and_then(|telemetry| {
        telemetry.hook_span(SpanKind::HookAsync, &spec.name, spec.trigger, &room_id)
    });
    let ran = run(handler, payload, context, spec.timeout, |_| None).await;
    end_observer_span(span, &ran);
    ran
}

/// Runs `handler` within `timeout` and reads what it produced as the core
/// takes it.
pub(crate) async fn run<P: Send + 'static>(
    handler: Arc<dyn HookHandler<P>>,
    payload: P,
    context: RoomContext,
    timeout: Duration,
    check: impl FnOnce(&mut HookResult<P>) -> Option<String>,
) -> HookOutcome<P> {
    match tokio::time::timeout(timeout, handler.call(payload, context)).await {
        Ok(Ok(Some(mut result))) => match check(&mut result) {
            // A content a struct literal built past its model's check: the
            // hook reads as raising the `ValueError` Python's model raises
            // when built.
            Some(refusal) => HookOutcome::Raised(ErrorRecord::new("ValueError", refusal)),
            None => HookOutcome::Returned(result),
        },
        Ok(Ok(None)) => HookOutcome::invalid(NO_RESULT),
        Ok(Err(error)) => HookOutcome::Raised(raised(&*error)),
        Err(_elapsed) => HookOutcome::TimedOut,
    }
}

/// Runs `kept`, a hook the core asked to run on `event`, within `timeout`:
/// an event's hook on the event, its content checked as its model checks
/// it; a session's start's hook on the session `event` reports, what it
/// returns discarded, as an observer's. A tool call's hook never runs on
/// an event.
pub(crate) async fn run_kept(
    kept: Kept,
    event: RoomEvent,
    context: RoomContext,
    timeout: Duration,
) -> HookOutcome<RoomEvent> {
    match kept {
        Kept::Event(handler) => run(handler, event, context, timeout, refused_content).await,
        Kept::Session(handler) => {
            let Some(session) = SessionStartedEvent::of(&event) else {
                return HookOutcome::invalid("the event reports no session's start");
            };
            match run(handler, session, context, timeout, |_| None).await {
                HookOutcome::Returned(_) => {
                    HookOutcome::Returned(HookResult::new(HookAction::Allow))
                }
                HookOutcome::Raised(error) => HookOutcome::Raised(error),
                HookOutcome::TimedOut => HookOutcome::TimedOut,
                HookOutcome::Invalid { what, .. } => HookOutcome::invalid(what),
            }
        }
        Kept::ToolCall(_) => HookOutcome::Raised(ErrorRecord::new(
            "HookError",
            "a tool call's hook does not run on an event",
        )),
        Kept::Status(_) => HookOutcome::Raised(ErrorRecord::new(
            "HookError",
            "a delivery status's hook does not run on an event",
        )),
        Kept::StatusEntry(_) | Kept::Observation(_) | Kept::Input(_) | Kept::Turn(_) => {
            HookOutcome::Raised(ErrorRecord::new(
                "HookError",
                "a hook whose payload is no event does not run on an event",
            ))
        }
    }
}

/// What refuses a content `result` carries, its replacement's or an
/// injected event's, as its model would have refused it when built; each
/// content kept is written as its constructor writes it.
pub(crate) fn refused_content(result: &mut HookResult) -> Option<String> {
    result
        .event
        .iter_mut()
        .chain(
            result
                .injected_events
                .iter_mut()
                .map(|injected| &mut injected.event),
        )
        .find_map(|event| event.check().err())
        .map(|refusal| refusal.to_string())
}
