//! One proactive delivery executed and reported, its hooks around it (RFC
//! §22.1, §22.3), as the Python implementation's `execute_delivery` runs
//! it: a request its form refuses is reported blocked, `BEFORE_DELIVER`
//! may block it or rewrite its content, the strategy delivers it, and
//! `AFTER_DELIVER` observes its actual outcome, whatever it is.

use std::sync::Arc;

use roomkit_core::model::{
    ChannelId, ChannelType, DeliveryError, EventContent, EventSource, EventStatus, EventType,
    HookAction, RoomContext, RoomEvent, TextContent, Visibility,
};
use roomkit_core::ops::FrameworkEvent as CoreEvent;
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookSpec, HookTrigger};
use roomkit_core::rules::{idempotency, publications};
use serde_json::Value;

use super::item::DeliveryItem;
use super::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use super::serialization::deserialize_strategy;
use super::strategy::{DeliveryContext, DeliveryStrategy};
use crate::driver;
use crate::error::RoomKitError;
use crate::hooks::{self, Kept};
use crate::kit::{HookEngine, Inner, RoomKit};
use crate::store::{ConversationStore, InMemoryStore};
use crate::tasks::Kind;

/// What [`execute_delivery`] takes beside the kit and the item: Python's
/// `execute_delivery(kit, item, *, strategy=None, hook_engine=None)`.
pub struct ExecuteDeliveryOptions<S = InMemoryStore> {
    /// Python `strategy=None`: the strategy that delivers the item; `None`
    /// rebuilds the one the item keeps, serialized.
    pub strategy: Option<Arc<dyn DeliveryStrategy<S>>>,
    /// Python `hook_engine=None`: the engine whose `BEFORE_DELIVER` and
    /// `AFTER_DELIVER` hooks run; `None` takes the kit's.
    pub hook_engine: Option<HookEngine<S>>,
}

impl<S> Default for ExecuteDeliveryOptions<S> {
    fn default() -> Self {
        Self {
            strategy: None,
            hook_engine: None,
        }
    }
}

impl<S> std::fmt::Debug for ExecuteDeliveryOptions<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecuteDeliveryOptions")
            .field(
                "strategy",
                &self.strategy.as_ref().map(|_| "DeliveryStrategy"),
            )
            .field("hook_engine", &self.hook_engine)
            .finish()
    }
}

impl DeliveryItem {
    /// `outcome`, the item's.
    fn owned(&self, outcome: DeliveryOutcome) -> DeliveryOutcome {
        DeliveryOutcome {
            delivery_item_id: Some(self.id.clone()),
            ..outcome
        }
    }
}

/// Executes `item` once through `kit` and reports its actual outcome, a
/// hook's refusal included (RFC §22.1, §22.3): Python's `execute_delivery`.
/// A request its form refuses is reported blocked; `BEFORE_DELIVER` may
/// block it or rewrite its content; the strategy, `options`' or the one
/// the item keeps, delivers it; `AFTER_DELIVER` observes the outcome,
/// written on the item. A strategy that reports no outcome leaves it
/// unknown; one that raises fails it.
pub async fn execute_delivery<S: ConversationStore>(
    kit: &RoomKit<S>,
    item: &mut DeliveryItem,
    options: ExecuteDeliveryOptions<S>,
) -> DeliveryOutcome {
    let hooks = options
        .hook_engine
        .as_ref()
        .map_or_else(|| kit.clone(), |engine| engine.kit().clone());
    item.outcome = None;
    if let Some(refusal) = reject_invalid(&hooks, item).await {
        return refusal;
    }
    let content = before_deliver(&hooks, item).await;
    let mut effective = item.clone();
    if let Some(content) = content {
        let strategy = options
            .strategy
            .unwrap_or_else(|| deserialize_strategy(&item.strategy));
        let delivery = DeliveryContext {
            content: content.clone(),
            channel_id: item.channel_id.clone(),
            metadata: item.metadata.clone(),
            addressed_to: item.addressed_to.clone(),
            idempotency_key: item.idempotency_key.clone(),
            session_id: item.session_id.clone(),
            chain_depth: item.chain_depth,
            instruction: item.instruction,
            ..DeliveryContext::new(kit.clone(), item.room_id.clone(), String::new())
        };
        let outcome = match strategy.deliver(delivery).await {
            Ok(Some(outcome)) => outcome,
            Ok(None) => DeliveryOutcome {
                reason: Some("strategy_outcome_unknown".to_owned()),
                ..DeliveryOutcome::new(DeliveryOutcomeStatus::Unknown)
            },
            Err(error) => strategy_failed(&error),
        };
        item.outcome = Some(item.owned(outcome));
        effective.content = content;
    }
    effective.outcome.clone_from(&item.outcome);
    after_deliver(&hooks, &effective).await;
    item.outcome
        .clone()
        .unwrap_or_else(|| item.owned(DeliveryOutcome::new(DeliveryOutcomeStatus::Unknown)))
}

/// A strategy that raised `error`: its execution failed, its class and
/// message reported.
pub(crate) fn strategy_failed(error: &RoomKitError) -> DeliveryOutcome {
    let error = DeliveryError::new(error.class_name(), error.to_string());
    DeliveryOutcome::failed("strategy_failed", error)
}

/// The refusal of a request its form makes invalid, reported to
/// `AFTER_DELIVER` before anything is enqueued or executed: a session named
/// with no channel or with an address, a blank key, which names no
/// publication (RFC §22.1, §13.4).
pub(crate) async fn reject_invalid<S: ConversationStore>(
    kit: &RoomKit<S>,
    item: &mut DeliveryItem,
) -> Option<DeliveryOutcome> {
    let request = &*item;
    let reason = if request.session_id.is_some()
        && (request.channel_id.is_none() || request.addressed_to.is_some())
    {
        "invalid_session_target"
    } else if request
        .idempotency_key
        .as_deref()
        .is_some_and(|key| !idempotency::is_key(key))
    {
        "empty_idempotency_key"
    } else {
        return None;
    };
    item.outcome = Some(item.owned(DeliveryOutcome::blocked(reason)));
    after_deliver(kit, item).await;
    item.outcome.clone()
}

/// Runs `BEFORE_DELIVER` on `item` (RFC §22.3): the content to deliver, as
/// its hooks left it, or `None` once a hook blocked it, its outcome set.
/// The trigger fails open: a hook that raises, times out or returns
/// nothing lets the delivery through unless it declared `fail_closed`, and
/// hooks that cannot run deliver it as it is.
async fn before_deliver<S: ConversationStore>(
    kit: &RoomKit<S>,
    item: &mut DeliveryItem,
) -> Option<String> {
    let trigger = HookTrigger::BeforeDeliver;
    let inner = &kit.inner;
    if !inner.config().hooks.has_hooks(Some(trigger)) {
        return Some(item.content.clone());
    }
    let event = hook_event(inner, item, trigger);
    let Some(context) = hook_context(inner, item, trigger).await else {
        return Some(item.content.clone());
    };
    let ended = run_chain(inner, event, &context).await;
    if let Some((reason, blocked_by)) = ended.blocked {
        let reason = reason
            .or(blocked_by)
            .unwrap_or_else(|| "before_deliver_blocked".to_owned());
        item.outcome = Some(item.owned(DeliveryOutcome::blocked(reason)));
        return None;
    }
    start_observers(inner, trigger, &ended.event, &context);
    // A rewrite to a content other than a text leaves the delivery's own.
    Some(text_of(&ended.event.content).unwrap_or_else(|| item.content.clone()))
}

/// Starts `AFTER_DELIVER`'s hooks on `item`'s outcome (RFC §22.3), in the
/// background: what they do never changes the outcome.
async fn after_deliver<S: ConversationStore>(kit: &RoomKit<S>, item: &DeliveryItem) {
    let trigger = HookTrigger::AfterDeliver;
    let inner = &kit.inner;
    if !inner.config().hooks.has_hooks(Some(trigger)) {
        return;
    }
    let event = hook_event(inner, item, trigger);
    if let Some(context) = hook_context(inner, item, trigger).await {
        start_observers(inner, trigger, &event, &context);
    }
}

/// The room's context for `item`'s hooks; `None`, logged, when it cannot
/// be read.
async fn hook_context<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    item: &DeliveryItem,
    trigger: HookTrigger,
) -> Option<RoomContext> {
    let room_id = &item.room_id;
    let recent = inner.config().recent_events();
    match driver::load_context(&*inner.store, room_id, recent).await {
        Ok(Some(context)) => Some(context),
        Ok(None) => {
            tracing::warn!(target: "roomkit::delivery", item = %item.id, ?trigger, "the delivery's room is not found: its hooks do not run");
            None
        }
        Err(error) => {
            tracing::warn!(target: "roomkit::delivery", item = %item.id, ?trigger, %error, "the delivery's hooks could not run");
            None
        }
    }
}

/// The event the delivery hooks are handed (RFC §22.3), as the Python
/// implementation's `build_delivery_hook_event` writes it: the framework's,
/// visible to no member (`internal`), an `INSTRUCTION` for an instruction;
/// its content, address and key the delivery's, or for `AFTER_DELIVER` the
/// publication's it reports, the key the application's; its status the
/// outcome's (`PENDING` before it); its metadata the delivery's, with its
/// item's id, its session, its channel, its strategy and, after it, its
/// outcome and its error.
fn hook_event<S>(inner: &Arc<Inner<S>>, item: &DeliveryItem, trigger: HookTrigger) -> RoomEvent {
    let request = item;
    let after = trigger == HookTrigger::AfterDeliver;
    let published = item
        .outcome
        .as_ref()
        .filter(|_| after)
        .and_then(|outcome| outcome.inbound.as_ref())
        .and_then(|inbound| inbound.event.as_ref());
    let mut metadata = request.metadata.clone();
    metadata.insert("delivery_item_id".to_owned(), Value::from(item.id.clone()));
    metadata.insert(
        "session_id".to_owned(),
        Value::from(request.session_id.clone()),
    );
    let mut status = EventStatus::Pending;
    if after {
        let (reported, error) = reported(item.outcome.as_ref());
        status = reported;
        if let Some(outcome) = &item.outcome {
            let written = serde_json::to_value(outcome).unwrap_or_default();
            metadata.insert("delivery_outcome".to_owned(), written);
        }
        metadata.insert("error".to_owned(), Value::from(error));
    }
    let channel_id = request.channel_id.as_ref().map(ToString::to_string);
    metadata.insert("channel_id".to_owned(), Value::from(channel_id));
    metadata.insert("strategy".to_owned(), Value::from(item.strategy_name()));
    let content = published
        .and_then(|event| text_of(&event.content))
        .unwrap_or_else(|| request.content.clone());
    let environment = inner.environment();
    let source = EventSource::new(ChannelId::system(), ChannelType::SYSTEM);
    let r#type = if request.instruction {
        EventType::Instruction
    } else {
        EventType::Message
    };
    let id = environment.new_event_id();
    let now = environment.now();
    let content = TextContent::new(content);
    RoomEvent {
        status,
        visibility: Visibility::Internal,
        addressed_to: match published {
            Some(event) => event.addressed_to.clone(),
            None => request.addressed_to.clone(),
        },
        idempotency_key: match published {
            Some(event) => event
                .idempotency_key
                .as_deref()
                .map(|key| publications::application_key(key).to_owned()),
            None => request.idempotency_key.clone(),
        },
        metadata,
        ..RoomEvent::at(id, request.room_id.clone(), r#type, source, content, now)
    }
}

/// The body of `content` when it is a text, as Python reads a
/// `TextContent`.
fn text_of(content: &EventContent) -> Option<String> {
    if let EventContent::Text(text) = content {
        return Some(text.body.clone());
    }
    None
}

/// The status an `AFTER_DELIVER` event reports for `outcome`, and its
/// error: the outcome's error message, else its reason, for an
/// unsuccessful one; `None` for one with neither, as Python writes null.
fn reported(outcome: Option<&DeliveryOutcome>) -> (EventStatus, Option<String>) {
    let Some(outcome) = outcome else {
        return (EventStatus::Delivered, None);
    };
    let status = match outcome.status {
        DeliveryOutcomeStatus::Sent => EventStatus::Delivered,
        DeliveryOutcomeStatus::Blocked => EventStatus::Blocked,
        DeliveryOutcomeStatus::Unavailable | DeliveryOutcomeStatus::Failed => EventStatus::Failed,
        DeliveryOutcomeStatus::Queued | DeliveryOutcomeStatus::Unknown => EventStatus::Pending,
    };
    let error = outcome
        .unsuccessful()
        .then(|| {
            outcome
                .error
                .as_ref()
                .map(|error| error.message.clone())
                .or_else(|| outcome.reason.clone())
        })
        .flatten();
    (status, error)
}

/// Where a SYNC chain of `BEFORE_DELIVER` hooks ended.
struct ChainEnd {
    /// The event as its last modify left it.
    event: RoomEvent,
    /// Its block, `(reason, blocked_by)`, when a hook blocked it.
    blocked: Option<(Option<String>, Option<String>)>,
}

/// Runs the SYNC `BEFORE_DELIVER` hooks of `event`'s room on it, in chain
/// order (RFC §9.3, §9.4), as the Python implementation's
/// `run_sync_hooks`: a block stops the chain, a modify hands its event to
/// the next hook, and a hook that produced no usable result is skipped
/// unless it declared `fail_closed`, which blocks naming it. A hook that
/// runs past its timeout is reported (RFC §8.2 `hook_timeout`).
async fn run_chain<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    event: RoomEvent,
    context: &RoomContext,
) -> ChainEnd {
    let trigger = HookTrigger::BeforeDeliver;
    let chain = inner.config().hooks.chain(trigger, &event);
    let mut current = event;
    for hook in chain {
        let Some((Kept::Event(handler), spec)) = inner.hook_with_spec(hook) else {
            continue;
        };
        let ran = hooks::run(
            handler,
            current.clone(),
            context.clone(),
            spec.timeout,
            hooks::refused_content,
        )
        .await;
        let problem = match ran {
            HookOutcome::Returned(result) => match (result.action, result.event) {
                (HookAction::Block, _) => {
                    let blocked = (result.reason, Some(spec.name));
                    return ChainEnd {
                        event: current,
                        blocked: Some(blocked),
                    };
                }
                (HookAction::Modify, Some(modified)) => {
                    current = modified;
                    continue;
                }
                (HookAction::Modify, None) => "hook_invalid_result",
                (HookAction::Allow, _) => continue,
            },
            HookOutcome::Raised(_) => "hook_error",
            HookOutcome::Invalid { .. } => "hook_invalid_result",
            HookOutcome::TimedOut => {
                report_timeout(inner, &spec, &current);
                "hook_timeout"
            }
        };
        if spec.fail_closed {
            let reason = format!("{problem}:{}", spec.name);
            return ChainEnd {
                event: current,
                blocked: Some((Some(reason), Some(spec.name))),
            };
        }
        tracing::warn!(target: "roomkit::delivery", hook = %spec.name, %problem, "a BEFORE_DELIVER hook produced no usable result: the delivery goes on");
    }
    ChainEnd {
        event: current,
        blocked: None,
    }
}

/// Emits the `hook_timeout` framework event of the hook `spec`, which ran
/// past its timeout on `event` (RFC §8.2).
fn report_timeout<S>(inner: &Arc<Inner<S>>, spec: &HookSpec, event: &RoomEvent) {
    let timeout = CoreEvent::hook_timeout(
        event.room_id.clone(),
        spec.name.clone(),
        spec.trigger,
        spec.timeout.as_secs_f64(),
    );
    drop(inner.emit(timeout));
}

/// Starts the observers `trigger` runs on `event` in its room, each in the
/// background within its timeout (RFC §9.2): its ASYNC hooks, every hook of
/// an ASYNC trigger. Once the kit is shut down, none runs.
fn start_observers<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    trigger: HookTrigger,
    event: &RoomEvent,
    context: &RoomContext,
) {
    for hook in inner.config().hooks.observers(trigger, event) {
        let Some((kept, spec)) = inner.hook_with_spec(hook) else {
            continue;
        };
        if !matches!(kept, Kept::Event(_)) {
            continue;
        }
        let observer = Arc::clone(inner);
        let (event, context) = (event.clone(), context.clone());
        let observing = async move {
            let room_id = event.room_id.clone();
            let outcome = hooks::run_kept(kept, event, context, spec.timeout).await;
            observer.report_observer(&spec, trigger, room_id, &outcome);
        };
        if inner.tasks.spawn_alone(Kind::Observer, observing).is_none() {
            tracing::debug!(target: "roomkit::hooks", ?trigger, "the kit is shut down: a delivery's hook is not run");
        }
    }
}
