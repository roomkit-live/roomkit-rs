//! The execution of one event's delivery set (RFC §10.2 step 3, §10.1 step
//! 14).
//!
//! Every target is called at once, so a driver may run them concurrently. A
//! transport target is told through `on_event()`, then delivered to when it
//! sends; an intelligence target is asked through `on_event()`, which is a
//! turn. Each target receives the event transcoded for its capabilities and
//! a context holding only what it may see (RFC §7.5 rule 8).

use std::collections::BTreeMap;

use super::stamp;
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, DeliveryError, DeliveryResult, DeliveryStatus,
    ErrorRecord, Metadata, Observation, ProviderResult, RoomContext, RoomEvent, Task,
};
use crate::ops::protocol::{EffectId, Notice, Outcome, Request};
use crate::ops::{Cx, Progress};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions::{self, DeliveryPlan};
use crate::rules::transcoding;

/// Which event a delivery set broadcasts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SetKind {
    /// The inbound event itself: its deliveries are reported (RFC §8.2).
    Root,
    /// A response that re-entered.
    Response,
    /// An event a hook injected. It asks no agent (RFC §9.4); its transport
    /// targets' tasks and observations are collected, as at any delivery,
    /// and the events they return do not re-enter (gap G-033).
    Injected,
}

/// The responses one target returned.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Response {
    pub(super) responder: ChannelBinding,
    pub(super) events: Vec<RoomEvent>,
}

/// What a finished delivery set produced.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct SetOutcome {
    /// The responses, in the order their channels answered.
    pub(super) responses: Vec<Response>,
    /// The intelligence channels not asked at the chain-depth limit.
    pub(super) not_asked: Vec<ChannelBinding>,
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    /// The transports delivered to, with their results.
    pub(super) delivery_results: BTreeMap<ChannelId, DeliveryResult>,
    /// The transports delivered to without failure, in plan order.
    pub(super) delivered: Vec<ChannelId>,
    /// The targets that failed, with what failed.
    pub(super) failures: BTreeMap<ChannelId, String>,
    /// How many targets the set had.
    pub(super) targets: usize,
    /// The merged response metadata of the intelligence targets (RFC §10.1).
    pub(super) response_metadata: Metadata,
    /// The first error an intelligence target's turn met.
    pub(super) error: Option<ErrorRecord>,
}

#[derive(Debug)]
struct Target {
    binding: ChannelBinding,
    role: Role,
    event: RoomEvent,
    context: Box<RoomContext>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Transport { delivers: bool },
    Agent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Call {
    OnEvent,
    Deliver,
}

/// The delivery set of one committed event.
#[derive(Debug)]
pub(super) struct DeliverySet {
    kind: SetKind,
    event: RoomEvent,
    context: Box<RoomContext>,
    targets: Vec<Target>,
    calls: BTreeMap<EffectId, (usize, Call)>,
    outcome: SetOutcome,
}

impl DeliverySet {
    /// The set of `event`, planned as `plan`, with the room's whole context.
    pub(super) fn new(
        kind: SetKind,
        event: RoomEvent,
        plan: DeliveryPlan,
        context: Box<RoomContext>,
    ) -> Self {
        let targets = targets_of(&plan, &event, &context);
        let outcome = SetOutcome {
            not_asked: plan.not_asked,
            targets: targets.len(),
            ..SetOutcome::default()
        };
        Self {
            kind,
            event,
            context,
            targets,
            calls: BTreeMap::new(),
            outcome,
        }
    }

    /// The event the set broadcasts.
    pub(super) fn event(&self) -> &RoomEvent {
        &self.event
    }

    /// The room's whole context at the commit.
    pub(super) fn context(&self) -> &RoomContext {
        &self.context
    }

    /// The kind of event the set broadcasts.
    pub(super) fn kind(&self) -> SetKind {
        self.kind
    }

    /// Calls every target's `on_event()` at once.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        for (index, target) in self.targets.iter().enumerate() {
            let effect = cx.io.issue(Request::OnEvent {
                binding: target.binding.clone(),
                event: target.event.clone(),
                context: target.context.clone(),
            });
            self.calls.insert(effect, (index, Call::OnEvent));
        }
        self.progress()
    }

    /// Takes the driver's answer to one of the set's calls.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<SetOutcome> {
        let Some((index, call)) = self.calls.remove(&effect) else {
            debug_assert!(
                false,
                "delivery set received {effect:?}, which it did not issue"
            );
            return self.progress();
        };
        match call {
            Call::OnEvent => match outcome.into_output() {
                Ok(output) => self.on_output(cx, index, *output),
                Err(error) => self.on_event_failed(cx, index, &error),
            },
            Call::Deliver => match outcome.into_output() {
                Ok(output) => self.on_delivered(index, *output),
                Err(error) => self.on_delivery_failed(index, &error),
            },
        }
        self.progress()
    }

    fn progress(&mut self) -> Progress<SetOutcome> {
        if self.calls.is_empty() {
            Progress::Done(std::mem::take(&mut self.outcome))
        } else {
            Progress::Waiting
        }
    }

    /// A target's `on_event()` returned (RFC §10.2 step 3c).
    fn on_output(&mut self, cx: &mut Cx<'_>, index: usize, output: ChannelOutput) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if self.kind == SetKind::Injected {
            self.outcome.tasks.extend(output.tasks);
            self.outcome.observations.extend(output.observations);
            self.deliver_if_transport(cx, index);
            return;
        }
        if !output.metadata_updates.is_empty() {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %self.event.room_id,
                channel_id = %target.binding.channel_id,
                "a channel returned room metadata updates, which this step does not apply yet"
            );
        }
        let responder = target.binding.clone();
        let is_agent = target.role == Role::Agent;
        self.outcome.tasks.extend(output.tasks);
        self.outcome.observations.extend(output.observations);
        if is_agent {
            self.outcome
                .response_metadata
                .extend(output.response_metadata);
            if let Some(error) = &output.error {
                self.turn_failed(cx, index, error);
            }
        }
        if output.responded && !output.response_events.is_empty() {
            self.outcome.responses.push(Response {
                responder,
                events: output.response_events,
            });
        }
        self.deliver_if_transport(cx, index);
    }

    /// A transport that sends is delivered to once told (RFC §10.2 step 3d).
    fn deliver_if_transport(&mut self, cx: &mut Cx<'_>, index: usize) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if target.role != (Role::Transport { delivers: true }) {
            return;
        }
        let effect = cx.io.issue(Request::Deliver {
            binding: target.binding.clone(),
            event: target.event.clone(),
            context: target.context.clone(),
        });
        self.calls.insert(effect, (index, Call::Deliver));
    }

    /// A target's `on_event()` raised: a turn that failed, or a transport
    /// that is not delivered to.
    fn on_event_failed(&mut self, cx: &mut Cx<'_>, index: usize, error: &ErrorRecord) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if self.kind == SetKind::Injected {
            return;
        }
        if target.role == Role::Agent {
            self.turn_failed(cx, index, error);
        } else {
            self.fail(index, error.message.clone());
        }
    }

    /// An intelligence target's turn failed (RFC §9.2 `ON_ERROR`, category
    /// `generation`): the event stays delivered, the error is reported.
    fn turn_failed(&mut self, cx: &mut Cx<'_>, index: usize, error: &ErrorRecord) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let id = cx.env.new_event_id();
        let now = cx.env.now();
        let payload = stamp::error_event(id, &target.binding, &self.event, error, now);
        let hooks = cx
            .config
            .hooks
            .observers(HookTrigger::OnError, &payload.source);
        if !hooks.is_empty() {
            cx.io.notify(Notice::AsyncHooks {
                trigger: HookTrigger::OnError,
                hooks,
                event: Box::new(payload),
                context: target.context.clone(),
            });
        }
        if self.outcome.error.is_none() {
            self.outcome.error = Some(error.clone());
        }
        self.fail(index, error.message.clone());
    }

    /// A transport's `deliver()` returned: sent, unless its provider refused.
    fn on_delivered(&mut self, index: usize, output: ChannelOutput) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let channel_id = target.binding.channel_id.clone();
        let (result, refusal) = delivery_result(channel_id.clone(), output.provider_result);
        self.outcome
            .delivery_results
            .insert(channel_id.clone(), result);
        self.outcome.tasks.extend(output.tasks);
        self.outcome.observations.extend(output.observations);
        match refusal {
            Some(message) => self.fail(index, message),
            None => self.outcome.delivered.push(channel_id),
        }
    }

    /// A transport's `deliver()` raised.
    fn on_delivery_failed(&mut self, index: usize, error: &ErrorRecord) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let channel_id = target.binding.channel_id.clone();
        self.outcome.delivery_results.insert(
            channel_id.clone(),
            DeliveryResult {
                channel_id,
                status: DeliveryStatus::Failed,
                provider_message_id: None,
                error: Some(DeliveryError {
                    code: error.error_type.clone(),
                    message: error.message.clone(),
                    retryable: false,
                }),
                retry_after: None,
                provider_result: None,
            },
        );
        self.fail(index, error.message.clone());
    }

    fn fail(&mut self, index: usize, message: String) {
        if let Some(target) = self.targets.get(index) {
            self.outcome
                .failures
                .insert(target.binding.channel_id.clone(), message);
        }
    }
}

/// The set's targets, transports first then agents, each with the event
/// transcoded for it and the context it may see.
fn targets_of(plan: &DeliveryPlan, event: &RoomEvent, context: &RoomContext) -> Vec<Target> {
    let transports = plan.transports.iter().map(|target| {
        let role = Role::Transport {
            delivers: target.delivers,
        };
        (&target.binding, role)
    });
    let agents = plan.agents.iter().map(|binding| (binding, Role::Agent));
    transports
        .chain(agents)
        .map(|(binding, role)| Target {
            event: stamp::for_target(
                event,
                transcoding::transcode(&event.content, &binding.capabilities),
            ),
            context: Box::new(permissions::context_for(binding, context)),
            binding: binding.clone(),
            role,
        })
        .collect()
}

/// The result of a delivery the transport returned from, with the
/// provider's refusal when it did not accept the message (RFC §5.13).
fn delivery_result(
    channel_id: ChannelId,
    provider_result: Option<ProviderResult>,
) -> (DeliveryResult, Option<String>) {
    let refusal = provider_result
        .as_ref()
        .filter(|result| !result.success)
        .map(|result| {
            result
                .error
                .clone()
                .unwrap_or_else(|| "the provider refused the message".to_owned())
        });
    let result = DeliveryResult {
        channel_id,
        status: if refusal.is_some() {
            DeliveryStatus::Failed
        } else {
            DeliveryStatus::Sent
        },
        provider_message_id: provider_result
            .as_ref()
            .and_then(|result| result.provider_message_id.clone()),
        error: refusal.clone().map(|message| DeliveryError {
            code: "provider_refused".to_owned(),
            message,
            retryable: false,
        }),
        retry_after: None,
        provider_result,
    };
    (result, refusal)
}
