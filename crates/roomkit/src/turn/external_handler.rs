//! Hook callbacks for registered handlers, including calls outside a turn.

mod callbacks;
mod ordinary;
mod tool_methods;
pub use callbacks::ToolHooks;

use super::{
    Arc, ChannelId, ChannelType, ConversationStore, GateChain, GateEnd, HookExecution, HookOutcome,
    HookSpec, HookTrigger, Inner, Metadata, RoomContext, RoomId, ToolCall, ToolCallChain,
    ToolCallEvent, ToolCallReport, TurnKit, Value, hooks,
};
use crate::external_tools::{
    ExternalToolBridge, ExternalToolDecision, ExternalToolReport, note_report,
};
use crate::hooks::HookHandler;

type Registered = Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)>;

impl<S: ConversationStore> Inner<S> {
    pub(crate) fn wire_external_tools(
        self: &Arc<Self>,
        channel: &ChannelId,
        bridge: &ExternalToolBridge,
        wiring: crate::turns::TurnWiring,
    ) {
        let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(self));
        let before_kit = Arc::clone(&kit);
        let before_telemetry = self.telemetry.clone();
        let report_telemetry = self.telemetry.clone();
        let before_wiring = wiring.clone();
        let registered = channel.clone();
        let report_channel = channel.clone();
        bridge.wire(
            channel.to_string(),
            Arc::new(move |call, room| {
                let wiring = before_wiring.clone();
                let kit = Arc::clone(&before_kit);
                let channel = registered.clone();
                Box::pin(crate::telemetry::within_kit(
                    &before_telemetry,
                    None,
                    async move {
                        let Some(room) = room else {
                            return ExternalToolDecision::default();
                        };
                        Notification::new(kit, wiring, room, channel, HookTrigger::BeforeToolUse)
                            .await
                            .before(call)
                            .await
                    },
                ))
            }),
            Arc::new(move |report, room| {
                let wiring = wiring.clone();
                let kit = Arc::clone(&kit);
                let channel = report_channel.clone();
                Box::pin(crate::telemetry::within_kit(
                    &report_telemetry,
                    None,
                    async move {
                        let Some(room) = room else {
                            return;
                        };
                        Notification::new(kit, wiring, room, channel, HookTrigger::OnToolCall)
                            .await
                            .report(report)
                            .await;
                    },
                ))
            }),
        );
    }
}

struct Notification {
    wiring: crate::turns::TurnWiring,
    kit: Arc<dyn TurnKit>,
    room: RoomId,
    channel: ChannelId,
    hooks: Registered,
    context: Option<RoomContext>,
}

impl Notification {
    async fn new(
        kit: Arc<dyn TurnKit>,
        wiring: crate::turns::TurnWiring,
        room: RoomId,
        channel: ChannelId,
        trigger: HookTrigger,
    ) -> Self {
        let hooks = kit.tool_hooks(trigger, (&room, &channel), &ChannelType::AI);
        Self {
            wiring,
            kit,
            room,
            channel,
            hooks,
            context: None,
        }
        .resolve(trigger, None)
        .await
    }

    async fn resolve(mut self, trigger: HookTrigger, carrying: Option<&RoomContext>) -> Self {
        if !self.hooks.is_empty() {
            self.context = self
                .wiring
                .hook_context(&self.room, trigger, carrying)
                .await;
        }
        self
    }

    async fn before(&self, call: ToolCall) -> ExternalToolDecision {
        let event = self.event(&ToolCallReport::new(call));
        match self.before_chain(&event).await {
            Some(chain) => external_decision(chain, &event.arguments),
            None => ExternalToolDecision {
                allowed: false,
                ..ExternalToolDecision::default()
            },
        }
    }

    async fn before_chain(&self, event: &ToolCallEvent) -> Option<GateChain> {
        if !self.hooks.is_empty() && self.context.is_none() {
            return None;
        }
        let chain = self.gate(event.clone()).await;
        if chain.ended.is_none() {
            self.observe(&chain.current);
        }
        self.emit("before_tool_use", Self::before_data(event, &chain))
            .await;
        Some(chain)
    }

    fn before_data(event: &ToolCallEvent, chain: &GateChain) -> Metadata {
        let (allowed, reason) = chain.verdict();
        Metadata::from_iter([
            ("tool_name".to_owned(), Value::from(event.name.as_str())),
            (
                "tool_call_id".to_owned(),
                Value::from(event.tool_call_id.as_str()),
            ),
            ("allowed".to_owned(), Value::Bool(allowed)),
            ("reason".to_owned(), reason.map_or(Value::Null, Value::from)),
        ])
    }

    async fn gate(&self, event: ToolCallEvent) -> GateChain {
        let mut chain = GateChain {
            current: event,
            rewritten: None,
            ended: None,
        };
        let Some(context) = &self.context else {
            return chain;
        };
        for (spec, handler) in self
            .hooks
            .iter()
            .filter(|(s, _)| s.execution == HookExecution::Sync)
        {
            let ran = hooks::run_traced(
                spec,
                Arc::clone(handler),
                chain.current.clone(),
                context.clone(),
                |_| None,
            )
            .await;
            self.timeout(spec, &ran);
            chain.ended = chain.read(spec, ran);
            if chain.ended.is_some() {
                break;
            }
        }
        chain
    }

    async fn report(&self, report: ExternalToolReport) {
        let mut event = self.event(&ToolCallReport::new(report.call));
        event.result = Some(Value::from(report.result));
        event.is_error = report.is_error;
        event.cancelled = report.cancelled;
        event.refused = report.refused;
        event.refused_but_ran = report.refused_but_ran;
        event.error_detail = report.error_detail;
        if !event.refused && !event.cancelled {
            self.report_chain(&event).await;
        }
        // The SYNC chain and context read may fail before this point. A
        // handler raising after delegation has already made this report.
        note_report(&event.tool_call_id);
        if !crate::external_tools::claim_external_report() {
            return;
        }
        self.observe_completed(&event).await;
    }

    async fn observe_completed(&self, event: &ToolCallEvent) {
        self.observe(event);
        self.emit("tool_call", Self::completed_data(event)).await;
    }

    fn completed_data(event: &ToolCallEvent) -> Metadata {
        let mut data = Metadata::from_iter([
            ("tool_name".to_owned(), Value::from(event.name.as_str())),
            (
                "tool_call_id".to_owned(),
                Value::from(event.tool_call_id.as_str()),
            ),
            (
                "channel_type".to_owned(),
                Value::from(event.channel_type.as_str()),
            ),
        ]);
        for (key, set) in [
            ("is_error", event.is_error),
            ("cancelled", event.cancelled),
            ("refused", event.refused),
            ("refused_but_ran", event.refused_but_ran),
        ] {
            if set {
                data.insert(key.to_owned(), Value::Bool(true));
            }
        }
        data
    }

    async fn report_chain(&self, event: &ToolCallEvent) {
        let Some(context) = &self.context else {
            return;
        };
        let mut chain = ToolCallChain::new(event.clone());
        let _blocked = chain
            .run(&self.hooks, context, |spec, ran| self.timeout(spec, ran))
            .await;
    }

    fn observe(&self, event: &ToolCallEvent) {
        let Some(context) = &self.context else {
            return;
        };
        for (spec, handler) in self
            .hooks
            .iter()
            .filter(|(s, _)| s.execution == HookExecution::Async)
        {
            let observing = hooks::observe_traced(
                spec.clone(),
                Arc::clone(handler),
                event.clone(),
                context.clone(),
            );
            let observing = crate::context::ExecutionContext::current().observing(0, observing);
            let (kit, room, channel, spec) = (
                Arc::clone(&self.kit),
                self.room.clone(),
                self.channel.clone(),
                spec.clone(),
            );
            let bounded = async move {
                let ran = observing.await;
                timeout(&kit, &room, &channel, &spec, &ran);
                if let HookOutcome::Raised(error) = ran {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "an external tool observer raised");
                }
            };
            // Context layers move their input by value. Keep the observer's
            // tracing state on the heap before passing through those layers.
            let task = crate::external_tools::observer_context(Box::pin(bounded));
            let _closed = self.kit.spawn_observer(task);
        }
    }

    fn event(&self, report: &ToolCallReport) -> ToolCallEvent {
        report.event(&self.channel, &ChannelType::AI, &self.room, self.kit.now())
    }

    async fn emit(&self, kind: &str, data: Metadata) {
        self.wiring
            .notify(kind, &self.room, &self.channel, data)
            .await;
    }

    fn timeout(&self, spec: &HookSpec, ran: &HookOutcome<ToolCallEvent>) {
        timeout(&self.kit, &self.room, &self.channel, spec, ran);
    }
}

fn external_decision(chain: GateChain, original: &Metadata) -> ExternalToolDecision {
    let mut decision = ExternalToolDecision::default();
    if chain
        .rewritten
        .as_ref()
        .is_some_and(|args| !args.is_object())
    {
        decision.allowed = false;
        return decision;
    }
    decision.arguments = chain
        .rewritten
        .and_then(|value| value.as_object().cloned())
        .or_else(|| (chain.current.arguments != *original).then_some(chain.current.arguments));
    match chain.ended {
        Some(GateEnd::Blocked(reason)) => {
            decision.allowed = false;
            decision.reason = reason;
        }
        Some(GateEnd::Failed { detail, .. }) => {
            decision.allowed = false;
            decision.detail = Some(detail);
        }
        None => {}
    }
    decision
}

fn timeout(
    kit: &Arc<dyn TurnKit>,
    room: &RoomId,
    channel: &ChannelId,
    spec: &HookSpec,
    ran: &HookOutcome<ToolCallEvent>,
) {
    if !matches!(ran, HookOutcome::TimedOut) {
        return;
    }
    let data = Metadata::from_iter([
        ("hook_name".to_owned(), Value::from(spec.name.as_str())),
        (
            "trigger".to_owned(),
            serde_json::to_value(spec.trigger).unwrap_or_default(),
        ),
        (
            "timeout".to_owned(),
            Value::from(spec.timeout.as_secs_f64()),
        ),
    ]);
    kit.emit(
        "hook_timeout".to_owned(),
        (Some(room.clone()), Some(channel.clone()), None),
        data,
    );
}
