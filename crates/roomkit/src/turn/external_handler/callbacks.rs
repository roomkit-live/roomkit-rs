//! The four callbacks a text channel obtains from its executor (RFC §9.3).

mod verdict;
use super::{ExternalToolDecision, Notification, external_decision, note_report};
use crate::model::ChannelId;
use crate::turns::{EmitOptions, TurnFuture, TurnWiring};
use crate::{HookExecution, HookTrigger, RoomKitError, ToolCallEvent, ToolCallVerdict};
use std::sync::Arc;

type Claim = Arc<dyn Fn() -> bool + Send + Sync>;

/// The gate, verdict and observer callbacks of the executor that wired a channel.
#[derive(Clone, Debug)]
pub struct ToolHooks(TurnWiring);

impl TurnWiring {
    /// The tool-call callbacks a channel can bind to its own identifier.
    pub fn tools(&self) -> ToolHooks {
        ToolHooks(self.clone())
    }
}

impl ToolHooks {
    /// Bind the gate that allows, refuses or rewrites a tool's arguments.
    pub fn build_before_tool_call_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(ToolCallEvent) -> TurnFuture<'static, Result<ExternalToolDecision, RoomKitError>>
    + Send
    + Sync
    + use<> {
        let binding = Bound::new(self, channel_id);
        move |event| {
            let binding = binding.clone();
            Box::pin(async move {
                let Some(n) = binding
                    .notification(&event, HookTrigger::BeforeToolUse)
                    .await
                else {
                    return Ok(ExternalToolDecision::default());
                };
                if n.unreachable() {
                    return Ok(ExternalToolDecision {
                        allowed: false,
                        ..Default::default()
                    });
                }
                let chain = n.gate(event.clone()).await;
                if chain.ended.is_none() {
                    n.observe(&chain.current);
                }
                n.emit_result("before_tool_use", Notification::before_data(&event, &chain))
                    .await?;
                Ok(external_decision(chain, &event.arguments))
            })
        }
    }

    /// Bind the synchronous result chain and its observers. The optional claim
    /// suppresses a duplicate report without changing the model's verdict.
    pub fn build_tool_call_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(
        ToolCallEvent,
        Option<Claim>,
    ) -> TurnFuture<'static, Result<Option<ToolCallVerdict>, RoomKitError>>
    + Send
    + Sync
    + use<> {
        let binding = Bound::new(self, channel_id);
        move |event, claim| {
            let binding = binding.clone();
            Box::pin(async move {
                let Some(n) = binding.notification(&event, HookTrigger::OnToolCall).await else {
                    return Ok(None);
                };
                n.judge(event, claim.as_deref()).await
            })
        }
    }

    /// Bind the report for a tool run by another provider. The observers
    /// receive its original outcome even if a synchronous hook rewrites it.
    pub fn build_tool_report_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(ToolCallEvent) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync + use<>
    {
        Bound::new(self, channel_id).reporter(true)
    }

    /// Bind the observer for an outcome already decided, including a refusal.
    pub fn build_tool_observer_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(ToolCallEvent) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync + use<>
    {
        Bound::new(self, channel_id).reporter(false)
    }
}

#[derive(Clone)]
struct Bound {
    wiring: TurnWiring,
    channel: ChannelId,
}
impl Bound {
    fn new(tools: &ToolHooks, channel: &ChannelId) -> Self {
        Self {
            wiring: tools.0.clone(),
            channel: channel.clone(),
        }
    }

    async fn notification(
        &self,
        event: &ToolCallEvent,
        trigger: HookTrigger,
    ) -> Option<Notification> {
        let room = event.room_id.clone()?;
        let kit = Arc::clone(&self.wiring.kit);
        let hooks = kit.tool_hooks(trigger, (&room, &event.channel_id), &event.channel_type);
        Some(
            Notification::with_hooks(
                kit,
                self.wiring.clone(),
                room,
                self.channel.clone(),
                trigger,
                hooks,
            )
            .await,
        )
    }

    fn reporter(
        self,
        external: bool,
    ) -> impl Fn(ToolCallEvent) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync {
        move |event| {
            let binding = self.clone();
            Box::pin(async move {
                let claim = crate::current_tool_call_scope().and_then(|scope| {
                    scope.report_claim(&event.tool_call_id, binding.channel.as_str())
                });
                let Some(n) = binding.notification(&event, HookTrigger::OnToolCall).await else {
                    return Ok(());
                };
                if n.unreachable() && event.result.is_none() {
                    return Ok(());
                }
                if external {
                    if !event.cancelled && !event.refused {
                        n.report_chain(&event).await;
                    }
                    note_report(&event.tool_call_id);
                }
                if claim.map_or_else(crate::external_tools::claim_external_report, |right| {
                    right()
                }) {
                    n.tell(&event).await?;
                }
                Ok(())
            })
        }
    }
}

impl Notification {
    fn unreachable(&self) -> bool {
        !self.hooks.is_empty() && self.context.is_none()
    }

    async fn emit_result(
        &self,
        kind: &str,
        data: crate::model::Metadata,
    ) -> Result<(), RoomKitError> {
        self.wiring
            .emit(
                kind,
                EmitOptions {
                    room_id: Some(self.room.clone()),
                    channel_id: Some(self.channel.clone()),
                    data: Some(data),
                    ..Default::default()
                },
            )
            .await
    }

    async fn tell(&self, event: &ToolCallEvent) -> Result<(), RoomKitError> {
        self.observe(event);
        self.emit_result("tool_call", Self::completed_data(event))
            .await
    }

    async fn judge(
        self,
        event: ToolCallEvent,
        claim: Option<&(dyn Fn() -> bool + Send + Sync)>,
    ) -> Result<Option<ToolCallVerdict>, RoomKitError> {
        if self.unreachable() {
            let closed = self
                .hooks
                .iter()
                .find(|(spec, _)| spec.execution == HookExecution::Sync && spec.fail_closed);
            let (verdict, observed) = match closed {
                Some((spec, _)) => {
                    let (verdict, observed) =
                        verdict::withheld(event, &format!("hook_error:{}", spec.name));
                    (Some(verdict), observed)
                }
                None => (None, event),
            };
            if observed.result.is_some() && claim.is_none_or(|right| right()) {
                self.tell(&observed).await?;
            }
            return Ok(verdict);
        }
        let (verdict, observed) = self.decide(event).await;
        if observed.result.is_some() && claim.is_none_or(|right| right()) {
            // Once claimed, the observers and the emitter are still owed if the caller is dropped.
            tokio::spawn(crate::keep_context(
                async move { self.tell(&observed).await },
            ))
            .await
            .map_err(|error| RoomKitError::Runtime(error.to_string()))??;
        }
        Ok(Some(verdict))
    }
}
