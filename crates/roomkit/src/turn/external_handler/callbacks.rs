//! The four callbacks a text channel obtains from its executor (RFC §9.3).

mod verdict;
use super::{ExternalToolDecision, Notification, note_report};
use crate::model::ChannelId;
use crate::turns::{EmitOptions, TurnFuture, TurnWiring};
use crate::{HookExecution, RoomKitError, ToolCallEvent, ToolCallVerdict};
use std::sync::Arc;

type Claim = Arc<dyn Fn() -> bool + Send + Sync>;

/// The gate, verdict and observer callbacks of the executor that wired a channel.
#[derive(Clone, Debug)]
pub struct ToolHooks(pub(super) TurnWiring);

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
                let (decision, _) = binding
                    .tools
                    .decide_before_tool_use(&event, &binding.channel, None)
                    .await?;
                Ok(decision)
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
                binding
                    .tools
                    .judge_tool_call(event, &binding.channel, None, claim.as_deref())
                    .await
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
    tools: ToolHooks,
    channel: ChannelId,
}
impl Bound {
    fn new(tools: &ToolHooks, channel: &ChannelId) -> Self {
        Self {
            tools: tools.clone(),
            channel: channel.clone(),
        }
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
                let claim = || {
                    if external {
                        note_report(&event.tool_call_id);
                    }
                    claim
                        .as_ref()
                        .map_or_else(crate::external_tools::claim_external_report, |right| {
                            right()
                        })
                };
                if external {
                    binding
                        .tools
                        .report_tool_call(&event, &binding.channel, Some(&claim))
                        .await
                } else {
                    binding
                        .tools
                        .observe_failed_tool_call(&event, &binding.channel, Some(&claim))
                        .await
                }
            })
        }
    }
}

impl Notification {
    pub(super) fn unreachable(&self) -> bool {
        !self.hooks.is_empty() && self.context.is_none()
    }

    pub(super) async fn emit_result(
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
        ToolHooks(self.wiring.clone())
            .tell_tool_call(event, self.context.as_ref(), &self.channel)
            .await
    }

    pub(super) async fn judge(
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
