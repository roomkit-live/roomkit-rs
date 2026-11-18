//! The tool-hook operations available directly on an executor's wiring.
use super::HookOutcome;
use super::{ExternalToolDecision, Notification, ToolCallChain, ToolHooks, external_decision};
use crate::model::{ChannelId, HookAction, Metadata, RoomContext, RoomId};
use crate::turns::{EmitOptions, SyncPipelineResult};
use crate::{HookTrigger, RoomKitError, ToolCallEvent, ToolCallVerdict};
use std::sync::Arc;

type Claim<'a> = &'a (dyn Fn() -> bool + Send + Sync);

impl ToolHooks {
    /// Judge a served or unserved call. A refused claim suppresses its report alone.
    ///
    /// # Errors
    /// Propagates a failure to emit the call's framework event.
    pub async fn judge_tool_call(
        &self,
        event: ToolCallEvent,
        channel_id: &ChannelId,
        carrying: Option<&RoomContext>,
        claim: Option<Claim<'_>>,
    ) -> Result<Option<ToolCallVerdict>, RoomKitError> {
        let Some(room) = &event.room_id else {
            return Ok(None);
        };
        let n = self
            .select(&event, room, channel_id, HookTrigger::OnToolCall)
            .resolve(HookTrigger::OnToolCall, carrying)
            .await;
        n.judge(event, claim).await
    }

    /// Tell asynchronous observers and emit the outcome of an already claimed report.
    /// The supplied context is used directly.
    ///
    /// # Errors
    /// Propagates a failure to emit the call's framework event.
    pub async fn tell_tool_call(
        &self,
        observed: &ToolCallEvent,
        context: Option<&RoomContext>,
        channel_id: &ChannelId,
    ) -> Result<(), RoomKitError> {
        if let Some(context) = context {
            // The reference dispatches observers under str(event.room_id), including "None".
            let room = match &observed.room_id {
                Some(room) => room.clone(),
                None => RoomId::new("None")?,
            };
            let mut n = self.select(observed, &room, channel_id, HookTrigger::OnToolCall);
            n.context = Some(context.clone());
            n.observe(observed);
        }
        self.0
            .emit(
                "tool_call",
                EmitOptions {
                    room_id: observed.room_id.clone(),
                    channel_id: Some(channel_id.clone()),
                    data: Some(Notification::completed_data(observed)),
                    ..Default::default()
                },
            )
            .await
    }

    /// Run the synchronous `ON_TOOL_CALL` chain in the selected room, without reporting.
    /// Returns no outcome if a matching hook needs a context that cannot be read.
    pub async fn run_tool_call_chain(
        &self,
        event: ToolCallEvent,
        room_id: &RoomId,
        carrying: Option<&RoomContext>,
    ) -> Option<(SyncPipelineResult<ToolCallEvent>, Option<RoomContext>)> {
        let n = self
            .select(&event, room_id, &event.channel_id, HookTrigger::OnToolCall)
            .resolve(HookTrigger::OnToolCall, carrying)
            .await;
        if n.unreachable() {
            return None;
        }
        let mut result = SyncPipelineResult::default();
        let mut chain = ToolCallChain::new(event);
        if let Some(context) = &n.context {
            let blocked = chain
                .run(&n.hooks, context, |spec, ran| {
                    n.timeout(spec, ran);
                    if let HookOutcome::Returned(returned) = ran {
                        result.tasks.extend_from_slice(&returned.tasks);
                        result
                            .observations
                            .extend_from_slice(&returned.observations);
                        result.metadata.extend(returned.metadata.clone());
                        if returned.action == HookAction::Block {
                            result.allowed = false;
                            result.blocked_by = Some(spec.name.clone());
                            result.reason.clone_from(&returned.reason);
                        }
                    }
                })
                .await;
            if let Some(reason) = blocked
                && result.allowed
            {
                result.allowed = false;
                result.failed_closed = true;
                result.blocked_by = chain.errors.last().map(|(name, _)| name.clone());
                result.reason = Some(reason);
            }
        }
        result.hook_errors = chain
            .errors
            .into_iter()
            .map(|(hook, error)| {
                Metadata::from_iter([("hook".into(), hook.into()), ("error".into(), error.into())])
            })
            .collect();
        result.event = Some(chain.current);
        Some((result, n.context))
    }

    /// Report the original outcome of a provider's call, running synchronous hooks
    /// only if the call actually ran. A claim runs between the chain and its observers.
    ///
    /// # Errors
    /// Propagates a failure to emit the call's framework event.
    pub async fn report_tool_call(
        &self,
        event: &ToolCallEvent,
        channel_id: &ChannelId,
        claim: Option<Claim<'_>>,
    ) -> Result<(), RoomKitError> {
        self.report(event, channel_id, !event.cancelled && !event.refused, claim)
            .await
    }

    /// Notify asynchronous observers of a failed, refused or cancelled call.
    ///
    /// # Errors
    /// Propagates a failure to emit the call's framework event.
    pub async fn observe_failed_tool_call(
        &self,
        event: &ToolCallEvent,
        channel_id: &ChannelId,
        claim: Option<Claim<'_>>,
    ) -> Result<(), RoomKitError> {
        self.report(event, channel_id, false, claim).await
    }

    /// Return the gate's decision and the context on which it was based.
    ///
    /// # Errors
    /// Propagates a failure to emit the gate's framework event.
    pub async fn decide_before_tool_use(
        &self,
        event: &ToolCallEvent,
        channel_id: &ChannelId,
        carrying: Option<&RoomContext>,
    ) -> Result<(ExternalToolDecision, Option<RoomContext>), RoomKitError> {
        let Some(room) = &event.room_id else {
            return Ok((ExternalToolDecision::default(), None));
        };
        let n = self
            .select(event, room, channel_id, HookTrigger::BeforeToolUse)
            .resolve(HookTrigger::BeforeToolUse, carrying)
            .await;
        if n.unreachable() {
            return Ok((
                ExternalToolDecision {
                    allowed: false,
                    ..Default::default()
                },
                None,
            ));
        }
        let chain = n.gate(event.clone()).await;
        if chain.ended.is_none() {
            n.observe(&chain.current);
        }
        n.emit_result("before_tool_use", Notification::before_data(event, &chain))
            .await?;
        Ok((external_decision(chain, &event.arguments), n.context))
    }

    async fn report(
        &self,
        event: &ToolCallEvent,
        channel_id: &ChannelId,
        run_chain: bool,
        claim: Option<Claim<'_>>,
    ) -> Result<(), RoomKitError> {
        let Some(room) = &event.room_id else {
            return Ok(());
        };
        let n = self
            .select(event, room, channel_id, HookTrigger::OnToolCall)
            .resolve(HookTrigger::OnToolCall, None)
            .await;
        if n.unreachable() && event.result.is_none() {
            return Ok(());
        }
        if run_chain {
            n.report_chain(event).await;
        }
        if claim.is_none_or(|claim| claim()) {
            self.tell_tool_call(event, n.context.as_ref(), channel_id)
                .await?;
        }
        Ok(())
    }

    fn select(
        &self,
        event: &ToolCallEvent,
        room: &RoomId,
        channel: &ChannelId,
        trigger: HookTrigger,
    ) -> Notification {
        let kit = &self.0.kit;
        let hooks = kit.tool_hooks(trigger, (room, &event.channel_id), &event.channel_type);
        Notification {
            wiring: self.0.clone(),
            kit: Arc::clone(kit),
            room: room.clone(),
            channel: channel.clone(),
            hooks,
            context: None,
        }
    }
}
