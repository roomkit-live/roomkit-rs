//! The storage and helper behavior inherited by Python handler subclasses.

use roomkit::external_tools::{ExternalToolBridge, ExternalToolReport};
use roomkit::model::{Metadata, ToolCall};

use super::{
    FireBeforeHookOptions, FireOnToolHookOptions, OnToolCancelledOptions, OnToolRefusedOptions,
};

/// The raw verdict of the inherited approval helper, before policy wording.
/// Python exposes this through its protected helper as `BeforeToolDecision`.
#[doc(hidden)]
pub type BeforeToolDecision = roomkit::external_tools::ExternalToolDecision;

/// Embed this state in a custom [`super::ExternalToolHandler`] (RFC §9.3).
/// Its identity and callbacks are replaced together when a kit registers it.
#[derive(Debug, Default)]
pub struct ExternalToolHandlerState {
    bridge: ExternalToolBridge,
}

impl ExternalToolHandlerState {
    /// The channel that last registered this handler; empty before registration.
    pub fn channel_id(&self) -> String {
        self.bridge.channel_id()
    }

    /// The adapter's registration bridge, outside the application API.
    #[doc(hidden)]
    pub fn bridge(&self) -> ExternalToolBridge {
        self.bridge.clone()
    }

    /// Delegate an approval to `BEFORE_TOOL_USE` without registering a local tool execution.
    /// The decision preserves an explicit argument override, even if unchanged or empty.
    pub async fn fire_before_hook(
        &self,
        tool_name: &str,
        tool_input: &Metadata,
        options: FireBeforeHookOptions,
    ) -> BeforeToolDecision {
        let call = ToolCall::new(options.tool_call_id, tool_name, tool_input.clone());
        self.bridge.before(call, options.room_id).await
    }

    /// Delegate a provider's report. Hook rewrites cannot change the provider's actual result.
    pub async fn fire_on_tool_hook(
        &self,
        tool_name: &str,
        tool_input: &Metadata,
        result: &str,
        options: FireOnToolHookOptions,
    ) {
        let call = ToolCall::new(options.tool_call_id, tool_name, tool_input.clone());
        self.bridge
            .report(
                ExternalToolReport {
                    call,
                    result: result.to_owned(),
                    is_error: options.is_error,
                    cancelled: options.cancelled,
                    refused: options.refused,
                    error_detail: options.error_detail,
                    refused_but_ran: options.refused_but_ran,
                },
                options.room_id,
            )
            .await;
    }

    /// The default cancelled report, for a custom override delegating to its inherited behavior.
    pub async fn on_tool_cancelled(
        &self,
        tool_name: &str,
        tool_input: &Metadata,
        options: OnToolCancelledOptions,
    ) {
        let result = crate::dumps(&serde_json::json!({"error": "Tool call cancelled",
            "tool": tool_name, "hint": "The turn ended before its result."}));
        self.fire_on_tool_hook(
            tool_name,
            tool_input,
            &result,
            FireOnToolHookOptions {
                is_error: true,
                cancelled: true,
                tool_call_id: options.tool_call_id,
                room_id: options.room_id,
                ..FireOnToolHookOptions::default()
            },
        )
        .await;
    }

    /// The default refusal report, for an override delegating to inherited behavior.
    pub async fn on_tool_refused(
        &self,
        tool_name: &str,
        tool_input: &Metadata,
        reason: &str,
        options: OnToolRefusedOptions,
    ) {
        self.fire_on_tool_hook(
            tool_name,
            tool_input,
            reason,
            FireOnToolHookOptions {
                is_error: true,
                refused: true,
                tool_call_id: options.tool_call_id,
                room_id: options.room_id,
                error_detail: options.detail,
                ..FireOnToolHookOptions::default()
            },
        )
        .await;
    }
}
