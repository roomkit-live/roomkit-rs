//! Controls and observes provider-executed tools (RFC §6.4, §9.3).

mod options;
mod state;

use std::future::Future;
use std::pin::Pin;

use roomkit::model::Metadata;

use super::{ToolError, ToolPolicy, policy::call_admitted};
pub use options::{
    FireBeforeHookOptions, FireOnToolHookOptions, OnToolCancelledOptions, OnToolRefusedOptions,
    OnToolResultOptions, PolicyExternalToolHandlerOptions, ProcessToolCallOptions,
};
pub use state::{BeforeToolDecision, ExternalToolHandlerState};

/// A handler's asynchronous decision or report. Errors retain their class for observers.
pub type ExternalToolFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ToolError>> + Send + 'a>>;

/// Python's `ToolDecision`: an external call's approval and optional overrides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolDecision {
    /// Whether the provider may run the call.
    pub approved: bool,
    /// Replacement arguments; None leaves the original unchanged.
    pub modified_input: Option<Metadata>,
    /// Replacement result; ACP cannot apply either kind of override.
    pub result: Option<String>,
    /// A refusal's words for the model, empty by default.
    pub reason: String,
    /// What failed for observers alone, never included in the model's refusal.
    pub detail: Option<String>,
}

impl ToolDecision {
    /// The required approval, with Python's defaults for the other fields.
    pub fn new(approved: bool) -> Self {
        Self {
            approved,
            modified_input: None,
            result: None,
            reason: String::new(),
            detail: None,
        }
    }
}

/// A provider's permission and report integration (RFC §9.3).
///
/// Embed a default [`ExternalToolHandlerState`] and return it from `state`.
/// Custom decisions delegate to its `fire_before_hook`; custom reports to
/// `fire_on_tool_hook`. Registration supplies the channel identity and hooks;
/// construction does not start the handler's service.
pub trait ExternalToolHandler: Send + Sync + 'static {
    /// Storage for the identity and hook callbacks Python subclasses inherit.
    fn state(&self) -> &ExternalToolHandlerState;

    /// Decide before execution; this may wait for a human approval.
    fn process_tool_call<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision>;

    /// Report a provider's result, preserving its outcome and call id.
    fn on_tool_result<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()>;

    /// A call cut by its turn, before a result. The default reports cancellation.
    fn on_tool_cancelled<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.state()
                .on_tool_cancelled(tool_name, tool_input, options)
                .await;
            Ok(())
        })
    }

    /// A call refused before execution. Only asynchronous observers hear it.
    fn on_tool_refused<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        reason: &'a str,
        options: OnToolRefusedOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.state()
                .on_tool_refused(tool_name, tool_input, reason, options)
                .await;
            Ok(())
        })
    }

    /// The registered channel, empty before registration; read it when a call arrives.
    fn channel_id(&self) -> String {
        self.state().channel_id()
    }

    /// Start the handler's service. The default does nothing.
    fn start(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Release the handler's service. The default does nothing.
    fn stop(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// A policy gate followed by the kit's approval hooks (RFC §21.1, §9.3).
#[derive(Debug, Default)]
pub struct PolicyExternalToolHandler {
    state: ExternalToolHandlerState,
    policy: Option<ToolPolicy>,
}

impl PolicyExternalToolHandler {
    /// Python's `PolicyExternalToolHandler(policy=None)`.
    pub fn new(options: PolicyExternalToolHandlerOptions) -> Self {
        Self {
            state: ExternalToolHandlerState::default(),
            policy: options.policy,
        }
    }
}

impl ExternalToolHandler for PolicyExternalToolHandler {
    fn state(&self) -> &ExternalToolHandlerState {
        &self.state
    }

    fn process_tool_call<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            if self
                .policy
                .as_ref()
                .is_some_and(|policy| !call_admitted(policy, tool_name))
            {
                return Ok(ToolDecision {
                    reason: format!(
                        "Tool '{tool_name}' is not permitted by the agent's tool policy."
                    ),
                    ..ToolDecision::new(false)
                });
            }
            let before = self
                .state
                .fire_before_hook(
                    tool_name,
                    tool_input,
                    FireBeforeHookOptions {
                        tool_call_id: options.tool_call_id,
                        room_id: options.room_id,
                    },
                )
                .await;
            if before.allowed {
                return Ok(ToolDecision {
                    modified_input: before.arguments,
                    ..ToolDecision::new(true)
                });
            }
            if let Some(detail) = &before.detail {
                tracing::warn!(target: "roomkit_ai::tools", tool_name, %detail, "BEFORE_TOOL_USE refused the call");
            }
            Ok(ToolDecision {
                reason: before
                    .reason
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| format!("Tool '{tool_name}' denied by pre-execution hook.")),
                detail: before.detail,
                ..ToolDecision::new(false)
            })
        })
    }

    fn on_tool_result<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.state
                .fire_on_tool_hook(
                    tool_name,
                    tool_input,
                    result,
                    FireOnToolHookOptions {
                        is_error: options.is_error,
                        tool_call_id: options.tool_call_id,
                        room_id: options.room_id,
                        refused_but_ran: options.refused_but_ran,
                        error_detail: options.error_detail,
                        ..FireOnToolHookOptions::default()
                    },
                )
                .await;
            Ok(())
        })
    }
}
