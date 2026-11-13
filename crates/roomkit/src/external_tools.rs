//! Registration bridge for external tool handlers in adapter crates.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::channel::BoxFuture;
use crate::model::{Metadata, RoomId, ToolCall};

/// A permission decision that preserves the presence of an argument override.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ExternalToolDecision {
    /// Whether the call may run.
    pub allowed: bool,
    /// None leaves the provider's arguments unchanged.
    pub arguments: Option<Metadata>,
    /// A blocking hook's words for the provider.
    pub reason: Option<String>,
    /// A failing hook's error for observers alone.
    pub detail: Option<String>,
}

impl Default for ExternalToolDecision {
    fn default() -> Self {
        Self {
            allowed: true,
            arguments: None,
            reason: None,
            detail: None,
        }
    }
}

/// A handler report with the original independent Python flags.
#[doc(hidden)]
#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Python reports preserve four independent outcome flags"
)]
pub struct ExternalToolReport {
    /// The provider's call.
    pub call: ToolCall,
    /// The provider's result text.
    pub result: String,
    /// The provider reported a failure.
    pub is_error: bool,
    /// The turn cut the call.
    pub cancelled: bool,
    /// The handler refused the call.
    pub refused: bool,
    /// A rejected call still ran.
    pub refused_but_ran: bool,
    /// What failed, for observers alone.
    pub error_detail: Option<String>,
}

type Before =
    Arc<dyn Fn(ToolCall, Option<RoomId>) -> BoxFuture<'static, ExternalToolDecision> + Send + Sync>;
type Report =
    Arc<dyn Fn(ExternalToolReport, Option<RoomId>) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default)]
struct Wiring {
    channel_id: String,
    before: Option<Before>,
    report: Option<Report>,
}

/// The handler's registration identity and callbacks, shared without an adapter dependency.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct ExternalToolBridge(Arc<Mutex<Wiring>>);

impl ExternalToolBridge {
    /// The last channel that registered this handler, empty before registration.
    pub fn channel_id(&self) -> String {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .channel_id
            .clone()
    }

    pub(crate) fn wire(&self, channel_id: String, before: Before, report: Report) {
        let mut wiring = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !wiring.channel_id.is_empty() && wiring.channel_id != channel_id {
            tracing::warn!(target: "roomkit::framework", previous = %wiring.channel_id,
                %channel_id, "external tool handler already wired to another channel; using the last registration");
        }
        *wiring = Wiring {
            channel_id,
            before: Some(before),
            report: Some(report),
        };
    }

    /// Run the registered gate; an unwired handler has no hook to refuse a call.
    pub async fn before(&self, call: ToolCall, room_id: Option<RoomId>) -> ExternalToolDecision {
        let callback = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .before
            .clone();
        match callback {
            Some(callback) => callback(call, room_id).await,
            None => ExternalToolDecision::default(),
        }
    }

    /// Report through the kit that registered the handler, without holding its wiring lock.
    pub async fn report(&self, report: ExternalToolReport, room_id: Option<RoomId>) {
        let callback = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .report
            .clone();
        if let Some(callback) = callback {
            callback(report, room_id).await;
        }
    }
}

#[derive(Clone)]
struct ReportWatch {
    call_id: String,
    reached: Arc<AtomicBool>,
}

tokio::task_local! { static REPORT: Option<ReportWatch>; }

/// Run one handler report and tell whether it reached the framework's observers.
/// Each attempt owns its witness: reused ids and concurrent calls never share it.
#[doc(hidden)]
pub async fn watch_external_tool_report<F: Future>(call_id: &str, future: F) -> (F::Output, bool) {
    let reached = Arc::new(AtomicBool::new(false));
    let watch = ReportWatch {
        call_id: call_id.to_owned(),
        reached: Arc::clone(&reached),
    };
    let result = REPORT.scope(Some(watch), future).await;
    (result, reached.load(Ordering::Acquire))
}

pub(crate) fn note_report(call_id: &str) {
    let _outside = REPORT.try_with(|watch| {
        if let Some(watch) = watch
            && watch.call_id == call_id
        {
            watch.reached.store(true, Ordering::Release);
        }
    });
}

impl std::fmt::Debug for ExternalToolBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalToolBridge")
            .field("channel_id", &self.channel_id())
            .finish_non_exhaustive()
    }
}

/// Copy the report witness with the other context inherited by child tasks.
pub(crate) fn within_current_report<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let watch = REPORT.try_with(Clone::clone).ok().flatten();
    REPORT.scope(watch, future)
}
