//! The one log line a failed turn or task gets (RFC §15.2), as the Python
//! implementation's `roomkit.core._failure_log.log_failure` writes it.
//!
//! A provider's error is no code defect: it is logged at the level its
//! status calls for, with the provider and the status in the line (RFC
//! §15.3), or at DEBUG when the caller receives the error itself and owns
//! its line. A turn that ended before its answer is expected, a WARN. Any
//! other failure is unexpected, an ERROR, its line Python's, the error's
//! type and text in its fields where Python's traceback has them. The
//! component that raises leaves the line to the one that catches, so an
//! incident is logged once.

use roomkit_core::model::ErrorRecord;
use serde_json::Value;
use tracing::Level;

/// Where a failure's line goes: Python's logger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Log {
    /// The framework's, which a turn's stream logs under (Python's
    /// `roomkit.framework`).
    Framework,
    /// The tasks', which a delegated task logs under (Python's
    /// `roomkit.tasks`).
    Tasks,
}

/// The types of a turn that ended before its answer: an expected end,
/// never a defect (Python's `TurnCutShortError` and its subclass).
const CUT_SHORT: [&str; 2] = ["TurnCutShortError", "TaskCutShortError"];

/// Logs that `what` failed with `error`, once, at the level its cause calls
/// for; a provider's error at DEBUG when `caller_logs`, the caller
/// receiving it and owning its line (Python's `log_failure`).
pub(crate) fn log_failure(log: Log, error: &ErrorRecord, what: &str, caller_logs: bool) {
    let (level, line) = line_of(error, what, caller_logs);
    let (error_type, error) = (error.error_type.as_str(), error.message.as_str());
    macro_rules! emit {
        ($target:literal) => {
            match level {
                Level::ERROR => tracing::error!(target: $target, error_type, error, "{line}"),
                Level::WARN => tracing::warn!(target: $target, error_type, error, "{line}"),
                Level::INFO => tracing::info!(target: $target, error_type, error, "{line}"),
                Level::DEBUG => tracing::debug!(target: $target, error_type, error, "{line}"),
                Level::TRACE => tracing::trace!(target: $target, error_type, error, "{line}"),
            }
        };
    }
    match log {
        Log::Framework => emit!("roomkit::framework"),
        Log::Tasks => emit!("roomkit::tasks"),
    }
}

/// The level and the text of the line saying that `what` failed with
/// `error`.
fn line_of(error: &ErrorRecord, what: &str, caller_logs: bool) -> (Level, String) {
    if CUT_SHORT.contains(&error.error_type.as_str()) {
        return (Level::WARN, format!("{what} failed: {}", error.message));
    }
    let Some(provider) = error.attributes.get("provider") else {
        return (Level::ERROR, format!("{what} failed"));
    };
    let status = error.attributes.get("status_code").and_then(Value::as_u64);
    let level = if caller_logs {
        Level::DEBUG
    } else {
        provider_error_level(status)
    };
    let provider = provider.as_str().unwrap_or_default();
    let status = status.map_or_else(|| "None".to_owned(), |status| status.to_string());
    let line = format!(
        "{what} failed (provider={provider}, status={status}): {}",
        error.message
    );
    (level, line)
}

/// ERROR for a missing model (404) or a server fault (5xx), which need
/// someone to act; WARN for a transient: no status (a connection refused, a
/// timeout), a 429 or another 4xx (Python's `provider_error_level`).
fn provider_error_level(status: Option<u64>) -> Level {
    match status {
        Some(404 | 500..) => Level::ERROR,
        _ => Level::WARN,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::Metadata;
    use serde_json::json;

    use super::*;

    /// A provider's error with `status`, as roomkit-ai records one.
    fn provider_error(status: Option<u16>) -> ErrorRecord {
        let mut attributes = Metadata::new();
        attributes.insert("provider".to_owned(), json!("mock"));
        attributes.insert("status_code".to_owned(), json!(status));
        ErrorRecord {
            attributes: Box::new(attributes),
            ..ErrorRecord::new("ProviderError", "mock: model gone")
        }
    }

    #[test]
    fn a_missing_model_or_a_server_fault_is_an_error_and_a_transient_a_warning() {
        let levels: Vec<Level> = [Some(404), Some(500), Some(503), Some(429), Some(400), None]
            .into_iter()
            .map(|status| line_of(&provider_error(status), "turn", false).0)
            .collect();
        assert_eq!(
            levels,
            [
                Level::ERROR,
                Level::ERROR,
                Level::ERROR,
                Level::WARN,
                Level::WARN,
                Level::WARN
            ]
        );
    }

    #[test]
    fn a_provider_error_its_caller_receives_is_a_debug_line_naming_provider_and_status() {
        assert_eq!(
            line_of(&provider_error(Some(503)), "turn", true),
            (
                Level::DEBUG,
                "turn failed (provider=mock, status=503): mock: model gone".to_owned()
            )
        );
        assert_eq!(
            line_of(&provider_error(None), "turn", false).1,
            "turn failed (provider=mock, status=None): mock: model gone"
        );
    }

    #[test]
    fn a_cut_short_turn_is_a_warning_and_another_failure_an_error_whoever_reads_it() {
        let cut = ErrorRecord::new("TaskCutShortError", "ended max_rounds");
        let defect = ErrorRecord::new("RuntimeError", "bug");
        assert_eq!(
            (
                line_of(&cut, "Task t", true),
                line_of(&defect, "Task t", true)
            ),
            (
                (Level::WARN, "Task t failed: ended max_rounds".to_owned()),
                (Level::ERROR, "Task t failed".to_owned())
            )
        );
    }
}
