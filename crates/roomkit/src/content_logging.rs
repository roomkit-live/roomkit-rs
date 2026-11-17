//! The process-wide content-log opt-in, independent of telemetry span data.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

fn state() -> &'static AtomicBool {
    static ENABLED: OnceLock<AtomicBool> = OnceLock::new();
    initialize(&ENABLED, || {
        std::env::var("ROOMKIT_LOG_CONTENT").unwrap_or_default()
    })
}

fn initialize(state: &OnceLock<AtomicBool>, read: impl FnOnce() -> String) -> &AtomicBool {
    state.get_or_init(|| {
        let value = read();
        AtomicBool::new(matches!(
            value.trim().to_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ))
    })
}

/// Whether ordinary content logs may contain raw payloads, off by default.
///
/// `ROOMKIT_LOG_CONTENT` enables this process-wide switch when its trimmed,
/// case-insensitive value is `1`, `true`, `yes` or `on`. Rust initializes it on
/// the first getter, setter or content-log access, the counterpart of Python's
/// module initialization. Later environment changes do not reread the switch.
/// DEBUG filtering still applies, independently of this opt-in. This does not
/// configure [`telemetry::TelemetryConfig::record_sensitive_data`](crate::telemetry::TelemetryConfig::record_sensitive_data).
#[must_use]
pub fn content_logging_enabled() -> bool {
    state().load(Ordering::Relaxed)
}

/// Enable or disable raw content in ordinary logs process-wide.
///
/// Overrides the initial `ROOMKIT_LOG_CONTENT` choice. It changes neither
/// application payloads nor telemetry span configuration; DEBUG logging still
/// needs to be enabled by the application's tracing subscriber.
pub fn set_content_logging(enabled: bool) {
    state().store(enabled, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{AtomicBool, OnceLock, Ordering, initialize};
    use std::cell::Cell;

    #[test]
    fn changed_environment_is_not_read_after_initialization() {
        let state = OnceLock::<AtomicBool>::new();
        assert!(initialize(&state, || "true".into()).load(Ordering::Relaxed));
        let reread = Cell::new(false);
        let retained = initialize(&state, || {
            reread.set(true);
            "false".into()
        });
        assert!(retained.load(Ordering::Relaxed));
        assert!(!reread.get());
    }
}
