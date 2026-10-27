//! The warnings `tracing` emits while a test runs, for a test that checks
//! one is logged. A test crate includes it with `#[path]`.

use std::fmt::{Debug, Write as _};
use std::sync::{Arc, Mutex, PoisonError};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::{DefaultGuard, Interest};
use tracing::{Event, Level, Metadata as Callsite, Subscriber};

/// The warnings `tracing` emits on this thread while the guard lives, each
/// as its message and fields written out.
#[derive(Clone, Default)]
pub(crate) struct Warnings(Arc<Mutex<Vec<String>>>);

impl Warnings {
    pub(crate) fn capture() -> (Self, DefaultGuard) {
        let warnings = Self::default();
        let guard = tracing::subscriber::set_default(warnings.clone());
        (warnings, guard)
    }

    /// The warnings that contain `text`.
    pub(crate) fn containing(&self, text: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|warning| warning.contains(text))
            .cloned()
            .collect()
    }
}

struct Line<'line>(&'line mut String);

impl Visit for Line<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        write!(self.0, "{}={value:?} ", field.name()).unwrap();
    }
}

impl Subscriber for Warnings {
    fn register_callsite(&self, _callsite: &'static Callsite<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, callsite: &Callsite<'_>) -> bool {
        *callsite.level() == Level::WARN
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut line = String::new();
        event.record(&mut Line(&mut line));
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}
