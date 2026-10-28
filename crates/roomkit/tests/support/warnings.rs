//! The warnings and errors `tracing` emits while a test runs, for a test
//! that checks one is logged. A test crate includes it with `#[path]`.
//!
//! One subscriber is the test binary's global default, installed by the
//! first capture, and keeps the lines of the thread that captures, while
//! its guard lives. A subscriber set for one thread misses events: the
//! interest a callsite caches is computed again whenever one comes or goes,
//! and races the same callsite firing in another test's thread.

use std::cell::RefCell;
use std::fmt::{Debug, Write as _};
use std::sync::{Arc, Mutex, Once, PoisonError};

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata as Callsite, Subscriber};

thread_local! {
    /// Where the lines of this thread go while it captures.
    static CAPTURING: RefCell<Option<Warnings>> = const { RefCell::new(None) };
}

/// The warnings and errors `tracing` emits on this thread while the guard
/// lives, each as its level, then its message and fields written out.
#[derive(Clone, Default)]
pub(crate) struct Warnings(Arc<Mutex<Vec<String>>>);

/// Captures this thread's lines while it lives.
pub(crate) struct Capturing(());

impl Warnings {
    pub(crate) fn capture() -> (Self, Capturing) {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| tracing::subscriber::set_global_default(Sink).unwrap());
        let warnings = Self::default();
        CAPTURING.with(|capturing| *capturing.borrow_mut() = Some(warnings.clone()));
        (warnings, Capturing(()))
    }

    /// The lines that contain `text`.
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

impl Drop for Capturing {
    fn drop(&mut self) {
        CAPTURING.with(|capturing| capturing.borrow_mut().take());
    }
}

struct Line<'line>(&'line mut String);

impl Visit for Line<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        write!(self.0, "{}={value:?} ", field.name()).unwrap();
    }
}

/// The binary's subscriber: every warning and error, kept by the thread
/// that emits it when it captures.
struct Sink;

impl Subscriber for Sink {
    fn register_callsite(&self, callsite: &'static Callsite<'static>) -> Interest {
        if *callsite.level() <= Level::WARN {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::WARN)
    }

    fn enabled(&self, callsite: &Callsite<'_>) -> bool {
        *callsite.level() <= Level::WARN
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        CAPTURING.with(|capturing| {
            let Some(warnings) = &*capturing.borrow() else {
                return;
            };
            let mut line = format!("{} ", event.metadata().level());
            event.record(&mut Line(&mut line));
            warnings
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(line);
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}
