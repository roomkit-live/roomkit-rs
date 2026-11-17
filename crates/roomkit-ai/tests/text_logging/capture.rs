use std::fmt::{Debug, Write as _};
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};

#[derive(Clone, Default)]
pub(super) struct Lines(Arc<Mutex<Vec<String>>>);
struct RecordLine<'a>(&'a mut String);
impl Visit for RecordLine<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        write!(self.0, " {}={value:?}", field.name()).unwrap();
    }
}
impl Subscriber for Lines {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut line = format!("{} {}", event.metadata().level(), event.metadata().target());
        event.record(&mut RecordLine(&mut line));
        self.0.lock().unwrap().push(line);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

impl Lines {
    pub(super) fn read(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}
