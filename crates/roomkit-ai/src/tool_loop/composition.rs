//! A generation's tool arguments as sizes on the realtime bus, never their
//! content (RFC §6.4). Python's `_ToolCallDeltaCoalescer`.

use serde_json::{Value, json};
use tokio::time::Instant;

use super::thinking::ThinkingPublication;
use super::tool_events::ToolEvents;
use crate::provider::StreamToolCallDelta;

/// The identity and size of a provider's slot. Argument text is never kept.
struct Call {
    index: u32,
    id: String,
    name: String,
    chars: usize,
}

/// One attempt's snapshots in encounter order, and its terminal frame.
pub(super) struct Composition {
    events: Option<ToolEvents>,
    calls: Vec<Call>,
    flush_ms: f64,
    flush_chars: usize,
    pending: usize,
    last_publish: Instant,
}

impl Composition {
    pub(super) fn new(events: Option<ToolEvents>, settings: ThinkingPublication) -> Self {
        Self {
            events,
            calls: Vec::new(),
            flush_ms: settings.coalesce_ms,
            flush_chars: settings.coalesce_chars,
            pending: 0,
            last_publish: Instant::now(),
        }
    }

    /// A slot's first fragment publishes immediately, even without an id.
    /// Later fragments spend the size/time window shared with reasoning.
    pub(super) fn add(&mut self, delta: StreamToolCallDelta) {
        if self.events.is_none() {
            return;
        }
        let chars = delta.arguments_delta.chars().count();
        let Some(call) = self.calls.iter_mut().find(|call| call.index == delta.index) else {
            self.calls.push(Call {
                index: delta.index,
                id: delta.id,
                name: delta.name,
                chars,
            });
            self.flush();
            return;
        };
        if !delta.id.is_empty() {
            call.id = delta.id;
        }
        if !delta.name.is_empty() {
            call.name = delta.name;
        }
        call.chars = call.chars.saturating_add(chars);
        self.pending = self.pending.saturating_add(chars);
        if self.flush_ms <= 0.0
            || self.pending >= self.flush_chars
            || self.last_publish.elapsed().as_secs_f64() * 1000.0 >= self.flush_ms
        {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let Some(events) = &self.events else {
            return;
        };
        let calls: Vec<Value> = self
            .calls
            .iter()
            .map(|call| {
                json!({
                    "id": call.id, "name": call.name, "arguments_chars": call.chars,
                })
            })
            .collect();
        events.composing(calls);
        self.pending = 0;
        self.last_publish = Instant::now();
    }

    /// Exactly one terminal for an attempt that published. Residual sizes
    /// need no flush: the complete call carries its arguments at START.
    pub(super) fn close(&mut self) {
        if self.calls.is_empty() {
            return;
        }
        self.calls.clear();
        self.pending = 0;
        if let Some(events) = &self.events {
            events.composing(Vec::new());
        }
    }
}

impl Drop for Composition {
    /// A dropped generation (cancellation, timeout or a stopped consumer)
    /// closes its last open composition too.
    fn drop(&mut self) {
        self.close();
    }
}
