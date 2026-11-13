//! Active ACP calls: ordered stream markers, room publications and hooks.

mod call;
mod closing;
mod encoding;
mod reports;

use std::sync::{Mutex, PoisonError};

use roomkit::model::{ChannelId, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{ResponseItem, TurnHost};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use call::{Call, End};
pub(super) use closing::Closing;
use reports::{Pending, Reports};

#[derive(Default)]
struct State {
    closed: bool,
    // Insertion order is the order unfinished calls close in.
    calls: Vec<Call>,
}

pub(super) struct Tools {
    state: Mutex<State>,
    host: Option<TurnHost>,
    room_id: RoomId,
    channel_id: ChannelId,
    reports: Reports,
}

impl Tools {
    pub(super) fn new(
        host: Option<TurnHost>,
        room_id: RoomId,
        channel_id: ChannelId,
        answers_room: bool,
    ) -> Self {
        let reports = Reports::new(host.clone());
        Self {
            state: Mutex::default(),
            host: host.filter(|_| answers_room),
            room_id,
            channel_id,
            reports,
        }
    }

    /// The channel has already decoded the protocol. No lock spans a hook
    /// await; closing the reader cannot race a new call into the closed turn.
    pub(super) fn update(
        &self,
        update: &Value,
        sender: &mpsc::UnboundedSender<ResponseItem>,
    ) -> Option<Pending> {
        let id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return None;
        }
        let index = state
            .calls
            .iter()
            .position(|call| call.request.id == id)
            .unwrap_or_else(|| {
                state.calls.push(Call::new(id));
                state.calls.len() - 1
            });
        let call = state.calls.get_mut(index)?;
        call.merge(update);
        if !call.started {
            call.started = true;
            self.start(call, sender);
        }
        if !call.finished
            && matches!(
                update.get("status").and_then(Value::as_str),
                Some("completed" | "failed")
            )
        {
            let end = call.end(
                update.get("status").and_then(Value::as_str) == Some("failed"),
                false,
            );
            let _gone = sender.send(ResponseItem::ToolCallEnd(end.row.clone()));
            return Some(self.announce(end));
        }
        None
    }

    fn start(&self, call: &Call, sender: &mpsc::UnboundedSender<ResponseItem>) {
        let _gone = sender.send(ResponseItem::ToolCallStart(call.start()));
        self.publish(EphemeralEventType::ToolCallStart, json!({
            "tool_calls": [{"id": call.request.id, "name": call.request.name, "arguments": call.request.arguments}], "round": 0
        }));
    }

    fn announce(&self, end: End) -> Pending {
        self.publish(EphemeralEventType::ToolCallEnd, json!({
            "tool_calls": [{"id": end.row.tool_id, "name": end.row.tool_name,
                "result": end.preview, "status": end.row.status}], "round": 0, "duration_ms": end.row.duration_ms
        }));
        self.reports.send(end.report)
    }

    /// Finishing is idempotent, including a Drop racing an arriving update.
    /// A consumed end stores the closing rows; Drop only announces the ends.
    pub(super) fn finish(&self) -> (Vec<ResponseItem>, Vec<Pending>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if std::mem::replace(&mut state.closed, true) {
            return (Vec::new(), Vec::new());
        }
        let mut rows = Vec::new();
        let mut pending = Vec::new();
        for call in &mut state.calls {
            if call.finished {
                continue;
            }
            let end = call.end(true, true);
            rows.push(ResponseItem::ToolCallEnd(end.row.clone()));
            pending.push(self.announce(end));
        }
        (rows, pending)
    }

    pub(super) fn count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .calls
            .len()
    }

    fn start_reports(&self) {
        self.reports.start();
    }

    fn publish(&self, kind: EphemeralEventType, value: Value) {
        let Some(host) = &self.host else {
            return;
        };
        let Value::Object(mut data) = value else {
            return;
        };
        data.insert("channel_id".to_owned(), json!(self.channel_id));
        host.publish_ephemeral(EphemeralEvent {
            channel_id: Some(self.channel_id.clone()),
            data,
            ..EphemeralEvent::new(self.room_id.clone(), kind, self.channel_id.to_string())
        });
    }
}
