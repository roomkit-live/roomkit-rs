//! Active ACP calls: ordered stream markers, room publications and hooks.

mod call;
mod closing;
mod encoding;
mod handler;
mod reports;

use std::sync::{Mutex, PoisonError};

use roomkit::ResponseItem;
use roomkit::model::{ChannelId, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::turns::ChannelTurnHooks;
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

impl State {
    fn merge(&mut self, update: &Value) -> Option<&mut Call> {
        if self.closed {
            return None;
        }
        let id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())?;
        let index = self
            .calls
            .iter()
            .position(|call| call.request.id == id)
            .unwrap_or_else(|| {
                self.calls.push(Call::new(id));
                self.calls.len() - 1
            });
        let call = self.calls.get_mut(index)?;
        call.merge(update);
        Some(call)
    }
}

pub(super) struct Tools {
    state: Mutex<State>,
    outlet: Option<ChannelTurnHooks>,
    room_id: RoomId,
    channel_id: ChannelId,
    reports: Reports,
    publishes_text: bool,
}

impl Tools {
    pub(super) fn new(
        callbacks: Option<ChannelTurnHooks>,
        room_id: RoomId,
        channel_id: ChannelId,
        outlet: Option<ChannelTurnHooks>,
        publishes_text: bool,
        handler: Option<std::sync::Arc<dyn roomkit_ai::ExternalToolHandler>>,
    ) -> Self {
        let reports = Reports::new(callbacks, handler, room_id.clone());
        Self {
            state: Mutex::default(),
            outlet,
            room_id,
            channel_id,
            reports,
            publishes_text,
        }
    }

    /// The channel has already decoded the protocol. No lock spans a hook
    /// await; closing the reader cannot race a new call into the closed turn.
    pub(super) fn update(
        &self,
        update: &Value,
        sender: &mpsc::UnboundedSender<ResponseItem>,
    ) -> Option<Pending> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let call = state.merge(update)?;
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

    /// Keep the call before awaiting approval, so teardown can cancel it.
    /// The decision is kept on the call by [`Tools::record`], once it is
    /// the answer.
    pub(super) async fn decide(
        &self,
        update: &Value,
        session: &str,
    ) -> super::permissions::Decision {
        let call = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.merge(update).map(|call| {
                call.asked = true;
                call.request.clone()
            })
        };
        let Some(call) = call else {
            return super::permissions::Decision::default();
        };
        self.reports.decide(call, session).await
    }

    /// Keep `decision`, the answer to the agent's permission request for
    /// the call `update` names, on that call: an abandoned one is never
    /// kept, so the cut reports the call cancelled.
    pub(super) fn record(&self, update: &Value, decision: &super::permissions::Decision) {
        let Some(id) = update.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(call) = state.calls.iter_mut().find(|call| call.request.id == id) {
            call.decision = Some(decision.clone());
        }
    }

    fn start(&self, call: &Call, sender: &mpsc::UnboundedSender<ResponseItem>) {
        let _gone = sender.send(ResponseItem::ToolCallStart(call.start()));
        self.publish_start(call);
    }

    fn publish_start(&self, call: &Call) {
        let mut entry = serde_json::Map::from_iter([
            ("id".to_owned(), json!(call.request.id)),
            ("name".to_owned(), json!(call.request.name)),
        ]);
        if self.publishes_text {
            entry.insert("arguments".to_owned(), json!(call.request.arguments));
        }
        self.publish(
            EphemeralEventType::ToolCallStart,
            json!({
                "tool_calls": [entry], "round": 0
            }),
        );
    }

    fn announce(&self, end: End) -> Pending {
        let mut entry = serde_json::Map::from_iter([
            ("id".to_owned(), json!(end.row.tool_id)),
            ("name".to_owned(), json!(end.row.tool_name)),
            ("status".to_owned(), json!(end.row.status)),
        ]);
        if self.publishes_text {
            entry.insert("result".to_owned(), json!(end.preview));
        }
        self.publish(
            EphemeralEventType::ToolCallEnd,
            json!({
                "tool_calls": [entry], "round": 0, "duration_ms": end.row.duration_ms
            }),
        );
        self.reports.send(end.report, end.observe_only)
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
        // A call whose permission the agent asked is closed even if the agent
        // never announced it: its refusal or failure is reported; an
        // approval, or a decision still pending or abandoned, cut by the
        // turn's end is reported cancelled (RFC §9.3).
        for call in &mut state.calls {
            if call.finished || (!call.started && !call.asked) {
                continue;
            }
            if !call.started {
                call.started = true;
                rows.push(ResponseItem::ToolCallStart(call.start()));
                self.publish_start(call);
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
        let Some(outlet) = &self.outlet else {
            return;
        };
        let Value::Object(mut data) = value else {
            return;
        };
        data.insert("channel_id".to_owned(), json!(self.channel_id));
        outlet.publish_ephemeral(EphemeralEvent {
            channel_id: Some(self.channel_id.clone()),
            data,
            ..EphemeralEvent::new(self.room_id.clone(), kind, self.channel_id.to_string())
        });
    }
}
