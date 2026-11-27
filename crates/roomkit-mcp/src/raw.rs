//! Preserve host-owned responses before the SDK's typed result union reads them.
//!
//! Tickets travel in non-serialized request extensions. Only live requests keep
//! a slot; cancellation removes it even if the transport has not sent it yet.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use rmcp::model::{
    ClientJsonRpcMessage, ClientNotification, ClientRequest, GetExtensions, RequestId,
};
use roomkit::model::Metadata;
use serde_json::{Value, json};

#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    pending: HashMap<RequestId, Weak<Mutex<Option<Value>>>>,
    initialization: Option<RequestId>,
    handshake: Value,
    /// The `tools/call` requests sent, neither answered nor cancelled.
    calls: HashSet<RequestId>,
}

/// The fields of which a `tools/call` result carries at least one.
const CALL_RESULT_FIELDS: [&str; 3] = ["content", "structuredContent", "isError"];

/// What the SDK reads in place of a `tools/call` result with none of
/// [`CALL_RESULT_FIELDS`]: an object of no result type it knows.
const NO_CALL_RESULT: &str = "roomkit/noCallResult";

/// This marker is local to the transport and never appears in JSON.
#[derive(Clone)]
pub(crate) struct Slot {
    response: Weak<Mutex<Option<Value>>>,
    params: Metadata,
}

impl Slot {
    pub(crate) fn params(&self) -> &Metadata {
        &self.params
    }
}

pub(crate) struct Ticket {
    capture: Capture,
    value: Arc<Mutex<Option<Value>>>,
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Capture {
    pub(crate) fn ticket(&self) -> Ticket {
        Ticket {
            capture: self.clone(),
            value: Arc::default(),
        }
    }

    pub(crate) fn outgoing(&self, message: &ClientJsonRpcMessage) {
        if let Some(id) = cancelled(message) {
            lock(&self.0).calls.remove(id);
        }
        let ClientJsonRpcMessage::Request(request) = message else {
            return;
        };
        let mut state = lock(&self.0);
        if matches!(request.request, ClientRequest::InitializeRequest(_)) {
            state.initialization = Some(request.id.clone());
        }
        if matches!(request.request, ClientRequest::CallToolRequest(_)) {
            state.calls.insert(request.id.clone());
        }
        if let Some(slot) = request.request.extensions().get::<Slot>()
            && slot.response.upgrade().is_some()
        {
            state
                .pending
                .insert(request.id.clone(), Weak::clone(&slot.response));
        }
    }

    /// Preserve the raw result, then give the SDK an acknowledgement for this
    /// explicitly untyped request. Ordinary tools and protocol messages retain
    /// their normal SDK parsing.
    pub(crate) fn incoming(&self, mut message: Value) -> Value {
        // Request ids belong to each direction independently. A server ping
        // may reuse a live client id and must not consume its response ticket.
        if message.get("method").is_some()
            || message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || (message.get("result").is_some() == message.get("error").is_some())
        {
            return message;
        }
        let Some(id) = message
            .get("id")
            .and_then(|id| serde_json::from_value(id.clone()).ok())
        else {
            return message;
        };
        let mut state = lock(&self.0);
        if state.initialization.as_ref() == Some(&id) {
            state.initialization = None;
            if let Some(result) = message.get("result") {
                state.handshake = result.clone();
            }
            crate::protocol::compatible_handshake(&mut message);
        }
        if state.calls.remove(&id) {
            refuse_an_empty_call_result(&mut message);
        }
        if let Some(slot) = state.pending.remove(&id).and_then(|weak| weak.upgrade())
            && let Some(result) = message.get_mut("result")
        {
            *lock(&slot) = Some(std::mem::replace(result, json!({})));
        }
        message
    }

    pub(crate) fn server_info(&self) -> Metadata {
        let mut info = lock(&self.0)
            .handshake
            .get("serverInfo")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        info.retain(|_, value| !value.is_null());
        if let Some(icons) = info.get_mut("icons").and_then(Value::as_array_mut) {
            for icon in icons.iter_mut().filter_map(Value::as_object_mut) {
                icon.retain(|_, value| !value.is_null());
            }
        }
        info
    }

    pub(crate) fn server_extensions(&self) -> std::collections::BTreeMap<String, Metadata> {
        lock(&self.0)
            .handshake
            .pointer("/capabilities/extensions")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .filter_map(|(name, value)| value.as_object().map(|v| (name.clone(), v.clone())))
            .collect()
    }
}

/// The request a `notifications/cancelled` the client sends names.
fn cancelled(message: &ClientJsonRpcMessage) -> Option<&RequestId> {
    let ClientJsonRpcMessage::Notification(notification) = message else {
        return None;
    };
    let ClientNotification::CancelledNotification(cancelled) = &notification.notification else {
        return None;
    };
    cancelled.params.request_id.as_ref()
}

/// A `tools/call` result with none of [`CALL_RESULT_FIELDS`] fails the
/// call, as rmcp 3.9 read it and as Python's SDK refuses a result without
/// `content`: rmcp 3.11 and later read it as an empty result. The SDK's
/// copy of such a result reads as an unexpected response.
fn refuse_an_empty_call_result(message: &mut Value) {
    let Some(result) = message.get_mut("result").and_then(Value::as_object_mut) else {
        return;
    };
    if CALL_RESULT_FIELDS
        .iter()
        .any(|field| result.contains_key(*field))
    {
        return;
    }
    *result = serde_json::Map::from_iter([(NO_CALL_RESULT.to_owned(), Value::Bool(true))]);
}

impl Ticket {
    pub(crate) fn marker(&self, params: Metadata) -> Slot {
        Slot {
            response: Arc::downgrade(&self.value),
            params,
        }
    }
    pub(crate) fn take(&self) -> Option<Value> {
        lock(&self.value).take()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mine = Arc::downgrade(&self.value);
        lock(&self.capture.0)
            .pending
            .retain(|_, slot| !slot.ptr_eq(&mine));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{ClientRequest, CustomRequest};

    fn message(ticket: &Ticket, id: u32) -> ClientJsonRpcMessage {
        let mut request = CustomRequest::new("host/read", Some(json!({})));
        request.extensions.insert(ticket.marker(Metadata::new()));
        ClientJsonRpcMessage::request(
            ClientRequest::CustomRequest(request),
            RequestId::Number(i64::from(id)),
        )
    }

    #[test]
    fn cancellation_before_or_after_send_retains_no_slot() {
        for sent in [false, true] {
            let capture = Capture::default();
            let ticket = capture.ticket();
            let message = message(&ticket, 1);
            if sent {
                capture.outgoing(&message);
            }
            drop(ticket);
            if !sent {
                capture.outgoing(&message);
            }
            assert!(lock(&capture.0).pending.is_empty());
        }
    }

    #[test]
    fn late_and_unrelated_responses_cannot_fill_another_ticket() {
        let capture = Capture::default();
        let first = capture.ticket();
        capture.outgoing(&message(&first, 1));
        drop(first);
        let second = capture.ticket();
        capture.outgoing(&message(&second, 2));
        let late = json!({"jsonrpc":"2.0","id":1,"result":{"old":true}});
        assert_eq!(capture.incoming(late.clone()), late);
        assert!(second.take().is_none());
        let current = json!({"jsonrpc":"2.0","id":2,"result":{"contents":[],"vendor":7}});
        assert_eq!(capture.incoming(current.clone())["result"], json!({}));
        assert_eq!(second.take(), Some(current["result"].clone()));
        assert!(lock(&capture.0).pending.is_empty());
    }

    #[test]
    fn a_server_request_with_the_same_id_leaves_the_client_ticket_intact() {
        let capture = Capture::default();
        let ticket = capture.ticket();
        capture.outgoing(&message(&ticket, 7));
        let ping = json!({"jsonrpc":"2.0","id":7,"method":"ping"});
        assert_eq!(capture.incoming(ping.clone()), ping);
        let reply = json!({"jsonrpc":"2.0","id":7,"result":{"value":1}});
        let _acknowledgement = capture.incoming(reply);
        assert_eq!(ticket.take(), Some(json!({"value":1})));
    }

    #[test]
    fn the_handshake_snapshot_is_not_replaced_when_its_id_is_reused() {
        let capture = Capture::default();
        let initialize: ClientJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc":"2.0","id":0,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"test","version":"1"}
            }
        }))
        .expect("valid initialize fixture");
        capture.outgoing(&initialize);
        let _handshake = capture.incoming(json!({
            "jsonrpc":"2.0","id":0,"result":{"serverInfo":{"name":"kept","version":"1"}}
        }));
        let _other = capture.incoming(json!({"jsonrpc":"2.0","id":0,"result":{"other":1}}));
        assert_eq!(capture.server_info().get("name"), Some(&json!("kept")));
    }

    /// Only the answer to a live `tools/call` is refused for carrying none
    /// of content, structured content and `isError`: an empty answer to
    /// another request, or to a call the client cancelled, reaches the SDK
    /// as sent.
    #[test]
    fn only_a_live_call_s_empty_result_is_refused() {
        let capture = Capture::default();
        for (id, method) in [(1, "tools/call"), (2, "ping"), (3, "tools/call")] {
            let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":{"name":"t"}});
            capture.outgoing(&serde_json::from_value(request).expect("valid request fixture"));
        }
        let cancelled = json!({"jsonrpc":"2.0","method":"notifications/cancelled",
            "params":{"requestId":3}});
        capture.outgoing(&serde_json::from_value(cancelled).expect("valid notification fixture"));
        let read: Vec<Value> = (1..=3)
            .map(|id| {
                capture.incoming(json!({"jsonrpc":"2.0","id":id,"result":{}}))["result"].clone()
            })
            .collect();
        assert_eq!(read, [json!({NO_CALL_RESULT: true}), json!({}), json!({})]);
    }
}
