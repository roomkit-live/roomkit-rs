//! Write only explicitly marked host parameters verbatim, with the
//! protocol's own `_meta` entries on a 2026-07-28 connection, which a
//! caller's do not replace. Ordinary MCP requests retain SDK serialization
//! and its progress metadata, but for a request without parameters of its
//! own, which goes without the progress token: with no `params` member on an
//! earlier revision (RFC §21.2, RK-21.2-046), with the protocol's `_meta`
//! alone on 2026-07-28.
#[cfg(feature = "http")]
use rmcp::model::ClientJsonRpcMessage;
use rmcp::model::{ClientRequest, Extensions, GetExtensions, GetMeta, RequestMetaObject};
use roomkit::model::Metadata;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::protocol::PROTOCOL_META;
use crate::raw::Slot;

#[cfg(feature = "http")]
pub(crate) fn message(message: &ClientJsonRpcMessage) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(message)?;
    if let ClientJsonRpcMessage::Request(request) = message {
        restore(&request.request, &mut value);
    }
    Ok(value)
}

/// A request's `params` as RoomKit sends them: a host request's as its
/// slot holds them, the protocol's own `_meta` entries over its caller's,
/// an SDK request's as the SDK wrote them, and, for a request without
/// parameters of its own, no progress token, as Python's SDK sends it (RFC
/// §21.2, RK-21.2-046).
fn restore(request: &ClientRequest, value: &mut Value) {
    let Some(message) = value.as_object_mut() else {
        return;
    };
    if let Some(slot) = request.extensions().get::<Slot>() {
        match with_protocol_meta(slot.params().cloned(), request.get_meta()) {
            Some(params) => {
                message.insert("params".into(), Value::Object(params));
            }
            None => {
                message.shift_remove("params");
            }
        }
    } else if let Some(params) = message.get_mut("params").and_then(Value::as_object_mut)
        && only_the_progress_token(request, params)
        && let Some(meta) = params.get_mut("_meta").and_then(Value::as_object_mut)
    {
        meta.shift_remove("progressToken");
        if meta.is_empty() {
            message.shift_remove("params");
        }
    }
}

/// `params` with the protocol's own `_meta` entries of `meta` (a 2026-07-28
/// request's revision, client information and capabilities), which replace
/// any of a caller's under those names.
fn with_protocol_meta(params: Option<Metadata>, meta: &RequestMetaObject) -> Option<Metadata> {
    let protocol: Vec<(&str, &Value)> = PROTOCOL_META
        .iter()
        .filter_map(|key| meta.get(*key).map(|value| (*key, value)))
        .collect();
    if protocol.is_empty() {
        return params;
    }
    let mut params = params.unwrap_or_default();
    let written = params
        .entry("_meta")
        .or_insert_with(|| Value::Object(Metadata::new()));
    if let Some(written) = written.as_object_mut() {
        for (key, value) in protocol {
            written.insert(key.to_owned(), value.clone());
        }
    }
    Some(params)
}

/// Whether an SDK request's `params` hold nothing but a `_meta` of the
/// progress token rmcp writes on every request (the first `tools/list`
/// page's, say) and the protocol's own entries: RoomKit asks no progress of
/// a request, and Python's SDK writes a token only for a caller that does.
fn only_the_progress_token(
    request: &ClientRequest,
    params: &serde_json::Map<String, Value>,
) -> bool {
    let Some(token) = request.get_meta().get_progress_token() else {
        return false;
    };
    let Ok(token) = serde_json::to_value(token) else {
        return false;
    };
    let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
        return false;
    };
    params.len() == 1
        && meta.get("progressToken") == Some(&token)
        && meta
            .keys()
            .all(|key| key == "progressToken" || PROTOCOL_META.contains(&key.as_str()))
}

/// Only the bounded stdio transport's outbound request type changes. Its
/// inbound request/response/error parser and writer framing stay in the SDK.
#[derive(Debug, Clone)]
pub(crate) struct Request(pub(crate) ClientRequest);

impl Serialize for Request {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.0).map_err(serde::ser::Error::custom)?;
        restore(&self.0, &mut value);
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        ClientRequest::deserialize(deserializer).map(Self)
    }
}

impl GetExtensions for Request {
    fn extensions(&self) -> &Extensions {
        self.0.extensions()
    }
    fn extensions_mut(&mut self) -> &mut Extensions {
        self.0.extensions_mut()
    }
}

impl GetMeta for Request {
    type Metadata = RequestMetaObject;
    fn get_meta(&self) -> &Self::Metadata {
        self.0.get_meta()
    }
    fn get_meta_mut(&mut self) -> &mut Self::Metadata {
        self.0.get_meta_mut()
    }
}
