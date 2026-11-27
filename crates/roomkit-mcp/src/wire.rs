//! Write only explicitly marked host parameters verbatim. Ordinary MCP
//! requests retain SDK serialization and its progress metadata, but for a
//! request without parameters of its own, which goes with no `params`
//! member (RFC §21.2, RK-21.2-046).
#[cfg(feature = "http")]
use rmcp::model::ClientJsonRpcMessage;
use rmcp::model::{ClientRequest, Extensions, GetExtensions, GetMeta, RequestMetaObject};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

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
/// slot holds them, an SDK request's as the SDK wrote them, and none at all
/// for a request without parameters, as Python's SDK sends it (RFC §21.2,
/// RK-21.2-046).
fn restore(request: &ClientRequest, value: &mut Value) {
    let Some(message) = value.as_object_mut() else {
        return;
    };
    if let Some(slot) = request.extensions().get::<Slot>() {
        if let Some(params) = slot.params() {
            message.insert("params".into(), Value::Object(params.clone()));
        } else {
            message.shift_remove("params");
        }
    } else if only_the_progress_token(request, message.get("params")) {
        message.shift_remove("params");
    }
}

/// Whether an SDK request's `params` hold nothing but the progress token
/// rmcp writes on every request (the first `tools/list` page's, say):
/// RoomKit asks no progress of a request, and Python's SDK writes a token
/// only for a caller that does.
fn only_the_progress_token(request: &ClientRequest, params: Option<&Value>) -> bool {
    let Some(token) = request.get_meta().get_progress_token() else {
        return false;
    };
    serde_json::to_value(token)
        .is_ok_and(|token| params == Some(&json!({"_meta": {"progressToken": token}})))
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
