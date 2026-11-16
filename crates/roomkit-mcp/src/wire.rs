//! Write only explicitly marked host parameters verbatim. Ordinary MCP
//! requests retain SDK serialization and its progress metadata.
#[cfg(feature = "http")]
use rmcp::model::ClientJsonRpcMessage;
use rmcp::model::{ClientRequest, Extensions, GetExtensions, GetMeta, RequestMetaObject};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::raw::Slot;

#[cfg(feature = "http")]
pub(crate) fn message(message: &ClientJsonRpcMessage) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(message)?;
    if let ClientJsonRpcMessage::Request(request) = message {
        restore(&request.request, &mut value);
    }
    Ok(value)
}

fn restore(request: &ClientRequest, value: &mut Value) {
    if let Some(slot) = request.extensions().get::<Slot>() {
        value["params"] = Value::Object(slot.params().clone());
    }
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
