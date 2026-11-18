//! The whole request a distributed backend carries (RFC §10.6.3).
//! Events stay typed as they do in the memory backend. Serializing a large
//! event does not count it towards the local limit on the rest of a request.

use roomkit_core::model::{Metadata, RoomEvent};
use serde::{Deserialize, Serialize};

use super::RequestRest;
use crate::turns::memory::CarriedRequest;
use crate::turns::pipe::MAX_MESSAGE_BYTES;

#[derive(Deserialize, Serialize)]
struct Rest {
    context: Metadata,
    #[serde(flatten)]
    fields: Metadata,
}

#[derive(Deserialize, Serialize)]
struct Context<H> {
    #[serde(default)]
    recent_events: H,
    #[serde(flatten)]
    fields: Metadata,
}

#[derive(Deserialize, Serialize)]
struct Complete<E, H: Default> {
    event: E,
    context: Context<H>,
    #[serde(default)]
    channel_rows: H,
    #[serde(flatten)]
    fields: Metadata,
}

impl CarriedRequest {
    /// Read the owner's full request and split its events for the native executor.
    pub(crate) fn from_wire(body: &str) -> Result<Self, String> {
        let wire: Complete<RoomEvent, Vec<RoomEvent>> =
            serde_json::from_str(body).map_err(|error| error.to_string())?;
        let rest = Rest {
            fields: wire.fields,
            context: wire.context.fields,
        };
        let rest = serde_json::to_string(&rest).map_err(|error| error.to_string())?;
        validate_rest(&rest)?;
        Ok(Self {
            rest,
            event: wire.event,
            history: wire.context.recent_events,
            channel_rows: wire.channel_rows,
        })
    }

    /// Serialize all three sets of event data, without copying their contents.
    pub(crate) fn to_wire(&self) -> Result<String, String> {
        validate_rest(&self.rest)?;
        let rest: Rest = serde_json::from_str(&self.rest).map_err(|error| error.to_string())?;
        let wire = Complete {
            event: &self.event,
            context: Context {
                fields: rest.context,
                recent_events: self.history.as_slice(),
            },
            channel_rows: self.channel_rows.as_slice(),
            fields: rest.fields,
        };
        serde_json::to_string(&wire).map_err(|error| error.to_string())
    }
}

pub(crate) fn validate_rest(raw: &str) -> Result<RequestRest, String> {
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err(format!(
            "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
            raw.len()
        ));
    }
    serde_json::from_str(raw).map_err(|error| error.to_string())
}
