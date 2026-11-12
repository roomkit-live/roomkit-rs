//! Public ACP lifecycle calls, with the kit left alive.

use std::sync::Arc;

use roomkit::Channel;
use serde_json::Value;

use crate::calls::{Called, Unreadable, field};
use crate::run::{Run, lock};

pub(crate) async fn close(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    let id: String = field(spec, "channel_id")?;
    let channel = lock(&run.acp_channels)
        .get(&id)
        .cloned()
        .ok_or_else(|| "acp.close requires an ACP channel".to_owned())?;
    channel.close().await;
    Ok(Ok(Value::Null))
}
