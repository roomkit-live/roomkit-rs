//! An agent's lifecycle on the status bus (RFC §19.8): Python's
//! `post_agent_lifecycle`, the one entry point every strategy and the
//! delegations post through.

use roomkit_core::model::{ChannelId, Metadata};
use serde_json::json;

use crate::kit::RoomKit;
use crate::orchestration::{PostOptions, StatusLevel};
use crate::store::ConversationStore;
use crate::tasks::text::bounded_text;

/// The characters of an entry's detail.
const DETAIL_LIMIT: usize = 200;

/// The characters of a completed entry's whole outcome, kept under
/// `result` when its detail was cut.
const RESULT_LIMIT: usize = 4_000;

/// Posts `agent_id`'s `level` under `action` on the kit's status bus, its
/// `detail` bounded at 200 characters, a completed one's whole outcome kept
/// under `metadata["result"]` when the detail was cut: Python's
/// `post_agent_lifecycle(kit, agent_id, level, *, action, detail,
/// metadata)`. Observability only: the bus records it in the background.
pub(crate) fn post_agent_lifecycle<S: ConversationStore>(
    kit: &RoomKit<S>,
    agent_id: &ChannelId,
    level: StatusLevel,
    action: &str,
    detail: &str,
    mut metadata: Metadata,
) {
    let summary = bounded_text(detail, DETAIL_LIMIT);
    if level == StatusLevel::Completed && summary != detail {
        metadata
            .entry("result")
            .or_insert_with(|| json!(bounded_text(detail, RESULT_LIMIT)));
    }
    let options = PostOptions {
        detail: summary,
        metadata: Some(metadata),
    };
    kit.status_bus()
        .post(agent_id.as_str(), action, level, options);
}
