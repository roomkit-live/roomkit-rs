//! Public room reads and full timer sweeps, never store substitutes (RFC §5.1).

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::Run;
use roomkit::model::{ChannelId, RoomId};
use roomkit::{
    CheckAllTimersOptions, ConversationStore, GetBindingOptions, GetRoomOptions,
    GetTimelineOptions, ListBindingsOptions, ListMembersOptions,
};
use serde_json::Value;
use std::sync::Arc;

pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let organization_id = optional::<String>(spec, "organization_id")?;
    if name == "check_all_timers" {
        let outcome = run
            .kit()
            .check_all_timers(CheckAllTimersOptions { organization_id })
            .await;
        return Ok(finish(run, name, outcome, dump));
    }
    let room = field::<RoomId>(spec, "room_id")?;
    Ok(match name {
        "get_room" => finish(
            run,
            name,
            run.kit()
                .get_room(&room, GetRoomOptions { organization_id })
                .await,
            dump,
        ),
        "get_binding" => {
            let channel = field::<ChannelId>(spec, "channel_id")?;
            finish(
                run,
                name,
                run.kit()
                    .get_binding(&room, &channel, GetBindingOptions { organization_id })
                    .await,
                dump,
            )
        }
        "list_bindings" => finish(
            run,
            name,
            run.kit()
                .list_bindings(&room, ListBindingsOptions { organization_id })
                .await,
            dump,
        ),
        "list_members" => {
            let options = ListMembersOptions {
                organization_id,
                include_left: optional(spec, "include_left")?.unwrap_or(false),
            };
            finish(
                run,
                name,
                run.kit().list_members(&room, options).await,
                dump,
            )
        }
        "get_timeline" => {
            let options = GetTimelineOptions {
                organization_id,
                offset: optional(spec, "offset")?.unwrap_or(0),
                limit: optional(spec, "limit")?.unwrap_or(50),
                visibility_filter: optional(spec, "visibility_filter")?,
                after_index: optional(spec, "after_index")?,
                before_index: optional(spec, "before_index")?,
                newest_first: optional(spec, "newest_first")?.unwrap_or(false),
                include_blocked: optional(spec, "include_blocked")?.unwrap_or(false),
            };
            finish(
                run,
                name,
                run.kit().get_timeline(&room, options).await,
                dump,
            )
        }
        _ => return Err(format!("unknown public room query: {name}")),
    })
}

/// Read every room for observation and idle detection; the store's default is
/// one page, which cannot prove a sweep beyond that page.
/// The observation connection stays open even when the scenario closes every kit.
pub(crate) async fn all_rooms(run: &Run) -> Result<Vec<roomkit::model::Room>, roomkit::StoreError> {
    let store = run.observer_store();
    let mut rooms = Vec::new();
    loop {
        let options = roomkit::store::ListRoomsOptions {
            limit: 100,
            offset: u64::try_from(rooms.len()).unwrap(),
        };
        let page = store.list_rooms(options).await?;
        let last = page.len() < 100;
        rooms.extend(page);
        if last {
            return Ok(rooms);
        }
    }
}

/// Arguments of the public reads and sweep, beside organization scope.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "get_room" | "list_bindings" => &["room_id"],
        "get_binding" => &["room_id", "channel_id"],
        "list_members" => &["room_id", "include_left"],
        "get_timeline" => &[
            "room_id",
            "offset",
            "limit",
            "visibility_filter",
            "after_index",
            "before_index",
            "newest_first",
            "include_blocked",
        ],
        "check_all_timers" => &[],
        _ => return None,
    })
}
