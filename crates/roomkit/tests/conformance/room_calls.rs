//! Public room transitions, named timer checks and setup through those same APIs (RFC §5.1).

use std::sync::Arc;

use roomkit::model::RoomId;
use roomkit::{
    ArchiveRoomOptions, CheckRoomTimersOptions, CloseRoomOptions, PauseRoomOptions,
    ReopenRoomOptions, ResumeRoomOptions,
};
use serde_json::Value;

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::Run;

/// Makes the room call the scenario names, forwarding its organization scope.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let room = field::<RoomId>(spec, "room_id")?;
    let organization_id = optional::<String>(spec, "organization_id")?;
    let outcome = match name {
        "pause_room" => {
            run.kit()
                .pause_room(&room, PauseRoomOptions { organization_id })
                .await
        }
        "resume_room" => {
            run.kit()
                .resume_room(&room, ResumeRoomOptions { organization_id })
                .await
        }
        "reopen_room" => {
            run.kit()
                .reopen_room(&room, ReopenRoomOptions { organization_id })
                .await
        }
        "close_room" => {
            run.kit()
                .close_room(&room, CloseRoomOptions { organization_id })
                .await
        }
        "archive_room" => {
            run.kit()
                .archive_room(&room, ArchiveRoomOptions { organization_id })
                .await
        }
        "check_timers" => {
            run.kit()
                .check_room_timers(&room, CheckRoomTimersOptions { organization_id })
                .await
        }
        _ => return Err(format!("unknown room call: {name}")),
    };
    Ok(finish(run, name, outcome, dump))
}

/// Reaches a setup status before observation, without writing the store directly.
pub(crate) async fn seed_status(run: &Arc<Run>, room: &Value) -> Result<(), Unreadable> {
    let id = field::<RoomId>(room, "id")?;
    let status = room["status"].as_str();
    if status == Some("paused") {
        run.kit()
            .pause_room(&id, PauseRoomOptions::default())
            .await
            .map_err(|e| e.to_string())?;
    }
    if matches!(status, Some("closed" | "archived")) {
        run.kit()
            .close_room(&id, CloseRoomOptions::default())
            .await
            .map_err(|e| e.to_string())?;
    }
    if status == Some("archived") {
        run.kit()
            .archive_room(&id, ArchiveRoomOptions::default())
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
