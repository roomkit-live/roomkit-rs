//! An executor run as in a process of its own (README: an executor's
//! `own_process`), on the isolated runtime a killed worker has: a turn busy
//! with its CPU there stalls nothing beside it. The channels it hosts are
//! scripted channels whose reactions hold no gate and make no action other
//! than a host verb, as Python's runner, whose executor runs on a loop of its
//! own, requires: what the run's gates and records share stays on the run's
//! own loop.

use roomkit::RoomKitError;
use serde_json::Value;

/// What a reaction of a channel such an executor hosts may hold.
const REACTION: &[&str] = &[
    "when",
    "repeat",
    "delay_ms",
    "holds_cpu_ms",
    "do",
    "respond",
    "output",
    "fail",
];

/// Refuses the setup when `worker` cannot run in a process of its own: a
/// channel it hosts is no scripted channel, or one of its reactions holds a
/// gate or makes an action other than a host verb.
pub(super) fn check(worker: &super::Worker, channels: &[Value]) -> Result<(), RoomKitError> {
    let problems: Vec<String> = channels
        .iter()
        .filter(|channel| channel["declared"] == true)
        .filter(|channel| channel["id"].as_str().is_some_and(|id| hosts(worker, id)))
        .flat_map(|channel| problems(&worker.id, channel))
        .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(RoomKitError::Value(problems.join("; ")))
    }
}

fn problems(worker: &str, channel: &Value) -> Vec<String> {
    let place = format!("executor {worker}, channel {}", channel["id"]);
    let mut found = Vec::new();
    if channel.get("model").is_some() || channel.get("acp").is_some() {
        found.push(format!(
            "{place}: a process of its own hosts scripted channels alone"
        ));
    }
    for reaction in channel["on_event"].as_array().into_iter().flatten() {
        let extra = reaction
            .as_object()
            .into_iter()
            .flatten()
            .any(|(member, _)| !REACTION.contains(&member.as_str()));
        let action = reaction["do"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|action| {
                !action["call"]
                    .as_str()
                    .is_some_and(|call| call.starts_with("host."))
            });
        if extra || action {
            found.push(format!(
                "{place}: no gate nor action but a host verb in a process of its own"
            ));
        }
    }
    found
}

/// Whether `worker` declares `channel`, by name or by a prefix ending in `*`.
fn hosts(worker: &super::Worker, channel: &str) -> bool {
    worker.hosts.iter().flatten().any(|host| {
        host == channel
            || host
                .strip_suffix('*')
                .is_some_and(|prefix| channel.starts_with(prefix))
    })
}
