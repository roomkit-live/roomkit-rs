//! Scripted strategies installed on a room (roomkit-specs
//! `conformance/README.md`, "Setup"; RFC §19.7): a host's own strategy, the
//! agents it names by their channel ids, what its install does (a gate,
//! kit calls made as a reaction's, then a refusal of the room) and what its
//! own uninstall step does. One object stands for each id, whatever the
//! room it is installed in.

use std::collections::HashMap;
use std::sync::Arc;

use roomkit::model::{ChannelId, RoomId};
use roomkit::orchestration::{StrategyAgent, StrategyFuture};
use roomkit::{
    ClaimRoomStrategyOptions, InstallStrategyOptions, Orchestration, RoomKitError,
    RoomStrategyOptions, UninstallStrategyOptions,
};
use serde_json::{Map, Value, json};

use crate::calls::{Called, Unreadable, field, finish};
use crate::identity::RunSlot;
use crate::run::{Run, Scope};

/// A strategy the kit holds, as the scenarios' runner builds it.
pub(crate) type Strategy = Arc<dyn Orchestration<crate::Store>>;

/// The members of a scripted strategy, and of its install and uninstall.
pub(crate) const STRATEGY: &[&str] = &["id", "agents", "install", "uninstall"];
pub(crate) const STRATEGY_STEP: &[&str] = &["gate", "do", "refuse"];
pub(crate) const STRATEGY_UNINSTALL: &[&str] = &["gate", "do"];

/// A scripted strategy: it attaches its agents by id, then does what its
/// install says, as a reaction does, in the room it is installed in.
struct ScriptedStrategy {
    agents: Vec<ChannelId>,
    install: Value,
    uninstall: Value,
    run: RunSlot,
}

impl Orchestration<crate::Store> for ScriptedStrategy {
    fn agents(&self) -> Vec<StrategyAgent> {
        self.agents
            .iter()
            .cloned()
            .map(StrategyAgent::from)
            .collect()
    }

    fn install<'call>(
        &'call self,
        _kit: &'call crate::Kit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            step(&self.run, &self.install, room_id).await;
            match self.install["refuse"].as_str() {
                Some(reason) => Err(RoomKitError::Value(reason.to_owned())),
                None => Ok(()),
            }
        })
    }

    fn uninstall<'call>(
        &'call self,
        _kit: &'call crate::Kit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            step(&self.run, &self.uninstall, room_id).await;
            Ok(())
        })
    }
}

/// Holds at the step's gate, then makes its actions, `$room` the room.
async fn step(slot: &RunSlot, step: &Value, room_id: &RoomId) {
    let Some(run) = slot.run() else { return };
    let reaction: Map<String, Value> = ["gate", "do"]
        .into_iter()
        .filter_map(|key| step.get(key).map(|value| (key.to_owned(), value.clone())))
        .collect();
    let scope = Scope::in_room(room_id.as_str());
    // A strategy's step names no failure: `perform` fails only on one.
    drop(crate::scripted::perform(&run, &Value::Object(reaction), &scope).await);
}

/// The scripted strategies of a scenario's setup, by id, each acting for
/// the run `slot` holds once it is set, and the one `setup.kit.orchestration`
/// names; a strategy the runner cannot read is a problem of the setup.
pub(crate) fn strategies(
    setup: &Value,
    slot: &RunSlot,
) -> Result<(HashMap<String, Strategy>, Option<Strategy>), Unreadable> {
    let mut strategies = HashMap::new();
    for spec in setup["strategies"].as_array().into_iter().flatten() {
        let id = field::<String>(spec, "id")?;
        let agents = crate::calls::optional::<Vec<ChannelId>>(spec, "agents")?.unwrap_or_default();
        let strategy: Strategy = Arc::new(ScriptedStrategy {
            agents,
            install: spec["install"].clone(),
            uninstall: spec["uninstall"].clone(),
            run: slot.clone(),
        });
        strategies.insert(id, strategy);
    }
    let default = match setup["kit"].get("orchestration") {
        None => None,
        Some(name) => {
            let name = name
                .as_str()
                .ok_or("setup.kit.orchestration names no strategy")?;
            let named = strategies.get(name).cloned();
            Some(named.ok_or_else(|| format!("no scripted strategy {name:?}"))?)
        }
    };
    Ok((strategies, default))
}

/// The strategy `name` names among the run's.
pub(crate) fn named(run: &Run, name: &str) -> Result<Strategy, Unreadable> {
    crate::run::lock(&run.strategies)
        .get(name)
        .cloned()
        .ok_or_else(|| format!("no scripted strategy {name:?}"))
}

/// The strategy a `create_room` call names: an id, `null` for none, the
/// kit's default when absent.
#[expect(
    clippy::option_option,
    reason = "CreateRoomOptions::orchestration's three cases"
)]
pub(crate) fn room_orchestration(
    run: &Run,
    spec: &Value,
) -> Result<Option<Option<Strategy>>, Unreadable> {
    match spec.get("orchestration") {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(name) => {
            let name = name.as_str().ok_or("orchestration names no strategy")?;
            Ok(Some(Some(named(run, name)?)))
        }
    }
}

/// The arguments of the strategy calls, for the shape check.
pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        "install_strategy" | "claim_room_strategy" => &["room_id", "strategy"],
        "uninstall_strategy" | "room_strategy" => &["room_id"],
        _ => return None,
    })
}

/// Makes the strategy call `name`.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let room = field::<RoomId>(spec, "room_id")?;
    let organization_id = spec["organization_id"].as_str().map(str::to_owned);
    let kit = run.kit();
    Ok(match name {
        "install_strategy" => {
            let strategy = named(run, &field::<String>(spec, "strategy")?)?;
            let options = InstallStrategyOptions { organization_id };
            let installed = kit.install_strategy(&room, strategy, options).await;
            finish(run, name, installed, |()| Value::Null)
        }
        "claim_room_strategy" => {
            let strategy = named(run, &field::<String>(spec, "strategy")?)?;
            let options = ClaimRoomStrategyOptions { organization_id };
            let claimed = kit.claim_room_strategy(&room, strategy, options).await;
            finish(run, name, claimed, |()| Value::Null)
        }
        "uninstall_strategy" => {
            let options = UninstallStrategyOptions { organization_id };
            let uninstalled = kit.uninstall_strategy(&room, options).await;
            finish(run, name, uninstalled, |held| json!(held))
        }
        _ => {
            let options = RoomStrategyOptions { organization_id };
            Ok(strategy_id(run, kit.room_strategy(&room, options))?)
        }
    })
}

/// The id of the scripted strategy `strategy` is, null for none.
fn strategy_id(run: &Run, strategy: Option<Strategy>) -> Result<Value, Unreadable> {
    let Some(strategy) = strategy else {
        return Ok(Value::Null);
    };
    crate::run::lock(&run.strategies)
        .iter()
        .find(|(_, scripted)| Arc::ptr_eq(scripted, &strategy))
        .map(|(id, _)| json!(id))
        .ok_or_else(|| "the room holds a strategy no scenario named".to_owned())
}
