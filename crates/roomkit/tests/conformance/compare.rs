//! How a run's records are compared with a scenario's expectations
//! (roomkit-specs `conformance/README.md`, "Expectations", "Normalization",
//! "Matching").

use std::collections::{BTreeSet, HashMap};

use serde_json::{Value, json};

use crate::matching::{Names, framework_mismatches, mismatches};
use crate::normalize::{Normalizer, Observed, is_lifecycle_record};
use crate::run::{Run, lock};

pub(crate) fn compare(run: &Run, scenario: &Value, observed: &Observed) -> Vec<String> {
    let mut normalizer = Normalizer::new(observed);
    // What the `$as` of the steps and of `expect` named (README, "Matching").
    let mut names = Names::default();
    let mut found = Vec::new();
    for check in lock(&run.checks).iter() {
        found.extend(outcome_mismatches(
            check.expect.as_ref(),
            &check.outcome,
            (&mut normalizer, &mut names),
            &check.path,
        ));
    }
    found.extend(lifecycle_never_delivered(observed));
    found.extend(indexes_increase(observed));
    let rooms = observed
        .timelines
        .keys()
        .map(|room| (normalizer.room(room), room.clone()))
        .collect();
    let mut comparison = Comparison {
        normalizer,
        observed,
        expect: &scenario["expect"],
        rooms,
        found,
        names,
    };
    comparison.records();
    comparison.hooks();
    comparison.channel_calls();
    comparison.side_effects();
    comparison.models();
    comparison.found
}

/// The comparison of a run's records with a scenario's expectations, the
/// rooms named by their aliases.
struct Comparison<'a> {
    normalizer: Normalizer,
    observed: &'a Observed,
    expect: &'a Value,
    rooms: HashMap<String, String>,
    found: Vec<String>,
    names: Names,
}

impl Comparison<'_> {
    fn room_of(&self, alias: &str) -> String {
        self.rooms
            .get(alias)
            .cloned()
            .unwrap_or_else(|| alias.to_owned())
    }

    /// The records of the rooms the run created and the scenario does not
    /// name, in the order of their `room_created`: `$created` in `rooms`
    /// (README, "Normalization"), null for one with no record.
    fn created(&self) -> Value {
        let mut minted: Vec<(u64, &String)> = self
            .rooms
            .iter()
            .filter_map(|(alias, room)| {
                let rank = alias.strip_prefix("$r")?.parse().ok()?;
                Some((rank, room))
            })
            .collect();
        minted.sort_unstable();
        Value::Array(
            minted
                .into_iter()
                .map(|(_, room)| {
                    self.observed
                        .rooms
                        .get(room)
                        .cloned()
                        .unwrap_or(Value::Null)
                })
                .collect(),
        )
    }

    fn check(&mut self, expected: &Value, actual: Option<&Value>, path: &str) {
        let actual = actual.map(|actual| self.normalizer.value(actual));
        self.found
            .extend(mismatches(expected, actual.as_ref(), path, &mut self.names));
    }

    fn records(&mut self) {
        let expect = self.expect;
        for (alias, expected) in expect["timeline"].as_object().into_iter().flatten() {
            let kept: Vec<Value> = self
                .observed
                .timelines
                .get(&self.room_of(alias))
                .into_iter()
                .flatten()
                .filter(|event| !is_lifecycle_record(event))
                .cloned()
                .collect();
            self.check(
                expected,
                Some(&Value::Array(kept)),
                &format!("timeline.{alias}"),
            );
        }
        for (alias, expected) in expect["rooms"].as_object().into_iter().flatten() {
            if alias == "$created" {
                let created = self.created();
                self.check(expected, Some(&created), "rooms.$created");
                continue;
            }
            let room = self.observed.rooms.get(&self.room_of(alias));
            self.check(expected, room, &format!("rooms.{alias}"));
        }
        for (alias, channels) in expect["bindings"].as_object().into_iter().flatten() {
            let held = self.observed.bindings.get(&self.room_of(alias));
            for (channel, expected) in channels.as_object().into_iter().flatten() {
                let binding = held.and_then(|held| held.get(channel));
                self.check(expected, binding, &format!("bindings.{alias}.{channel}"));
            }
        }
        for (alias, expected) in expect["lifecycle"].as_object().into_iter().flatten() {
            let records: Vec<Value> = self
                .observed
                .timelines
                .get(&self.room_of(alias))
                .into_iter()
                .flatten()
                .filter(|event| is_lifecycle_record(event))
                .cloned()
                .collect();
            // An implementation that writes no lifecycle record passes (RFC
            // §5.1).
            if !records.is_empty() {
                self.check(
                    expected,
                    Some(&Value::Array(records)),
                    &format!("lifecycle.{alias}"),
                );
            }
        }
        let events = framework_events(
            (&mut self.normalizer, &mut self.names),
            &self.observed.framework_events,
            (&expect["framework_events"], "framework_events"),
        );
        self.found.extend(events);
        let events = framework_events(
            (&mut self.normalizer, &mut self.names),
            &self.observed.ai_events,
            (&expect["ai_events"], "ai_events"),
        );
        self.found.extend(events);
    }

    fn hooks(&mut self) {
        let expect = self.expect;
        for (name, expected) in expect["hooks"].as_object().into_iter().flatten() {
            let calls = self.observed.hooks.get(name).cloned().unwrap_or_default();
            self.check(
                expected,
                Some(&Value::Array(calls)),
                &format!("hooks.{name}"),
            );
        }
        if let Some(order) = expect["hook_order"].as_array() {
            let named: BTreeSet<&str> = order.iter().filter_map(Value::as_str).collect();
            let actual: Vec<Value> = self
                .observed
                .hook_order
                .iter()
                .filter(|name| named.contains(name.as_str()))
                .map(|name| Value::String(name.clone()))
                .collect();
            self.check(
                &expect["hook_order"],
                Some(&Value::Array(actual)),
                "hook_order",
            );
        }
    }

    fn channel_calls(&mut self) {
        let expect = self.expect;
        for (kind, recorded) in [
            ("deliveries", &self.observed.deliveries),
            ("contexts", &self.observed.contexts),
            ("streamed", &self.observed.streamed),
            ("room_callbacks", &self.observed.room_callbacks),
            ("turns", &self.observed.turns),
            ("ingested", &self.observed.ingested),
            ("compacted", &self.observed.compacted),
        ] {
            for (alias, channels) in expect[kind].as_object().into_iter().flatten() {
                let per_room = recorded.get(&self.room_of(alias));
                for (channel, expected) in channels.as_object().into_iter().flatten() {
                    let events = per_room
                        .and_then(|per_room| per_room.get(channel))
                        .cloned()
                        .unwrap_or_default();
                    let path = format!("{kind}.{alias}.{channel}");
                    self.check(expected, Some(&Value::Array(events)), &path);
                }
            }
        }
    }

    fn models(&mut self) {
        let expect = self.expect;
        for (strategy, expected) in expect["dispatch_turns"].as_object().into_iter().flatten() {
            let turns = self
                .observed
                .dispatch_turns
                .get(strategy)
                .cloned()
                .unwrap_or_default();
            let path = format!("dispatch_turns.{strategy}");
            self.check(expected, Some(&Value::Array(turns)), &path);
        }
        if let Some(expected) = expect.get("resolver") {
            let calls = Value::Array(self.observed.resolver.clone());
            self.check(expected, Some(&calls), "resolver");
        }
        if let Some(expected) = expect.get("router_calls") {
            let calls = Value::Array(self.observed.router_calls.clone());
            self.check(expected, Some(&calls), "router_calls");
        }
        for (agent, expected) in expect["ai_routes"].as_object().into_iter().flatten() {
            let asked = self
                .observed
                .ai_routes
                .get(agent)
                .cloned()
                .unwrap_or_default();
            self.check(
                expected,
                Some(&Value::Array(asked)),
                &format!("ai_routes.{agent}"),
            );
        }
        if let Some(expected) = expect.get("media_resolver") {
            let calls = Value::Array(self.observed.media_resolver.clone());
            self.check(expected, Some(&calls), "media_resolver");
        }
        for (kind, entries) in [
            ("tool_audit", &self.observed.tool_audit),
            ("session_audit", &self.observed.session_audit),
        ] {
            if let Some(expected) = expect.get(kind) {
                self.check(expected, Some(&Value::Array(entries.clone())), kind);
            }
        }
        for (channel, expected) in expect["models"].as_object().into_iter().flatten() {
            let requests = self
                .observed
                .models
                .get(channel)
                .cloned()
                .unwrap_or_default();
            self.check(
                expected,
                Some(&Value::Array(requests)),
                &format!("models.{channel}"),
            );
        }
        self.acp();
    }

    fn acp(&mut self) {
        for (kind, recorded) in [
            ("acp_wire", &self.observed.acp_wire),
            ("acp_permissions", &self.observed.acp_permissions),
            ("acp_handlers", &self.observed.acp_handlers),
            ("acp_prompts", &self.observed.acp_prompts),
            ("acp_mcp_requests", &self.observed.acp_mcp_requests),
        ] {
            for (channel, expected) in self.expect[kind].as_object().into_iter().flatten() {
                let requests = recorded.get(channel).cloned().unwrap_or_default();
                self.check(
                    expected,
                    Some(&Value::Array(requests)),
                    &format!("{kind}.{channel}"),
                );
            }
        }
    }

    fn side_effects(&mut self) {
        let expect = self.expect;
        for (kind, recorded) in [
            ("observations", &self.observed.observations),
            ("thinking_events", &self.observed.thinking_events),
            ("tool_events", &self.observed.tool_events),
            ("text_events", &self.observed.text_events),
            ("tasks", &self.observed.tasks),
            ("participants", &self.observed.participants),
        ] {
            for (alias, expected) in expect[kind].as_object().into_iter().flatten() {
                let items = recorded
                    .get(&self.room_of(alias))
                    .cloned()
                    .unwrap_or_default();
                self.check(
                    expected,
                    Some(&Value::Array(items)),
                    &format!("{kind}.{alias}"),
                );
            }
        }
        for (label, expected) in expect["actions"].as_object().into_iter().flatten() {
            let outcomes = self
                .observed
                .actions
                .get(label)
                .cloned()
                .unwrap_or_default();
            let path = format!("actions.{label}");
            let found = outcomes_of(
                expected,
                &outcomes,
                (&mut self.normalizer, &mut self.names),
                &path,
            );
            self.found.extend(found);
        }
    }
}

/// A label's outcomes, in order, or in any order under `$unordered`.
fn outcomes_of(
    expected: &Value,
    outcomes: &[Value],
    (normalizer, names): (&mut Normalizer, &mut Names),
    path: &str,
) -> Vec<String> {
    let unordered = expected.get("$unordered").and_then(Value::as_array);
    let wanted = unordered
        .or_else(|| expected.as_array())
        .cloned()
        .unwrap_or_default();
    if outcomes.len() != wanted.len() {
        return vec![format!(
            "{path}: expected {} runs, got {}",
            wanted.len(),
            outcomes.len()
        )];
    }
    if unordered.is_none() {
        return wanted
            .iter()
            .zip(outcomes)
            .enumerate()
            .flat_map(|(position, (want, got))| {
                let path = format!("{path}[{position}]");
                outcome_mismatches(Some(want), got, (normalizer, names), &path)
            })
            .collect();
    }
    let mut remaining: Vec<&Value> = outcomes.iter().collect();
    for want in &wanted {
        let position = remaining.iter().position(|got| {
            let mut trial = names.clone();
            let matched =
                outcome_mismatches(Some(want), got, (normalizer, &mut trial), path).is_empty();
            if matched {
                *names = trial;
            }
            matched
        });
        match position {
            Some(position) => {
                remaining.remove(position);
            }
            None => return vec![format!("{path}: no outcome matches {want}")],
        }
    }
    Vec::new()
}

fn framework_events(
    (normalizer, names): (&mut Normalizer, &mut Names),
    recorded: &[Value],
    (expected, member): (&Value, &str),
) -> Vec<String> {
    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    let events = normalizer.value(&Value::Array(recorded.to_vec()));
    for event in events.as_array().into_iter().flatten() {
        let room = event["room_id"].as_str().unwrap_or("*").to_owned();
        grouped.entry(room).or_default().push(event.clone());
    }
    let mut found = Vec::new();
    for (alias, events) in expected.as_object().into_iter().flatten() {
        let actual = grouped.get(alias).cloned().unwrap_or_default();
        let events = events.as_array().cloned().unwrap_or_default();
        found.extend(framework_mismatches(
            &events,
            &actual,
            &format!("{member}.{alias}"),
            names,
        ));
    }
    found
}

fn outcome_mismatches(
    expected: Option<&Value>,
    outcome: &Value,
    (normalizer, names): (&mut Normalizer, &mut Names),
    path: &str,
) -> Vec<String> {
    let error = &outcome["error"];
    let empty = json!({});
    let expected = expected.unwrap_or(&empty);
    if let Some(want) = expected.get("error") {
        let want = if want.is_string() {
            json!({ "name": want })
        } else {
            want.clone()
        };
        if error.is_null() {
            return vec![format!(
                "{path}: expected the error {want}, the call returned {}",
                outcome["result"]
            )];
        }
        return mismatches(&want, Some(error), &format!("{path}.error"), names);
    }
    if !error.is_null() {
        return vec![format!("{path}: unexpected error {error}")];
    }
    match expected.get("result") {
        Some(want) => {
            let actual = normalizer.value(&outcome["result"]);
            mismatches(want, Some(&actual), &format!("{path}.result"), names)
        }
        None => Vec::new(),
    }
}

/// A room's indexes are given out once each (RFC §8.1).
fn indexes_increase(observed: &Observed) -> Vec<String> {
    let mut found = Vec::new();
    for (room, events) in &observed.timelines {
        let indexes: Vec<Option<u64>> =
            events.iter().map(|event| event["index"].as_u64()).collect();
        if indexes.windows(2).any(|pair| pair[1] <= pair[0]) {
            found.push(format!(
                "timeline.{room}: an index is held twice: {indexes:?}"
            ));
        }
    }
    found
}

/// A lifecycle record (RFC §5.1) is never delivered, nor handed to a turn.
fn lifecycle_never_delivered(observed: &Observed) -> Vec<String> {
    let records: BTreeSet<&str> = observed
        .timelines
        .values()
        .flatten()
        .filter(|event| is_lifecycle_record(event))
        .filter_map(|event| event["id"].as_str())
        .collect();
    let mut found = Vec::new();
    for (kind, recorded) in [
        ("deliveries", &observed.deliveries),
        ("turns", &observed.turns),
    ] {
        for (room, channels) in recorded {
            for (channel, events) in channels {
                if events
                    .iter()
                    .any(|event| event["id"].as_str().is_some_and(|id| records.contains(id)))
                {
                    found.push(format!(
                        "{kind}.{room}.{channel}: lifecycle record delivered"
                    ));
                }
            }
        }
    }
    found
}
