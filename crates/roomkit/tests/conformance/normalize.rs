//! What a run produced, normalized as the suite compares it: roomkit-specs
//! `conformance/README.md`, "Normalization". A room the run created is
//! `$r1`, `$r2`... in the order of its `room_created`; an event of a
//! timeline is `<room>#<rank>`, its rank among the room's events once the
//! lifecycle records (RFC §5.1, which an implementation MAY write) are left
//! out, and its `index` is that rank; any other event id is `$e1`, `$e2`...
//! by first appearance; a timestamp is `@1`, `@2`... by its order among the
//! run's instants.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{Map, Value};

const ID_KEYS: [&str; 5] = [
    "id",
    "event_id",
    "responds_to",
    "parent_event_id",
    "target_event_id",
];

/// A record the framework writes of a lifecycle change (RFC §5.1): its
/// source is the framework's, its visibility `internal`, its content a
/// system one.
pub(crate) fn is_lifecycle_record(event: &Value) -> bool {
    event["source"]["channel_id"] == "system"
        && event["visibility"] == "internal"
        && event["content"]["type"] == "system"
}

/// What a run produced, in the RFC's serialized form, before normalization.
#[derive(Debug, Default)]
pub(crate) struct Observed {
    pub(crate) literal_rooms: BTreeSet<String>,
    pub(crate) created_rooms: Vec<String>,
    pub(crate) timelines: BTreeMap<String, Vec<Value>>,
    pub(crate) rooms: BTreeMap<String, Value>,
    pub(crate) bindings: BTreeMap<String, Map<String, Value>>,
    pub(crate) framework_events: Vec<Value>,
    pub(crate) hooks: BTreeMap<String, Vec<Value>>,
    pub(crate) hook_order: Vec<String>,
    pub(crate) deliveries: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    pub(crate) turns: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    pub(crate) observations: BTreeMap<String, Vec<Value>>,
    pub(crate) tasks: BTreeMap<String, Vec<Value>>,
    pub(crate) steps: Vec<Value>,
    pub(crate) actions: BTreeMap<String, Vec<Value>>,
}

impl Observed {
    /// The created rooms that are not the scenario's own, in creation order.
    pub(crate) fn generated_rooms(&self) -> Vec<&String> {
        let mut seen = BTreeSet::new();
        self.created_rooms
            .iter()
            .filter(|room| !self.literal_rooms.contains(*room) && seen.insert(room.as_str()))
            .collect()
    }
}

/// Rewrites the ids, indexes and instants of one run's records.
pub(crate) struct Normalizer {
    rooms: HashMap<String, String>,
    events: HashMap<String, String>,
    kept: HashMap<String, Vec<u64>>,
    others: HashMap<String, String>,
    instants: Vec<String>,
}

impl Normalizer {
    pub(crate) fn new(observed: &Observed) -> Self {
        let mut rooms: HashMap<String, String> = observed
            .literal_rooms
            .iter()
            .map(|room| (room.clone(), room.clone()))
            .collect();
        for (position, room) in observed.generated_rooms().into_iter().enumerate() {
            rooms.insert(room.clone(), format!("$r{}", position + 1));
        }
        let mut events = HashMap::new();
        let mut kept = HashMap::new();
        for (room_id, timeline) in &observed.timelines {
            let alias = rooms
                .get(room_id)
                .cloned()
                .unwrap_or_else(|| room_id.clone());
            let held: Vec<&Value> = timeline
                .iter()
                .filter(|e| !is_lifecycle_record(e))
                .collect();
            kept.insert(
                room_id.clone(),
                held.iter().filter_map(|e| e["index"].as_u64()).collect(),
            );
            for (rank, event) in held.iter().enumerate() {
                if let Some(id) = event["id"].as_str() {
                    events.insert(id.to_owned(), format!("{alias}#{rank}"));
                }
            }
        }
        let mut instants: Vec<String> = Vec::new();
        collect_instants(&observed_values(observed), &mut instants);
        instants.sort_by_key(|instant| instant_key(instant));
        instants.dedup_by_key(|instant| instant_key(instant));
        let mut normalizer = Self {
            rooms,
            events,
            kept,
            others: HashMap::new(),
            instants,
        };
        // The ids of no timeline are named in the order the step outcomes,
        // then the framework events, show them first.
        normalizer.value(&Value::Array(observed.steps.clone()));
        normalizer.value(&Value::Array(observed.framework_events.clone()));
        normalizer
    }

    /// The alias of a room.
    pub(crate) fn room(&self, room_id: &str) -> String {
        self.rooms
            .get(room_id)
            .cloned()
            .unwrap_or_else(|| room_id.to_owned())
    }

    fn rank(&self, room_id: &str, index: u64) -> u64 {
        match self.kept.get(room_id) {
            None => index,
            Some(kept) => kept.partition_point(|held| *held < index) as u64,
        }
    }

    /// `value` with its ids, indexes and instants normalized.
    pub(crate) fn value(&mut self, value: &Value) -> Value {
        self.value_under(value, None)
    }

    fn value_under(&mut self, value: &Value, key: Option<&str>) -> Value {
        match value {
            Value::Object(object) => self.object(object),
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|item| self.value_under(item, key))
                    .collect(),
            ),
            Value::String(text) => Value::String(self.string(text, key)),
            scalar @ (Value::Null | Value::Bool(_) | Value::Number(_)) => scalar.clone(),
        }
    }

    fn object(&mut self, object: &Map<String, Value>) -> Value {
        let event_room = object
            .get("room_id")
            .and_then(Value::as_str)
            .filter(|_| object.contains_key("source") && object.contains_key("index"))
            .map(str::to_owned);
        let mut result = Map::new();
        for (key, item) in object {
            let normalized = match (&event_room, key.as_str(), item.as_u64()) {
                (Some(room_id), "index", Some(index)) => Value::from(self.rank(room_id, index)),
                _ => self.value_under(item, Some(key)),
            };
            result.insert(key.clone(), normalized);
        }
        Value::Object(result)
    }

    fn string(&mut self, text: &str, key: Option<&str>) -> String {
        if let Some(alias) = self.rooms.get(text) {
            return alias.clone();
        }
        if let Some(alias) = self.events.get(text) {
            return alias.clone();
        }
        if let Some(alias) = self.others.get(text) {
            return alias.clone();
        }
        if key.is_some_and(|key| ID_KEYS.contains(&key)) {
            let next = format!("$e{}", self.others.len() + 1);
            return self.others.entry(text.to_owned()).or_insert(next).clone();
        }
        if is_instant(text) {
            let key = instant_key(text);
            let rank = self
                .instants
                .partition_point(|held| instant_key(held) < key);
            return format!("@{}", rank + 1);
        }
        text.to_owned()
    }
}

fn observed_values(observed: &Observed) -> Value {
    let mut all = Vec::new();
    all.extend(observed.steps.iter().cloned());
    all.extend(observed.actions.values().flatten().cloned());
    all.extend(observed.framework_events.iter().cloned());
    all.extend(observed.hooks.values().flatten().cloned());
    for per_room in observed.turns.values().chain(observed.deliveries.values()) {
        all.extend(per_room.values().flatten().cloned());
    }
    all.extend(observed.timelines.values().flatten().cloned());
    all.extend(observed.rooms.values().cloned());
    all.extend(observed.bindings.values().map(|b| Value::Object(b.clone())));
    all.extend(observed.observations.values().flatten().cloned());
    all.extend(observed.tasks.values().flatten().cloned());
    Value::Array(all)
}

fn collect_instants(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::Object(object) => object
            .values()
            .for_each(|item| collect_instants(item, found)),
        Value::Array(items) => items.iter().for_each(|item| collect_instants(item, found)),
        Value::String(text) => {
            if is_instant(text) {
                found.push(text.clone());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Whether `text` is an RFC 3339 instant in UTC, as RFC §1.4 writes one.
fn is_instant(text: &str) -> bool {
    let bytes = text.as_bytes();
    let shape = |position: usize, byte: u8| bytes.get(position) == Some(&byte);
    bytes.len() >= 20
        && shape(4, b'-')
        && shape(7, b'-')
        && shape(10, b'T')
        && shape(13, b':')
        && shape(16, b':')
        && text.ends_with('Z')
        && bytes[..4].iter().all(u8::is_ascii_digit)
}

/// An instant as a sortable key: its date and time, then its fraction of a
/// second padded to nanoseconds, so `.75406` and `.754060` compare equal.
fn instant_key(text: &str) -> (String, String) {
    let body = text.trim_end_matches('Z');
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    (whole.to_owned(), format!("{fraction:0<9}"))
}
