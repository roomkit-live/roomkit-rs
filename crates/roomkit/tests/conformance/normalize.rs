//! What a run produced, normalized as the suite compares it: roomkit-specs
//! `conformance/README.md`, "Normalization". A room the run created is
//! `$r1`, `$r2`... in the order of its `room_created`; an index is read
//! without the lifecycle records (RFC §5.1, which an implementation MAY
//! write) at or below it, an event's `index` and a room's `event_count`,
//! `latest_index` and `delivered_index` alike; an event of a timeline is
//! `<room>#<index>`; any other event id is `$e1`, `$e2`... by first
//! appearance; a timestamp is `@1`, `@2`... by its order among the run's
//! instants.

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
    /// The framework events of an agent's turn, apart (README, `ai_events`).
    pub(crate) ai_events: Vec<Value>,
    pub(crate) hooks: BTreeMap<String, Vec<Value>>,
    pub(crate) hook_order: Vec<String>,
    pub(crate) deliveries: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    /// Per room and scripted channel, the contexts its deliveries and its
    /// turns were handed.
    pub(crate) contexts: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    /// Per room and streaming channel, the items its `deliver_stream` read.
    pub(crate) streamed: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    /// Per room and scripted channel, its room callbacks (RFC §6.1).
    pub(crate) room_callbacks: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    pub(crate) turns: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    /// Per room and scripted agent with a scripted memory, the events its
    /// memory was handed.
    pub(crate) ingested: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    pub(crate) compacted: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    /// Per strategy with a scripted dispatch policy, the turns it was handed.
    pub(crate) dispatch_turns: BTreeMap<String, Vec<Value>>,
    pub(crate) observations: BTreeMap<String, Vec<Value>>,
    pub(crate) thinking_events: BTreeMap<String, Vec<Value>>,
    /// The tool call events each room's subscribers read (README: `tool_events`).
    pub(crate) tool_events: BTreeMap<String, Vec<Value>>,
    pub(crate) text_events: BTreeMap<String, Vec<Value>>,
    pub(crate) tasks: BTreeMap<String, Vec<Value>>,
    pub(crate) steps: Vec<Value>,
    pub(crate) actions: BTreeMap<String, Vec<Value>>,
    pub(crate) models: BTreeMap<String, Vec<Value>>,
    /// What each scripted ACP agent received, by its channel.
    pub(crate) acp_wire: BTreeMap<String, Vec<Value>>,
    pub(crate) acp_handlers: BTreeMap<String, Vec<Value>>,
    pub(crate) acp_permissions: BTreeMap<String, Vec<Value>>,
    pub(crate) acp_prompts: BTreeMap<String, Vec<Value>>,
    pub(crate) acp_mcp_requests: BTreeMap<String, Vec<Value>>,
    pub(crate) acp_context_requests: BTreeMap<String, Vec<Value>>,
    /// Per room, the participants the store holds.
    pub(crate) participants: BTreeMap<String, Vec<Value>>,
    /// The scripted resolver's calls, in order.
    pub(crate) resolver: Vec<Value>,
    /// The kit's media resolver's calls, in order: `{url, room_id}`.
    pub(crate) media_resolver: Vec<Value>,
    pub(crate) router_calls: Vec<Value>,
    /// Per scripted agent with a `router`, what it was asked, in order.
    pub(crate) ai_routes: BTreeMap<String, Vec<Value>>,
    /// The human-input requests the run showed, in the order it first
    /// showed them: `$p1`, `$p2`...
    pub(crate) requests: Vec<String>,
    /// The run's tool auditor's entries, and its session auditor's.
    pub(crate) tool_audit: Vec<Value>,
    pub(crate) session_audit: Vec<Value>,
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
    lifecycle: HashMap<String, Vec<i64>>,
    others: HashMap<String, String>,
    requests: HashMap<String, String>,
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
        let mut lifecycle = HashMap::new();
        for (room_id, timeline) in &observed.timelines {
            let alias = rooms
                .get(room_id)
                .cloned()
                .unwrap_or_else(|| room_id.clone());
            let records: Vec<i64> = timeline
                .iter()
                .filter(|event| is_lifecycle_record(event))
                .filter_map(|event| event["index"].as_i64())
                .collect();
            for event in timeline.iter().filter(|event| !is_lifecycle_record(event)) {
                if let (Some(id), Some(raw)) = (event["id"].as_str(), event["index"].as_i64()) {
                    let index = without_records(&records, raw);
                    events.insert(id.to_owned(), format!("{alias}#{index}"));
                }
            }
            lifecycle.insert(room_id.clone(), records);
        }
        let mut instants: Vec<String> = Vec::new();
        collect_instants(&observed_values(observed), &mut instants);
        instants.sort_by_key(|instant| instant_key(instant));
        instants.dedup_by_key(|instant| instant_key(instant));
        let mut normalizer = Self {
            rooms,
            events,
            lifecycle,
            others: HashMap::new(),
            requests: observed
                .requests
                .iter()
                .enumerate()
                .map(|(position, id)| (id.clone(), format!("$p{}", position + 1)))
                .collect(),
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

    /// `raw` without the room's lifecycle records at or below it.
    fn index(&self, room_id: &str, raw: i64) -> i64 {
        without_records(self.lifecycle.get(room_id).map_or(&[], Vec::as_slice), raw)
    }

    /// A count of indexes given out, without the lifecycle records'.
    fn count(&self, room_id: &str, raw: i64) -> i64 {
        let records = self.lifecycle.get(room_id).map_or(&[][..], Vec::as_slice);
        raw - i64::try_from(records.partition_point(|record| *record < raw)).unwrap_or(0)
    }

    /// The normalized value of `object`'s member `key`, when it holds an
    /// index of its room: an event's `index`, a room's counters.
    fn index_member(&self, object: &Map<String, Value>, key: &str, raw: i64) -> Option<Value> {
        let room_of = |member: &str| object.get(member).and_then(Value::as_str);
        if key == "index" && object.contains_key("source") {
            return room_of("room_id").map(|room| Value::from(self.index(room, raw)));
        }
        // The indexes a delivery set skipped (RFC §8.2), read as an event's.
        let skipped = object.get("event").and_then(Value::as_str) == Some("delivery_skipped");
        if skipped && matches!(key, "from_index" | "to_index") {
            return room_of("room_id").map(|room| Value::from(self.index(room, raw)));
        }
        if !(object.contains_key("event_count") && object.contains_key("status")) {
            return None;
        }
        let room = room_of("id")?;
        match key {
            "event_count" => Some(Value::from(self.count(room, raw))),
            "latest_index" => {
                let index = self.index(room, raw);
                Some(if index >= 0 {
                    Value::from(index)
                } else {
                    Value::Null
                })
            }
            "delivered_index" => Some(Value::from(self.index(room, raw).max(-1))),
            _ => None,
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
        // A participant's or an identity's id is the scenario's own (RFC
        // §5.5, §5.6), and an ACP option's id, are not event ids.
        let own_id = object.contains_key("identification")
            || object.contains_key("channel_addresses")
            || (matches!(
                object.get("type").and_then(Value::as_str),
                Some("select" | "boolean")
            ) && object.contains_key("currentValue"));
        let mut result = Map::new();
        for (key, item) in object {
            let index = item
                .as_i64()
                .and_then(|raw| self.index_member(object, key, raw));
            let normalized = match index {
                Some(index) => index,
                None if own_id && key == "id" => item.clone(),
                None => self.value_under(item, Some(key)),
            };
            result.insert(self.key(key), normalized);
        }
        Value::Object(result)
    }

    /// An object's key, an event of a timeline's id named as its event.
    fn key(&self, key: &str) -> String {
        self.events
            .get(key)
            .cloned()
            .unwrap_or_else(|| key.to_owned())
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
        if let Some(alias) = self.requests.get(text) {
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

/// `raw` without the lifecycle records (`records`, in index order) at or
/// below it.
pub(crate) fn without_records(records: &[i64], raw: i64) -> i64 {
    raw - i64::try_from(records.partition_point(|record| *record <= raw)).unwrap_or(0)
}

fn observed_values(observed: &Observed) -> Value {
    let mut all = Vec::new();
    all.extend(observed.steps.iter().cloned());
    all.extend(observed.actions.values().flatten().cloned());
    all.extend(observed.framework_events.iter().cloned());
    all.extend(observed.hooks.values().flatten().cloned());
    all.extend(observed.dispatch_turns.values().flatten().cloned());
    for per_room in observed
        .turns
        .values()
        .chain(observed.ingested.values())
        .chain(observed.deliveries.values())
        .chain(observed.streamed.values())
        .chain(observed.room_callbacks.values())
    {
        all.extend(per_room.values().flatten().cloned());
    }
    all.extend(observed.timelines.values().flatten().cloned());
    all.extend(observed.rooms.values().cloned());
    all.extend(observed.bindings.values().map(|b| Value::Object(b.clone())));
    all.extend(observed.observations.values().flatten().cloned());
    all.extend(observed.tasks.values().flatten().cloned());
    all.extend(observed.participants.values().flatten().cloned());
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

#[cfg(test)]
mod tests {
    use super::{Normalizer, Observed};
    use serde_json::json;

    #[test]
    fn acp_option_ids_are_not_event_ids() {
        let options = json!([
            {"id":"model","type":"select","currentValue":"opus"},
            {"id":"fast","type":"boolean","currentValue":true},
        ]);
        let observed = Observed {
            steps: vec![json!({"result": options})],
            ..Observed::default()
        };
        let mut normalizer = Normalizer::new(&observed);
        assert_eq!(normalizer.value(&options), options);
        assert_eq!(
            normalizer.value(&json!({"configId":"model"})),
            json!({"configId":"model"})
        );
        assert_eq!(
            normalizer.value(&json!({"event_id":"unrecorded"})),
            json!({"event_id":"$e1"})
        );
    }
}
