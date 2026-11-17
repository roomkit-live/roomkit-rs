//! The local registry projects fields into skill metadata. Applications reading
//! a document directly keep the key order and the text-or-list values.

use roomkit::model::Metadata;
use serde_json::Value;

use super::{Fields, Gates, SkillToolNames};

pub(super) trait Output: Default {
    const REGISTRY: bool;

    fn field(&mut self, field: usize, key: &str, value: Option<SkillToolNames>);
    fn gate(&mut self, gate: Gates);
}

impl Output for Fields {
    const REGISTRY: bool = true;

    fn field(&mut self, field: usize, key: &str, value: Option<SkillToolNames>) {
        let text = value.and_then(|value| match value {
            SkillToolNames::Text(text) => Some(text),
            SkillToolNames::List(items) => (!items.is_empty()).then(|| items.join(", ")),
        });
        if let Some(slot) = self.slot(field) {
            *slot = text;
        } else {
            self.record_extra(key, text);
        }
    }

    fn gate(&mut self, gate: Gates) {
        let held = if gate.key == "requires" {
            &mut self.requires
        } else {
            &mut self.allowed_tools
        };
        gate.finish(held);
    }
}

impl Output for Metadata {
    const REGISTRY: bool = false;

    fn field(&mut self, _field: usize, key: &str, value: Option<SkillToolNames>) {
        put(self, key, value);
    }

    fn gate(&mut self, gate: Gates) {
        put(self, gate.key, gate.value);
    }
}

pub(super) fn put(fields: &mut Metadata, key: &str, value: Option<SkillToolNames>) {
    let value = match value {
        Some(SkillToolNames::Text(text)) => Value::String(text),
        Some(SkillToolNames::List(items)) => Value::from(items),
        None => Value::Null,
    };
    fields.insert(key.to_owned(), value);
}
