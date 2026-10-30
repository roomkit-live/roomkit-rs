//! Some endpoints omit output from the terminal event. Completed item events
//! then supply the snapshot, in output-index order, only after terminal success.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::wire;
use crate::error::OpenAiError;

#[derive(Default)]
pub(super) struct Items {
    completed: BTreeMap<u64, Value>,
    pending: BTreeSet<u64>,
}

impl Items {
    pub(super) fn record(&mut self, event: &Value, done: bool) -> Result<(), OpenAiError> {
        let index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .filter(|index| *index < 4096)
            .ok_or_else(|| wire::malformed("Responses item has an invalid output index"))?;
        let item = event
            .get("item")
            .filter(|item| item.is_object())
            .ok_or_else(|| wire::malformed("Responses item event has no item"))?;
        if done {
            if let Some(previous) = self.completed.get(&index)
                && previous != item
            {
                return Err(wire::malformed("Responses completed items conflict"));
            }
            self.completed.insert(index, item.clone());
            self.pending.remove(&index);
        } else if !self.completed.contains_key(&index) {
            self.pending.insert(index);
        }
        Ok(())
    }

    pub(super) fn finish(&self, response: &mut Value) -> Result<(), OpenAiError> {
        // A full snapshot remains authoritative. Missing/non-array output is
        // invalid, not an invitation to repair an unknown response format.
        if !response
            .get("output")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return Ok(());
        }
        if !self.pending.is_empty()
            && response.get("status").and_then(Value::as_str) == Some("completed")
        {
            return Err(wire::malformed(
                "Responses ended with unfinished output items",
            ));
        }
        let mut output = Vec::with_capacity(self.completed.len());
        for (expected, (index, item)) in self.completed.iter().enumerate() {
            if *index != expected as u64 {
                return Err(wire::malformed(
                    "Responses completed output has missing items",
                ));
            }
            output.push(item.clone());
        }
        if let Some(object) = response.as_object_mut() {
            object.insert("output".into(), Value::Array(output));
        }
        Ok(())
    }
}
