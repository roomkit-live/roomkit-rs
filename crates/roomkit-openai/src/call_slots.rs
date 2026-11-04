//! A streamed response's call fragments folded into calls, one slot per call:
//! Python's `ToolCallSlots`.
//!
//! A fragment belongs to the call its stream index names, unless it starts
//! another call: a server that sends each call whole may tag every one with
//! index 0, and folding them together would run one call made of two. No
//! composition event names a call here, so a call's id is its server's, empty
//! until it gives one no other call holds; [`CallIds`](roomkit_ai::CallIds)
//! mints the rest once the response is over.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::request::RawCall;

/// A call as its fragments built it so far.
#[derive(Debug, Default)]
struct Slot {
    id: String,
    name: String,
    arguments: String,
    /// Whether a fragment carried a function, even one with no name and no
    /// arguments: that is a call, for the loop to refuse.
    function: bool,
}

/// One fragment of a call, as a chunk carries it.
#[derive(Debug)]
pub(crate) struct Fragment<'a> {
    /// The stream index it names.
    pub(crate) index: u64,
    /// The server's id, when it gave one.
    pub(crate) id: Option<&'a str>,
    /// The tool's name, when it gave one.
    pub(crate) name: Option<&'a str>,
    /// The piece of its arguments' text.
    pub(crate) arguments: &'a str,
    /// Whether it carried a function.
    pub(crate) function: bool,
}

/// The calls of a streamed response, in the order they came.
#[derive(Debug, Default)]
pub(crate) struct CallSlots {
    slots: Vec<Slot>,
    by_index: HashMap<u64, usize>,
    taken: HashSet<String>,
}

impl CallSlots {
    /// Folds `fragment` in, into the call its index names unless it starts
    /// another. Opened by a new JSON object under no new id or name, it is a
    /// second call to the held call's tool.
    pub(crate) fn fold(&mut self, fragment: &Fragment<'_>) {
        let mut name = fragment.name.map(str::to_owned);
        let mut position = self.by_index.get(&fragment.index).copied();
        if let Some(held) = position.and_then(|at| self.slots.get(at))
            && held.starts_another_call(fragment)
        {
            if name.is_none() && fragment.id.is_none_or(|id| id == held.id) {
                name = Some(held.name.clone());
            }
            position = None;
        }
        let at = position.unwrap_or_else(|| self.new_slot(fragment.index));
        self.fill(at, fragment, name.as_deref());
    }

    fn new_slot(&mut self, index: u64) -> usize {
        self.slots.push(Slot::default());
        let at = self.slots.len() - 1;
        self.by_index.insert(index, at);
        at
    }

    /// Adds `fragment` to the call at `at`, named `name`: the server's id once
    /// no other call holds it, the name once given, the arguments appended.
    fn fill(&mut self, at: usize, fragment: &Fragment<'_>, name: Option<&str>) {
        let Some(slot) = self.slots.get_mut(at) else {
            return;
        };
        slot.function |= fragment.function;
        if let Some(id) = fragment.id
            && slot.id.is_empty()
            && !self.taken.contains(id)
        {
            id.clone_into(&mut slot.id);
            self.taken.insert(id.to_owned());
        }
        if let Some(name) = name {
            name.clone_into(&mut slot.name);
        }
        slot.arguments.push_str(fragment.arguments);
    }

    /// The calls once the response is over, each with its argument text: a
    /// slot that never carried a function is no call.
    pub(crate) fn finish(self) -> Vec<RawCall> {
        self.slots
            .into_iter()
            .filter(|slot| slot.function || !slot.name.is_empty() || !slot.arguments.is_empty())
            .map(|slot| RawCall::new(slot.id, slot.name, slot.arguments))
            .collect()
    }
}

impl Slot {
    /// Whether `fragment`, on this call's index, starts another call. Another
    /// server id says so. Without one: another name once this call is whole;
    /// the same name bringing arguments of its own after whole ones; or, after
    /// whole arguments, a fragment that opens another JSON object. A server may
    /// repeat a call's name on every fragment, so the same name with nothing or
    /// blanks starts nothing.
    fn starts_another_call(&self, fragment: &Fragment<'_>) -> bool {
        if fragment
            .id
            .is_some_and(|id| !self.id.is_empty() && id != self.id)
        {
            return true;
        }
        if self.name.is_empty() {
            return false;
        }
        let held = self.arguments.trim();
        let whole = held.is_empty() || serde_json::from_str::<Value>(held).is_ok();
        let Some(name) = fragment.name else {
            return !held.is_empty() && whole && fragment.arguments.trim_start().starts_with('{');
        };
        if name != self.name {
            return whole;
        }
        !fragment.arguments.trim().is_empty() && !held.is_empty() && whole
    }
}
