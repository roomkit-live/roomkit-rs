//! A streamed response's call fragments folded into calls, one slot per call:
//! Python's `ToolCallSlots`.
//!
//! A fragment belongs to the call its stream index names, unless it starts
//! another call: a server that sends each call whole may tag every one with
//! index 0, and folding them together would run one call made of two. No
//! composition event crosses here, so a call's id is its server's, empty until
//! it gives one no other call holds before the call is named;
//! [`CallIds`](roomkit_ai::CallIds) mints the rest once the response is over.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::request::RawCall;

/// A call as its fragments built it so far.
#[derive(Debug, Default)]
struct Slot {
    id: String,
    /// The server's id when it was no text (a number, `true`): the slot
    /// holds it as Python's does, and fails the read once it is a call.
    odd_id: Option<Value>,
    name: String,
    arguments: String,
    /// Whether a fragment carried a function, even one with no name and no
    /// arguments: that is a call, for the loop to refuse.
    function: bool,
    /// Whether a composition event would have named the call, on a wire that
    /// sends them: it keeps the id it had then.
    announced: bool,
}

/// A fragment's id, as Python's `ToolCallSlots` reads what the server wrote.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FragmentId<'a> {
    /// None: absent, `null` or empty text (Python's `None` or `""`).
    None,
    /// The server's id.
    Text(&'a str),
    /// No text, and read as false: `0`, `false`, `[]`, `{}`. No id, but no
    /// absent one either: it names no call the fragment could continue.
    False,
    /// No text, and read as true.
    True(&'a Value),
}

impl FragmentId<'_> {
    /// Whether Python reads it as true.
    fn is_true(self) -> bool {
        matches!(self, Self::Text(_) | Self::True(_))
    }
}

/// One fragment of a call, as a chunk carries it.
#[derive(Debug)]
pub(crate) struct Fragment<'a> {
    /// The stream index it names.
    pub(crate) index: u64,
    /// The server's id, as it wrote it.
    pub(crate) id: FragmentId<'a>,
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
    /// The ids that are no text a slot took.
    taken_odd: Vec<Value>,
}

impl CallSlots {
    /// Folds `fragment` in, into the call its index names unless it starts
    /// another. Opened by a new JSON object under no new id or name, or the
    /// held call's own, it is a second call to the held call's tool; under an
    /// id that is no text, a call with no name, for the loop to refuse.
    ///
    /// # Errors
    ///
    /// Why the read fails: an id that is no text and that no set can hold
    /// (a list, an object), where Python asks whether a call holds it.
    pub(crate) fn fold(&mut self, fragment: &Fragment<'_>) -> Result<(), String> {
        let mut name = fragment.name.map(str::to_owned);
        let mut position = self.by_index.get(&fragment.index).copied();
        if let Some(held) = position.and_then(|at| self.slots.get(at))
            && held.starts_another_call(fragment)
        {
            if name.is_none() && held.is_continued_by(fragment.id) {
                name = Some(held.name.clone());
            }
            position = None;
        }
        let at = position.unwrap_or_else(|| self.new_slot(fragment.index));
        self.fill(at, fragment, name.as_deref())
    }

    fn new_slot(&mut self, index: u64) -> usize {
        self.slots.push(Slot::default());
        let at = self.slots.len() - 1;
        self.by_index.insert(index, at);
        at
    }

    /// Adds `fragment` to the call at `at`, named `name`: the server's id once
    /// no other call holds it, the name once given, the arguments appended.
    fn fill(
        &mut self,
        at: usize,
        fragment: &Fragment<'_>,
        name: Option<&str>,
    ) -> Result<(), String> {
        let Some(slot) = self.slots.get_mut(at) else {
            return Ok(());
        };
        slot.function |= fragment.function;
        if fragment.id.is_true() && !slot.has_server_id() && !slot.announced {
            match fragment.id {
                FragmentId::Text(id) if !self.taken.contains(id) => {
                    id.clone_into(&mut slot.id);
                    self.taken.insert(id.to_owned());
                }
                FragmentId::True(id @ (Value::Array(_) | Value::Object(_))) => {
                    return Err(format!("a call's id is no text: {id}"));
                }
                FragmentId::True(id) if !self.taken_odd.contains(id) => {
                    slot.odd_id = Some(id.clone());
                    self.taken_odd.push(id.clone());
                }
                FragmentId::None
                | FragmentId::False
                | FragmentId::Text(_)
                | FragmentId::True(_) => {}
            }
        }
        let first_name = name.is_some() && slot.name.is_empty();
        if let Some(name) = name {
            name.clone_into(&mut slot.name);
        }
        slot.arguments.push_str(fragment.arguments);
        slot.announced |= !slot.name.is_empty() && (first_name || !fragment.arguments.is_empty());
        Ok(())
    }

    /// The calls once the response is over, each with its argument text: a
    /// slot that never carried a function is no call.
    ///
    /// # Errors
    ///
    /// Why the read fails: a call that took an id that is no text, which
    /// Python's call refuses as its id.
    pub(crate) fn finish(self) -> Result<Vec<RawCall>, String> {
        self.slots
            .into_iter()
            .filter(|slot| slot.function || !slot.name.is_empty() || !slot.arguments.is_empty())
            .map(|slot| match slot.odd_id {
                Some(id) => Err(format!("a call's id is no text: {id}")),
                None => Ok(RawCall::new(slot.id, slot.name, slot.arguments)),
            })
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
        if fragment.id.is_true() && self.has_server_id() && !self.holds(fragment.id) {
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

    /// Whether the call took the server's id, text or not.
    fn has_server_id(&self) -> bool {
        !self.id.is_empty() || self.odd_id.is_some()
    }

    /// Whether `id` is the id the call took.
    fn holds(&self, id: FragmentId<'_>) -> bool {
        match id {
            FragmentId::Text(id) => self.odd_id.is_none() && id == self.id,
            FragmentId::True(id) => self.odd_id.as_ref() == Some(id),
            FragmentId::None | FragmentId::False => false,
        }
    }

    /// Whether a fragment under `id` that starts another call with no name
    /// is a second call to this one's tool: under no id, or this call's own
    /// (Python's `call_id in (None, "", held["id"])`); an id that is no
    /// text and reads as false names no call.
    fn is_continued_by(&self, id: FragmentId<'_>) -> bool {
        matches!(id, FragmentId::None) || self.holds(id)
    }
}
