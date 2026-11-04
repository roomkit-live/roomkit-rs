//! What a turn's reader was told of its generations, for the record of a
//! turn cut before its loop's end could tell it (RFC §6.4): Python's
//! `_GenerationsRead`.

use roomkit_core::model::Metadata;
use serde_json::Value;

use super::{LoopEndMarker, LoopEndReason, UsageMarker};

/// The record's key holding what the turn's generations used.
const AI_USAGE: &str = "ai_usage";
/// The record's key naming why the loop ended.
const LOOP_END_REASON: &str = "loop_end_reason";

/// A generation told its usage ends one. The next counts as in flight from
/// the end of the calls that followed it, or from a segment break, until it
/// tells: the safe side, a reader that stops right there reading incomplete.
/// A provider's own calls run within a generation and change nothing.
#[derive(Debug)]
pub(super) struct GenerationsRead {
    usage: Metadata,
    last_usage: Option<Metadata>,
    complete: bool,
    model: Option<String>,
    provider: Option<String>,
    in_flight: bool,
    open_calls: usize,
}

impl Default for GenerationsRead {
    fn default() -> Self {
        Self {
            usage: Metadata::new(),
            last_usage: None,
            complete: true,
            model: None,
            provider: None,
            in_flight: true,
            open_calls: 0,
        }
    }
}

impl GenerationsRead {
    /// A generation told what it used.
    pub(super) fn told(&mut self, marker: &UsageMarker) {
        add_usage(&mut self.usage, &marker.usage);
        self.last_usage = Some(marker.usage.clone());
        self.complete = self.complete && reports_counters(&marker.usage);
        self.model.clone_from(&marker.model);
        self.provider.clone_from(&marker.provider);
        self.in_flight = false;
    }

    /// A call started.
    pub(super) fn call_started(&mut self) {
        self.open_calls = self.open_calls.saturating_add(1);
    }

    /// A call ended: the last one open lets the next generation start.
    pub(super) fn call_ended(&mut self) {
        self.open_calls = self.open_calls.saturating_sub(1);
        if self.open_calls == 0 {
            self.in_flight = true;
        }
    }

    /// The loop goes on after a round without a call.
    pub(super) fn next_generation(&mut self) {
        self.in_flight = true;
    }

    /// The record of a turn cut here, as its loop's end would have written
    /// it from what the generations told; with no counter told, no usage. A
    /// stream that told nothing has no more to say than its end: an ACP
    /// agent's or a text-only channel's reports no usage at all, and a first
    /// generation in flight cannot be told from them.
    pub(super) fn cut_record(&self) -> Metadata {
        if self.last_usage.is_none() {
            let mut record = Metadata::new();
            record.insert(LOOP_END_REASON.to_owned(), Value::from("cancelled"));
            return record;
        }
        let marker = LoopEndMarker {
            usage: self.usage.clone(),
            last_usage: self.last_usage.clone(),
            usage_complete: Some(self.complete && !self.in_flight),
            model: self.model.clone(),
            provider: self.provider.clone(),
            ..LoopEndMarker::new(LoopEndReason::Cancelled)
        };
        let mut record = marker.record();
        if self.usage.is_empty() {
            record.shift_remove(AI_USAGE);
        }
        record
    }
}

/// Whether `used` holds a generation's input and output counters.
fn reports_counters(used: &Metadata) -> bool {
    ["input_tokens", "output_tokens"]
        .iter()
        .all(|key| used.get(*key).and_then(Value::as_u64).is_some())
}

/// Adds `used`'s integer counters to `total`'s.
fn add_usage(total: &mut Metadata, used: &Metadata) {
    for (name, used) in used {
        let Some(used) = used.as_i64() else {
            continue;
        };
        let sum = total
            .get(name)
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .saturating_add(used);
        total.insert(name.clone(), Value::from(sum));
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[derive(Clone, Copy)]
    enum Read {
        Told,
        Start,
        End,
        Break,
    }

    fn told() -> UsageMarker {
        UsageMarker {
            usage: json!({ "input_tokens": 5, "output_tokens": 2 })
                .as_object()
                .cloned()
                .unwrap_or_default(),
            ..UsageMarker::default()
        }
    }

    // Python: tests/test_channels/test_loop_end_marker.py::test_a_cut_reads_incomplete_with_a_generation_in_flight
    #[test]
    fn a_cut_reads_incomplete_with_a_generation_in_flight() {
        use Read::{Break, End, Start, Told};
        for (items, complete) in [
            (vec![Told], true),
            (vec![Told, Start], true),
            (vec![Told, Start, Start, End], true),
            (vec![Told, Start, End], false),
            (vec![Told, Break], false),
            (vec![Start, End, Told], true),
            (vec![Told, Start, End, Start], false),
            (vec![Told, Start, End, Start, End, Told], true),
        ] {
            let mut read = GenerationsRead::default();
            for item in &items {
                match item {
                    Told => read.told(&told()),
                    Start => read.call_started(),
                    End => read.call_ended(),
                    Break => read.next_generation(),
                }
            }
            assert_eq!(read.cut_record()["ai_usage_complete"], json!(complete));
        }
    }

    // Python: tests/test_channels/test_loop_end_marker.py::test_a_stream_that_told_nothing_records_only_its_end
    #[test]
    fn a_stream_that_told_nothing_records_only_its_end() {
        let record = GenerationsRead::default().cut_record();
        assert_eq!(
            Value::Object(record),
            json!({ "loop_end_reason": "cancelled" })
        );
    }
}
