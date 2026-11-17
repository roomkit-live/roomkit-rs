//! A cascade's caller record, merged as one synchronous operation.

use super::{Metadata, ResponseMetadata, TURNS_KEY, Value};

impl ResponseMetadata {
    /// Internal cascade reporting: Python's `merge_caller_record` (RFC §6.4,
    /// §10.1 step 18). Existing readers keep this record. Concurrent reports
    /// retain each channel's entry, and every key write still counts.
    #[doc(hidden)]
    pub fn merge_caller_record(&self, record: &Metadata) {
        let mut inner = self.lock();
        let mut turns = match inner.data.get(TURNS_KEY) {
            Some(Value::Object(turns)) => turns.clone(),
            _ => Metadata::new(),
        };
        if let Some(Value::Object(ended)) = record.get(TURNS_KEY) {
            turns.extend(ended.clone());
        }
        // A scalar or empty `turns` survives when no channel entries replace
        // it. Its original key write also counts before the merged one.
        inner.writes += u64::try_from(record.len()).unwrap_or(u64::MAX);
        inner.wrote(|key| record.contains_key(key));
        inner.data.extend(record.clone());
        if !turns.is_empty() {
            inner.writes += 1;
            inner.wrote(|key| key == TURNS_KEY);
            inner.data.insert(TURNS_KEY.into(), Value::Object(turns));
        }
    }
}
