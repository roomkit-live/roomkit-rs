//! The data a caller takes from its cascade, without driving its lifetime.

use std::collections::BTreeMap;

use roomkit_core::model::{
    ChannelId, DeliveryResult, ErrorRecord, Metadata, ResponseMetadata, RoomEvent,
};
use serde_json::Value;

use super::InboundResult;

/// The six data fields [`InboundResult::report_cascade`] reads from a cascade.
///
/// This borrowed view has no waiting or cancellation operations. Reporting
/// copies its values into the result's owned collections: later changes to the
/// source mapping, events or nested metadata require another report. Python's
/// delivery mapping and list elements remain aliased instead. The result's
/// own [`ResponseMetadata`] stays shared with its existing readers.
#[derive(Clone, Copy, Debug)]
pub struct CascadeReport<'a> {
    /// The cascade's transport deliveries, replacing the result's mapping.
    pub delivery_results: &'a BTreeMap<ChannelId, DeliveryResult>,
    /// Unavailable targets, in their original order, including repetitions.
    pub unavailable_targets: &'a [ChannelId],
    /// The caller record to merge; a live source record supplies `to_metadata()`.
    pub response_metadata: &'a Metadata,
    /// The cascade's responses, replacing the result's list in this order.
    pub response_events: &'a [RoomEvent],
    /// Its error, used only when the result has no earlier error.
    pub error: Option<&'a ErrorRecord>,
    /// Its cancellation reason, including `None` to clear an earlier reason.
    pub cancelled: Option<&'a str>,
}

impl InboundResult {
    /// Reports the cascade's current data without waiting for or cancelling it.
    ///
    /// Deliveries and responses replace their old collections. Availability
    /// is replaced unless this result is a duplicate, whose `None` or existing
    /// list stays unchanged. The caller record keeps its identity: ordinary
    /// keys overwrite, and `turns` entries merge by channel, replacing each
    /// incoming channel's whole entry. Each report counts those key writes,
    /// even when its values have not changed.
    ///
    /// The first error stays; cancellation is always replaced. The event,
    /// duplicate/block decision, reason and delivery handle are untouched.
    /// See [`CascadeReport`] for the owned-value translation from Python.
    pub fn report_cascade(&mut self, cascade: CascadeReport<'_>) {
        self.delivery_results.clone_from(cascade.delivery_results);
        if !self.duplicate {
            self.unavailable_targets = Some(cascade.unavailable_targets.to_vec());
        }
        merge_record(&self.response_metadata, cascade.response_metadata);
        self.response_events = cascade.response_events.to_vec();
        if self.error.is_none() {
            self.error = cascade.error.cloned();
        }
        self.cancellation_reason = cascade.cancelled.map(str::to_owned);
    }
}

/// Python's `merge_caller_record`, shared with the deferred result's backfill.
pub(super) fn merge_record(into: &ResponseMetadata, record: &Metadata) {
    let mut turns = match into.get("turns") {
        Some(Value::Object(turns)) => turns,
        _ => Metadata::new(),
    };
    if let Some(Value::Object(ended)) = record.get("turns") {
        turns.extend(ended.clone());
    }
    // Keep non-object and empty `turns` values when no channel entries replace
    // them. The original key write also counts before the merged one.
    into.update(record.clone());
    if !turns.is_empty() {
        into.insert("turns", Value::Object(turns));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roomkit_core::ops;
    use serde_json::json;

    use super::*;
    use crate::inbound::DeliveryHandle;
    use crate::tasks::Tasks;

    #[tokio::test]
    async fn deferred_wait_and_public_report_share_the_record_merge() {
        let decided = ops::InboundResult {
            response_metadata: Metadata::from_iter([("turns".into(), json!(3))]),
            ..Default::default()
        };
        let handle = DeliveryHandle::over(decided, Arc::new(Tasks::default()));
        let mut result = handle.wait().await.unwrap();
        let record = result.response_metadata.clone();
        assert_eq!(record.get("turns"), Some(json!(3)));
        assert_eq!(record.writes(), 1);

        result.report_cascade(CascadeReport {
            delivery_results: &BTreeMap::new(),
            unavailable_targets: &[],
            response_metadata: &Metadata::from_iter([(
                "turns".into(),
                json!({"a": {"end": "done"}}),
            )]),
            response_events: &[],
            error: None,
            cancelled: None,
        });
        assert_eq!(record.writes(), 3);
        let waited = handle.wait().await.unwrap();
        assert_eq!(
            waited.response_metadata.get("turns"),
            Some(json!({"a": {"end": "done"}}))
        );
        assert_eq!(record.writes(), 5);
    }
}
