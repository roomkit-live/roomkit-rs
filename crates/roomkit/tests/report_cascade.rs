//! Python `tests/test_report_cascade.py`, with owned source values as approved
//! for Rust: later source mutations become visible only on another report.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeMap;

use roomkit::model::{
    ChannelId, ChannelType, DeliveryResult, DeliveryResultStatus, ErrorRecord, EventSource,
    InboundMessage, Metadata, ResponseMetadata, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CascadeReport, CreateRoomOptions, InboundResult, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use serde_json::{Value, json};

#[path = "report_cascade/concurrent.rs"]
mod concurrent;

#[derive(Default)]
struct Report {
    deliveries: BTreeMap<ChannelId, DeliveryResult>,
    unavailable: Vec<ChannelId>,
    record: Metadata,
    events: Vec<RoomEvent>,
    error: Option<ErrorRecord>,
    cancelled: Option<String>,
}

impl Report {
    fn view(&self) -> CascadeReport<'_> {
        CascadeReport {
            delivery_results: &self.deliveries,
            unavailable_targets: &self.unavailable,
            response_metadata: &self.record,
            response_events: &self.events,
            error: self.error.as_ref(),
            cancelled: self.cancelled.as_deref(),
        }
    }
}

fn channel(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn metadata(value: Value) -> Metadata {
    serde_json::from_value(value).unwrap()
}

fn event(body: &str) -> RoomEvent {
    RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(channel("sms"), ChannelType::SMS),
        TextContent::new(body),
    )
}

#[test]
fn deliveries_replace_the_mapping_and_copy_its_values() {
    let mut result = InboundResult {
        delivery_results: BTreeMap::from([(
            channel("old"),
            DeliveryResult::new(channel("old"), DeliveryResultStatus::Sent),
        )]),
        ..Default::default()
    };
    let mut report = Report {
        deliveries: BTreeMap::from([(
            channel("sms"),
            DeliveryResult::new(channel("sms"), DeliveryResultStatus::Sent),
        )]),
        ..Default::default()
    };
    result.report_cascade(report.view());
    assert_eq!(result.delivery_results, report.deliveries);
    report.deliveries.insert(
        channel("late"),
        DeliveryResult::new(channel("late"), DeliveryResultStatus::Queued),
    );
    report
        .deliveries
        .get_mut(&channel("sms"))
        .unwrap()
        .provider_message_id = Some("provider-1".to_owned());
    assert_eq!(result.delivery_results.len(), 1);
    assert_eq!(
        result.delivery_results[&channel("sms")].provider_message_id,
        None
    );
    result.report_cascade(report.view());
    assert_eq!(result.delivery_results, report.deliveries);
    report.deliveries.clear();
    result.report_cascade(report.view());
    assert!(result.delivery_results.is_empty());
}

#[test]
fn availability_is_copied_except_on_a_duplicate() {
    for duplicate in [false, true] {
        for initial in [None, Some(vec![channel("previous")])] {
            let mut result = InboundResult {
                duplicate,
                unavailable_targets: initial.clone(),
                ..Default::default()
            };
            let mut report = Report {
                unavailable: vec![channel("missing"), channel("missing"), channel("absent")],
                ..Default::default()
            };
            let expected = if duplicate {
                initial.clone()
            } else {
                Some(report.unavailable.clone())
            };
            result.report_cascade(report.view());
            assert_eq!(result.unavailable_targets, expected);
            report.unavailable.clear();
            assert_eq!(result.unavailable_targets, expected);
            result.report_cascade(report.view());
            assert_eq!(
                result.unavailable_targets,
                if duplicate { initial } else { Some(vec![]) }
            );
        }
    }
}

#[test]
fn reporting_keeps_the_shared_target_record_and_copies_source_values() {
    let record = ResponseMetadata::from(metadata(json!({"keep": 1, "replace": "old"})));
    let mut result = InboundResult {
        response_metadata: record.clone(),
        ..Default::default()
    };
    let mut report = Report {
        record: metadata(json!({"replace": "new", "added": [1]})),
        ..Default::default()
    };
    let before = record.writes();
    result.report_cascade(report.view());
    assert_eq!(
        record.to_metadata(),
        metadata(json!({"keep": 1, "replace": "new", "added": [1]}))
    );
    assert_eq!(record.writes() - before, 2);
    report.record.insert("replace".into(), json!("later"));
    report.record["added"]
        .as_array_mut()
        .unwrap()
        .push(json!(2));
    assert_eq!(record.get("replace"), Some(json!("new")));
    assert_eq!(record.get("added"), Some(json!([1])));
    assert_eq!(record.writes() - before, 2);
    result.response_metadata.insert("from-result", json!(true));
    assert_eq!(record.get("from-result"), Some(json!(true)));
}

#[test]
fn turns_merge_per_channel_and_repeated_reports_count_every_write() {
    let cases = [
        (json!({}), json!({}), json!({}), 0),
        (
            json!({"turns": {"a": {"end": "old"}}}),
            json!({}),
            json!({"turns": {"a": {"end": "old"}}}),
            1,
        ),
        (json!({}), json!({"turns": {}}), json!({"turns": {}}), 1),
        (json!({}), json!({"turns": 3}), json!({"turns": 3}), 1),
        (
            json!({"turns": "old"}),
            json!({}),
            json!({"turns": "old"}),
            0,
        ),
        (
            json!({"turns": "old"}),
            json!({"turns": null}),
            json!({"turns": null}),
            1,
        ),
        (
            json!({"turns": {"a": {"end": "old"}}}),
            json!({"turns": 3}),
            json!({"turns": {"a": {"end": "old"}}}),
            2,
        ),
        (
            json!({"turns": {"a": {"end": "old"}}}),
            json!({"turns": {"b": {"end": "new"}}}),
            json!({"turns": {"a": {"end": "old"}, "b": {"end": "new"}}}),
            2,
        ),
        (
            json!({"turns": {"a": {"end": "old", "usage": 1}, "b": {"end": "kept"}}}),
            json!({"turns": {"a": {"end": "new"}}}),
            json!({"turns": {"a": {"end": "new"}, "b": {"end": "kept"}}}),
            2,
        ),
    ];
    for (initial, incoming, expected, writes) in cases {
        let record = ResponseMetadata::from(metadata(initial));
        let mut result = InboundResult {
            response_metadata: record.clone(),
            ..Default::default()
        };
        let report = Report {
            record: metadata(incoming.clone()),
            ..Default::default()
        };
        let before = record.writes();
        for count in 1..=2 {
            result.report_cascade(report.view());
            assert_eq!(record.to_metadata(), metadata(expected.clone()));
            assert_eq!(record.writes() - before, count * writes);
            assert_eq!(report.record, metadata(incoming.clone()));
        }
    }
}

#[test]
fn responses_replace_the_list_and_copy_its_events() {
    let first = event("first");
    let second = event("second");
    let mut result = InboundResult {
        response_events: vec![event("previous")],
        ..Default::default()
    };
    let mut report = Report {
        events: vec![second.clone(), first, second],
        ..Default::default()
    };
    result.report_cascade(report.view());
    assert_eq!(result.response_events, report.events);
    report.events[0]
        .metadata
        .insert("later".into(), json!(true));
    assert!(!result.response_events[0].metadata.contains_key("later"));
    report.events.clear();
    assert_eq!(result.response_events.len(), 3);
    result.report_cascade(report.view());
    assert_eq!(result.response_events, Vec::<RoomEvent>::new());
}

#[test]
fn the_first_error_stays_and_cancellation_is_always_replaced() {
    for existing in [None, Some(ErrorRecord::new("ValueError", "earlier"))] {
        for incoming in [None, Some(ErrorRecord::new("RuntimeError", "cascade"))] {
            for cancelled in [None, Some("cut")] {
                let mut result = InboundResult {
                    error: existing.clone(),
                    cancellation_reason: Some("previous".into()),
                    ..Default::default()
                };
                let mut report = Report {
                    error: incoming.clone(),
                    cancelled: cancelled.map(str::to_owned),
                    ..Default::default()
                };
                let expected = existing.clone().or(incoming.clone());
                result.report_cascade(report.view());
                assert_eq!(result.error, expected);
                assert_eq!(result.cancellation_reason.as_deref(), cancelled);
                report.error = Some(ErrorRecord::new("RuntimeError", "later"));
                report.cancelled = None;
                result.report_cascade(report.view());
                assert_eq!(result.error, expected.or(report.error));
                assert_eq!(result.cancellation_reason, None);
            }
        }
    }
}

#[tokio::test]
async fn reporting_keeps_the_decision_event_and_actual_delivery_handle() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let room = kit.create_room(CreateRoomOptions::default()).await.unwrap();
    kit.attach_channel(&room.id, &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let mut result = kit
        .process_inbound(
            InboundMessage::new(channel("sms"), "sender", TextContent::new("hello")),
            ProcessInboundOptions {
                room_id: Some(room.id),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let handle = result.delivery.clone().expect("a deferred handle");
    handle.wait().await.unwrap();
    let event = result.event.clone();
    result.blocked = true;
    result.duplicate = true;
    result.reason = Some("decided".to_owned());
    result.report_cascade(Report::default().view());
    assert_eq!(result.event, event);
    assert!(result.blocked && result.duplicate);
    assert_eq!(result.reason.as_deref(), Some("decided"));
    assert_eq!(result.delivery, Some(handle));
    kit.close().await.unwrap();
}
