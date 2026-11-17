//! Concurrent callers of the public report share one response record.

use std::sync::Barrier;

use roomkit::InboundResult;
use roomkit::model::ResponseMetadata;
use serde_json::json;

use super::{Report, metadata};

#[test]
fn concurrent_reports_keep_both_turns_in_the_shared_record() {
    let record = ResponseMetadata::new();
    let start = Barrier::new(2);
    std::thread::scope(|scope| {
        for name in ["a", "b"] {
            let record = record.clone();
            let start = &start;
            scope.spawn(move || {
                let mut result = InboundResult {
                    response_metadata: record,
                    ..Default::default()
                };
                let report = Report {
                    // Copy enough independent values for the threads to overlap
                    // even when the test runner is pinned to a single CPU.
                    record: metadata(json!({
                        "payload": vec![json!({"sample": 1}); 16_384],
                        "turns": {name: {"end": "done"}},
                    })),
                    ..Default::default()
                };
                start.wait();
                result.report_cascade(report.view());
            });
        }
    });
    assert_eq!(
        record.get("turns"),
        Some(json!({"a": {"end": "done"}, "b": {"end": "done"}}))
    );
    assert_eq!(record.writes(), 6);
}

#[test]
fn a_report_preserves_a_value_edit_that_overlaps_its_merge() {
    let record = ResponseMetadata::from(metadata(json!({"turns": {"seed": 1}})));
    let editing = Barrier::new(2);
    let reported = Barrier::new(2);
    let runs = std::thread::scope(|scope| {
        let editor = scope.spawn(|| {
            let mut runs = 0;
            record.with_value("turns", |turns| {
                runs += 1;
                if runs == 1 {
                    editing.wait();
                    reported.wait();
                }
                turns
                    .as_object_mut()
                    .unwrap()
                    .insert("edited".into(), json!(true));
            });
            runs
        });
        editing.wait();
        let mut result = InboundResult {
            response_metadata: record.clone(),
            ..Default::default()
        };
        result.report_cascade(
            Report {
                record: metadata(json!({"turns": {"reported": 1}})),
                ..Default::default()
            }
            .view(),
        );
        reported.wait();
        editor.join().unwrap()
    });
    assert_eq!(runs, 2);
    assert_eq!(
        record.get("turns"),
        Some(json!({"seed": 1, "reported": 1, "edited": true}))
    );
    assert_eq!(record.writes(), 2);
}
