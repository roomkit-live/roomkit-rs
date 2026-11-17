//! Python: `tests/orchestration/test_speak_queue_change.py`.
//! Public queue-change data, without exercising executor presence (RFC §19.7.5).

use std::collections::BTreeSet;

use roomkit::model::RoomId;
use roomkit::orchestration::SpeakQueueChange as OrchestrationChange;
use roomkit::orchestration::strategies::SpeakQueueChange as DiscussionChange;
use roomkit::{SpeakQueue, SpeakQueueChange, SpeakQueueEvent};
use serde_json::json;

#[test]
fn turn_skipped_is_a_public_queue_change_and_json_string() {
    let change: SpeakQueueChange = serde_json::from_str("\"turn_skipped\"").unwrap();
    let orchestration: OrchestrationChange = change;
    let discussion: DiscussionChange = change;
    assert_eq!(orchestration, discussion);
    assert_eq!(format!("{change:?}"), "TurnSkipped");
    assert_eq!(serde_json::to_string(&change).unwrap(), "\"turn_skipped\"");
    assert_eq!(
        serde_json::from_str::<SpeakQueueChange>(&serde_json::to_string(&change).unwrap()).unwrap(),
        change
    );
}

#[test]
fn a_queue_event_keeps_turn_skipped_and_its_default_fields() {
    let queue = SpeakQueue {
        speaking: None,
        queue: Vec::new(),
        listening: BTreeSet::new(),
        asked: Vec::new(),
        waiting: false,
        over: false,
    };
    let change = serde_json::from_value(json!("turn_skipped")).unwrap();
    let room_id = RoomId::new("r1").unwrap();
    let event = SpeakQueueEvent::new(room_id.clone(), queue.clone(), change);
    assert_eq!(event.room_id, room_id);
    assert_eq!(event.queue, queue);
    assert_eq!(event.change, change);
    assert_eq!(event.channel_ids, Vec::new());
    assert_eq!(event.event_id, None);
    assert_eq!(
        json!({"change": event.change}),
        json!({"change": "turn_skipped"})
    );
}
