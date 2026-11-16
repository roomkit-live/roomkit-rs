//! A room's ephemerals as a subscriber reads them (RFC §8.4, §17.2).

use std::sync::{Arc, Weak};

use roomkit::model::RoomId;
use roomkit::realtime::EphemeralEvent;
use roomkit::{PublishTypingOptions, SubscribeRoomOptions};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;

pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit_of(spec)?;
    let room = field::<RoomId>(spec, "room_id")?;
    let organization_id = optional::<String>(spec, "organization_id")?;
    Ok(match name {
        "subscribe_room" => {
            let subscriber = field::<String>(spec, "subscriber")?;
            run.record_subscriber(&subscriber, None);
            let recorder = Arc::downgrade(run);
            let record = move |event: EphemeralEvent| {
                let (recorder, subscriber) = (Weak::clone(&recorder), subscriber.clone());
                async move {
                    if let Some(run) = recorder.upgrade() {
                        let read = json!({
                            "type": event.r#type,
                            "room_id": event.room_id,
                            "user_id": event.user_id,
                        });
                        run.record_subscriber(&subscriber, Some(read));
                    }
                }
            };
            let options = SubscribeRoomOptions { organization_id };
            let outcome = kit.subscribe_room(&room, record, options).await;
            finish(run, name, outcome, |_| Value::Null)
        }
        "publish_typing" => {
            let options = PublishTypingOptions {
                is_typing: optional(spec, "is_typing")?.unwrap_or(true),
                data: None,
                organization_id,
            };
            let user = field::<String>(spec, "user_id")?;
            let published = kit.publish_typing(&room, &user, options).await;
            finish(run, name, published, |()| Value::Null)
        }
        other => return Err(format!("no call {other}")),
    })
}
