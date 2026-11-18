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
        "channel.publish" => return publish_from_worker(run, room, spec).await,
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

/// A scripted channel publishes from the actual context its turn left it.
async fn publish_from_worker(run: &Run, room: RoomId, spec: &Value) -> Result<Called, Unreadable> {
    let worker: String = field(spec, "executor")?;
    let host = roomkit::current_turn_host().ok_or("channel.publish outside a worker turn")?;
    if crate::run::lock(&run.turn_workers).get(host.turn_id()) != Some(&worker) {
        return Err(format!("channel.publish did not run on executor {worker}"));
    }
    let workers = run.workers.as_ref().ok_or("no executor fixture")?;
    if let Some(gate) = optional::<String>(spec, "gate")? {
        run.gates.hold(&gate).await;
    }
    let channel: roomkit::model::ChannelId = field(spec, "channel_id")?;
    let mut event = EphemeralEvent::new(room, field(spec, "kind")?, channel.as_str());
    event.channel_id = Some(channel);
    event.data = optional(spec, "data")?.unwrap_or_default();
    host.publish_from_channel(event);
    let taken = workers.publication_taken(&worker).await;
    Ok(finish(run, "channel.publish", taken, |()| Value::Null))
}
