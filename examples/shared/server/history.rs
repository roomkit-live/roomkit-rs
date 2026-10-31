//! What a connection is sent of its conversation's past as it opens, as an
//! integrator of the Python implementation reads it: the newest page of
//! the timeline (`get_timeline`), then what the `websocket` channel may
//! know of it (`visible_events`, RFC §7.5 rule 8).

use std::collections::HashMap;
use std::error::Error;

use roomkit::model::{ParticipantId, RoomContext, RoomId};
use roomkit::{ConversationStore, GetTimelineOptions, visible_events};
use serde_json::{Value, json};

use super::App;
use crate::row_fields;

impl App {
    /// The `history` frame of the room `room_id`: `{"type": "history",
    /// "room_id": "...", "events": [...]}`, each of the room's newest
    /// events the channel may know, oldest first, as a delivery frames it,
    /// with its participant's `name` when it has one. The page is cut
    /// before the channel's view is taken, as in the Python implementation,
    /// so it may hold fewer events than asked. A binding that does not
    /// read is sent none, the check `visible_events` leaves to its caller.
    pub(super) async fn history_frame(
        &self,
        room_id: &RoomId,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let query = GetTimelineOptions {
            newest_first: true,
            limit: self.history,
            ..GetTimelineOptions::default()
        };
        let page = self.kit.get_timeline(room_id, query).await?;
        let (Some(room), bindings, participants) =
            self.kit.store().load_room_context(room_id).await?
        else {
            return Err(format!("room {room_id} is gone").into());
        };
        let reads = bindings
            .iter()
            .find(|binding| binding.channel_id == *self.websocket.id())
            .is_some_and(|binding| binding.access.reads());
        let mut context = RoomContext::new(room);
        context.bindings = bindings;
        context.participants = participants;
        context.recent_events = if reads { page } else { Vec::new() };
        let names: HashMap<&ParticipantId, &str> = context
            .participants
            .iter()
            .filter_map(|person| Some((&person.id, person.display_name.as_deref()?)))
            .collect();
        let events: Vec<Value> = visible_events(&context, self.websocket.id())
            .iter()
            .map(|event| {
                let mut fields = row_fields(event);
                let name = event
                    .source
                    .participant_id
                    .as_ref()
                    .and_then(|id| names.get(id));
                if let Some(name) = name {
                    fields.insert("name".to_owned(), json!(name));
                }
                Value::Object(fields)
            })
            .collect();
        let frame = json!({ "type": "history", "room_id": room_id, "events": events });
        Ok(frame.to_string())
    }
}
