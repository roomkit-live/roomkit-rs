//! The kit's timeline read, the Python implementation's `get_timeline`
//! (RFC §14.1, "Reading a timeline"): a page of what the room received, the
//! refused events on request, a room of another organization not found.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{ChannelId, ChannelType, HookResult, InboundMessage, RoomId, TextContent};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, GetTimelineOptions, KitError, ProcessInboundOptions,
    RoomKit, RoomKitOptions, StoreError, text_of,
};

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit whose room `room_1`, of the organization `acme`, received
/// "one", refused "spam" with a hook, then received "two" and "three".
async fn a_room_that_refused_one_message() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
    kit.register_channel(sms.clone()).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        organization_id: Some("acme".to_owned()),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    kit.attach_channel(&room_id(), sms.id(), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.before_broadcast("no_spam", |event, _| async move {
        if text_of(&event.content) == "spam" {
            Ok(HookResult::block("spam", BlockOptions::default()))
        } else {
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    for text in ["one", "spam", "two", "three"] {
        let message = InboundMessage::new(sms.id().clone(), "+15551234567", TextContent::new(text));
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    kit
}

/// The texts of the page `query` reads.
async fn texts(kit: &RoomKit, query: GetTimelineOptions) -> Vec<String> {
    kit.get_timeline(&room_id(), query)
        .await
        .unwrap()
        .iter()
        .map(|event| text_of(&event.content))
        .collect()
}

#[tokio::test]
async fn a_page_holds_what_the_room_received_and_the_refused_on_request() {
    let kit = a_room_that_refused_one_message().await;
    let newest = GetTimelineOptions {
        newest_first: true,
        limit: 3,
        ..GetTimelineOptions::default()
    };
    let audited = GetTimelineOptions {
        include_blocked: true,
        ..newest.clone()
    };
    assert_eq!(
        (texts(&kit, newest).await, texts(&kit, audited).await),
        (
            vec!["one".to_owned(), "two".to_owned(), "three".to_owned()],
            vec!["spam".to_owned(), "two".to_owned(), "three".to_owned()]
        )
    );
}

#[tokio::test]
async fn a_room_that_does_not_exist_or_is_another_organization_s_is_not_found() {
    let kit = a_room_that_refused_one_message().await;
    let missing = RoomId::new("room_9").unwrap();
    let of = |organization: &str| GetTimelineOptions {
        organization_id: Some(organization.to_owned()),
        ..Default::default()
    };
    let other = kit.get_timeline(&room_id(), of("globex")).await;
    let own = kit.get_timeline(&room_id(), of("acme")).await;
    assert_eq!(
        (
            kit.get_timeline(&missing, GetTimelineOptions::default())
                .await
                .err(),
            other.err(),
            own.map(|events| events.len()).ok()
        ),
        (
            Some(KitError::Store(StoreError::RoomNotFound(missing))),
            Some(KitError::Store(StoreError::RoomNotFound(room_id()))),
            Some(4)
        )
    );
}

/// The room is read before the query is checked, as the Python
/// implementation's `get_timeline` does: a room that does not exist is not
/// found, whatever its query names, both cursors included.
#[tokio::test]
async fn get_timeline_reads_the_room_before_the_query() {
    let kit = a_room_that_refused_one_message().await;
    let missing = RoomId::new("room_9").unwrap();
    let both_cursors = GetTimelineOptions {
        after_index: Some(0),
        before_index: Some(3),
        ..Default::default()
    };
    let read = kit.get_timeline(&missing, both_cursors).await;
    assert_eq!(
        read,
        Err(KitError::Store(StoreError::RoomNotFound(missing)))
    );
}
