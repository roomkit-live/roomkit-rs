//! The kit's timeline read, the Python implementation's `get_timeline`
//! (RFC §14.1, "Reading a timeline"): a page of what the room received, the
//! refused events on request, a room of another organization not found.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventContent, HookResult, InboundMessage, RoomId};
use roomkit::testing::RecordingTransport;
use roomkit::{KitError, RoomKit, StoreError, TimelineQuery, text_of};

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit whose room `room_1`, of the organization `acme`, received
/// "one", refused "spam" with a hook, then received "two" and "three".
async fn a_room_that_refused_one_message() -> RoomKit {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
    kit.register_channel(sms.clone()).unwrap();
    kit.for_organization("acme")
        .create_room(&room_id())
        .await
        .unwrap();
    kit.attach(&room_id(), sms.id()).await.unwrap();
    kit.before_broadcast("no_spam", |event, _| async move {
        if text_of(&event.content) == "spam" {
            Ok(HookResult::block("spam"))
        } else {
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    for text in ["one", "spam", "two", "three"] {
        let message =
            InboundMessage::new(sms.id().clone(), "+15551234567", EventContent::text(text));
        kit.process_inbound_in(&room_id(), message).await.unwrap();
    }
    kit
}

/// The texts of the page `query` reads.
async fn texts(kit: &RoomKit, query: TimelineQuery) -> Vec<String> {
    kit.get_timeline(&room_id(), &query)
        .await
        .unwrap()
        .iter()
        .map(|event| text_of(&event.content))
        .collect()
}

#[tokio::test]
async fn a_page_holds_what_the_room_received_and_the_refused_on_request() {
    let kit = a_room_that_refused_one_message().await;
    let newest = TimelineQuery {
        newest_first: true,
        limit: 3,
        ..TimelineQuery::default()
    };
    let audited = TimelineQuery {
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
    let other = kit
        .for_organization("globex")
        .get_timeline(&room_id(), &TimelineQuery::default())
        .await;
    let own = kit
        .for_organization("acme")
        .get_timeline(&room_id(), &TimelineQuery::default())
        .await;
    assert_eq!(
        (
            kit.get_timeline(&missing, &TimelineQuery::default())
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
