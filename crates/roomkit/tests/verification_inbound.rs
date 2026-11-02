//! The inbound pipeline's verification step on the tokio runtime (RFC
//! §10.1 step 5b, §11.7.4): what the shared scenarios (roomkit-specs
//! `conformance/scenarios/verification/`) cannot say, a code a scenario
//! does not know, each test translating the Python implementation's test
//! of its name.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeMap;
use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::identity::{MockIdentityResolver, MockIdentityResolverOptions};
use roomkit::model::{
    ChannelId, ChannelType, EventContent, Identity, IdentityId, InboundMessage, RoomId,
    TextContent, VerificationPolicy, VerificationStatus,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions,
    RequestVerificationOptions, RoomKit, RoomKitOptions, VerificationOfOptions,
};

const ALICE: &str = "+15550000001";

fn alice() -> IdentityId {
    IdentityId::new("id-alice").unwrap()
}

fn sms() -> ChannelId {
    ChannelId::new("sms").unwrap()
}

fn room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// A kit verifying its senders, Alice's number resolving to `id-alice`, with
/// the room `room-1` its SMS channel is attached to (Python's `_kit`).
async fn kit() -> RoomKit {
    let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
        mapping: BTreeMap::from([(ALICE.to_owned(), Identity::new(alice()))]),
        ..MockIdentityResolverOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(resolver)),
        verification_policy: Some(VerificationPolicy::default()),
        ..RoomKitOptions::default()
    });
    kit.register_channel(RecordingTransport::new(
        sms(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room(), &sms(), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// Alice's message `body` into the room: whether it was blocked, and why.
async fn text(kit: &RoomKit, body: &str) -> (bool, Option<String>) {
    let message = InboundMessage::new(sms(), ALICE, TextContent::new(body));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    (result.blocked, result.reason)
}

/// The texts the room's timeline holds.
async fn bodies(kit: &RoomKit) -> Vec<String> {
    let options = ListEventsOptions {
        limit: 50,
        ..ListEventsOptions::default()
    };
    let events = kit.store().list_events(&room(), options).await.unwrap();
    events
        .into_iter()
        .filter_map(|event| {
            if let EventContent::Text(content) = event.content {
                Some(content.body)
            } else {
                None
            }
        })
        .collect()
}

// Python: tests/test_verification_inbound.py::test_a_code_reply_is_consumed_and_verifies
#[tokio::test]
async fn a_code_reply_is_consumed_and_verifies() {
    let kit = kit().await;
    text(&kit, "hello").await;
    let options = RequestVerificationOptions {
        method: "code".to_owned(),
        ..RequestVerificationOptions::default()
    };
    let (request, code) = kit
        .request_verification(&alice(), &room(), &sms(), options)
        .await
        .unwrap();
    let wrong = format!("{:06}", (code.parse::<u32>().unwrap() + 1) % 1_000_000);

    let refused = text(&kit, &wrong).await;
    let accepted = text(&kit, &format!(" {code} ")).await;

    let consumed = (true, Some("verification_code".to_owned()));
    assert_eq!((refused, accepted), (consumed.clone(), consumed));
    let stored = bodies(&kit).await.join(" ");
    assert!(
        !stored.contains(&code) && !stored.contains(&wrong),
        "{stored}"
    );
    let state = kit
        .verification_of(&alice(), VerificationOfOptions::default())
        .await
        .unwrap();
    let request = kit
        .store()
        .get_verification_request(&request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (state.status, request.attempts),
        (VerificationStatus::Verified, 1)
    );
    kit.close().await.unwrap();
}

/// A code reply is attempted in its request's room only (RFC §11.7.4, "in
/// its room"; Python's `request.room_id != room_id`): the same digits in
/// another room are an ordinary message, held there, the request left
/// open with no attempt counted.
#[tokio::test]
async fn a_code_reply_in_another_room_is_an_ordinary_message() {
    let kit = kit().await;
    let other = RoomId::new("room-2").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(other.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&other, &sms(), AttachChannelOptions::default())
        .await
        .unwrap();
    let options = RequestVerificationOptions {
        method: "code".to_owned(),
        ..RequestVerificationOptions::default()
    };
    let (request, code) = kit
        .request_verification(&alice(), &room(), &sms(), options)
        .await
        .unwrap();

    let message = InboundMessage::new(sms(), ALICE, TextContent::new(code));
    let options = ProcessInboundOptions {
        room_id: Some(other),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();

    let request = kit
        .store()
        .get_verification_request(&request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (result.blocked, result.event.is_some(), request.attempts),
        (false, true, 0)
    );
    kit.close().await.unwrap();
}
