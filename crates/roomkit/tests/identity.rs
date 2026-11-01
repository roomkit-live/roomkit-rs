//! Identity resolution on the tokio runtime (RFC §10.1 step 5, §11): what a
//! scenario cannot say. The conformance scenarios `identity/*` pin what each
//! resolution outcome comes to; this pins the kit's own reads of its
//! settings, a channel's declaration, a resolver that fails, and the Python
//! tests a scenario stands for until the copy is synced.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::identity::{IdentityFuture, IdentityResolver, MockIdentityResolver};
use roomkit::model::{
    ChannelId, ChannelType, EventType, IdentificationStatus, IdentityResult, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ConversationStore, CreateRoomOptions, HookExecution,
    HookOptions, HookTrigger, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};

fn resolving(channel_types: Option<BTreeSet<ChannelType>>) -> RoomKit {
    RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(MockIdentityResolver::default())),
        identity_channel_types: channel_types,
        ..RoomKitOptions::default()
    })
}

// Python: tests/test_identity_pipeline.py::TestTheRestrictionIsAskedOnceAndAnsweredEverywhere::test_no_resolver_means_no_channel_type_is_enabled
#[test]
fn no_resolver_means_no_channel_type_is_enabled() {
    let kit = RoomKit::new(RoomKitOptions::default());
    assert_eq!(
        (
            kit.identity_enabled_for(&ChannelType::SMS),
            kit.identity_enabled_for(&ChannelType::WEBSOCKET),
        ),
        (false, false)
    );
}

// Python: tests/test_identity_pipeline.py::TestTheRestrictionIsAskedOnceAndAnsweredEverywhere::test_no_restriction_means_every_channel_type_is_enabled
#[test]
fn no_restriction_means_every_channel_type_is_enabled() {
    let kit = resolving(None);
    assert_eq!(
        (
            kit.identity_enabled_for(&ChannelType::SMS),
            kit.identity_enabled_for(&ChannelType::WEBSOCKET),
        ),
        (true, true)
    );
}

// Python: tests/test_identity_pipeline.py::TestTheRestrictionIsAskedOnceAndAnsweredEverywhere::test_a_restriction_answers_for_the_types_it_names_and_against_the_rest
#[test]
fn a_restriction_answers_for_the_types_it_names_and_against_the_rest() {
    let kit = resolving(Some(BTreeSet::from([ChannelType::SMS])));
    assert_eq!(
        (
            kit.identity_enabled_for(&ChannelType::SMS),
            kit.identity_enabled_for(&ChannelType::WEBSOCKET),
        ),
        (true, false)
    );
}

/// The kit reads back the resolver and the timeout it was built with, as
/// Python's `identity_resolver` and `identity_timeout` properties do.
#[test]
fn the_kit_reads_back_its_resolver_and_its_timeout() {
    let plain = RoomKit::new(RoomKitOptions::default());
    let kit = RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(MockIdentityResolver::default())),
        identity_timeout: Duration::from_millis(250),
        ..RoomKitOptions::default()
    });
    assert_eq!(
        (
            plain.identity_resolver().is_some(),
            plain.identity_timeout(),
            kit.identity_resolver().is_some(),
            kit.identity_timeout(),
        ),
        (
            false,
            Duration::from_secs(10),
            true,
            Duration::from_millis(250)
        )
    );
}

// Python: tests/test_identity_pipeline.py::TestWhichChannelsNameTheirOwnSenders::test_a_channel_that_carries_addresses_leaves_the_default
#[test]
fn a_channel_that_carries_addresses_leaves_the_default() {
    let transport = RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    assert!(!transport.sender_is_participant());
}

/// A resolver that raises.
struct Failing;

impl IdentityResolver for Failing {
    fn resolve<'a>(
        &'a self,
        _message: &'a InboundMessage,
        _context: &'a RoomContext,
    ) -> IdentityFuture<'a> {
        Box::pin(async { Err("the directory is down".into()) })
    }
}

/// What a resolver raises fails the message, as the Python implementation
/// propagates it out of `process_inbound`, with nothing of it stored.
#[tokio::test]
async fn a_resolver_that_raises_fails_the_message_with_nothing_stored() {
    let kit = RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(Failing)),
        ..RoomKitOptions::default()
    });
    let sms = ChannelId::new("sms").unwrap();
    kit.register_channel(RecordingTransport::new(
        sms.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("room_1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &sms, AttachChannelOptions::default())
        .await
        .unwrap();
    let message = InboundMessage::new(sms, "+15551234567", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    };
    let error = kit.process_inbound(message, options).await.unwrap_err();
    let messages = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.r#type == EventType::Message)
        .count();
    kit.close().await.unwrap();
    assert_eq!(
        (
            matches!(&error, RoomKitError::Identity(record) if record.message == "the directory is down"),
            messages,
        ),
        (true, 0)
    );
}

/// A resolver that refuses every sender, as Python's `UnknownResolver` of
/// `tests/test_identity_hooks.py`.
struct Rejecting;

impl IdentityResolver for Rejecting {
    fn resolve<'a>(
        &'a self,
        _message: &'a InboundMessage,
        _context: &'a RoomContext,
    ) -> IdentityFuture<'a> {
        Box::pin(async {
            Ok(IdentityResult {
                message: Some("Unknown sender".to_owned()),
                ..IdentityResult::new(IdentificationStatus::Rejected)
            })
        })
    }
}

// Python: tests/test_identity_hooks.py::TestIdentityUnknownHook::test_unknown_hook_fires
#[tokio::test]
async fn unknown_hook_fires() {
    let kit = RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(Rejecting)),
        ..RoomKitOptions::default()
    });
    let fired = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    kit.hook(
        HookTrigger::OnIdentityUnknown,
        move |_: RoomEvent, _| {
            let observed = Arc::clone(&observed);
            async move { observed.store(true, Ordering::SeqCst) }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "test_unknown".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let sms = ChannelId::new("sms1").unwrap();
    kit.register_channel(RecordingTransport::new(
        sms.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &sms, AttachChannelOptions::default())
        .await
        .unwrap();
    let message = InboundMessage::new(sms, "stranger", TextContent::new("hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            fired.load(Ordering::SeqCst)
        ),
        (true, Some("Unknown sender"), true)
    );
}
