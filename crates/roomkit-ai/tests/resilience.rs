//! What a generation's retries and its fallback provider leave that a
//! scenario does not see (RFC §6.4; roomkit-specs
//! `conformance/scenarios/resilience/`): the turn's error and its source,
//! and a retry policy the channel refuses.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::error::Error;
use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource,
    RetryPolicy, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiError, ProviderError};

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A policy that tries once more, at once.
fn one_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 1,
        base_delay_seconds: 0.001,
        ..RetryPolicy::default()
    }
}

/// The provider errors of `error`'s source chain, outermost first.
fn provider_errors(error: &(dyn Error + 'static)) -> Vec<String> {
    let mut found = Vec::new();
    let mut next = Some(error);
    while let Some(error) = next {
        if let Some(failure) = error.downcast_ref::<ProviderError>() {
            found.push(failure.message.clone());
        }
        next = error.source();
    }
    found
}

// Python: tests/test_channels/test_ai_retry.py::TestFallbackProvider::test_fallback_failure_raises_original_error
#[tokio::test]
async fn a_fallback_that_fails_leaves_the_primary_s_error_its_own_the_source() {
    let primary = ScriptedProvider::new([
        ScriptedRound::answering("")
            .refusing(503, "primary error")
            .retrying(),
        ScriptedRound::answering("")
            .refusing(503, "primary error")
            .retrying(),
    ]);
    let fallback =
        ScriptedProvider::new([ScriptedRound::answering("").refusing(500, "fallback error")]);
    let options = AiChannelOptions {
        retry_policy: Some(one_retry()),
        fallback_provider: Some(Arc::new(fallback.clone())),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), primary.clone(), options).unwrap();
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(ChannelId::new("sms1").unwrap(), ChannelType::SMS),
        EventContent::Text(TextContent::new("hello")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(ChannelId::new("ai1").unwrap(), room_id(), ChannelType::AI)
    };
    let context = RoomContext::new(Room::new(room_id()));
    let error = agent.on_event(event, binding, context).await.unwrap_err();
    assert_eq!(
        (
            provider_errors(&*error),
            primary.requests().len(),
            fallback.requests().len()
        ),
        (
            vec!["primary error".to_owned(), "fallback error".to_owned()],
            2,
            1
        )
    );
}

#[test]
fn a_retry_policy_of_no_delay_is_refused() {
    let options = AiChannelOptions {
        retry_policy: Some(RetryPolicy {
            base_delay_seconds: 0.0,
            ..RetryPolicy::default()
        }),
        ..AiChannelOptions::default()
    };
    let refused = AiChannel::new(
        ChannelId::new("ai1").unwrap(),
        ScriptedProvider::new([]),
        options,
    )
    .err()
    .unwrap();
    assert!(
        matches!(&refused, AiError::RetryPolicy { reason } if reason.starts_with("base_delay_seconds: Input should be greater than 0")),
        "{refused}"
    );
}
