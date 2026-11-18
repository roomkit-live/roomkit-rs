//! Application errors through the public owner and standalone executor.
use super::{Agent, State, channel, kit_with_options, options, room};
use roomkit::turns::wire::{ChannelControl, ErrorRecord};
use roomkit::turns::{ChannelCall, ErrorClasses};
use roomkit::{RoomKitError, RoomKitOptions, TurnExecutor, TurnExecutorOptions};
use serde_json::json;
use std::error::Error;
use std::future::ready;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[error("quota {0}")]
pub(super) struct Quota(pub(super) u64);

fn classes() -> ErrorClasses {
    let mut classes = ErrorClasses::default();
    classes.register(
        "billing:Quota",
        |record| record.attributes.get("limit")?.as_u64().map(Quota),
        |error: &Quota| {
            let mut record = ErrorRecord::new("billing:Quota", error.to_string());
            record.args = vec![json!(error.0)];
            record.attributes.insert("limit".into(), json!(error.0));
            record.bases = vec!["billing:Failure".into()];
            record
        },
    );
    classes
}

#[tokio::test]
async fn the_owner_and_its_executor_share_application_error_classes() {
    let (kit, _, _) = kit_with_options(RoomKitOptions {
        error_classes: classes(),
        ..Default::default()
    })
    .await;
    let error = kit
        .channel_call(&channel("agent"), &room(), "quota", options())
        .await
        .unwrap_err();
    assert_eq!(
        error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<Quota>()
            .unwrap()
            .0,
        7
    );
    let wire = ErrorClasses::default().record(&error);
    assert_eq!(wire.kind, "billing:Quota");
    assert_eq!(wire.args, [json!(7)]);
    assert_eq!(wire.attributes.get("limit"), Some(&json!(7)));
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_standalone_worker_uses_its_own_error_registry() {
    for (classes, expected) in [
        (classes(), "billing:Quota"),
        (ErrorClasses::default(), "ChannelError"),
    ] {
        let worker = TurnExecutor::new(
            |_| ready(Ok(Some(Agent(Arc::new(State::default()))))),
            TurnExecutorOptions {
                error_classes: classes,
                ..Default::default()
            },
        )
        .unwrap();
        let call = ChannelCall::new(channel("agent"), room(), "quota");
        let answer = worker
            .control(ChannelControl::ChannelCall(Box::new(call)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer.error.unwrap().kind, expected);
        worker.close().await;
    }
}

#[test]
fn a_foreign_subclass_retains_its_record_when_its_nearest_registered_base_is_used() {
    let registry = classes();
    let mut original = ErrorRecord::new("shop:PlanQuota", "plan limit");
    original.bases = vec!["shop:Unavailable".into(), "billing:Quota".into()];
    original.attributes.insert("limit".into(), json!(23));
    original.args = vec![json!("premium"), json!(23)];
    original.traceback = Some("worker line 12".into());
    original.causes.push(registry.record(&Quota(4)));
    for (reader, expected) in [(ErrorClasses::default(), None), (registry, Some(23))] {
        let error = reader.rebuild(&original);
        let RoomKitError::RemoteTurn {
            detail: Some(detail),
            ..
        } = &error
        else {
            panic!("expected a structured remote error");
        };
        let quota = |detail: &roomkit::turns::RemoteTurnDetails| {
            detail
                .native_error()
                .and_then(|err| err.downcast_ref::<Quota>())
                .map(|quota| quota.0)
        };
        assert_eq!(quota(detail), expected);
        assert_eq!(quota(detail.causes().first().unwrap()), expected.map(|_| 4));
        assert_eq!(reader.record(&error), original);
    }
}
