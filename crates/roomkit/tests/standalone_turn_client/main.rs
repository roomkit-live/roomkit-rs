//! An application drives a standalone room owner without supplying a store.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
mod backend;
mod close;
mod lifecycle;
mod ports;
use backend::Backend;
use roomkit::RoomKitError;
use roomkit::turns::wire::{Control, ErrorRecord, Frame, WireOutput};
use roomkit::turns::{
    ChannelCall, ChannelControl, TurnClient, TurnClientOptions, TurnRequest, TurnRunOptions,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn fixture() -> TurnRequest {
    let row: Value = serde_json::from_str(
        include_str!("../fixtures/turns/python-case1.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    serde_json::from_value(row.get("message").unwrap().clone()).unwrap()
}
async fn run(client: &TurnClient) -> Result<roomkit::turns::TurnClientOutput, RoomKitError> {
    let r = fixture();
    client
        .run(
            r.event,
            &r.binding,
            r.context,
            TurnRunOptions {
                strategy_turn: true,
            },
        )
        .await
}
fn accepted() -> Frame {
    Frame::Accepted {
        executor_id: "application".into(),
    }
}
fn output(streams: bool) -> Frame {
    Frame::Output {
        output: WireOutput {
            responded: true,
            ..Default::default()
        },
        streams,
    }
}

#[tokio::test]
async fn runs_without_a_local_worker_and_keeps_previous_turns_and_hooks() {
    let backend = Arc::new(Backend::default());
    let client = TurnClient::new(
        roomkit::turns::TurnBackendHandle::new(Arc::<Backend>::clone(&backend)),
        TurnClientOptions::default(),
    );
    let r = fixture();
    for name in ["z", "a", "z"] {
        client.enable_room_hook(&r.room_id, name);
    }
    assert_eq!(client.room_hook_names(&r.room_id), ["z", "a"]);
    assert!(client.disable_room_hook(&r.room_id, "z"));
    let mut worker = backend.connect();
    worker.send(accepted());
    worker.send(output(false));
    worker.send(Frame::End);
    let answer = run(&client).await.unwrap();
    let source: &roomkit::model::ChannelOutput = &answer;
    assert!(source.responded);
    assert!(answer.responded);
    assert!(answer.response_stream.is_none());
    let request = backend.requests.lock().unwrap()[0].clone();
    assert_eq!(request.room_hooks, ["a"]);
    assert!(request.strategy_turn);
    assert!(request.previous_turn.is_none());
    // A later submission refused before handover must not replace that record.
    assert!(run(&client).await.is_err());
    let call = ChannelCall::new(r.channel_id.clone(), r.room_id.clone(), "freeze");
    client
        .control(ChannelControl::ChannelCall(Box::new(call)))
        .await
        .unwrap();
    let controls = backend.controls.lock().unwrap().clone();
    let ChannelControl::ChannelCall(call) = &controls[0] else {
        panic!("expected a channel call")
    };
    let previous = call.previous_turn.as_ref().unwrap();
    assert_eq!(previous.turn_id, request.turn_id);
    assert_eq!(previous.executor_id.as_deref(), Some("application"));
    client.close().await.unwrap();
    assert_eq!(*backend.registrations.lock().unwrap(), ["attend", "close"]);
    assert_eq!(backend.released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rebuilds_the_original_error_before_output_and_during_a_pulled_stream() {
    let backend = Arc::new(Backend::default());
    let mut classes = roomkit::turns::ErrorClasses::default();
    classes.register::<std::io::Error>(
        "example:Recoverable",
        |row| Some(std::io::Error::other(row.message.clone())),
        |error| ErrorRecord::new("example:Recoverable", error.to_string()),
    );
    let options = TurnClientOptions {
        error_classes: classes,
        ..TurnClientOptions::default()
    };
    let client = TurnClient::new(
        roomkit::turns::TurnBackendHandle::new(Arc::<Backend>::clone(&backend)),
        options,
    );
    let mut error = ErrorRecord::new("example:Unavailable", "retry after the backup");
    error.bases = vec!["example:Recoverable".into()];
    error.args = vec![json!(3)];
    error.traceback = Some("peer frame".into());
    for streaming in [false, true] {
        let mut w = backend.connect();
        w.send(accepted());
        let raised = if streaming {
            w.send(output(true));
            let mut stream = run(&client).await.unwrap().response_stream.unwrap();
            assert!(w.controls.try_recv().is_err());
            let read = tokio::spawn(async move {
                std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
                    .await
                    .unwrap()
                    .unwrap_err()
            });
            assert!(matches!(
                w.controls.recv().await,
                Some(Control::Pull { .. })
            ));
            w.send(Frame::Error {
                error: error.clone(),
            });
            w.send(Frame::End);
            *read.await.unwrap().downcast::<RoomKitError>().unwrap()
        } else {
            w.send(Frame::Error {
                error: error.clone(),
            });
            w.send(Frame::End);
            run(&client).await.unwrap_err()
        };
        let RoomKitError::RemoteTurn {
            detail: Some(detail),
            ..
        } = raised
        else {
            panic!("expected a remote error with its record")
        };
        assert_eq!(detail.record(), &error);
        assert!(detail.native_error().unwrap().is::<std::io::Error>());
    }
    client.close().await.unwrap();
}
