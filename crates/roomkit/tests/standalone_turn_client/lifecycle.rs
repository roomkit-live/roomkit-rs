//! Concurrent handover and shutdown through the standalone client.
use super::{Backend, accepted, fixture, output, run};
use roomkit::SteerOptions;
use roomkit::model::InjectMessage;
use roomkit::turns::wire::{Control, Frame};
use roomkit::turns::{TurnClient, TurnClientOptions};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[tokio::test]
async fn steering_waits_for_the_worker_to_finish_admission_and_close_releases_an_unread_stream() {
    let backend = Arc::new(Backend::default());
    let client = TurnClient::new(
        Arc::<Backend>::clone(&backend),
        TurnClientOptions::default(),
    );
    let r = fixture();
    let mut worker = backend.connect();
    let (entered, continue_admission) = backend.delay_next();
    let run = tokio::spawn({
        let client = client.clone();
        async move { run(&client).await }
    });
    entered.await.unwrap();
    let mut steer = Box::pin(client.steer(
        &r.room_id,
        &r.channel_id,
        InjectMessage::new("continue"),
        SteerOptions::default(),
    ));
    assert!(matches!(
        steer.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    continue_admission.send(()).unwrap();
    worker.send(accepted());
    worker.send(output(true));
    let stream = run.await.unwrap().unwrap().response_stream.unwrap();
    let ack = async {
        let Some(Control::Steer { steer_id, .. }) = worker.controls.recv().await else {
            panic!("expected steering")
        };
        worker.send(Frame::SteerResult {
            steer_id,
            loops: Some(1),
        });
    };
    let (reached, ()) = tokio::join!(steer, ack);
    assert_eq!(reached, 1);
    tokio::time::timeout(Duration::from_secs(1), client.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backend.released.load(Ordering::SeqCst), 1);
    drop(stream);
}

#[tokio::test]
async fn only_the_requests_tenant_reads_its_inputs_and_an_answer_or_close_withdraws_them() {
    use roomkit::{
        InputRequestOptions,
        turns::wire::{InputOutcome, InputRequest},
    };
    use serde_json::json;
    let backend = Arc::new(Backend::default());
    let client = TurnClient::new(
        Arc::<Backend>::clone(&backend),
        TurnClientOptions::default(),
    );
    let mut worker = backend.connect();
    let mut r = fixture();
    r.context.room.organization_id = Some("team".into());
    let ask = |id: &str| {
        Frame::InputRequest{request:Box::new(serde_json::from_value::<InputRequest>(json!({"type":"input_request","request_id":id,"tool_name":"ask","tool_call_id":"model-call","arguments":{"q":"time?"},"actor_id":"operator"})).unwrap())}
    };
    worker.send(accepted());
    worker.send(ask("one"));
    worker.send(ask("two"));
    worker.send(output(true));
    let stream = client
        .run(
            r.event,
            &r.binding,
            r.context,
            roomkit::turns::TurnRunOptions::default(),
        )
        .await
        .unwrap()
        .response_stream
        .unwrap();
    let snapshot = client.inputs().request("one").unwrap();
    assert_eq!(snapshot.room_id, r.room_id.as_str());
    assert_eq!(snapshot.channel_id, r.channel_id.as_str());
    assert_eq!(snapshot.tool_call_id, "model-call");
    assert_eq!(snapshot.actor_id.as_deref(), Some("operator"));
    assert_eq!(snapshot.arguments["q"], "time?");
    assert_eq!(
        client.input_request(
            "one",
            &InputRequestOptions {
                organization_id: Some("team".into())
            }
        ),
        Some(snapshot)
    );
    assert!(
        client
            .input_request(
                "one",
                &InputRequestOptions {
                    organization_id: Some("other".into())
                }
            )
            .is_none()
    );
    let inputs = client.inputs();
    let acknowledge = async {
        let Some(Control::InputAnswer {
            request_id,
            outcome,
            value,
            ..
        }) = worker.controls.recv().await
        else {
            panic!("input answer")
        };
        assert_eq!(outcome, InputOutcome::Answered);
        assert_eq!(value.as_deref(), Some("noon"));
        assert_eq!(request_id, "one");
        worker.send(Frame::InputTaken {
            request_id: request_id.clone(),
            taken: true,
        });
        worker.send(Frame::InputClosed {
            request_id,
            reason: "answered".into(),
        });
        // A framework event acts as a barrier after the withdrawal on the same pipe.
        worker.send(Frame::HostCall {
            call_id: "barrier".into(),
            verb: "read".into(),
            arguments: roomkit::model::Metadata::new(),
            context: roomkit::turns::wire::CallContext::default(),
        });
        let Some(Control::HostResult { .. }) = worker.controls.recv().await else {
            panic!("barrier result")
        };
    };
    let (taken, ()) = tokio::join!(inputs.answer("one", "noon"), acknowledge);
    assert!(taken);
    assert!(inputs.request("one").is_none());
    assert!(inputs.request("two").is_some());
    client.close().await.unwrap();
    assert!(inputs.request("two").is_none());
    drop(stream);
}
