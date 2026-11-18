//! Standalone owner services are called with the same request and scope as the Python client.
use super::{accepted, backend::Backend, fixture, output, run};
use roomkit::model::{ChannelId, Metadata, RoomEvent, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::turns::wire::{CallContext, Control, Frame, PublicationFrame, ToolCall, ToolOutcome};
use roomkit::turns::{
    OwnerEvents, OwnerHost, OwnerTool, OwnerTools, RoomCall, RoomOwner, RoomPublication,
    TurnClient, TurnClientOptions, TurnFuture, TurnRequest,
};
use roomkit::{ChannelError, RoomKitError};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Services {
    calls: Mutex<Vec<String>>,
    events: Mutex<Vec<String>>,
}
impl OwnerHost for Services {
    fn call<'a>(
        &'a self,
        request: &'a TurnRequest,
        verb: &'a str,
        args: Metadata,
    ) -> TurnFuture<'a, Result<Value, ChannelError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(verb.into());
            Ok(json!([request.turn_id, args.get("arg").unwrap()]))
        })
    }
    fn call_between_turns<'a>(
        &'a self,
        message: &'a RoomCall,
    ) -> TurnFuture<'a, Result<Value, ChannelError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(message.verb.clone());
            Ok(json!(message.turn_id))
        })
    }
    fn channel_rows<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        first: Option<u64>,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, ChannelError>> {
        Box::pin(async move {
            let r = fixture();
            assert_eq!((room, channel), (&r.room_id, &r.channel_id));
            assert_eq!(first, r.context.recent_events.iter().map(|e| e.index).min());
            Ok(vec![r.event])
        })
    }
}
impl OwnerTools for Services {
    fn declared(&self, _: &ChannelId, _: &RoomId) -> Vec<OwnerTool> {
        vec![OwnerTool::new(json!({"name":"lookup"}))]
    }
    fn serve<'a>(
        &'a self,
        request: &'a TurnRequest,
        call: ToolCall,
    ) -> TurnFuture<'a, Result<roomkit::turns::wire::WireResult, ChannelError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(call.name);
            assert_eq!(call.call_id, "tool");
            assert_eq!(call.context.tool_call_id, "model-call");
            if call.arguments.contains_key("refuse") {
                return Err(roomkit::ChannelRefusalError::new("unavailable").into());
            }
            if call.arguments.contains_key("parts") {
                Ok(roomkit::turns::wire::WireResult::Parts {
                    parts: vec![
                        json!({"type":"text","text":"one"}),
                        json!({"type":"text","text":"two"}),
                    ],
                })
            } else {
                Ok(json!([request.turn_id, "ordinary"]).into())
            }
        })
    }
}
impl OwnerEvents for Services {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            assert_eq!(
                event.room_created_at,
                Some(fixture().context.room.created_at)
            );
            self.events.lock().unwrap().push(channel.into());
            Ok(())
        })
    }
    fn emit<'a>(
        &'a self,
        kind: &'a str,
        room: Option<&'a str>,
        channel: Option<&'a str>,
        _: Option<&'a str>,
        _: Metadata,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let r = fixture();
            assert_eq!(
                (room, channel),
                (Some(r.room_id.as_str()), Some(r.channel_id.as_str()))
            );
            self.events.lock().unwrap().push(kind.into());
            Ok(())
        })
    }
    fn reaches_room<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a ChannelId,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async { Ok(true) })
    }
}

fn framework(kind: &str) -> Frame {
    Frame::FrameworkEvent {
        event_type: kind.into(),
        room_id: None,
        channel_id: None,
        event_id: None,
        data: Metadata::new(),
    }
}
fn ephemeral(room: &RoomId, channel: &ChannelId) -> Frame {
    let mut event = EphemeralEvent::new(room.clone(), EphemeralEventType::TypingStart, "user");
    event.channel_id = Some(channel.clone());
    Frame::Ephemeral {
        channel: format!("room:{room}"),
        event: Value::Object(event.to_dict()),
    }
}

#[tokio::test]
async fn custom_ports_read_the_submitted_request_and_reject_an_unrelated_room() {
    let backend = Arc::new(Backend::default());
    let service = Arc::new(Services::default());
    let client = TurnClient::new(
        roomkit::turns::TurnBackendHandle::new(Arc::<Backend>::clone(&backend)),
        TurnClientOptions {
            host: Some(Arc::<Services>::clone(&service)),
            tools: Some(Arc::<Services>::clone(&service)),
            events: Some(Arc::<Services>::clone(&service)),
            ..TurnClientOptions::default()
        },
    );
    let r = fixture();
    let mut worker = backend.connect();
    worker.send(accepted());
    let task = tokio::spawn({
        let client = client.clone();
        async move { run(&client).await }
    });
    verify_callbacks(&r, &mut worker).await;
    verify_parts(&mut worker).await;
    verify_refusal(&mut worker).await;
    worker.send(framework("room_closed"));
    worker.send(framework("before_tool_use"));
    worker.send(ephemeral(&r.room_id, &r.channel_id));
    worker.send(output(false));
    worker.send(Frame::End);
    task.await.unwrap().unwrap();
    let request = backend.requests.lock().unwrap()[0].clone();
    assert_eq!(request.channel_rows, [r.event]);
    assert_eq!(
        request.owner_tools,
        [OwnerTool::new(json!({"name":"lookup"}))]
    );
    let mut message = RoomCall::new(
        r.channel_id.clone(),
        r.room_id.clone(),
        "other",
        request.turn_id.clone(),
        "between",
    );
    assert!(
        client
            .room_call(message.clone())
            .await
            .unwrap()
            .error
            .is_some()
    );
    message.executor_id = "application".into();
    assert_eq!(
        client.room_call(message).await.unwrap().result,
        json!(request.turn_id)
    );
    assert_eq!(
        *service.calls.lock().unwrap(),
        ["lookup_host", "lookup", "lookup", "lookup", "between"]
    );
    let out = RoomPublication::new(
        r.channel_id,
        r.room_id.clone(),
        "other",
        request.turn_id,
        PublicationFrame::try_from(framework("ai_response")).unwrap(),
    );
    client.room_publication(out).await.unwrap();
    client.close().await.unwrap();
    assert_eq!(
        *service.events.lock().unwrap(),
        ["before_tool_use".into(), format!("room:{}", r.room_id)]
    );
}

#[expect(
    clippy::panic,
    reason = "unexpected control frames fail the protocol test"
)]
async fn verify_callbacks(r: &TurnRequest, worker: &mut super::backend::Worker) {
    let context = CallContext {
        tool_call_id: "model-call".into(),
        room_id: r.room_id.to_string(),
        channel_id: r.channel_id.to_string(),
        ..CallContext::default()
    };
    for room in [r.room_id.as_str(), "unrelated"] {
        worker.send(Frame::HostCall {
            call_id: "host".into(),
            verb: "lookup_host".into(),
            arguments: Metadata::from_iter([("arg".into(), json!(42))]),
            context: CallContext {
                room_id: room.into(),
                ..context.clone()
            },
        });
        let Some(Control::HostResult { result, error, .. }) = worker.controls.recv().await else {
            panic!("host reply")
        };
        if room == r.room_id.as_str() {
            assert_eq!(result.get(1), Some(&json!(42)));
            assert!(error.is_none());
        } else {
            assert!(error.is_some());
        }
        worker.send(Frame::ToolCall {
            call_id: "tool".into(),
            name: "lookup".into(),
            arguments: Metadata::new(),
            context: CallContext {
                room_id: room.into(),
                ..context.clone()
            },
        });
        let Some(Control::ToolResult {
            outcome, result, ..
        }) = worker.controls.recv().await
        else {
            panic!("tool reply")
        };
        if room == r.room_id.as_str() {
            let result = result.unwrap();
            assert_eq!(result.pointer("/value/1"), Some(&json!("ordinary")));
            assert!(result.get("parts").is_none());
        }
        assert_eq!(
            outcome,
            if room == r.room_id.as_str() {
                ToolOutcome::Served
            } else {
                ToolOutcome::Unserved
            }
        );
    }
}

async fn verify_parts(worker: &mut super::backend::Worker) {
    worker.send(Frame::ToolCall {
        call_id: "tool".into(),
        name: "lookup".into(),
        arguments: Metadata::from_iter([("parts".into(), json!(true))]),
        context: CallContext {
            tool_call_id: "model-call".into(),
            ..CallContext::default()
        },
    });
    let reply = worker.controls.recv().await.unwrap();
    assert!(
        matches!(reply,Control::ToolResult {outcome:ToolOutcome::Served,result:Some(result),..} if result==json!({"parts":[{"type":"text","text":"one"},{"type":"text","text":"two"}]}))
    );
}

async fn verify_refusal(worker: &mut super::backend::Worker) {
    worker.send(Frame::ToolCall {
        call_id: "tool".into(),
        name: "lookup".into(),
        arguments: Metadata::from_iter([("refuse".into(), json!(true))]),
        context: CallContext {
            tool_call_id: "model-call".into(),
            ..CallContext::default()
        },
    });
    let reply = worker.controls.recv().await.unwrap();
    assert!(
        matches!(reply,Control::ToolResult{outcome:ToolOutcome::Refused,error:Some(error),..} if error.kind=="ChannelRefusalError" && error.message=="unavailable")
    );
}
