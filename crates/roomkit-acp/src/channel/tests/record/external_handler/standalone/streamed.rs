//! A handler retains the public stream's record after abandonment or failure.

use super::*;
use crate::channel::tests::record::room;
use crate::channel::tests::record::{abandoned::Abandon, room_with_transport};
use roomkit::model::{ChannelCategory, ChannelOutput, InboundMessage};
use roomkit::{ChannelDeclaration, ChannelError};

#[derive(Clone, Copy)]
enum Ending {
    Abandoned,
    Error,
}

struct Capturing {
    inner: AcpChannel<Piped>,
    record: Arc<Mutex<Option<ResponseMetadata>>>,
}

impl Channel for Capturing {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id()
    }

    fn channel_type(&self) -> ChannelType {
        self.inner.channel_type()
    }

    fn category(&self) -> ChannelCategory {
        self.inner.category()
    }

    fn declaration(&self) -> ChannelDeclaration {
        self.inner.declaration()
    }

    fn external_tool_bridge(&self) -> Option<roomkit::external_tools::ExternalToolBridge> {
        self.inner.external_tool_bridge()
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.inner.handle_inbound(message, context).await
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let output = self.inner.on_event(event, binding, context).await?;
        *self.record.lock().unwrap() = Some(output.response_metadata.clone());
        Ok(output)
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.inner.deliver(event, binding, context).await
    }

    async fn close(&self) {
        self.inner.close().await;
    }
}

// Python: test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn[abandoned].
// Literal chain depth and realtime context clearing are separate contracts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_reader_keeps_the_handlers_record_across_a_nested_scope() {
    tokio::time::timeout(PATIENCE, check(Ending::Abandoned))
        .await
        .unwrap();
}

// Python: test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn[error].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_stream_keeps_the_handlers_record_across_a_nested_scope() {
    tokio::time::timeout(PATIENCE, check(Ending::Error))
        .await
        .unwrap();
}

async fn check(ending: Ending) {
    let handler = Arc::new(Recording::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Recording>::clone(&handler));
    let output_record = Arc::new(Mutex::new(None));
    let channel = Capturing {
        inner: channel,
        record: Arc::clone(&output_record),
    };
    let foreign = TurnRecord::default();
    foreign.current().insert("foreign", json!(true));
    let kit = match ending {
        Ending::Abandoned => room_with_transport(channel, Abandon::new(foreign.clone())).await,
        Ending::Error => room(channel).await,
    };
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.clone().scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    finish(&mut agent, &prompt, ending).await;
    let result = running.await.unwrap();
    if matches!(ending, Ending::Error) {
        assert!(result.error.is_some());
    }
    handler.recorded.notified().await;
    close(&kit, &mut agent).await;
    assert_records(&handler, &output_record, parent, foreign);
}

fn assert_records(
    handler: &Recording,
    output_record: &Mutex<Option<ResponseMetadata>>,
    parent: TurnRecord,
    foreign: TurnRecord,
) {
    let records = handler.seen.lock().unwrap().clone();
    assert_eq!(records.len(), 2, "before and after the nested scope");
    records[0].insert("identity_check", json!(true));
    assert_eq!(records[1].get("identity_check"), Some(json!(true)));
    let output = output_record.lock().unwrap().clone().unwrap();
    assert_eq!(output.get("identity_check"), Some(json!(true)));
    assert_eq!(records[0].get("observed"), Some(json!(true)));
    assert_eq!(records[0].get("nested"), None);
    for (record, key) in [(parent, "parent"), (foreign, "foreign")] {
        assert_eq!(record.current().len(), 1);
        assert_eq!(record.current().get(key), Some(json!(true)));
    }
    assert!(current_response_metadata().is_none());
}

async fn finish(agent: &mut Agent, prompt: &serde_json::Value, ending: Ending) {
    match ending {
        Ending::Abandoned => send_anchor(agent).await,
        Ending::Error => {
            let error = json!({"jsonrpc":"2.0","id":prompt["id"],
                "error":{"code":-32000,"message":"agent failed"}});
            agent
                .writer
                .write_all(format!("{error}\n").as_bytes())
                .await
                .unwrap();
        }
    }
}

async fn send_anchor(agent: &mut Agent) {
    let text = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"agent_message_chunk",
            "content":{"type":"text","text":"Anchor"}}
    }});
    agent
        .writer
        .write_all(format!("{text}\n").as_bytes())
        .await
        .unwrap();
}
