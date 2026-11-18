//! A Python room owner asks a Rust worker to use a captured host after its turn.
use super::TestResult;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{
    Channel, ChannelError, RedisTurnBackend, TurnExecutor, TurnExecutorOptions, TurnHost,
};
use serde_json::json;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

struct Worker {
    agent: ChannelId,
    hosts: Arc<Mutex<Vec<TurnHost>>>,
}

fn publish(host: &TurnHost, room: &RoomId, agent: &ChannelId, phase: &str) {
    let data = [("phase".to_owned(), json!(phase))].into_iter().collect();
    host.emit_turn_event("ai_response", data);
    let mut event = EphemeralEvent::new(room.clone(), EphemeralEventType::Custom, agent.as_str());
    event.channel_id = Some(agent.clone());
    event.data.insert("phase".into(), json!(phase));
    host.publish_from_channel(event);
}

impl Channel for Worker {
    fn channel_id(&self) -> ChannelId {
        self.agent.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Ok(roomkit::default_inbound_event(self, &message, &context))
    }
    async fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        Ok(ChannelOutput::empty())
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = roomkit::current_turn_host().ok_or("no host in worker turn")?;
        publish(&host, &event.room_id, &self.agent, "running");
        self.hosts
            .lock()
            .map_err(|_| "host lock failed")?
            .push(host);
        Ok(ChannelOutput::empty())
    }
}

fn reply(s: &str) -> TestResult {
    writeln!(io::stdout(), "{s}")?;
    io::stdout().flush()?;
    Ok(())
}

pub(super) async fn run(backend: &RedisTurnBackend) -> TestResult {
    let hosts = Arc::new(Mutex::new(Vec::new()));
    let worker = TurnExecutor::new(
        {
            let hosts = Arc::clone(&hosts);
            move |agent| {
                std::future::ready(Ok(Some(Worker {
                    agent,
                    hosts: Arc::clone(&hosts),
                })))
            }
        },
        TurnExecutorOptions {
            hosts: Some(["worker".into()].into()),
            executor_id: Some("after-turn-worker".into()),
            ..Default::default()
        },
    )?;
    backend.serve(worker.clone()).await?;
    reply("ready")?;
    let room = RoomId::new("r")?;
    let agent = ChannelId::new("worker")?;
    loop {
        let line = tokio::task::spawn_blocking(|| -> io::Result<String> {
            let mut line = String::new();
            io::stdin().read_line(&mut line)?;
            Ok(line)
        })
        .await??;
        if line.trim() == "stop" || line.is_empty() {
            break;
        }
        let first = hosts
            .lock()
            .map_err(|_| "host lock failed")?
            .first()
            .cloned()
            .ok_or("no retained host")?;
        match line.trim() {
            "late" => {
                publish(&first, &room, &agent, "after");
                let event = first
                    .send_event(
                        &room,
                        &agent,
                        TextContent::new("after the turn"),
                        Default::default(),
                    )
                    .await?;
                reply(event.id.as_str())?;
            }
            "stale" => {
                let error = first
                    .send_event(
                        &room,
                        &agent,
                        TextContent::new("stale work"),
                        Default::default(),
                    )
                    .await
                    .err()
                    .ok_or("the newer turn did not supersede the captured host")?;
                reply(&error.to_string())?;
            }
            _ => return Err("unrecognized peer command".into()),
        }
    }
    backend.close().await?;
    worker.close().await;
    Ok(())
}
