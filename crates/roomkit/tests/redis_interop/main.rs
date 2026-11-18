//! One declared agent's text turn, run on a separate Redis executor (§10.6.2, §10.6.7).
//! Set `REDIS_URL` and `ROOMKIT_PYTHON` (the reference checkout's interpreter with the Redis extra).

mod after_turn;
mod history;
mod liveness;

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent,
    RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    InboundResult, ProcessInboundOptions, RedisTurnBackend, RedisTurnBackendOptions, RoomKit,
    RoomKitError, RoomKitOptions, TurnExecutor, TurnExecutorOptions, TurnTimeouts,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn matrix() -> TestResult {
    let (Some(python), Some(_)) = (env::var_os("ROOMKIT_PYTHON"), env::var_os("REDIS_URL")) else {
        writeln!(
            io::stderr(),
            "Redis interoperability skipped: set ROOMKIT_PYTHON and REDIS_URL to run it"
        )?;
        return Ok(());
    };
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/turns/redis_interop.py");
    let status = Command::new(python)
        .arg(script)
        .arg("--rust-peer")
        .arg(env::current_exe()?)
        .status()?;
    if !status.success() {
        return Err(format!("Redis interoperability failed with {status}").into());
    }
    Ok(())
}

fn backend(timeouts: TurnTimeouts) -> TestResult<RedisTurnBackend> {
    let options = RedisTurnBackendOptions {
        key_prefix: env::var("ROOMKIT_INTEROP_PREFIX")?,
        timeouts,
        ..RedisTurnBackendOptions::default()
    };
    Ok(RedisTurnBackend::new(
        Some(&env::var("REDIS_URL")?),
        options,
    )?)
}

async fn executor(backend: &RedisTurnBackend, timeouts: TurnTimeouts) -> TestResult {
    let executor_id = env::var("ROOMKIT_INTEROP_EXECUTOR_ID")?;
    let options = TurnExecutorOptions {
        hosts: Some(BTreeSet::from(["worker".into()])),
        executor_id: Some(executor_id.clone()),
        timeouts,
        ..TurnExecutorOptions::default()
    };
    let worker = TurnExecutor::new(
        move |channel_id| {
            let tail = format!("from {executor_id}.");
            async move {
                let provider = ScriptedProvider::new([ScriptedRound::writing(["Hello ", &tail])]);
                AiChannel::new(channel_id, provider, AiChannelOptions::default())
                    .map(Some)
                    .map_err(|error| RoomKitError::Value(error.to_string()))
            }
        },
        options,
    )?;
    backend.serve(worker.clone()).await?;
    writeln!(io::stdout(), "ready")?;
    io::stdout().flush()?;
    let input = tokio::task::spawn_blocking(|| io::stdin().read_line(&mut String::new())).await;
    let closed = backend.close().await;
    worker.close().await;
    input??;
    closed?;
    Ok(())
}

fn message(row: &RoomEvent) -> TestResult<Value> {
    let EventContent::Text(text) = &row.content else {
        return Err(format!("expected a text message at index {}", row.index).into());
    };
    Ok(json!([row.source.channel_id, text.body]))
}

async fn open_room(kit: &RoomKit) -> TestResult<(RoomId, ChannelId)> {
    let tx = ChannelId::new("tx")?;
    let agent = ChannelId::new("worker")?;
    let room = RoomId::new("r")?;
    kit.register_channel(RecordingTransport::new(
        tx.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))?;
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await?;
    kit.attach_channel(&room, &tx, AttachChannelOptions::default())
        .await?;
    kit.attach_channel(
        &room,
        &agent,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..AttachChannelOptions::default()
        },
    )
    .await?;

    Ok((room, tx))
}

async fn send(
    kit: &RoomKit,
    room: &RoomId,
    tx: ChannelId,
    text: &str,
) -> TestResult<InboundResult> {
    let input = InboundMessage::new(tx, "u", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(input, options).await?;
    if let Some(error) = &result.error {
        return Err(format!("inbound failed: {error:?}").into());
    }
    Ok(result)
}

async fn observe(kit: &RoomKit) -> TestResult<Value> {
    let (room, tx) = open_room(kit).await?;
    let result = send(kit, &room, tx, "Hi.").await?;
    let rows = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await?;
    let messages: Vec<_> = rows
        .iter()
        .filter(|row| row.r#type == EventType::Message)
        .collect();
    let [question, answer] = messages.as_slice() else {
        return Err(format!("expected two messages, got {}", messages.len()).into());
    };
    let turns = result.response_metadata.get("turns");
    Ok(json!({
        "input": message(question)?,
        "answer": message(answer)?,
        "responds_to_input": answer.responds_to.as_ref() == Some(&question.id),
        "executor_id": answer.metadata.get("executor_id"),
        "loop_end_reason": turns.as_ref().and_then(|t| t.pointer("/worker/loop_end_reason")),
    }))
}

async fn peer(role: &str) -> TestResult {
    let timeouts = TurnTimeouts {
        accept: Duration::from_secs(3),
        heartbeat_interval: Duration::from_millis(200),
        heartbeat_timeout: Duration::from_secs(if role == "executor" { 1 } else { 2 }),
        cancel: Duration::from_secs(1),
        ..TurnTimeouts::default()
    };
    let backend = backend(timeouts)?;
    if role == "after-turn-worker" {
        return after_turn::run(&backend).await;
    }
    if role == "executor" {
        return executor(&backend, timeouts).await;
    }
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(backend),
        ..RoomKitOptions::default()
    });
    let observations = match role {
        "history-owner" => history::replay(&kit).await,
        "liveness-owner" => liveness::observe(&kit).await,
        _ => observe(&kit).await,
    };
    kit.close().await?;
    writeln!(io::stdout(), "{}", observations?)?;
    Ok(())
}

fn main() -> TestResult {
    match env::args().nth(1).as_deref() {
        Some(
            role
            @ ("owner" | "executor" | "history-owner" | "liveness-owner" | "after-turn-worker"),
        ) => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(peer(role)),
        Some("--list") => Ok(writeln!(io::stdout(), "redis_interop: test")?),
        Some("--ignored") => Ok(()),
        Some(filter) if !filter.starts_with('-') && !"redis_interop".contains(filter) => Ok(()),
        _ => matrix(),
    }
}
