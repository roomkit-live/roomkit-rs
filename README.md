# RoomKit for Rust

The Rust implementation of [RoomKit](https://github.com/roomkit-live/roomkit), a
framework for conversations between people, AI agents and programs. Connect
channels to a room and let RoomKit handle routing, hooks, permissions, streaming
and conversation state.

[Website](https://www.roomkit.live/) · [Specification](https://github.com/roomkit-live/roomkit-specs) ·
[Examples](examples/) · [Contributing](CONTRIBUTING.md)

## What it provides

- Rooms, channel bindings, participant identities, event visibility and routing.
- AI agents with streamed responses, tool calls, policies, hooks and orchestration.
- A provider contract the model providers implement: OpenAI-compatible Chat
  Completions and Responses, native Anthropic Messages and others live in
  [roomkit-providers](https://github.com/roomkit-live/roomkit-providers).
- MCP tools over stdio or Streamable HTTP, plus an Agent Client Protocol adapter.
- In-memory, SQLite and PostgreSQL storage, with lifecycle and cancellation handling.
- A core without I/O, separated from the Tokio runtime and external adapters.

This is a **0.x API**: pin a tested revision and read the [changelog](CHANGELOG.md)
before upgrading. The Python implementation is the API reference. Shared
conformance scenarios and the [API mapping](api/PYTHON_API.md) track behavior and
language-specific differences; support for one feature does not imply every
Python provider or media integration is available in Rust.

MCP result validation against a tool's `outputSchema` and the raw
`MCPToolProvider.call_tool_result` API are deferred beyond the first public
release (RMK-1732). Existing Rust MCP calls do not validate structured results
against the server's declared output schema.

## Try it locally

Install [Rust through rustup](https://rustup.rs/), then:

```sh
git clone https://github.com/roomkit-live/roomkit-rs.git
cd roomkit-rs
cargo run --locked --example echo_room
cargo run --locked --example agent_chain
```

The checkout selects its pinned Rust toolchain. Rust **1.92** is the minimum
supported version. The examples use fixtures and need no model account or
credentials.

This repository holds no model provider. The examples of an agent on a real
provider, a WebSocket service with its browser page among them, are in
[roomkit-providers](https://github.com/roomkit-live/roomkit-providers)
(`rust-examples/`), where they run against a local fake of the provider's wire,
or against your own OpenAI-compatible server.

## Add it to a Rust application

The crates are distributed from Git, not crates.io. Select a tested full commit
from this repository and replace `FULL_COMMIT_SHA` below. Use **the same URL and
revision for every RoomKit engine crate** so Cargo builds one set of types.

```toml
[dependencies]
roomkit = { git = "https://github.com/roomkit-live/roomkit-rs", rev = "FULL_COMMIT_SHA" }
roomkit-ai = { git = "https://github.com/roomkit-live/roomkit-rs", rev = "FULL_COMMIT_SHA" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

A model provider comes from
[roomkit-providers](https://github.com/roomkit-live/roomkit-providers), which
is built on these crates: pin a revision of it that names the engine revision
you use.

## A first room

This example connects a recording transport and an echo agent without network
access or model credentials:

```rust
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use roomkit::{CreateRoomOptions, ProcessInboundOptions, RoomKit};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kit = RoomKit::new(Default::default());
    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(
        ChannelId::new("ai")?,
        "You said: ",
        EchoAgentOptions { quote: true },
    );
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions { room_id: Some(room.clone()), ..Default::default() }).await?;
    kit.attach_channel(&room, sms.channel_id(), Default::default()).await?;
    kit.attach_channel(&room, agent.channel_id(), Default::default()).await?;

    let message =
        InboundMessage::new(sms.channel_id().clone(), "+15551234567", TextContent::new("Hello"));
    kit.process_inbound(message, ProcessInboundOptions { room_id: Some(room), ..Default::default() }).await?;
    assert_eq!(sms.texts(), ["You said: Hello"]);
    kit.close().await?;
    Ok(())
}
```

## Crates

| Crate | Responsibility |
| --- | --- |
| [`roomkit`](crates/roomkit) | Tokio runtime, rooms, channels, hooks, routing and orchestration |
| [`roomkit-ai`](crates/roomkit-ai) | Provider-independent AI channel, tool loop, skills and policies |
| [`roomkit-mcp`](crates/roomkit-mcp) | MCP client and tools; enable `http` for remote servers |
| [`roomkit-acp`](crates/roomkit-acp) | Agent Client Protocol adapter |
| [`roomkit-sqlite`](crates/roomkit-sqlite) | SQLite conversation store |
| [`roomkit-core`](crates/roomkit-core) | Models, rules and operations without I/O |

Applications normally depend on `roomkit` and the adapters they use. The
stores, locks and providers of a vendor or a service (Postgres, Redis,
OpenTelemetry, the model providers) are in
[roomkit-providers](https://github.com/roomkit-live/roomkit-providers).
`roomkit-core` and the effect protocol are implementation interfaces; see
[ARCHITECTURE.md](ARCHITECTURE.md) for the boundaries.

## Run an agent in another process

A room's process and an agent's process reach each other through a
`roomkit::turns::DistributedTurnBackend`. The backend's logic is this
crate's. It takes two services by their ports: a `TurnBroker` carries the
messages between processes, a `TurnRegistry` keeps what the processes
share. `InMemoryTurnBroker` and `InMemoryTurnRegistry` keep both in one
process, for tests and for nodes that share a process.

On Redis, the `roomkit-providers-redis` crate of the roomkit-providers
repository implements both ports and assembles the backend as
`RedisTurnBackend`, with the keys and the messages of the Python SDK's
backend: either process can run the Python SDK or the Rust SDK. This
crate holds no service's code.

The room owner and each worker create their own backend on the same
services. On the owner, create a
`roomkit::turns::TurnBackendHandle::new(Arc::new(backend))`, pass
`Some(handle.clone())` to `RoomKitOptions::turn_backend` and register the
remote agent with `ChannelDeclaration::new(channel_id)`.

On the worker, build a `TurnExecutor` with a channel factory and set
`TurnExecutorOptions::hosts` to the channel IDs or prefixes it serves.
Call `backend.serve(executor.clone()).await?` before accepting work. The owner
stores the resulting conversation events.

To try organization-based placement locally, run
`cargo run --example turns_executor_organizations`. Two workers serve three
rooms. The room in an unserved organization sees the agent offline.

A service that implements the ports checks them with
`roomkit::testing::check_turn_broker` and `check_turn_registry` from its
own tests.

The shared RFC scenarios with executors run on the in-memory broker and
registry. To check them with the in-memory conversation store on both Tokio
runtimes, run

```sh
ROOMKIT_CONFORMANCE_FILTER=executors/ cargo test --locked -p roomkit --test conformance -- every_scenario --nocapture
```

Set `ROOMKIT_CONFORMANCE_JUNIT` to a directory to retain the pass, failure
and skip counts for each runtime (CONTRIBUTING.md, "Conformance results").

## Examples and documentation

| Example | What it demonstrates |
| --- | --- |
| [echo_room](examples/echo_room.rs) | A room and its channels |
| [agent_chain](examples/agent_chain.rs) | Two agents answering each other, within the chain depth |
| [ai_router_local_or_hosted](examples/ai_router_local_or_hosted.rs) | A router choosing the model of each turn |
| [room_lifecycle](examples/room_lifecycle.rs) | Room lifecycle and timers |
| [telemetry](examples/telemetry.rs) | Traces and metrics |

Each example checks what it shows, with no model account and no network.

Build the API documentation locally:

```sh
cargo doc --workspace --no-deps --all-features --open
```

- [Architecture](ARCHITECTURE.md): crate boundaries and execution model.
- [Python API mapping](api/PYTHON_API.md): language-specific API forms.
- [Conformance scenarios](conformance/): shared behavioral checks.
- [Contributing](CONTRIBUTING.md): setup, tests and changes to the API.
- [Changelog](CHANGELOG.md): changes and compatibility notes.

## License

[MIT](LICENSE).
