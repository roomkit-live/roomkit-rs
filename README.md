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
- OpenAI-compatible Chat Completions, Responses and native Anthropic Messages.
- MCP tools over stdio or Streamable HTTP, plus an Agent Client Protocol adapter.
- In-memory, SQLite and PostgreSQL storage, with lifecycle and cancellation handling.
- A core without I/O, separated from the Tokio runtime and external adapters.

This is a **0.x API**: pin a tested revision and read the [changelog](CHANGELOG.md)
before upgrading. The Python implementation is the API reference. Shared
conformance scenarios and the [API mapping](api/PYTHON_API.md) track behavior and
language-specific differences; support for one feature does not imply every
Python provider or media integration is available in Rust.

## Try it locally

Install [Rust through rustup](https://rustup.rs/), then:

```sh
git clone https://github.com/roomkit-live/roomkit-rs.git
cd roomkit-rs
cargo run --locked --example echo_room
cargo run --locked --example server_agent
```

The checkout selects its pinned Rust toolchain. Rust **1.92** is the minimum
supported version. These default example runs use fixtures and need no model
account or credentials.

To run the example WebSocket service against your own OpenAI-compatible server:

```sh
OPENAI_BASE_URL=http://localhost:8000/v1 OPENAI_MODEL=your-model \
  cargo run --locked --example server_agent -- --serve
```

Set `OPENAI_API_KEY` if the server requires authentication. The service example
is a starting point for integration, not a production authentication policy.
[server_agent.rs](examples/server_agent.rs) shows the complete setup;
[web_chat.rs](examples/web_chat.rs) adds a browser interface.

## Add it to a Rust application

The crates are distributed from Git, not crates.io. Select a tested full commit
from this repository and replace `FULL_COMMIT_SHA` below. Use **the same URL and
revision for every RoomKit engine crate** so Cargo builds one set of types.

```toml
[dependencies]
roomkit = { git = "https://github.com/roomkit-live/roomkit-rs", rev = "FULL_COMMIT_SHA" }
roomkit-ai = { git = "https://github.com/roomkit-live/roomkit-rs", rev = "FULL_COMMIT_SHA" }
roomkit-openai = { git = "https://github.com/roomkit-live/roomkit-rs", rev = "FULL_COMMIT_SHA" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

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
| [`roomkit-openai`](crates/roomkit-openai) | OpenAI-compatible Chat Completions and Responses providers |
| [`roomkit-anthropic`](crates/roomkit-anthropic) | Native Anthropic Messages provider |
| [`roomkit-mcp`](crates/roomkit-mcp) | MCP client and tools; enable `http` for remote servers |
| [`roomkit-acp`](crates/roomkit-acp) | Agent Client Protocol adapter |
| [`roomkit-sqlite`](crates/roomkit-sqlite) | SQLite conversation store |
| [`roomkit-postgres`](crates/roomkit-postgres) | PostgreSQL store, locking and knowledge source |
| [`roomkit-core`](crates/roomkit-core) | Models, rules and operations without I/O |
| [`roomkit-provider-http`](crates/roomkit-provider-http) | Shared HTTP provider support |

Applications normally depend on `roomkit` and the adapters they use.
`roomkit-core` and the effect protocol are implementation interfaces; see
[ARCHITECTURE.md](ARCHITECTURE.md) for the boundaries.

## Examples and documentation

| Example | What it demonstrates |
| --- | --- |
| [echo_room](examples/echo_room.rs) | A room and its channels |
| [server_agent](examples/server_agent.rs) | A WebSocket service with an AI agent and SQLite |
| [web_chat](examples/web_chat.rs) | A browser connected to the service |
| [anthropic_agent](examples/anthropic_agent.rs) | Native Messages and tool discovery |
| [mcp_tools](examples/mcp_tools.rs) | MCP tools exposed to an agent |
| [room_lifecycle](examples/room_lifecycle.rs) | Room lifecycle and timers |
| [telemetry](examples/telemetry.rs) | Traces and metrics |

Each example checks its default scenario. Live provider calls are explicitly
opted into and may incur provider charges. Read the example's header before
running live mode.

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
