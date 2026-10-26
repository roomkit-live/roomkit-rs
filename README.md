# roomkit-rs

A Rust implementation of [RoomKit](https://github.com/roomkit-live/roomkit), the
multi-channel conversation framework: rooms, channels, hooks and permissions
for conversations between humans, AI agents and programs.

> **Status: pre-alpha.** Step 1 is done: the text path of the RFC's inbound
> and broadcast pipelines, with hooks, permissions, transcoding and the
> chain-depth limit, on a tokio runtime and an in-memory store. Step 2 is
> done: the rest of Level 0, from the room lifecycle and its timers,
> routing, edits and deletes and instructions to off-lock checks, the
> processing timeout, direct injection and the reentrant lock. Step 4,
> under way, brings a first usable version: an AI agent on an
> OpenAI-compatible endpoint (`roomkit-openai`), a SQLite store
> (`roomkit-sqlite`), and a server example that puts them together, with
> tools and streamed responses. The turn backend, skills and MCP come next,
> then the tag `v0.1.0` ([the plan](docs/steps/step-04.md)). The API will
> change.

## Why a Rust implementation

The Python implementation is the reference. This one exists for two reasons:

- **Embedding.** Its core performs no I/O and depends on no async runtime, so
  one core can run inside a Rust service, a desktop application, Python, and
  later mobile and WebAssembly hosts.
- **Testing the specification.** It is written from the
  [RoomKit RFC](https://github.com/roomkit-live/roomkit-specs), not ported from
  the Python code. Every place where the RFC does not say enough is recorded in
  [docs/rfc-gaps.md](docs/rfc-gaps.md).

## Quickstart

[`examples/server_agent.rs`](examples/server_agent.rs) is a whole service:
an axum server where each WebSocket connection is a person in a room, an AI
agent answering each message through an OpenAI-compatible endpoint
(`roomkit-openai`), and the rooms kept in a SQLite file (`roomkit-sqlite`),
so that a restarted server goes on with each conversation. Run its checks,
offline against a fake of the OpenAI wire, or serve it and chat with it:

```sh
cargo run --example server_agent
OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
    OPENAI_MODEL=gpt-6-luna cargo run --example server_agent -- --serve
websocat 'ws://127.0.0.1:8080/conversations/bakery?user=alice&name=Alice'
```

Its WebSocket channel is the example's own, a `Channel` in
[`examples/shared/websocket.rs`](examples/shared/websocket.rs) for a
service to copy and adapt; axum is a dependency of the examples only.

### A first room

The kit's calls on their own, with the crate's test channels:

```rust
use roomkit::RoomKit;
use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId};
use roomkit::testing::{EchoAgent, RecordingTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    kit.attach(&room, sms.id()).await?;
    kit.attach(&room, agent.id()).await?;

    let message = InboundMessage::new(sms.id().clone(), "+15551234567", EventContent::text("Bonjour"));
    kit.process_inbound_in(&room, message).await?;
    assert_eq!(sms.texts(), ["You said: Bonjour"]);
    Ok(())
}
```

## Crates

- `roomkit-core`: the sans-IO core, the RFC's model, rules and pipeline
  operations, with no I/O and no async runtime.
- `roomkit`: the runtime on tokio, `RoomKit`, the `Channel`,
  `ConversationStore` and `InboundRoomRouter` traits, the in-memory store.
- `roomkit-openai`: `OpenAiAgent`, an AI agent channel answering through
  an OpenAI-compatible Chat Completions endpoint.
- `roomkit-sqlite`: `SqliteStore`, a store in one SQLite file, on the Python
  implementation's schema: either implementation reads the other's file,
  as long as its channel types are ones Python lists. A type of your own
  (`ChannelType::new("slack")`) makes its room unreadable to Python until
  Python opens its type (RMK-986).

## Examples

[`examples/`](examples/) holds one runnable file per example. Each checks
what it shows and exits non-zero when a check fails. Run one from the
repository root, or all of them with `make examples`:

```sh
cargo run --example room_lifecycle
```

What they share, a kit on a clock that moves only when told, a scripted
agent, an SMS number, a WebSocket channel and the checks, is the `shared`
library in [`examples/shared/`](examples/shared/), so that each example
reads as the roomkit calls it shows.

The RFC's Appendix B flows:

- `echo_room`: B.1, an agent answers a customer on SMS.
- `new_conversation`: B.1, two senders on one number, a room each.
- `block_and_inject`: B.2, a hook blocks a message and injects notices.
- `agent_chain`: B.4, two agents answer each other up to the chain-depth limit.
- `advisor_joins`: B.5, mute an agent, make it whisper to an advisor, unmute it.
- `edit_delete_messages`: B.6 and B.7, an edit and a delete, native or as text.

Rooms and channels:

- `room_lifecycle`: pause, resume, close, reopen and archive, and the timers `check_timers` applies.
- `channel_mute_unmute`: a muted agent still reads, and its answers are stored blocked.
- `room_membership`: participants and their roles, a member who leaves, an agent banned.

Hooks:

- `hook_moderation`: `BEFORE_BROADCAST` hooks in priority order block and redact.
- `hook_inject_welcome`: a hook injects a welcome for each newcomer.
- `hook_off_lock_check`: a slow check off the room lock, bounded by `process_timeout`.

The pipeline:

- `instruction_event`: an instruction directs one agent and is never stored.
- `response_visibility`: who sees what, by the event's visibility and its binding's, and where a message's answers go.
- `send_event`: direct injection, from the host and from an agent's turn.
- `host_post`: the host writes to a room's people in its own name, from its host channel, attached on first use.

Routing and identities:

- `shared_sms_number`: one number, several correspondents, and a group's room.
- `paused_conversation`: a sender returns to the conversation its timer paused.
- `identity_addresses`: addresses linked to identities route to their person's room, and a room naming the identity keeps all its numbers.

Agents:

- `openai_agent`: an AI agent answers through an OpenAI-compatible endpoint, reading the conversation labelled by speaker; a local fake of the OpenAI wire stands in unless `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` are set.

A server:

- `server_agent`: the quickstart's service, an agent over WebSocket with its rooms in SQLite; its checks restart the server on the same file, and `--serve` keeps it up.

## Documentation

- [Architecture](docs/architecture.md): the sans-IO core, the effect protocol,
  the crates and their rules.
- [Roadmap](docs/roadmap.md): the steps, from the text path to voice.
- [Step 1](docs/steps/step-01.md): the text path, done.
- [Step 2](docs/steps/step-02.md): the rest of Level 0, done.
- [Step 2b](docs/steps/step-02b.md): the protocol and the public API in the
  shape a Python driver will call, done.
- [RMK-941](docs/steps/rmk-941.md): the code aligned with RMK-930's last RFC
  batch, done.
- [RMK-944](docs/steps/rmk-944.md): the code-hygiene findings of a review,
  with no behaviour change, done.
- [RMK-949](docs/steps/rmk-949.md): the code aligned with the RFC commits
  after RMK-941, the host's own voice and an ASYNC hook's budget among
  them, done.
- [RFC gaps](docs/rfc-gaps.md): what the specification leaves open, and the
  decisions taken here.

## License

MIT. See [LICENSE](LICENSE).
