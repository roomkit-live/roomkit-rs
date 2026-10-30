# roomkit-rs

A Rust implementation of [RoomKit](https://github.com/roomkit-live/roomkit), the
multi-channel conversation framework: rooms, channels, hooks and permissions
for conversations between humans, AI agents and programs.

> **Status: 0.1.0, a first usable version.** roomkit-rs holds Level 0 of
> the RFC with nothing left out: the inbound and broadcast pipelines with
> hooks, permissions, transcoding and the chain-depth limit, the room
> lifecycle and its timers, routing, edits and deletes, instructions,
> direct injection, the reentrant lock and the turn backend, on a tokio
> runtime; where it differs from the Python implementation is recorded in
> [docs/rfc-gaps.md](docs/rfc-gaps.md). Beside it, an AI agent with tools,
> streamed answers, skills and MCP servers over stdio (`roomkit-ai`,
> `roomkit-mcp`), on an OpenAI-compatible endpoint (`roomkit-openai`), and
> a SQLite store on the Python implementation's schema (`roomkit-sqlite`).
> The API will change before 1.0 ([the roadmap](docs/roadmap.md)).

## Why a Rust implementation

The Python implementation is the reference. This one exists for two reasons:

- **Embedding.** Its core performs no I/O and depends on no async runtime, so
  one core can run inside a Rust service or a desktop application, and later
  mobile and WebAssembly hosts. Behind the Python API it reaches CPU parity
  at best, and that step was stopped ([step 3](docs/steps/step-03.md)).
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

### In a service

Nothing is published to crates.io: a service takes the crates from this
repository at its tag, as git dependencies, with Rust 1.92 or later.

```toml
[dependencies]
roomkit = { git = "https://github.com/roomkit-live/roomkit-rs", tag = "v0.1.0" }
roomkit-ai = { git = "https://github.com/roomkit-live/roomkit-rs", tag = "v0.1.0" }
roomkit-openai = { git = "https://github.com/roomkit-live/roomkit-rs", tag = "v0.1.0" }
roomkit-sqlite = { git = "https://github.com/roomkit-live/roomkit-rs", tag = "v0.1.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`roomkit-mcp` comes the same way, for an MCP server's tools. The
repository is private, so Cargo fetches it with your GitHub access: over
HTTPS through git's credential helper, or over SSH with a key your
`ssh-agent` holds (`git = "ssh://git@github.com/roomkit-live/roomkit-rs.git"`),
the same URL on every line: two URLs are two sources, and every crate is
then built twice, its types two types. When Cargo's own git client cannot
authenticate,
`CARGO_NET_GIT_FETCH_WITH_CLI=true`, or `net.git-fetch-with-cli = true` in
`.cargo/config.toml`, has it run the `git` command with your configuration.

`server_agent` shows what the service writes: the store opened with
`SqliteStore::open(path)` and handed to `RoomKitOptions::with_store`, an
`OpenAiAgent` registered on the kit with its `OpenAiConfig`, and a
transport channel of its own whose messages it runs into their rooms. The
documentation of each crate shows its own setup, compiled as a doctest.

### A first room

The kit's calls on their own, with the crate's test channels:

```rust
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{CreateRoomOptions, ProcessInboundOptions, RoomKit};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kit = RoomKit::new(Default::default());
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions { room_id: Some(room.clone()), ..Default::default() }).await?;
    kit.attach_channel(&room, sms.id(), Default::default()).await?;
    kit.attach_channel(&room, agent.id(), Default::default()).await?;

    let message = InboundMessage::new(sms.id().clone(), "+15551234567", TextContent::new("Bonjour"));
    kit.process_inbound(message, ProcessInboundOptions { room_id: Some(room), ..Default::default() }).await?;
    assert_eq!(sms.texts(), ["You said: Bonjour"]);
    kit.close().await?;
    Ok(())
}
```

## Crates

A service depends on `roomkit` and on the crates it needs beside it:

- `roomkit`: the runtime on tokio: `RoomKit`, the `Channel`,
  `ConversationStore` and `InboundRoomRouter` traits, the hooks and the
  typed tool hooks, streamed delivery, the in-memory turn backend, the
  in-memory store, and the test channels of `roomkit::testing`. It
  re-exports the core's model as `roomkit::model`.
- `roomkit-ai`: `AiAgent`, the AI channel over any `AiProvider`: the tool
  loop, `Toolset`, `Tools` and `ToolPolicy`, streamed answers, skills from
  the host's directories, an MCP server's instructions, and a scripted
  provider for tests.
- `roomkit-openai`: `OpenAiAgent`, `roomkit-ai`'s AI channel over an
  OpenAI-compatible Chat Completions endpoint, or stateless Responses with
  `OpenAiConfig::with_api(OpenAiApi::Responses)` on `main` (unreleased).
  Both support streamed text and function tools.
- `roomkit-sqlite`: `SqliteStore`, a store in one SQLite file, on the Python
  implementation's schema, for one process at a time: either
  implementation reads a file the other wrote, as long as its channel
  types are ones Python lists, but the two never share a file live. A type
  of your own (`ChannelType::new("slack")`) makes its room unreadable to
  Python until Python opens its type (RMK-986).
- `roomkit-mcp`: `McpCommand` and `McpServer`, the tools of an MCP server
  started over stdio, served to an AI agent as a `Toolset`.

`roomkit-core` is the sans-IO core: the RFC's model, rules and pipeline
operations, with no I/O and no async runtime. It is no service's API: what
`roomkit` does not re-export of it, the effect protocol and the rules the
driver and the stores call, may change in any 0.x release. A
`ConversationStore` of your own, written from scratch rather than over
`InMemoryStore` or `SqliteStore`, needs some of those rules, which
`roomkit` does not re-export yet: an edit's change of its target
(`rules::edits`), the idempotency keys (`rules::idempotency`) and the
routing lookups' order (`rules::routing::recency`), taken from
`roomkit-core` with that caveat, as `roomkit-sqlite` takes them.

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

- `room_lifecycle`: pause, resume, close, reopen and archive, and the timers `check_all_timers` applies.
- `channel_mute_unmute`: a muted agent still reads, and its answers are stored blocked.
- `room_membership`: participants and their roles, a member who leaves, an agent banned.
- `close_with_work_in_flight`: the kit closes with a send stuck, a delivery queued, a turn streaming, a hook running and a call waiting for its room's lock: what each call reads back, and the reads answering after the close.

Hooks:

- `hook_moderation`: `BEFORE_BROADCAST` hooks in priority order block and redact.
- `hook_inject_welcome`: a hook injects a welcome for each newcomer.
- `hook_off_lock_check`: a slow check off the room lock, bounded by `process_timeout`.

The pipeline:

- `instruction_event`: an instruction directs one agent and is never stored.
- `response_visibility`: who sees what, by the event's visibility and its binding's, and where a message's answers go.
- `send_event`: direct injection, from the host and from an agent's turn.
- `host_post`: the host writes to a room's people in its own name, from its host channel, attached on first use.
- `names_that_read_alike`: senders whose names read alike, in case or in Unicode's confusables, each message committed with its author's name, rank and source.

Routing and identities:

- `shared_sms_number`: one number, several correspondents, and a group's room.
- `paused_conversation`: a sender returns to the conversation its timer paused.
- `identity_addresses`: addresses linked to identities route to their person's room, and a room naming the identity keeps all its numbers.

Agents:

- `openai_agent`: an AI agent answers through an OpenAI-compatible endpoint, reading the conversation labelled by speaker; a local fake of the OpenAI wire stands in unless `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` are set.
- `agent_tools`: five tool calls in one round, one served, one the tool policy refuses, one an approval hook blocks, one that fails, one cut at its bound.
- `close_from_a_tool`: an agent's tool closes the kit from inside its own turn: the close cuts that turn and lets the tool's call run on, returns at once with every channel closed, and the service's own close joins the same shutdown.
- `agent_tool_history`: a booking made through a tool, then a follow-up the model answers from the earlier call's result, which the next turn's history replays as the assistant's call and the tool's result.
- `tool_call_intent`: a call whose model wrote its intent for the person: the tool receives its own arguments, the hooks and both rows read the intent apart, and the next request reads the call with it.
- `streamed_answer`: an answer read as it is written by a streaming channel beside a buffered one, a tool call between two segments, a reader that stops.
- `agent_skills`: skills from `examples/skills/`, a strict discovery, an activation, a reference read, the paths that are refused, and the activation read again after a restart.
- `mcp_tools`: an MCP server's tools over stdio, a served call, an `isError` call, a tool the policy denies, the server's instructions, and connections that fail and leave no process.

A server:

- `server_agent`: the quickstart's service, an agent over WebSocket with its rooms in SQLite; its checks restart the server on the same file, and `--serve` keeps it up.
- `web_chat`: the same service with a page to chat with the agent in a browser, plain HTML, CSS and JavaScript compiled into the binary, answers read as they are written, the conversation's past sent as a page opens (`get_timeline`, `visible_events`); a socket from another origin is refused, and `--serve` keeps it up.

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
- [Step 3](docs/steps/step-03.md): the Rust core behind the Python API,
  stopped after milestone 2.
- [Step 4](docs/steps/step-04.md): a first usable version, an agent on a
  server, persisted, done; the tag waits for step 5.
- [Step 5](docs/steps/step-05.md): one public API, roomkit-rs's in
  Python's terms, then the tag v0.1.0; in progress.
- [Changelog](CHANGELOG.md): what 0.1.0 holds.
- [RFC gaps](docs/rfc-gaps.md): what the specification leaves open, and the
  decisions taken here.

## License

MIT. See [LICENSE](LICENSE).
