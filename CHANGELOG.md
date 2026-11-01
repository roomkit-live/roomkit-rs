# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-11-01

The first release of roomkit-rs, the Rust implementation of RoomKit: Level 0
of the RFC with nothing left out, on a tokio runtime, an AI channel with
tools, streamed answers, skills and MCP servers, a provider for
OpenAI-compatible endpoints, and a SQLite store on the Python
implementation's schema. Its public API is the Python implementation's
for every feature both have: the same names, parameters, defaults and
behaviour, checked by `make api-parity` against Python's API snapshot (roomkit
b938a855, after 0.111.0). What only the language forces is listed in `docs/architecture.md`,
every behaviour still different in `docs/rfc-gaps.md`, each with its card.

Nothing is published to crates.io: a service takes the crates from the
repository at the tag `v0.1.0`, as git dependencies, with Rust 1.92 or later
(README, "In a service"). The API will change before 1.0.

### Added

- **`roomkit`, the runtime.** `RoomKit` and its `RoomKitOptions`: channels
  registered and attached, rooms through their lifecycle and their timers,
  the inbound and broadcast pipelines (RFC §10) with hooks (`kit.hook`, on
  events, tool calls and session starts; hooks per room), permissions,
  transcoding, the chain-depth limit, routing, edits and deletes,
  instructions, direct injection, framework events (`kit.on`), read markers
  and ephemeral events (typing, presence, reactions), the turn
  backend and the kit's own turn executor, one `close()`. The `Channel`,
  `ConversationStore` and `InboundRoomRouter` traits, `InMemoryStore`,
  `DefaultInboundRoomRouter`, streamed delivery, a transport's retry
  policy, rate limit and circuit breaker, and `roomkit::testing`'s
  `RecordingTransport` and `EchoAgent`. The core's model is re-exported as
  `roomkit::model`.
- **`roomkit-ai`, the AI channel.** `AiChannel` over any `AiProvider`: the
  tool loop with its bounds, `AiTool`, `ToolHandler`, `Tool`,
  `compose_tool_handlers` and `ToolPolicy`, tool-call intents, streamed
  answers, skills from the host's directories, an MCP server's
  instructions, and `MockAiProvider` for tests.
- **`roomkit-openai`.** `OpenAiAiProvider` over an OpenAI-compatible Chat
  Completions endpoint, or stateless Responses (`OpenAiApi::Responses`),
  with streamed text and function tools, Python's model profile, and
  renewable bearer credentials (`TokenSource`).
- **`roomkit-sqlite`.** `SqliteStore`, one SQLite file on the Python
  implementation's schema (version 13; a version 12 file is migrated on
  open), with Python's full-text `search_events`; one writer and four
  read-only connections, for one process at a time.
- **`roomkit-mcp`.** `McpToolProvider`: an MCP server's tools over stdio,
  or Streamable HTTP with `features = ["http"]`, its connection bounded at
  60 seconds.
- **`roomkit-core`**, the sans-IO core the crates run on: the RFC's model,
  rules and pipeline operations, with no I/O and no async runtime. It is no
  service's API; what `roomkit` does not re-export of it may change in any
  0.x release.

### Known differences

`docs/rfc-gaps.md` lists what still differs from the Python
implementation, each with its card (RMK-1070 to RMK-1077, and the review's
RMK-1082 to RMK-1106 under RMK-1081), to close after this release.

[Unreleased]: https://github.com/roomkit-live/roomkit-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/roomkit-live/roomkit-rs/releases/tag/v0.1.0
