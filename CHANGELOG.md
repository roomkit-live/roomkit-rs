# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- The inbound pipeline keeps what a channel's `handle_inbound` parsed, as
  Python's does (RMK-1083): the message's type, visibility, address,
  thread, idempotency key, depth, response scope and instant fill only the
  fields the parse left unset, so an SMS with media is stored `MMS` and a
  key read off the wire dedupes a redelivery. An instruction carries
  `standalone` only when it stands alone. Breaking: `default_inbound_event`
  takes the channel, `default_inbound_event(self, &message, &context)`,
  and sources the event from its id, type and provider.
- The kit calls a channel's own address and streaming methods where
  Python's does (RMK-1082): `normalize_address` on every inbound sender
  before routing and on the correspondent an attach names,
  `conversation_address` and `reply_metadata` to route and record a
  sender, `recipient_address` and `recipient_metadata` to write an
  attach's recipient, and `supports_streaming_delivery_for` per room as a
  response streams. A channel that keeps the default `reply_metadata`
  records no recipient for a routed sender, as Python's does; a host's
  addresses on a `TransportChannel` attach are written in E.164.
- A delivery's context holds its event, as Python's broadcast context does
  (RMK-1070): the room's window read before the commit, then the committed
  event as its newest; a hook still reads the window.
- The default router's step 3 reads a room's history on the channel only
  up to the first other sender, and none for a `group` binding, as
  Python's does (RMK-1100).
- A delivery set with a failure emits `broadcast_partial_failure` first,
  then each `delivery_succeeded`, then each `delivery_failed`, Python's
  order (RMK-1095).
- `visible_events` reads the deleted marks of the received events alone,
  as Python's does (RMK-1102).
- A turn's link failure reads as Python's `TurnLinkError` says it, its
  reason no longer heading the message (RMK-1106).

- A store that keeps no private state of its own, the trait's provided
  `get_private_state` and `set_private_state` keeping it in the room's
  metadata, says so once with a warning, as Python's (RMK-1101).

- The in-memory realtime backend keeps one queued event per id, as
  Python's: an event published twice before its callback runs reaches it
  once (RMK-1113). A closed backend starts no subscription, and one dropped
  unclosed stops its tasks.

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
