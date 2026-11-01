# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- `DelegateOptions::conversation` and `RoomKit::start_task(room_id,
  agent_id, task, StartTaskOptions { hand_back, organization_id })`,
  Python's (RFC §23.3 step 4, §23.4, RMK-1144, RMK-1158): a worker reads
  its brief, then the parent room's last messages the notified channel may
  read, labelled and quoted as data; `start_task` starts an agent's
  background task on the agent, told how to work and where its findings
  go, handed the room's last 12 messages. `TurnHost::delegate` carries the
  conversation too.
- A member added again without an identity keeps the identification and
  the identity their record had, as Python's (RFC §5.5, RMK-1141).
- `RoomKit::cancel_task(task_id, CancelTaskOptions { organization_id })`,
  Python's (RFC §23.3, RMK-1144): a background task its runner holds ends
  `cancelled`, `ON_TASK_COMPLETED`, the status bus and the notified channel
  told; calls at once cancel it once; an ended task, an inline one, and one
  of another organization's room are left as they stand. A submit the
  runner refuses ends its task failed.
- `AiChannel` reads a round as Python's (RFC §6.4, §21.1): a call's
  arguments are repaired before their schema check, a hub tool's hoisted
  keys folded into `params` and a quoted primitive read as its declared
  type (RMK-1084); each tool result says whether its call failed
  (`is_error`, in its turn and replayed, RMK-1075); a call with no id gets
  `call_<name>_<hex>` (RMK-1074); a stream that ends without its done event
  completes its round (RMK-1092); a refusal's words escape what is past
  ASCII, as `json.dumps` writes them.
- `TurnHost::delegate`, `wait_task` and `room_tasks`, Python's (RFC
  §10.6.5, RMK-1145): a background delegation from the turn's room, its
  result continuing the turn's chain, the tasks the room's hosts delegated
  kept for `wait_task`, and the room's tasks as the status bus lists them.
  After the turn, a verb that needs it is refused before it is sent, a
  `RuntimeError` as Python's.
- `RoomKit::ensure_participant`, `resolve_participant`, `add_member`,
  `remove_member`, `rename_member` and `is_member`, as Python's (RFC §5.5,
  RMK-1140, RMK-1141), with `EnsureParticipantOptions`,
  `ResolveParticipantOptions`, `AddMemberOptions`, `RemoveMemberOptions`,
  `RenameMemberOptions` and `IsMemberOptions`, and the triggers
  `ON_PARTICIPANT_IDENTIFIED`, `_JOINED`, `_LEFT` and `_UPDATED`.
- Breaking: `RoomKitError::ParticipantNotFound`'s `channel_id` is an
  `Option<ChannelId>`, `None` for a member call.
- Breaking: a sender the identity step leaves pending gets a record of its
  own, `pending-` and 16 hex digits of the SHA-256 of the channel and the
  address, and the event is sent under it (RFC §11.2, RMK-1138): a sender
  naming itself after an identity no longer takes that identity's record.
  The address still names the record on its channel, for routing and for
  the identification an advisor gives it; a pending member the host added
  keeps their record. Records an earlier release left pending under an id
  their channel gave keep it: a room's participant whose id is an
  identity's may be such a squat, PENDING, or IDENTIFIED once that identity
  was resolved on another channel, the squatting channel then still in its
  `connected_via`.
- A resolver's `IdentificationStatus::Rejected` holds the message when no
  identity hook answers (RFC §11.2, RMK-1151): blocked with the resolver's
  `message`, else `identity_rejected`, nothing stored, as Python's.
- `RoomKit::delegate`, Python's task delegation to a child room (RFC §23.3,
  RMK-1143): the child room in the parent's organization, its agent and
  the shared channels with their parent bindings' permissions, the task
  told as a system message only intelligence reads and the agent's answer
  kept as the room's trace, streamed or not; inline (`wait`) or on the
  kit's task runner (`RoomKitOptions::task_runner`, `RoomKit::task_runner`,
  `roomkit::tasks::{TaskRunner, InMemoryTaskRunner}`), its result handed
  back to the notified channel (`relay`) or posted as its message
  (`post`). `ON_TASK_DELEGATED` and `ON_TASK_COMPLETED` fire in the parent
  room and the status bus follows the task. `DelegatedTask`,
  `DelegatedTaskResult`; `RoomKitError::Timeout`, Python's `TimeoutError`
  of `DelegatedTask::wait`.
- Python's `AIChannel(tool_call_intents_required=...)` (roomkit 121ea967) is
  `AiChannelOptions::tool_call_intents_required` (RMK-1067): no longer a
  Rust-only exception.
- A turn host's verb after its turn acts in its room's organization, as
  Python's (RMK-1149): one naming that organization is served, and a
  `send_event` for another room or organization raises
  `RoomKitError::Value`, no longer a `RemoteTurn` named `PermissionError`.
- `RoomKit::list_tasks`, a room's tasks of the status it names, for the
  organization it names (RFC §5.10, §17.2).
- The delivery strategies and the delivery queue, as Python's (RMK-1133,
  RFC §22.2): `DeliverOptions::strategy` and
  `RoomKitOptions::delivery_strategy` take `Immediate`, `WaitForIdle` or
  `Queued`, which batches compatible requests and tells each caller its own
  outcome; `RoomKitOptions::delivery_backend` makes `deliver` enqueue its
  request (`queued`) in a `DeliveryBackend`, the `InMemoryDeliveryBackend`
  by default, whose worker `RoomKit::start` starts and the kit's close
  stops: it retries a retryable failure after its delay and dead-letters a
  refusal. `roomkit::delivery` holds `execute_delivery`,
  `run_worker_loop`, `serialize_strategy` and `deserialize_strategy`.
- The status bus, Python's `roomkit.orchestration` (RFC §19.8):
  `StatusBus`, `StatusEntry`, `StatusLevel`, `InMemoryStatusBackend` and
  the `backend::StatusBackend` trait; `RoomKitOptions::status_bus` and
  `RoomKit::status_bus()`. An entry is the `status_posted` framework event,
  and one naming a room fires its `ON_STATUS_POSTED` hooks.
- `RoomKit::update_room_metadata`, `list_observations` and
  `submit_feedback` (RFC §5.1, §15.9), the last firing `ON_FEEDBACK` and
  emitting `feedback`.
- Breaking: `Observation.channel_id` is an `Option<ChannelId>`, `None`
  written `""` as Python writes the observation of a feedback naming no
  channel; `Observation::new` takes `Some(id)`, an id or `None`.
- `TurnHost::post_status`, `update_room_metadata` and `add_observation`,
  run on the room's owner during the turn and after it (RFC §10.6.5).
- A turn host's verb for another room or organization raises
  `RoomKitError::Value`, Python's `ValueError`, no longer a
  `RemoteTurn` named `PermissionError`.
- `FromUrlOptions::connect_timeout`, Python's `from_url(connect_timeout=60.0)`
  (RMK-1108): an HTTP server's handshake and discovery take the bound the
  caller gives, 60 seconds by default. Python now bounds its connection and
  refuses URL credentials, fragments and the transport's headers as Rust
  does (RMK-1068), so neither is a Rust-only exception any more.
- `RoomKit::deliver` brings content into a room from outside it, as
  Python's does (RMK-1133, RFC §22): a message the framework's own sender
  writes on the room's transport, its address kept, its outcome reported
  (`DeliveryOutcome`), `BEFORE_DELIVER` able to block or rewrite it and
  `AFTER_DELIVER` observing it. A keyed delivery names one publication in
  its room, stored under `roomkit:publication:<key>`: the pipeline drops a
  key in that namespace from any other message, and `send_event`, `post`
  and `commit_event` refuse one.
- `TurnTimeouts::silent_intervals` rounds a half to the even neighbour, as
  Python's `round` (RMK-1093): a 5 s heartbeat timeout over a 2 s interval
  is 2 silent intervals, no longer 3; the accept bound counts alike.
- `current_turn_host()` is `None` for a host that can no longer reach its
  owner, a host whose kit is gone, as Python's (RMK-1094).
- A parsed event keeps the direction its parse wrote, as Python's does
  (RMK-1123): a channel's echo of its own output stays outbound, the
  channel's own words, and authors none of the messages of the participant
  it names (RFC §10.3 Validation rule 3).
- A provider's delivery status reaches its room, as Python's does
  (RMK-1133): `process_delivery_status` dispatches a `DeliveryStatus` to
  the `ON_DELIVERY_STATUS` hooks of the room it names, else of the room
  its recipient's binding of its channel names (RFC §10.4), and returns
  while they run; `on_delivery_status` registers a handler of the status
  alone; `process_webhook` takes a `WebhookMeta` (`roomkit::providers::sms`)
  as a message received or a delivery status.
- Identity resolution at step 5 of the inbound pipeline, as Python's
  (RMK-1134): `RoomKitOptions::identity_resolver`, `identity_channel_types`
  and `identity_timeout`, the `IdentityResolver` trait and
  `MockIdentityResolver` (`roomkit::identity`), `kit.identity_hook` on
  `ON_IDENTITY_AMBIGUOUS` and `ON_IDENTITY_UNKNOWN`, `IdentityResult`,
  `IdentityHookResult`, `identity_enabled_for`, `identity_resolver`,
  `identity_timeout`, `Channel::sender_is_participant`, the
  `identity_resolved` and `identity_timeout` framework events, and the
  sender's record written identified or pending under the room lock;
  a resolver that fails fails the message, `RoomKitError::Identity`.
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
- Under the lock, a routed sender's record reads what the room heard on the
  channel over its whole timeline, as Python's `_heard_from_other` reads it
  (RMK-1071): a room that heard someone else there, however long ago, is
  no longer the sender's.
- From the review of the inbound fidelity fixes: an instruction whose parse
  sets an idempotency key is refused `instruction_not_idempotent`, a blank
  key a parse sets is none, and a message a turn sends is at least as deep
  as the turn's answer whatever depth its parse set (RMK-1083); the
  framework's own message is no other sender for routing (RMK-1100); a
  partial failure's `total` counts the deliveries alone, not an agent's
  turn (RMK-1095); a turn whose frames end or repeat before its output or
  in its stream reads as a lost executor, as Python's (RMK-1106).
- An agent whose turn fails with its provider's explicit refusal reports
  the refusal's code and `retryable` across the turn backend, as Python's
  does (RMK-1085): a refusal declared not retryable reads so.

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
