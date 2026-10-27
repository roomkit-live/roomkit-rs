# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it
publishes a release.

## [Unreleased]

Nothing is published yet: this section lists what the first release brings,
as it stands.

### Changed

- **An AI agent takes no inbound message** (step 4, milestone 8, review).
  `AiAgent` and `OpenAiAgent` refuse a message sent on their channel
  (`process_inbound` fails, nothing enters the room), as the reference
  implementation's AI channel does: an event under the agent's id was read
  as the agent's own words, and a forged tool row as one of its calls.
- **Every intelligence turn runs through the turn backend** (step 4,
  milestone 7, RFC §10.6). An intelligence channel's turn runs on the
  kit's own turn executor, behind the in-memory backend: its request, its
  output, each item of its stream and each verb of its host cross as
  messages serialized and parsed anew, the request's events alone handed
  over as they are. Its `on_event()` runs in a copy of the execution
  context of the call that submitted it, its stream in that of the call
  that first read it, so reentrancy, depth and budgets hold as before, and
  `on_event()` runs whole within its turn: `current_turn()` is the turn's
  in the work it does before its future too, where it was `None` before.
  Each side beats every 2 seconds, the executor from a task of its own: a
  turn busy for as long as it likes, its thread blocked included, is never
  lost; an executor or an owner silent for 10 seconds is. `TurnHost::send_event`
  crosses as a `host_call`, and after its turn as a `room_call`, refused
  once a later turn of the agent was sent in the room; its errors come
  back as the `KitError` they were when it carries no data, as the new
  `KitError::Turn` otherwise. A turn whose work panics fails `Panic` at
  once. The kit's shutdown cuts the turns first: each crosses as `cancel`,
  the tool calls it ran are reported `cancelled` to their observers and
  their ends stored `cancelled`, the text a reader was handed stored
  `cancelled`, within twice the cancel bound (10 seconds by default), and
  an inbound call running when it began answers `KitError::ShutDown`.
- **Breaking: `ChannelType` is an open name.** It was an enum; it is now a
  string newtype with the well-known names as constants
  (`ChannelType::SMS`, `ChannelType::TEAMS`, ..., `DISCORD` and `BUZZ`,
  which the Python implementation writes), serialized as the plain name,
  so any name a shared store holds is read and written back. A name is
  lowercase ASCII letters, digits, `_` and `-` (`ModelError::InvalidChannelType`
  otherwise), as every type of Python's enumeration; an identity's address
  under another key, which Python takes as written, is kept by the SQLite
  store as a member it does not read, and resolves nothing. Write
  `ChannelType::SMS` for `ChannelType::Sms`, and compare instead of
  matching. How a channel's senders are addressed is its declaration's
  (`ChannelDeclaration::addressing`, the type's default unless the channel
  sets another; any other name is addressed as written), and the type's
  `category()` is gone: a declaration names its category. A channel
  claiming `system`, or `host` without being a host channel, is refused at
  registration (`ConfigError::ReservedChannelType`).
- **Breaking: a smaller public surface** (step 4, milestone 4). What no
  crate outside its own names is crate-private: of the core's rules, 84
  items, the modules `rules::permissions` and `rules::host` whole among
  them (`routing::admitted`, which nothing called, is removed), and the
  constant `ops::DEFAULT_CONTEXT_WINDOW`; `StoreError::to_record`; the
  second path to the host channel's id, `roomkit::HOST_CHANNEL_ID`
  (`roomkit::model::HOST_CHANNEL_ID` stays). `roomkit_sqlite::OpenError::Database`
  holds a `StoreFailure` instead of `rusqlite::Error`, so the SQLite
  library's version is no part of the crate's API; SQLite's error is its
  source.
- **Breaking: the types that will grow are `#[non_exhaustive]`** (step 4,
  milestone 4), so that a member added later breaks no caller: the options
  (`RoomOptions`, `BindingOptions`, `HookSpec`, `HookFilter`), a channel's
  declaration (`ChannelDeclaration`, `ChannelCapabilities`, `RateLimit`,
  `RetryPolicy`), every content struct and `EventContent`, `HookTrigger`
  and `FrameworkEvent`; the errors already were. Build one from its
  `Default` or its constructor, then set fields: `let mut options =
  BindingOptions::default(); options.group = true;` for `BindingOptions {
  group: true, ..Default::default() }`. The store's inputs (`RoomUpdates`,
  `EventUpdates`, `BindingUpdate`, `RoomFilter`, `EventFilter`) are not
  marked, on purpose: a store reads every field, so a new field is a
  breaking change. The contents gain the constructors this needs, each
  taking the members the RFC requires (`MediaContent::new(url,
  mime_type)`, `EditContent::new(target, new_content)`,
  `DeleteContent::new(target)`, ...), and so does `RetryPolicy`; an edit's
  `edit_source` (null by default) and a delete's `delete_type` (`SENDER`
  by default) are set with `with_edit_source` and `with_delete_type`. A
  `match` on `EventContent`, `HookTrigger` or `FrameworkEvent` outside the
  core needs a wildcard arm. Each variant of `FrameworkEvent` is
  non-exhaustive too, so that a member RFC §8.2 adds to a row breaks no
  subscriber: a pattern on one names the members it reads and ends with
  `..`, and a driver or a test builds one with its constructor
  (`FrameworkEvent::room_paused(room_id, reason)`, one per variant, named
  after the RFC's event).
- **Breaking: `OpenAiAgent` is `roomkit-ai`'s AI channel** over the Chat
  Completions wire (step 4, milestone 5): a turn is one tool loop, its
  rows returned at its end, each round's text a message of its own; its
  constructor and configuration are unchanged. A hook on `BEFORE_TOOL_USE`
  or the new `ON_TOOL_CALL` is refused by `RoomKit::hook()`
  (`RegistrationError::PayloadMismatch`): their payload is a tool call,
  and they register through the typed registrations below.
  `transcript::Message` gains `tool_calls` and `tool_call_id`, and `Role`
  gains `Tool`.
- **A room's change made by a turn's tool runs its hooks at the turn's
  depth** (RMK-1004, RFC §8.3, §10.3). The observers of `ON_CHANNEL_ATTACHED`,
  `ON_CHANNEL_DETACHED`, `ON_CHANNEL_MUTED`, `ON_CHANNEL_UNMUTED`,
  `ON_ROOM_PAUSED` and `ON_ROOM_CLOSED` that a change made in the room's
  work fires run in that work, on its budget, their calls at least as deep
  as the turn's response, as a direct `update_event()`'s already did: an
  agent attaching a channel whose greeting it answers stops at the depth
  limit. `AttachChannel`, `DetachChannel`, `UpdateBinding` and the status
  changes gain `made_in(draw, floor)`.
- **A round runs at most 32 tool calls** (step 4, milestone 6, review). An
  AI agent's round runs the first 32 calls its generation made, as the
  Python implementation does; the rest never run, are not stored, are left
  out of the assistant message the model reads next, and are logged at
  `warn` with the tools they named. One completion could make thousands.
- **Breaking: an AI agent's turn is a stream** (step 4, milestone 6, RFC
  §6.1, §12.2 steps 13s and 14s). Within a kit, `AiAgent`'s turn answers
  with its loop as a stream, which the kit reads once the event's delivery
  set is over: each row is stored as it ends, through `BEFORE_BROADCAST`
  once, and delivered to the other targets while the turn goes on; nothing
  of the loop runs but as it is read. A chained stream, one a response's
  delivery asked for, draws one reentry pass as it is read, and past the
  cascade's budget is closed unread, nothing generated, its answer stored
  `BLOCKED` `reentry_loop_cap` (RFC §8.3); the trigger's own draws none, a
  buffered channel one pass per event, and a tool-call row asks no agent.
  `ChannelDeclaration::answers_as_stream`, which milestone 5 added for
  this, is gone. The turn's record, when no row carried it, is written on
  its last message once the turn is over, `ON_EVENT_UPDATED` firing; an
  event a tool sends is stored after its round's text and its call's
  start. A provider's error keeps the text it produced and fails the turn,
  its `ON_ERROR` category `streaming`. Outside a kit, `AiAgent::on_event`
  returns the rows buffered, as before. `OpenAiAgent` streams each
  completion (`stream`, `stream_options.include_usage`), so its requests'
  body changes.
- **A tool-call row reaches only the transports that take one**: a
  channel declares it (`ChannelDeclaration::receives_tool_calls`, true by
  default for `websocket` and `webhook`), so an SMS or a chat transport is
  never handed one, and its delivery never sends `[Tool call: x]`.

### Added

- **MCP servers over stdio** (step 4, milestone 9, RFC §21.2, §21.1, §6.4).
  The new crate `roomkit-mcp`, on `rmcp` 3.8 (its minor version pinned):
  `McpCommand` starts a server, a program and its arguments as a list,
  never through a shell, its environment cleared but for `HOME`, `LOGNAME`,
  `PATH`, `SHELL`, `TERM`, `USER` and the variables it is given, its
  standard error read and logged at debug level so it never blocks;
  `McpServer::connect` runs the MCP handshake and lists the server's tools
  once, following the cursor, within 60 seconds by default
  (`connect_timeout`), and a connection that fails part-way kills the
  process and waits for it before the error (`McpError`) is returned. An
  `McpServer` is a `Toolset`: an agent serves its tools under their bare
  names, a name another of its toolsets serves (a host tool, a skill's
  tool) refused when the agent takes it (`AiError::DuplicateTool`), its
  tool policy and its bound per call applying as to any tool. A call
  answers the result's text, a note for what is not text, and a result
  that says `isError` is a failed call whose words the model reads. What a
  server sends is bounded: a message at 4 MiB, the listing at 1,000 pages
  and 512 tools, a name at 128 characters, a description at 4,096, a
  schema at 32 KiB, a result at 100,000 characters. The process stops at
  `close()` (its input closed, 2 seconds of grace by default, then a kill),
  when the kit of the agent serving its tools shuts down, and when its last
  handle is dropped; one that exits on its own is waited for at once.
  `AiAgent::with_server_instructions` and
  `OpenAiAgent::with_server_instructions` carry what a server published
  (`McpServer::instructions`) in every system prompt, after the agent's
  own and before the skills, under the host's label for the server, in a
  `<server_instructions>` block whose forged closing tag is neutralised,
  cut at 8,192 characters (`AiError::ServerLabel`, `AiError::DuplicateServer`).
  The core's `rules::transcript::fence` (`roomkit::transcript::fence`)
  frames text from outside in such a block (RFC §6.4). `Channel::close`
  (RFC §6.1) and `Toolset::close`, each with a default that releases
  nothing: the kit's shutdown closes each channel once, and an AI agent
  closes its toolsets. The example `mcp_tools` shows them. rmcp's response
  cache is disabled, so no page of the listing stays resident.
- **A tool name OpenAI refuses fails `OpenAiAgent::with_tools`** (RFC
  §6.7). On OpenAI's own endpoint (`https://api.openai.com/v1`), a tool
  whose name is not `[A-Za-z0-9_-]{1,128}`, the names OpenAI accepts as the
  reference implementation measured them, is refused when the agent takes
  its toolset (`AiError::ToolName`, naming the tool and the rule), where it
  failed every turn's request at the vendor: an MCP server's `files.read`,
  say. A server behind another base URL decides itself.
- **Skills from the host's directories** (step 4, milestone 8, RFC §24,
  §6.4, §21.1). `roomkit_ai::SkillRegistry` discovers the skills of
  directories (`discover(&[dir])`, a slice) or registers one (`register`), strictly and
  atomically: the first skill that cannot be loaded fails the call with a
  `SkillError` naming it, and the registry is left as it was. A skill is a
  directory holding a `SKILL.md` whose frontmatter is read line by line,
  with no YAML reader, within RFC §24.1's bounds (65,536 characters, 64
  brackets open at once); a frontmatter the reader cannot read whole, and
  one declaring `allowed_tools` or `requires`, are refused. Names and
  descriptions follow the reference implementation's rules (kebab-case, 64
  characters, the directory's name; 1,024 characters), a `SKILL.md` takes
  256 KiB at most and the manifest 65,536 characters. A link to a skill
  directory is followed inside the directory discovery scans, refused out
  of it. `AiAgent::with_skills` and `OpenAiAgent::with_skills` give an
  agent its skills: each turn's system prompt carries, after the agent's
  own, the reference implementation's preamble and `<available_skills>`
  manifest (`SkillRegistry::to_prompt_xml`), then the instructions of the
  skills active in the room; the agent serves `activate_skill`, which
  answers a skill's instructions and references the first time and an
  acknowledgement after, and `read_skill_reference`, which reads a file of
  the skill's `references/` by a name of `[A-Za-z0-9._-]`, resolved with
  its links followed and inside that directory, 256 KiB at most; each
  file is opened, then checked through its handle: on Linux the path the
  kernel gives the open file (`/proc/self/fd`) must be inside, so one
  swapped for a link out meanwhile, even by an atomic exchange, is never
  read, and an unreadable `/proc` refuses it; on other Unix systems the
  path resolved anew must name the handle's device and inode. The two
  tools escape the agent's tool policy, as RFC §21.1 lists them. An
  activation counts once its call is served, for the room, four skills at
  most; a restarted agent reads the room's served activations again from
  the stored rows its turn is handed, those it wrote outbound. A turn that
  stands alone carries none, and records none in memory. No skill script runs. The example `agent_skills`, over
  `examples/skills/`, shows them.
- **The turn backend** (step 4, milestone 7, RFC §10.6):
  `RoomKit::set_turn_timeouts` with `TurnTimeouts` (accept, heartbeat
  interval and timeout, cancel; the RFC's defaults), `TurnError`, the
  failure a crossing carries, and `testing::TurnTap`, which records every
  message the kit's turns carry as it is serialized. The messages are
  written as the Python implementation's codec writes them, proven both
  ways by a cross-read fixture (`make turns-fixture`); a frame and a
  request are bounded at 16 MiB, and a reader that stops pulling holds
  the executor at a window of 64 items.
- **Streamed responses** (step 4, milestone 6, RFC §6.1, §12.2 steps 13s
  and 14s). A transport that declares
  `ChannelDeclaration::supports_streaming_delivery` reads an agent's answer
  as it is written, through `Channel::deliver_stream`: a `DeliveryStream`
  of `StreamItem`s, the text deltas, then each row once the store holds it,
  with its id and index (`DeliveryStream::next`); it is never delivered
  those rows again, and it is handed no row it may not read, nor anything
  more of an answer the room withdraws from it mid-turn (the agent muted,
  the channel detached, the agent's visibility narrowed away from it). The
  default
  reads the stream to its end, then delivers each row through `deliver()`.
  One such target per response, the first the rows reach; every other
  target receives the rows only. The reader sets the pace: one that stops,
  or a caller that leaves, leaves the text it was handed stored marked
  `cancelled`, the turn recording `cancelled`; a call running is let
  finish, one announced and not running closed `failed`. An intelligence
  channel answers with a stream through `TurnHost::respond_with_stream`,
  a `ResponseStream` of `ResponseItem`s. `OpenAiAgent` reads each
  completion's server-sent events over reqwest, with no SSE crate, the
  calls' fragments folded by index, bounded per line (1 MiB), per event
  (1 MiB) and per answer (4 MiB). `ScriptedRound::writing`, `in_pieces`
  and `failing` script a model's deltas and a failure mid-answer. The
  example `streamed_answer` and `server_agent`'s `&stream=1` connections
  show it.
- **Tools** (step 4, milestone 5, RFC §6.4, §9.3, §21). The `roomkit-ai`
  crate: `AiAgent`, the AI channel over any `AiProvider` (one call, a
  request in, the generation's text, calls and done event out), whose turn
  runs one tool loop: 50 rounds and 30 seconds per call by default
  (`with_max_rounds`, `with_tool_timeout`), a round's calls run
  concurrently, each through its gate in RFC §21.1's order (a call cut
  short, a tool the turn does not carry, the `ToolPolicy`, the arguments'
  schema, then `BEFORE_TOOL_USE`) with the reference implementation's
  texts. The room keeps a round's text as a MESSAGE, a `TOOL_CALL_START` per
  call, then the `TOOL_CALL_END` rows in call order; the last MESSAGE
  carries `loop_end_reason` and `ai_usage`; a later turn replays no call. A
  tool that raises reads as `Tool 'x' failed (<kind>)`, its message for the
  observers alone. `Toolset` serves many tools behind one call, and `Tools`
  holds the host's closures; `arguments::<T>()` reads a call's arguments,
  refusing them as `Invalid arguments` when they do not fit;
  `testing::ScriptedProvider` answers from a script. `OpenAiAgent` gains
  `with_tools`, `with_tool_policy`, `with_max_rounds` and
  `with_tool_timeout`, and declares the tools on the wire; a completion's
  members written `null` read as absent. The kit gains
  `before_tool_use`, `on_tool_call` and `observe_tool_calls` (with
  `ToolHookSpec`, `ToolCallEvent`, `ToolUse`, `ToolVerdict`), and
  `roomkit::current_turn()` gives the turn's `TurnHost`, which writes to
  the room at the turn's depth and runs those hooks. A call's observers
  run as tasks of its turn, and what they send is at least as deep as the
  turn was, once it is over too (RFC §8.3). `HookTrigger::OnToolCall` and
  `model::ToolCall` are new. The examples `agent_tools`, and
  `server_agent`'s order lookup, show them.

- **Repository.** The `roomkit-core`, `roomkit`, `roomkit-ai` and
  `roomkit-openai` crates and the `roomkit-examples` package; workspace lints; the gates `make all`, `make
  examples` and `make msrv`; CI for commit messages, formatting, lints,
  tests, documentation, the examples, the core's dependencies, the minimum
  Rust version, the dependency policy, and a weekly check against the
  security advisories; the architecture, roadmap, step and RFC gap
  documents. The minimum Rust version is written once, as the workspace's
  `rust-version`.
- **The model** (`roomkit_core::model`, RFC §5): rooms, events and every
  content type, bindings and permissions, capabilities, participants,
  identities, tasks, observations, inbound messages and direct events,
  channel outputs and hook results, with their invariants checked at
  construction and their JSON form. A room's `paused_at` and timers, a
  visibility's grammar and intersection (`Visibility::intersect`, gap
  G-009), the partial updates of a room, an event and a binding
  (`RoomUpdates`, `EventUpdates`, `BindingUpdate`, which carries over what
  it does not name, RFC §7.5 rule 6), and what a room is created and a
  channel attached with (`RoomOptions`, `BindingOptions`).
- **The rules** (`roomkit_core::rules`), functions with no I/O and no
  clock: permissions and delivery planning, with solicitation and
  addressing (§7.5, §10.2, §19.3); the visibility a hook's injected event
  and a refused record keep, never wider than their event's (gaps G-031,
  G-036); answers, at any depth of their message's cascade, kept within
  where its answers may go, which each response carries as its own
  `response_visibility` within its channel's, a response's hooks never
  widening it past that and any hook narrowing an event's scope, never
  widening or clearing it (§5.2, §5.12, §9.4, gap G-073); a channel's history
  withholding a soft-deleted event's content (gap G-062); transcoding and the
  text a reader takes, a tool call's and a system notice's included, a
  composite's empty parts skipped,
  a location's coordinates in decimal with a point (`45.0`), an edit a
  channel cannot apply one `Correction: ` text (§6.6, gap G-072); the
  chain-depth limit and a cascade's reentry budget, shared by the calls that
  draw on it (§8.3); hook registrations, the off-lock and locked chains and
  how a chain of SYNC outcomes decides (§9.1–9.4); the lifecycle's
  transitions, which refuse a status they neither leave from nor lead to
  (gap G-074); the timer transitions due at an instant (gap G-045); a
  detach's revocation, kept as the Python implementation keeps it, the
  list `roomkit:detached_channels`, the former `roomkit:detached` map read
  and folded into it, and the `roomkit:` metadata namespace (gaps G-049,
  G-053); an address in its channel's form, a phone number in E.164 by the
  rules of gap G-052 and an email address lower-case, how each channel
  type is addressed, its recipient and chat keys, and what a recipient or
  a chat is, a string that is not blank or an integer, never a float, an
  integer naming the address its digits write; the key absent, null or a
  blank string is no value, and any other value is not a recipient, which
  a delivery refuses and routing reads as someone else's address: no
  routed sender is admitted into a room through it, under its lock
  whatever strategy chose the room, the record never writes over it, a
  `group` binding's is not read, at step 1 included, and a warning names
  the binding and the value's type, never the value, logged by the
  default router and by the record, the rules answering it
  (`routing::routed`, `Refusal::NotARecipient`,
  `Recording::NotARecipient`; gaps G-051, G-057, G-075, G-078, RMK-965); the rooms an organization reaches (§17.2);
  the default routing strategy, its steps 1 and 3 over ACTIVE and PAUSED
  rooms, what keeps a routed room under its lock and what step 2 records of
  a routed sender (§10.4, gaps G-045, G-055, G-058, G-061); the validation
  of an edit or a delete against its target (§10.3, gap G-062); and what an
  instruction must be and what its responses carry (§10.1.1, gap G-065).
- **The effect protocol** (`roomkit_core::ops`): an `Operation` is polled
  for a `Step`, the notices to perform, the adopted entries settled, an
  inbound call's decision once known (`Decided`), and what comes `Next`;
  its `Effect`s each carry a `Request` of one family, hold (the room's
  lock, its delivery lane, its admission ticket), store, channel, hook or
  route, a deadline, the lock held on its behalf, the admission order it
  stands in and the reentry budget a delivery's calls draw on, and are
  answered by one `Outcome` of the same family, `Failed` or
  `DeadlinePassed`. `Operation::adopt` hands an operation a call its own
  work makes into its room, and `Operation::expire` expires one whose
  caller is gone. Every poll reads the environment the run started with;
  `Environment` takes `&self`, `SequentialEnvironment` is shared through an
  `Arc`.
- **The inbound pipeline** (`Inbound`, RFC §10.1, §10.2), for a message
  into a named room or one routed (§10.4): the sender in the channel's form;
  a missing room created with its id, its `ON_ROOM_CREATED` hooks awaited
  before `room_created` and the channel's attach, a session started on any
  channel but voice (gap G-050); an unbound channel attached unless it was
  detached (gap G-053); a routed sender's identity resolved once, the
  kit's router asked, the sender recorded on the room's binding under its
  lock, and the message routed once more when the room no longer admits
  them (gaps G-055, G-061). Then the channel's parse, the off-lock checks
  ordered by the room's admission ticket (§9.5.1, gap G-067), and the pass
  under the room lock: the due timers applied, the status gate, the
  idempotency key, the event's thread parent rooted to its thread's root,
  whatever the pass, the root it lands on read and a parent naming no event
  of the room dropped (§10.1 step 8, gap G-083), an edit's or a delete's
  target, the hooks, a thread parent a hook set rooted alike, the room
  read again after a hook that ran under the lock, the source's
  permissions, the commit with the room's counters and the event's place in
  the room's delivery lane. An accepted edit or delete fires
  `ON_EVENT_UPDATED` or `ON_EVENT_DELETED` once the lock is released, its
  payload the target, which the observers' filters read, and its context
  the room as the commit left it, the change the newest event of its
  history (§10.3, gap G-062). Delivery sets run in index order, one at a
  time per room; a transport delivery with no recipient is refused
  `no_recipient`, a target whose channel is no longer registered skipped
  or reported (gap G-048); responses re-enter with their own source, their
  verification cleared (gap G-035), and within their message's answers
  (gap G-073), injected events and records past the reentry budget are
  stored, a refused record emits
  `room_refused_event` (gap G-043), and step 15 stores the cascade's side
  effects and applies the room metadata updates channels return (gap
  G-049). An `INSTRUCTION` is decided without being stored, delivered to
  the agents it names, each handed the room's context, which a standalone
  instruction's turn reads none of (`turn_context`, re-exported by
  `roomkit`), and fingerprinted on every response (gaps G-042,
  G-065, G-066). Only the framework writes the visibility `internal`: a
  message carrying it, for itself or for its answers, is refused (gaps
  G-041, G-073), a response carrying it is committed with
  `none`, and a hook's modify of an event marked `internal` keeps it (gap
  G-041). The hooks registered ASYNC on
  `BEFORE_BROADCAST` run as observers once the chain allowed the event, on
  the payload its SYNC hooks left (§9.2, found by the shared conformance
  suite's `hooks/an-async-hook-on-a-sync-trigger-observes`, step 3
  milestone 2); they never ran. Each body is the cascade's work while it
  runs: a call it makes into the room draws one of the cascade's hook
  calls and is refused past the cap, as an `AFTER_BROADCAST` body's is, so
  an observer that posts on every event stops (§9.5.1, gap G-087).
- **Deadlines and callers that leave.** The kit's `process_timeout` bounds
  a call from its start to its decision, routing, the parse, the off-lock
  hooks, the ticket, the lock and the locked section, never the commit and
  what follows it (§13.6, gap G-068). A caller that stops waiting, its
  future dropped, never drops the call once anything it ran is committed:
  before the decision, it expires as one past its `process_timeout`, writes
  nothing more of its event and delivers every event it committed (gap
  G-076). Once the call is decided, its turns are cut (§10.1 "Explicit
  delivery cancellation", decision 2 of step 3, `Operation::cancel_turns`),
  as the Python implementation cuts them: a turn of its cascade not started
  never starts, one running is aborted and joined before the run ends, and
  no answer re-enters; its events are delivered to every other target, and
  the entries it adopted keep their turns. A cascade that fails still
  delivers what it committed.
- **Idempotency keys** (§13.4, gap G-079): a blank key, empty or
  whitespace only, is no key: a message, an injected event and a record
  carrying one are processed and stored as carrying none.
- **Direct calls.** `send_event` injects an event through the pipeline
  from its lock on, without a parse or step 2: a refusal before its commit
  is raised with nothing written (§10.5, gap G-069); its `DirectEvent` may
  say where its answers go (`response_visibility`), as a message does (gap
  G-073). `commit_event` commits
  a record under the room lock with the next index and nothing else: no
  hook, no delivery, no activity (gap G-071), its thread parent rooted as
  every committed event's, a record stored `BLOCKED` included (gap G-083);
  it is never a replay, and a
  key its channel holds fails it as the store refuses it (gap G-079). `update_event` and
  `delete_event`, the host's changes of stored events, run under the lock,
  fire their trigger after it with the room read then, and are a no-op on
  a missing target or room; a hard delete takes its replies one level deep
  and the changes of what it removes, returns the target's id, then the
  replies' and the changes' by index, and leaves the counters as they are
  (§10.3, gaps G-063, G-064).
- **The host's own voice** (§10.5, Appendix A.14.1, gap G-085):
  `RoomKit::post` and `OrganizationKit::post` write a `Post` to a room's
  people in the host's name, from a host channel (`ChannelType::HOST`,
  `HostChannel`, a transport, `INBOUND`, every media type), the one the post
  names or the kit's own, registered on first use under `HOST_CHANNEL_ID`
  (`host`). A post is a direct injection that addresses no agent unless it
  names one, speaks as a participant the host channel reaches
  (`InboundError::ParticipantNotFound` otherwise), attaches the host
  channel on a room's first post in one section under the room lock, a
  binding made meanwhile kept, and is refused with nothing written in a
  closed room or one the channel was detached from, from a channel that is
  not a host channel (`InboundError::NotAHostChannel`), and past its one
  `process_timeout`; its key makes a retry a replay. A message on a host
  channel is refused before anything is read
  (`InboundError::InboundOnHostChannel`), and so is an attach of one as an
  intelligence channel (`LifecycleError::HostAsIntelligence`), a post's
  first attach of one declared so included
  (`InboundError::HostAsIntelligence`). The
  `host_post` example shows it.
- **Reentrancy** (§13.5, §9.5.1 Reentrance, §10.1 step 18, gap G-070): code
  that runs inside a room's own work calls into the room without waiting
  for itself. A lifecycle change, a direct change or a record made under the
  lock takes the lock again through the caller's lease; `send_event` or
  `process_inbound_in` made under the lock or in the admission order is
  adopted by the call whose work made it and returns at its commit; a
  turn's or a delivery's call is a new arrival with its own ticket and
  `process_timeout`, drawing on the delivering cascade's budget when it
  comes into the room delivered in, refused past it first thing when it
  names that room, a routed message once step 2 kept its final room for
  it, after any re-route, and a call that creates a room, routed or named,
  charged before the room is created (a call into another existing room,
  and the cascade of a message whose room another call created first, have
  a budget of its own; no message is charged twice), and answered at its
  decision when it comes into its caller's room; a call made while a turn
  runs, into whatever room, is at least as deep as the turn's response
  (§10.6); work a turn or a delivery began starts fresh once that work is
  over, at depth 0 and on a budget of its
  own (gap G-070); a direct injection from the room's work reads its room
  before it draws on the budget, so a call its room refuses draws nothing
  and a caller its pass would refuse never receives an original (§17.2),
  then its key: one its channel holds makes it a replay, which returns the
  event the key committed and draws nothing, past the
  budget too, its record looking the key up again under the lock (gap
  G-081); the body of an ASYNC hook a cascade fires once an event's
  delivery set has run (`AFTER_BROADCAST`, a change's mutation trigger) is
  that cascade's work while it runs: its calls into the room draw on the
  cascade's hook calls, a counter of `ReentryBudget` kept apart from its
  passes (`Draw::HookCall`, beside `Draw::Pass`), at least one deeper than
  its event, and a direct update or delete it makes draws one too,
  `MutationError::ReentryLoopCap` past them, nothing written
  (`AfterDelivery`, `UpdateEvent::made_in`, `DeleteEvent::made_in`, gap
  G-087); a hook, a parse or a route under the room's lock or in its
  admission order carries its cascade's budget, so that a direct change
  it makes is that cascade's work, drawing nothing, its triggers' bodies on
  that budget however soon the body that started the cascade is over; a
  call that would wait for what its own context holds fails
  `KitError::Reentrant`. A call's ticket is released once no section of the
  call holds the room's lock, its decision and its delivery set not waiting
  for it. `roomkit::keep_context` keeps the context in
  a task a hook or a turn awaits as its own.
- **The room lifecycle** (§5.1, §5.7, §7.2, §7.5): `CreateRoom`,
  `AttachChannel`, `DetachChannel`, `UpdateBinding`, `PauseRoom`,
  `ResumeRoom`, `ReopenRoom`, `CloseRoom`, `ArchiveRoom` and `CheckTimers`,
  each but the creation one locked section, with their hooks, their
  framework events and their payloads (`LifecyclePayload`,
  `LifecycleChange`). A change that changes nothing writes and fires
  nothing (gap G-047); a transition from a status it does not leave from is
  refused (gap G-074). Every locked section applies the timer transitions
  that fell due, each at its instant, its framework event emitted at once
  and its trigger fired once the room's lock is released; a delivered event
  resumes a paused room (gap G-045).
- **Lifecycle records** (§5.1 Lifecycle records, gap G-046, step 3
  milestone 2): a channel's attach, the integrator's, step 2's or a first
  post's, its detach, mute and unmute, a binding's access, visibility and
  metadata change, and a timer's pause or close each write a record in the
  timeline, as the Python implementation does: an event of the change's
  type (`CHANNEL_ATTACHED`, `CHANNEL_DETACHED`, `CHANNEL_MUTED`,
  `CHANNEL_UNMUTED`, `CHANNEL_UPDATED`, `SYSTEM` for a timer), the
  framework's source, a `SystemContent` in the Python implementation's
  form, `internal`, at the room's next index, committed under the room's
  lock with the counters, no activity, never delivered and running no hook
  of its own. A room whose status refuses events takes none but a timer's
  record of the transition itself. A room with three attaches stores its
  first message at index 3. The integrator's pause, resume, reopen, close
  and archive, and a room's creation, write none.
- **Authors** (§6.4, §10.1 step 12, §14.1, gap G-089, step 3 milestone
  2): every event that names its sender is committed with the record of
  its author (`metadata.author`: name, rank, source), the room's register
  of authors ranking the sources whose names read alike, in case or in
  Unicode's confusables, digest for digest and rank for rank as the Python
  implementation does. The register is the room's private state, read with
  the room (`RoomContext::author_register`) and written in the event's
  commit (`RoomUpdates::private_state`), rebuilt from the timeline for a
  room that holds none (`StoreRequest::ListEvents`), merged with one the
  room's metadata still holds. A record stored `BLOCKED` takes none and
  loses one it came with, and a direct update that writes an event's
  metadata keeps the record it was committed with. The store gains `get_private_state` and
  `set_private_state`, whose defaults keep the value in the room's
  metadata; `InMemoryStore` keeps it apart. What reads alike is read over
  Unicode tables generated from roomkit (`scripts/build-unicode-tables.py`).
- **The conversation a turn reads** (§6.4, §10.1.1 rules 6 and 7, step 4
  milestone 1): `roomkit::transcript::conversation` gives an agent channel
  the room's latest messages before its turn's event, then the event, as
  `Message`s of the `User` or the `Assistant` role. Once the turns hold two
  speakers or more, or two people or more take part, each line of a
  participant's turn reads `Name: "line"`, its author's name ranked when it
  reads like another's (`Alice (2)`) or `@channel` for a sender with no
  name, a second such sender of the channel `@channel (2)`, speakers being
  counted by who they come from, and the turn's notes say how speakers are
  named. An instruction is the input, marked as the application's; one
  that stands alone reads no history and counts no one taking part. A copy
  of the runtime's marks in a participant's text is replaced, its letters
  read as the Python implementation's pattern reads them (any case, NFKC,
  confusables, a capital `I` for an `l`) and as a name's reading does, in
  any spacing, one that runs over consecutive messages included, and so is
  a mark's opening after any opening punctuation (`[`, `【`, `〔`, `(`…).
  The marks are the Python implementation's. `Role` and `Message` are
  `#[non_exhaustive]`; a message is built with `Message::new`.
- **The OpenAI agent** (`roomkit-openai`, §6.4, step 4 milestone 1):
  `OpenAiAgent`, an intelligence channel whose turn is one Chat Completions
  call to an OpenAI-compatible endpoint, not streamed and with no tool,
  sending its system prompt and the turn's conversation and answering with
  the completion's text. `OpenAiConfig` names the base URL (with no query
  or fragment, refused with `OpenAiError::BaseUrl`), the API key
  (an empty one sends no `Authorization`; `Debug` never prints it), the
  model, the system prompt, how many messages a turn sends at most (all
  those among the 50 events the kit's context holds, by default)
  and the request's timeout (120 seconds); `Debug` and `shown_base_url`
  show the base URL's scheme, host and port only. A provider error, a request that fails, an
  answer longer than 4 MiB or one that is no chat completion fails the turn
  with an `OpenAiError`: `ON_ERROR` fires and the caller reads it on its
  result. A provider error keeps the provider's type, code and message, on
  one line, bounded, the API key removed; nothing else of its answer.
- **A task a hook spawns** (§13.5, §9.5.1, decision 2 of step 3, gap
  G-070): a task spawned within a hook's or a turn's context
  (`keep_context`) acts for it while it runs, whether it awaits the task
  or not, as a task spawned in a Python hook does. Once the hook is over,
  the task's calls are new arrivals, waiting for the room's lock its pass
  still holds; they were refused as `Reentrant`.
- **The delivery cursor** (§5.1, §13.5, §14.1, gap G-091, step 3
  milestone 2): `Room::delivered_index`, the highest index whose delivery
  set has run, `-1` before any, crosses as the Python implementation
  writes it. The store gains `get_delivered_index` and
  `advance_delivered_index`, a compare-and-set from the index before, or
  from any lower value when forced, as Python's stores answer them; a
  room's update and an event's commit leave the cursor as stored. The
  tokio runtime's lane moves it at each index delivered, as Python's does:
  an event's index once its set has run, an index with no delivery set (a
  record stored `BLOCKED`, a lifecycle record) once committed, one index at
  a time.
- **Organizations** (§17.2, decision 2 of step 2b, gap G-077): every room
  operation acts for an organization or for none.
  `RoomKit::for_organization(organization_id)` returns an `OrganizationKit`
  whose calls are the kit's, made for the organization, and whose reads
  (`get_room`, `list_rooms`, `get_event`, `list_events`, `list_bindings`,
  `list_participants`, `get_participant`) see its rooms only. A room of
  another organization, or of none, is not found by its changes, its
  injections and its posts (`RoomNotFound`), before any hook runs or
  anything is drawn, nor by the reads of its records and its roster (the
  store's `RoomNotFound`), holds no event for its updates and deletes,
  and refuses its messages `OtherOrganization`, nothing written in it, its
  due timers included; a room it creates belongs to it, and its messages are
  routed among its rooms and identities only (gaps G-051, G-060). The kit
  itself acts for no organization.
- **The runtime** (`roomkit`), on tokio. `RoomKit` registers and
  unregisters channels, registers hooks (`hook`, `hook_handler` for a
  `HookHandler`, whose `call` returns a `HookFuture`, `before_broadcast`,
  `observe`), sets the router, the environment, the chain-depth limit and
  the `process_timeout`, creates rooms (`create_room`, `create_room_with`
  `RoomOptions`), attaches channels (`attach`, `attach_with`
  `BindingOptions`), detaches, updates, mutes and unmutes them, moves rooms
  through their lifecycle, routes a message (`process_inbound`) or runs one
  into a named room (`process_inbound_in`), injects, commits records,
  updates and deletes stored events, applies room timers on demand
  (`check_timers`) or in the background (`run_timers`, a `TimerSweeper`
  whose stop ends its sweep under way), publishes framework events
  (`subscribe`), and tracks the work it runs in the background: `idle()`
  waits until none runs, `shutdown()` ends it and refuses every later call
  (`KitError::ShutDown`). Its driver executes a run's effects concurrently,
  completing each as it finishes; a call keeps the environment it started
  with. The `Channel`, `InboundRoomRouter` and `ConversationStore` traits,
  `DefaultRouter`, whose `candidates` a router wrapping it reuses with the
  crate's `default_route`, the default strategy's decision,
  `InMemoryStore`, `StoreError` (an unavailable store's cause kept as its
  source: a `StoreFailure`, a type that equals only itself and reads as the
  error it holds, through `Deref`, `AsRef`, `Display` and `into_inner`,
  built with `new` or `From` whatever a boxed error is made from),
  `KitError`, and the testing helpers `RecordingTransport` and `EchoAgent`.
  The crate re-exports the core's model whole and, of its operations and
  rules, only what a caller names.
- **The store** (§14.1): rooms with partial updates and filters, bindings,
  participants by room, identities linked to and unlinked from addresses
  per organization, the routing lookups by binding and by participant,
  each finding the sender by address or by identity, and
  events, a commit writing the event, the room's counters and an edit's or
  a delete's target in one transaction, an index below `event_count`
  refused, a key its source channel holds in the room refused
  (`StoreError::IdempotencyKeyTaken`, nothing written, a blank key being
  no key), the stored `delivery_results` kept by every write but the
  commit, the idempotency lookup from a channel or within the room
  (`get_event_by_idempotency_key`), hard deletes and event counts (§13.4,
  §14.3, gaps G-079, G-080); a delete's commit marks its target's edits
  deleted, and a binding stored again keeps its place, as the Python
  implementation's stores do. A history hands an edit marked deleted as an
  edit, its new content withheld (§10.3).
- **The SQLite store** (`roomkit-sqlite`, step 4 milestone 2):
  `SqliteStore::open(path)`, a `ConversationStore` in one SQLite file on
  the Python implementation's schema, version 12, so that either
  implementation reads the other's file, as long as its channel types are
  ones Python lists (RMK-986): the same tables and indexed columns, each
  record's JSON in `data`; a record's members roomkit-rs does not model
  kept when it rewrites the record; the tables Python derives from the
  timeline kept right (idempotency keys, the full-text index, which a
  deleted message leaves, and the index sequences); the delivery cursor
  read from its column; the tables it does not use left alone; a new file,
  and its missing directory, created at version 12 as Python creates it,
  and a file of another version refused (`OpenError`). One connection on tokio's
  blocking threads, in WAL mode, for one process. The store suite and the
  conformance suite run on it, and a cross-read test reads and rewrites a
  file Python wrote (`make sqlite-fixture`).
- **The server example** (`server_agent`, step 4 milestone 3): an axum
  server where each WebSocket connection to
  `/conversations/{conversation}?user=...&name=...` is a participant of the
  conversation's room, written through the store, a new room made whole
  even when its first client leaves meanwhile, each text frame a
  message run into the room, and the room's deliveries sent on each of its
  connections as JSON frames, each send bounded, a client that stops
  reading dropped, reader included; a refused message, a failed turn or
  one a stop ended answered with an error frame, a message or a frame over the limit
  closed with 1009, a frame at its header, a stop with 1001; the ids a
  connection names checked (1 to 64 letters, digits, `_` or `-`, a name
  of 64 characters at most with no control character, else 400), and
  what a service must add said in its header (TLS, authentication, the
  `Origin` check, who may create rooms, a rate limit, a cap on
  connections); an `OpenAiAgent` answers each message, and a
  `SqliteStore` keeps the rooms, so that a restarted server goes on with
  each conversation's history. Its checks run offline, against the fake of
  the OpenAI wire and a temporary file: two people in one room, labelled
  as §6.4 asks, then a restart on the same file whose next turn reads the
  earlier ones. `--serve [ADDRESS]` keeps it up for a person to chat with.
  Its WebSocket channel is the example's (`examples/shared/websocket.rs`),
  not a crate; `axum`, `tokio-tungstenite` and `futures-util` are
  development dependencies of the examples only. The README's quickstart
  points at it.
- **A kit held weakly** (step 4, milestone 4): `RoomKit::downgrade()`
  gives a `WeakRoomKit`, whose `upgrade()` gives the kit back while a
  clone of it lives and `None` once the host dropped the last. A hook or a
  channel the kit holds holds its kit so: a `RoomKit` clone made a cycle,
  and the kit and its store, a SQLite file included, were never freed.
- **Examples**, one runnable file each, every one checking what it shows:
  the RFC's Appendix B flows (`echo_room`, `new_conversation`,
  `block_and_inject`, `agent_chain`, `advisor_joins`,
  `edit_delete_messages`), rooms and channels (`room_lifecycle`,
  `channel_mute_unmute`, `room_membership`), hooks (`hook_moderation`,
  `hook_inject_welcome`, `hook_off_lock_check`), the pipeline
  (`instruction_event`, `response_visibility`, `send_event`, `host_post`,
  `names_that_read_alike`),
  routing and
  identities (`shared_sms_number`, `paused_conversation`,
  `identity_addresses`), an agent (`openai_agent`, against a local
  fake of the OpenAI wire unless `OPENAI_BASE_URL`, `OPENAI_API_KEY` and
  `OPENAI_MODEL` name an endpoint), and a server (`server_agent`).
