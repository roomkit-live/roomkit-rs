# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Update reqwest to 0.13 and sha2 to 0.11, the versions roomkit-tools uses,
  so a host that links both compiles one copy of each. The OpenAI agent and
  MCP over HTTP keep rustls with the ring provider and the webpki roots, now
  given to reqwest as a rustls configuration: 0.13 has no ring-backed
  feature, and its default provider, aws-lc, is not compiled. Both clients
  offer HTTP/1.1 by ALPN and ask for uncompressed answers, as before, even
  in a host that enables reqwest's gzip feature.

- Hosts can opt into required intent fields with
  `with_tool_call_intents(true).with_tool_call_intents_required(true)` on AI
  and OpenAI agents. The requirement applies to model-facing schemas only;
  tool arguments, execution and the optional default remain unchanged.

- Integrate the compatibility features on the current main API, preserving
  tool-call intents, shutdown semantics and last-generation usage together.

- A tool call's intent, the sentence the model writes for the person with each
  call (RFC §6.4, RMK-1040), as Python's `AIChannel(tool_call_intents=True)`:
  `AiAgent::with_tool_call_intents(true)` (and `OpenAiAgent`'s) shows each
  tool's schema with the optional `_roomkit_intent` property, takes it out of
  a call's arguments before its start and its gate, and carries it, normalized,
  as `ToolCall::intent` and `ToolCallContent::intent`: on the hooks' event, both
  rows and the turn wire, written only when set. A closed schema no longer
  refuses the property; a tool whose schema declares it keeps it; the model
  reads its calls back with their intent. Off by default.

- Add optional Streamable HTTP MCP connections with JSON/SSE replies, explicit
  headers, bounded messages, cancellable discovery and session cleanup. Redirects
  and automatic tool replay after session expiry are disabled. Long-running tools
  retain the caller's execution deadline when they delay their HTTP headers.

- MCP connections support host labels, per-handle tool selection, transport status
  and joined cancellation during startup.

- Preserve bounded author images separately from transcript text. Vision-enabled
  agents resolve opaque references through a room-scoped host resolver, and the
  OpenAI adapter sends native image parts in Chat Completions and Responses.

- Add explicit cancellation requests on detached delivery handles, using scoped
  cascade cleanup while preserving committed events and shared providers.

- Accept omitted optional tool namespaces in Responses output for a single configured
  namespace; still reject explicit foreign namespaces and restore the configured
  namespace on replay.

- Assemble Responses output from completed item events when a successful terminal
  event omits its output, preserving text, tool calls, reasoning and usage.

- Resolve renewable OpenAI bearer credentials before each generation through a
  host-owned token source, with safe account errors. Responses can group tools
  under a namespace and rejects calls outside the configured namespace.


- AI turn records expose last-generation usage, model identity and accounting
  completeness separately from cumulative usage. Missing OpenAI token counts
  remain unknown instead of becoming zero. (RMK-1029)

The public API in the Python implementation's terms (step 5, lot 1: the
kit, its construction and its calls; lot 2: the model and the errors;
lot 3: the hooks, the framework events and the turn's host; lot 4: the
channels, the addresses, the routing and the store; lot 5: the skills,
the tool policy, the SQLite store, the provider contract, the AI channel,
the OpenAI provider and the MCP provider). The entries that name
`AiAgent`, `OpenAiAgent` or their builders read `AiChannel` and its
options, `McpCommand` and `McpServer` read `McpToolProvider`.

### Changed

- The kit's calls fail with `RoomKitError`, the Python implementation's
  base class, for `KitError`: its variants are flat, each named after one
  of Python's exception classes (`RoomNotFound`, `RoomAlreadyExists`,
  `RoomClosed`, `TransitionRefused`, `ProcessTimeout`, `ChangeRefused`,
  `ReentrantCall`, `ReentryLoopCap`, `ChannelNotFound`,
  `ChannelNotRegistered`, `ChannelAlreadyRegistered`,
  `ParticipantNotFound`, `IdentityNotFound`, `AddressLinked`,
  `EventNotFound`, `IdempotencyKeyTaken`), Python's builtins (`Value`,
  `Cancelled`, `ExceptionGroup`), and what a dependency raised (`Store`,
  `Lock`, `Channel`, `Router`). `roomkit` no longer re-exports the core's
  per-operation errors (`InboundError`, `LifecycleError`,
  `MutationError`, `ConfigError`, `RegistrationError`,
  `ProtocolError`). The one `RoomNotFound` reads a missing room and
  another organization's alike, where three variants did. Renamed: the
  entries below that name `KitError::ShutDown`, `ChannelsFailedToClose`,
  `Reentrant`, `UnknownChannel` and `Abandoned` read `KitClosed`,
  `ExceptionGroup`, `ReentrantCall`, `ChannelNotRegistered` and
  `Cancelled`; `Protocol`, `Stalled` and `NoEvent` are `Internal`.
- A direct injection refused before its commit raises Python's class for
  it, as `_raise_unless_committed` does: `RoomClosed`, `ProcessTimeout`,
  `ChangeRefused { reason }`, `ChannelNotFound` for a channel the room
  does not bind, `ReentryLoopCap`, `Value` for an instruction's refusal,
  where it was `InboundError::Refused { reason }`. `create_room` of a
  taken id is `RoomAlreadyExists` and `commit_event` under a key its
  channel holds `IdempotencyKeyTaken`, where both read as the store's
  failure; a store's `ValueError` is `Value`. `StoreError::RoomExists`
  is `RoomAlreadyExists`, `InvalidArgument` `Value(String)`.
- A channel of type `host` registers whatever its category and direction,
  as the Python implementation registers it; only one of type `system`
  is refused, as `RoomKitError::Value`.
- The model takes the Python implementation's shape. A model's `new`
  takes its required fields and sets Python's defaults, a field Python
  fills by a factory filled from the system clock and random ids:
  `RoomEvent::new(room_id, source, content)`, `Room::new(id)`,
  `Participant::new(id, room_id, channel_id)`, `ChannelBinding::new(
  channel_id, room_id, channel_type)`, `Task::new(id, room_id, title)`,
  `Observation::new(id, room_id, channel_id, content)`,
  `EventSource::new(channel_id, channel_type)` (inbound),
  `Identity::new(id)`, `InjectedEvent::new(event)`, `DeliveryResult::new`,
  `DeliveryError::new` (retryable), `ProviderResult::new`; the rest is set
  by struct update, which the models a caller builds now allow, their
  `#[non_exhaustive]` gone (the contents, `ChannelCapabilities`,
  `RateLimit`, `RetryPolicy`, `ChannelDeclaration`, `ToolCallEvent`,
  `TurnTimeouts`). The builder methods go (`with_edit_source`,
  `with_delete_type`, `RetryPolicy::new`, `ChannelOutput::respond` for
  `ChannelOutput::empty()`, `InjectedEvent::to_channels` and
  `stored_only`). `RoomEvent.event_type` is `r#type`, serialized `type`
  as before.
- A content converts into `EventContent`, and every parameter that takes
  one takes `impl Into<EventContent>`: `TextContent::new("Bonjour")` for
  `EventContent::text`, which goes. `MediaContent::new`,
  `AudioContent::new` and `VideoContent::new(url)` refuse a URL no reader
  may take, when built and when read (RFC §6.4): a link's or a `data:`
  URI's scheme lower-cased, a reference `<scheme>://` of two characters
  at least, not `file`, `ftp`, `javascript` or `vbscript`, with no `..`
  segment and no backslash; an audio's or a video's URL starting with
  `http://`, `https://` or `data:`; a negative duration refused.
  `VideoContent`'s MIME type defaults to `video/mp4`.
- A value a model refuses, which a struct literal can hold past its
  constructor, is refused at every door that writes it, nothing written:
  the kit's calls before anything is read (`RoomKitError::Value`), a
  hook's replacement or injected event (the hook reads as raising), a
  channel's answer (the channel reads as failing) and every store's write
  (`StoreError::Value`, through `roomkit::written`): a stored record a
  later read would refuse no longer poisons its room.
- `HookResult` carries Python's `action` (`HookAction`) and `event`, for
  `HookDecision`; `HookResult::allow(AllowOptions)`, `block(reason,
  BlockOptions)` and `modify(event, ModifyOptions)` take Python's
  keywords (`roomkit::model::hook`), and `new(action)` is Python's
  constructor; `with_reason`, `inject`, `with_task`, `with_observation`
  and `is_block` go. A block without a reason is refused when read, and
  one built by literal reads as a hook that raised Python's `ValueError`,
  as a modify without its event does.
- `InvalidCountryCode`'s tuple field is private, as Python's `ValueError`
  holds none: code that read the refused value through `.0` reads it in
  the error's message instead.
- `RoomKitError::ReentrantCall` and `AddressLinked` carry the values
  Python's messages name, and read as Python's: `ReentrantCall { room_id }`
  ("A message routed into room ... cannot be adopted: name the room to
  send into it"), `AddressLinked { channel_type }` ("This sms address is
  linked to another identity of the organization"), as does
  `StoreError::AddressLinked`, which no longer tells a caller the address
  it passed nor another identity's id.
- A call whose run the kit's close aborts reports the answers its cascade
  stored as its `response_events`, and so does the handle of a call that
  run adopted, as Python's callers read them (they were missing; the
  adopted call's were missing on a current-thread runtime only).
- A call's `delivery_results` reports every target that failed, as the
  Python implementation's does: a transport whose `on_event()` or
  `deliver()` raised, and an agent whose turn raised or answered an error,
  failed with the code `ChannelError` (a boxed error has no class name;
  Python writes the class's), retryable. A channel returns Python's
  `ProviderDeliveryError` as `RoomKitError::ProviderDelivery { result }`,
  read as Python reads it: the code `result.metadata["code"]`, else
  `result.error`, else `ProviderDeliveryFailed`; retryable as
  `metadata["retryable"]` says when a bool, else retryable; the result
  kept. A provider's refusal reads the same (it was `provider_refused`,
  not retryable), a deliver that raised is retryable (it was not), and a
  binding with no recipient reads Python's `NoRecipientError`: message
  `no_recipient`, its provider result kept.
- `InboundResult` gains Python's `duplicate` (a replayed key),
  `unavailable_targets` (the addresses no turn reached: the committed plan
  asks none of them, or the agent's channel was unregistered before its
  turn, which its delivery result then reports failed
  `channel_not_registered`) and `response_events` (the answers the call's
  cascade delivered; an adopted call's wait reads its own).
- `RoomEvent.consumption` and its three types go, `RateLimit.burst` and
  `ChannelCapabilities.rate_limit` too (step 5, point 5); `RateLimit`
  gains `max_per_hour` and refuses a limit not above 0,
  `ChannelCapabilities` the four audio and video members, and `Identity`
  `email`, `phone` and `external_ids`. `RetryPolicy` takes Python's
  defaults. A capability limit of 0 and a retry delay or multiplier not
  above 0 are refused, as Python's `gt=0` refuses them, when read and at
  every door that writes one. A file roomkit-sqlite and the Python implementation share now
  reads and rewrites with nothing lost and nothing added.
- `RoomContext::binding` is `get_binding`, beside Python's
  `other_channels` and `channels_by_type`; `ChannelBinding` gains `sends`
  and `can_write`. `DeliveryStatus` is `DeliveryResultStatus`.
- Hidden, as the Python implementation keeps them private: the
  `Access`, `ChannelDirection` and `ChannelMediaType` helpers,
  `ChannelCapabilities::carries`, `RoomStatus::accepts_events`,
  `Identity::uses`, `link` and `unlink`, `Visibility::channels` and
  `admits` (`ChannelSet::new(ids).map(Visibility::Channels)` builds the
  list form), `CompositeContent::into_parts`, the ids' `from_u128`,
  `ChannelId::system` and `host`, `HOST_CHANNEL_ID`,
  `MAX_COMPOSITE_DEPTH` and `roomkit::model::ToolCall`.
  `EventContent::composite_depth` and `Visibility::intersect` are
  crate-private.

- A stream's text deltas yielded without a wait between them cross the
  turn link as one item, their text joined and their number told in the
  item's `deltas`, up to the 64 a pull grants, sent once the stream waits
  and before any other frame, as the Python implementation's
  `FrameSender` sends them (RFC §10.6.4): a streaming target is handed
  them as one piece. 200 deltas yielded at once reach a target as four
  items, where it was handed 200.
- `RoomKit::new(RoomKitOptions { .. })` builds a kit with the Python
  implementation's constructor keywords: `store`, `inbound_router`,
  `max_chain_depth`, `agent_response_policy` and `process_timeout`;
  `RoomKitOptions::with_store(store)` gives the defaults over a store of
  another type. `RoomKit::with_store` and the setters (`set_router`,
  `set_environment`, `set_max_chain_depth`, `set_process_timeout`,
  `set_turn_timeouts`) go. A test builds a kit on its own clock and ids
  with `roomkit::testing::kit_with_environment`, where `Environment` and
  `SequentialEnvironment` now live; `SystemEnvironment` is no longer
  public.
- `process_timeout` is 30 seconds unless set, as Python's: a call was
  unbounded by default.
- A room the kit creates, step 2's for a message included, takes the kit's
  `agent_response_policy` unless it names one.
- A router takes Python's parameters: `InboundRoomRouter::route(channel_id,
  channel_type, RouteOptions)`, returning a boxed `RouterFuture`, with no
  store argument. `DefaultInboundRoomRouter::new(store, options)` replaces
  `DefaultRouter`, reading a channel's addresses through `channel_of`;
  `RouteQuery` and `RouteSender` leave the public API.
  `RoomKit::get_channel` gives the kit's view of a registered channel
  (`RegisteredChannel`).
- The kit's calls take the Python implementation's names, its required
  parameters positional and the rest in one `<Call>Options` with Python's
  names and defaults: `create_room(CreateRoomOptions)`, its id optional and
  its timers a `RoomTimers`; `attach_channel` (for `attach` and
  `attach_with`), `detach_channel`, `mute`, `unmute`; `send_event(room_id,
  channel_id, content, SendEventOptions)`, which gains `provider` and
  `created_at` (`DirectEvent` gains them as public fields too, so a
  literal of it without `..` no longer compiles); `post(room_id, content,
  PostOptions)`; `commit_event`,
  `update_event(UpdateEventOptions)`, `delete_event(DeleteEventOptions)`,
  `get_timeline(GetTimelineOptions)` (for `TimelineQuery`);
  `check_all_timers` (for `check_timers`) and the five transitions.
  `process_inbound(message, ProcessInboundOptions)` is one call: a
  `room_id` runs the message into that room, as `process_inbound_in` did.
- `ProcessInboundOptions { defer_delivery: true, .. }` returns at the
  commit, its blocking decisions made (RFC §10.1 step 18, detached
  completion): the result's `delivery`, a `DeliveryHandle`, has `done()`
  and `wait(&self)`, which reports the call as one that waited would have
  and may be called again, the result keeping the handle, as Python's. The
  handle is cheap to clone. A deferred call refused before the locked
  region gets none. A wait from the room's own work returns unwaited while
  the delivery is not done; one from admission work on a call that work
  adopted returns its delivery results (gap G-082). `InboundResult` is now
  `roomkit`'s, converted from the core's, and gains Python's
  `cancellation_reason`.
- Every call takes the organization it acts for in its options,
  `organization_id`; `for_organization` and `OrganizationKit` go. A room of
  another organization reads as not found, as Python reads it: `None` from
  `update_event`, no id from `delete_event`, `RoomNotFound` from the rest.
- New calls, as Python's: `get_room` (a missing room fails
  `RoomNotFound`), `get_binding`, `list_bindings`, `list_members` (the
  ACTIVE participants unless `include_left`), `check_room_timers`, and the
  binding setters `set_access`, `set_visibility` and
  `update_binding_metadata`, which merges key by key; `update_binding`
  goes.
- `detach_channel` returns whether the room bound the channel, `false`
  rather than an error for one it does not; `unregister_channel` hands the
  channel back, not closed, for its caller to close
  (`Option<Arc<dyn RegisteredChannel>>`, `None` for an unknown channel), as
  Python's; `attach_channel` refuses in Python's order: the visibility
  `internal`, then the room, then the channel's registration, then a host
  channel bound as an intelligence one; `get_timeline` reads the room before it checks the query, a
  missing room `RoomNotFound` whatever the query.
- `RoomOptions`, `BindingOptions`, `BindingUpdate` and `Post` leave
  `roomkit::model`.
- A message into a room of another organization, named or routed, fails
  `InboundError::RoomNotFound`, as RFC §17.2 reads such a room;
  `InboundError::OtherOrganization` goes.
- A room's creation drops a metadata key only the framework writes (under
  `roomkit:`, or of the room's private state) with a warning and writes the
  rest (RFC §5.1); it was refused.
- `RoomKit::close` replaces `shutdown`, as Python's `close` since RMK-1030
  (RFC §12.10.4). Once it starts, every call that runs an operation and
  every registration of a channel or a hook returns
  `RoomKitError::KitClosed`, whoever makes it, a hook, a tool or a
  channel's close included, while the reads (`get_room`, `get_timeline`,
  `get_binding`, `list_bindings`, `list_members`, `get_channel`, `store`)
  answer during the close and after it; `unregister_channel` returns a
  `Result` for that refusal. What has not started is cut at once: a
  delivery queued in a room's lane is dropped, never sent,
  `cancellation_reason` `kit_closed` with no delivery result, as is a set
  that would take its place from then on; a call waiting for its room's
  lock or admission ticket before its commit is cut there, nothing of it
  stored, a message reading `blocked`, its `reason` and
  `cancellation_reason` `kit_closed`, `send_event`, `commit_event` and a
  room change `KitClosed`; every model turn is cut (below). The running
  work, a hook, an observer, a store write, a room change, a transport
  send, gets one grace of 5 s from the close's start, then is cut: a call
  before its commit as one waiting for its lock; one whose commit had
  started reads its event back by its id within the unwind, 1 s, and
  reports it stored, `kit_closed`, or, the read failing, nothing stored,
  with a warning; a call past its decision has its calls in flight and
  from then on fail `kit_closed`, each target delivered keeping `sent`, a
  send cut reading `failed`, code `kit_closed`, retryable, the answers its
  cascade stored as its `response_events`. A call under way as the close
  started reads `kit_closed` even when its sends end within the grace, and
  never a failure the close caused as its error; a call that commits
  within the grace is stored and delivered to no one. Then the turn
  backend takes no more turns, the channels close once, one after the
  other, what their closes started is cut, work still running after its
  cut is named in one warning, and the kit is sealed. The close takes the
  grace, plus the turn backend's unwind, plus the channels' closes, plus
  at most 1 s twice: with a send, a write, an observer and a hook holding
  its room's lock that never end, it took 15 s, and takes 5 s. A write is no longer waited for three
  times and left running past the close: a commit of 7 s whose caller left
  is cut at 5 s, never stored. A failure the close causes is logged at
  debug; a write that fails on its own once its caller is gone is still
  logged as an error.
- The kit has one shutdown, as Python's since RMK-1015 (RFC §12.10.4,
  RMK-1017): the first `close` starts it in a task of its own, and every
  `close`, concurrent or later, waits for it and returns once it ended; a
  second `close` returned at once, before anything was closed. A `close`
  whose future is dropped stops only its own wait: the shutdown runs to its
  end, where it stopped half-way. A `close` from a channel's close returns
  at once.
- `RoomKit::close` returns `Result<(), RoomKitError>`, its end answering
  every caller, the first included, as Python's `close` raises:
  `RoomKitError::ExceptionGroup` names each channel whose `close`
  panicked, with what its panic said, the channels after it closed all the
  same, where the panic killed the shutdown and left them open; and
  `RoomKitError::ShutdownCancelled`, Python's `ShutdownCancelledError`,
  answers a `close` after a shutdown that never ended, its runtime dropped
  under it. A `close` that panics before it returns its future is caught
  as one whose future panics. `RoomKitError::KitClosed` is Python's
  `KitClosedError`.
- A `close` from the kit's own work, a hook's body, a hook a delivery
  fired, a channel's delivery, an agent's tool call or a task spawned with
  `keep_context`, neither waits for that work nor cuts it, whether it
  started the shutdown or joined one under way: it returns once every
  channel is closed, where it waited for the close's 5 s grace and was then
  cut with its work, leaving the channels open and the kit unsealed. A
  `close` from an observer the kit fires and forgets, an ASYNC hook or a
  tool call's observer, which starts with fresh marks, spares that
  observer alone. The deliveries queued behind the spared work's place in
  its room's lane are dropped at once, as every queued delivery, and the
  messages waiting for the room's lock or admission ticket it holds are
  cut at once, nothing of them stored. Once the kit is sealed, the spared
  work stores and delivers nothing: its writes and its channel calls are
  refused, a message whose own commit is so refused reading as one the
  close cut before its commit, nothing stored. The model turn such a
  `close` runs in is cut all the same: the tool's call awaiting the close
  runs on, its end row stored before the close returns, and what the turn
  does after the close is refused quietly, its executor watching its owner
  no more; a tool awaiting a close it started on a task of its own is cut
  with its turn.
- A turn's work, its model's rounds and its tools' calls, is cut at once
  as the kit's close starts, with no grace, as Python's (RFC §10.6.8,
  §12.10.4), while the other running work has its grace: what its reader
  read is stored, the text marked `cancelled`, an open tool call given its
  end row, `failed`, error `kit_closed`, all delivered to no one, the call
  reading `kit_closed`, no error. A turn's streaming target is cut with
  it, as Python cancels its readers, and the text it received is stored,
  marked `cancelled`: a stalled target held the close 15 s or more,
  storing nothing. A target waiting on its next item is dropped where it
  waits, never handed the stream's end, as Python's reader is cancelled
  inside its read. An agent writing then running a tool of 30 s, closed
  from outside, closes at 0.1 s, where it closed at 5.1 s.
- A direct call, a lifecycle change, a direct change of a stored event or
  `commit_event`, runs on a task of its own within its caller's execution
  context, as an inbound call does, and Python's task is driven: a caller
  that keeps the call's future but stops polling it holds no work of the
  close, which waited 8 s and named the call in its warning, and takes
  1 s when the call's write takes 1 s. Dropping the future still cancels
  the call. The close's bound for work that ignores its cut is its grace
  plus two unwinds, as Python's: the turn backend's unwind waits for its
  turns alone, where it waited for every tracked task (8 s became 7 s).
  A run the close aborts before it read its commit back warns as a
  read-back that failed, where a message's or a `send_event`'s abort
  logged nothing.
- The ASYNC hooks of `BEFORE_BROADCAST` observe an event once its pass is
  over, its delivery set placed in the room's lane, and a set placed
  first, the lane idle, runs from its placement, which the close does not
  drop, the rule the Python implementation aligns on (RMK-1039): an
  observer that closes the kit finds its set delivering, given the
  close's grace, its call reading `kit_closed` with the target `sent`,
  where the close dropped the set, or, placed behind another, queued,
  dropped, on a runtime of one thread or of several, whatever the store.
  A queued set's observers run while the set ahead delivers, as Python's.
  Outside a close the order is the same.
- A turn a tool closes the kit from fails a sibling tool's verb waiting
  for its owner, `owner_lost`, as Python's `TurnLinkError` reads, where
  the verb waited for an answer nobody read, the executor outliving the
  close.
- A turn cut with no streaming target records `loop_end_reason`
  `cancelled` on its text, beside `cancelled`, as Python's does.
- A delivery that fails with `RoomKitError::KitClosed`, a send whose call
  into the closing kit was refused, reads code `kit_closed`, retryable,
  as Python's delivery reads `KitClosedError.code`, and one that fails
  with any other `RoomKitError` reads its Python class's name
  (`RoomNotFoundError`, `ShutdownCancelledError`, `AssertionError` for a
  defect), as Python's reads the error's type, where both read
  `ChannelError`; a store's failure keeps `ChannelError`.
- A task the kit's work spawns, that calls the kit and waits on its own
  room, is spawned with `keep_context`, the Rust form of Python's
  `create_task` copying its context; spawned plainly, it blocks.
- A channel's output and a stream target's `metadata_updates` refuse the
  keys of the room's private state too (`author_register`,
  `_speak_queue_results`), as Python's writers do: a channel could forge
  the register of authors.
- A call from a room's own work naming another organization reads the room
  first and draws nothing on the work's reentry budget; a call back from
  another room's delivery waits for its delivery once the first delivery
  is over.
- `idle` leaves the public API for the hidden test seam
  `roomkit::testing::idle(&kit)`. `run_timers`, `TimerSweeper`,
  `KitError::NoRuntime` and `KitError::ZeroPeriod` go: the host calls
  `check_all_timers` on the period it chooses, as with Python.
- An AI agent's history replays its earlier rounds of tool calls (RFC
  §6.4 "The tools of earlier turns", RMK-1023), as Python's does since
  roomkit 09e9a4ec: each round at its place, after the input of the turn
  that made it and before its answer, as the assistant's calls then a tool
  message per result, the way the turn sent them. The arguments are the
  model's, the result what it read (an `ON_TOOL_CALL` rewrite included);
  only the calls a tool answered (served, failed, blocked), never a
  refusal, a call nothing served or one cancelled, nor a call to the
  agent's skills' tools; the last 8 of them, the 3 most recent with their
  result whole up to 6,000 characters, the older ones a 120-character
  preview. A round older than the history's first message is left out,
  and an instruction that stands alone replays none. A later turn used to
  read each round's text alone, so a follow-up about a result the answer
  left out found nothing. `transcript::conversation` takes the predicate
  saying which tools' calls are replayed (`replayed`).
- An intelligence channel's turn reads its history from the store as it
  starts: the room's latest 50 messages up to its event and the channel's
  latest 60 tool-call rows, as Python's AI channel reads them (RMK-1023).
  It read the pipeline's context, the latest 50 events of every kind, so
  that after a turn of 30 calls the next one lost its question and every
  round. Hooks and transports keep that context. The earlier edit and
  delete records are no longer in a turn's history; a deleted message
  still is, its content withheld, and a turn on an edit or a delete reads
  that event as its history's newest, as stored, as Python's does.
- Every row of a streamed response carries the turn's own
  `correlation_id`, a fresh one per response whatever the trigger
  carries, as Python stamps it, and the replay pairs a call's start and
  end by it. Two overlapping turns answering the same trigger and reusing
  an id were paired by that trigger, one call with the other's result.
  A deleted start row ends its round in a turn run through the kit too,
  whose history hands it with its content withheld.

- **One hook registration, Python's `RoomKit.hook`**: `kit.hook(trigger,
  handler, HookOptions { execution, priority, name, timeout,
  channel_types, channel_ids, directions, event_types, fail_closed,
  needs_lock })`, and `kit.hook_handler` for a handler type of its own.
  The handler takes the payload its trigger hands, written on its
  argument (`|event: RoomEvent, context|`): a `RoomEvent`, a
  `ToolCallEvent` for `BEFORE_TOOL_USE` and `ON_TOOL_CALL`, a
  `SessionStartedEvent` for `ON_SESSION_STARTED`; another is refused at
  registration. It returns a `HookResult`, an `Option` of one or `()`:
  `None` and `()` are Python's `None`, an unusable result, which lets the
  payload through unless the hook or its trigger fails closed.
  `before_broadcast`, `observe`, `before_tool_use`, `on_tool_call`,
  `observe_tool_calls`, `HookSpec`, `HookFilter`, `HookId`,
  `ToolHookSpec`, `ToolUse` and `ToolVerdict` go. A hook given no name is
  named `hook`, then `hook#2`, as Python names a `<lambda>`; `event_types`
  filters an event's type. A refused registration reads as Python's
  `ValueError`, word for word.
- **The tool hooks run as Python's.** `ToolCallEvent` is Python's: `name`,
  `tool_call_id`, `arguments`, `result` (`None` before the call ran and
  for a call nothing served), `is_error`, `cancelled`, `refused`,
  `structured_content`, `error_detail`, `intent`. In `BEFORE_TOOL_USE`, a
  `modify` is read by the next hook and runs the call; a rewrite under
  `metadata["arguments"]` is read by no later hook and the last one wins;
  arguments that are not an object refuse the call. In `ON_TOOL_CALL`,
  `metadata["result"]` and `metadata["structured_content"]` replace the
  result and its structured copy, a served call emptied reads `null`, a
  SYNC hook may serve a call nothing served, a block withholds the result
  with `{"error": "<reason>"}`, and a hook that fails closed withholds it
  as `hook_error`, `hook_timeout` or `hook_invalid_result:<name>`. A blank
  reason is kept as written, as Python keeps it. Each call emits the
  `before_tool_use` and `tool_call` framework events.
- **Framework events are Python's.** `kit.on(event_type, handler)` for
  `subscribe`; a `FrameworkEvent` is flat, Python's `type`, `room_id`,
  `channel_id`, `event_id`, `timestamp` and `data`, and its handlers are
  awaited before the call that emitted it goes on, as Python awaits them.
  An inbound event's `event_processed` follows its `AFTER_BROADCAST`
  hooks, once they are over, where it came before they started
  (RMK-1020).
- **A lifecycle hook's payload is Python's**: an event of the change's
  type from the framework's `system` source, its system content Python's
  code, message and data (`room_paused`, `room_closed_by_timer` with its
  threshold and the instant it fell due, `channel_attached` with the
  access and the visibility). `LifecyclePayload` and `LifecycleChange`
  go. A session's start reaches its hooks whatever their filters, as
  Python fires it.
- **The turn's host is Python's `TurnHost`**: `current_turn_host()`,
  `send_event(room_id, channel_id, content, SendEventOptions)`, `open()`
  and `reachable()`; `room_id`, `channel_id` and `context` go. A failure
  across the turn backend is `RoomKitError::RemoteTurn { message, kind,
  bases }` or `TurnLink { reason, message }`, Python's `RemoteTurnError`
  and `TurnLinkError`, for `TurnError`. `TurnTimeouts` keeps its fields,
  `silent_intervals` public, and its `with_*` builders go.
- `RecordingTransport::new(channel_id, channel_type,
  RecordingTransportOptions)` and `EchoAgent::new(channel_id, reply,
  EchoAgentOptions)` take Python's keywords; `id()` is `channel_id()`.
- Out of the public API, as Python keeps them private: `text_of`,
  `is_deleted`, `is_edited`, `turn_context`, `transcript`, `Transition`,
  `TransitionReason`, `RefusedOperation`, `ChainDepthData`,
  `CHANNEL_NOT_REGISTERED`, `DirectEvent` and `HookTrigger::execution`,
  `fails_closed` and `is_tool_trigger`. `fence` is
  `roomkit::tools::fence`, Python's `roomkit.tools.fence`.
- **A channel is Python's `Channel`**: what a Python channel tells by its
  constructor, class attributes, properties and overridable methods, a
  Rust channel tells by trait methods of the same names with Python's
  defaults (`channel_id`, `channel_type`, `category`, `direction`,
  `capabilities`, `provider_name`, `info`, `normalize_address`,
  `recipient_key`, `recipient_address`, `recipient_metadata`,
  `conversation_address`, `reply_metadata`, `recent_events_window`,
  `supports_streaming_delivery`, `declaration`, `extract_text`).
  `handle_inbound(message, context)` and `deliver` are required, as
  Python's abstract methods are; `EventDraft` goes, and
  `default_inbound_event(message, context)` builds the event a channel
  keeping no data parses. `deliver_stream(text_stream, event, binding,
  context)` takes the empty event the response renders under, and its
  default is Python's: one `deliver` of the joined text, where each of the
  response's rows was delivered. `ChannelDeclaration` is Python's model;
  the core's own declaration is `ChannelConfig`, no longer re-exported.
  Every transport is handed an agent's tool-call rows, as Python hands
  them: `receives_tool_calls` goes. `HostChannel::new(HostChannelOptions {
  channel_id })`, `"host"` by default, names no provider.
- **The store is Python's `ConversationStore`**, its methods' keyword
  arguments the options structs of `roomkit::store`, Python's
  `roomkit.store`: `create_room -> Room`, `update_room(room)` written
  whole but for the counters the store manages, `patch_room_metadata`,
  `delete_room`, `list_rooms` and `find_rooms` paged,
  `find_room_ids_by_channel`, `find_room_id_by_binding` (a binding naming
  its recipient under the channel's recipient key) and
  `find_room_id_by_participant`, `add_binding`, `update_binding`,
  `get_binding`, `remove_binding -> bool`, `load_room_context`,
  `get_conversation`, `add_participant`, `check_idempotency`,
  `commit_event(room_id, event, CommitEventOptions { target, activity })
  -> RoomEvent`, `update_event(event)`, `add_task`, `add_observation`,
  `list_tasks(status)`. A store implements what Python's ABC makes
  abstract; the rest is provided as Python provides it, so a store of the
  abstract methods alone serves the kit. `RoomFilter` is Python's
  (`statuses`, `organization_id`, `admits`); `EventQuery` is
  `ListEventsOptions`; `RoomCounters`, `TargetUpdate`, `RoomUpdates` and
  `EventUpdates` go.
- **The store gives an event its index**, the room's count of the indices
  it gave out, as Python's `commit_event` does: a stale index lands at
  the next one, and the kit goes on with the event the store returns.
  `StoreError::IndexTaken` and `IdentityExists` go. A commit that is the
  room's activity moves its `last_activity_at` and resumes a paused room,
  at the store's clock, the kit's while the kit writes.
- **The store answers as Python's**: a participant added under an id the
  room holds is refused `ParticipantExists`, and one updated that the
  room lacks `ParticipantNotFound`, with Python's messages; an identity
  created under a taken id replaces the stored one; a read of a room the
  store does not hold is empty (so `kit.get_binding` there fails
  `ChannelNotFound`, as Python's); `remove_binding` reports `false`;
  `set_private_state` on an unknown room writes nothing. A room's context
  holds its latest messages (`get_conversation`), as Python's kit reads
  it. A key the store holds but whose event it cannot read back makes a
  redelivery a blocked duplicate (`duplicate`), never processed again,
  and a direct injection returns the event it injected.
- **The default router reads the store as Python's does**: step 1 through
  `find_room_id_by_binding` then `find_room_id_by_participant`, step 3
  through `find_room_ids_by_channel`, never every room of the
  organization.
- **A channel's history reads deletions from the marks only**
  (`metadata.deleted`), as Python's: a DELETE row, which a host's
  `commit_event` stores unchecked, no longer withholds an edit of an event
  before the window.
- **A streamed response ends on Python's `LoopEndMarker`**: `ResponseItem`
  is `Text` and `LoopEnd(LoopEndMarker)`, the tool markers hidden;
  `End(Metadata)` goes. The marker is Python's (`reason`, `rounds`,
  `usage`, `max_rounds`, `timeout`, `budget_tokens`, `budget_usd`), with
  RMK-1029's `last_usage`, `usage_complete`, `model` and `provider`; the
  kit writes the turn's record from it.
- **The skills take Python's names**: `Skill` is Python's `Skill(metadata,
  instructions, path)`, its fields public, `name()` and `description()` its
  metadata's; `license()`, `compatibility()`, `instructions()` and `path()`
  go. `SkillRegistry` reads `get_skill`, `get_metadata`, `all_metadata`,
  `skill_names`, `skill_count` and `has_entries` for `get`, `skills`, `len`
  and `is_empty`; `register(skill_dir)` returns the skill's metadata;
  `discover(directories, DiscoverOptions)` takes Python's `strict`. A
  directory `register` cannot resolve fails `SkillError::Parse`, as
  Python's, where it failed `Discovery`.
- **`ToolPolicy` is Python's `ToolPolicy(allow=[], deny=[])`**: its
  `allow` and `deny` fields are public, built by struct update, and
  `is_allowed(tool_name)` reads it; `new()`, `allow(pattern)`,
  `deny(pattern)` and `permits(name)` go.
- **`AiProvider` is Python's provider contract** (`roomkit.providers.ai`):
  `model_name()` and `generate(&AiContext) -> AiResponse` are required;
  `name()`, `supports_streaming()`, `supports_structured_streaming()`,
  `generate_stream`, `generate_structured_stream` and `close` are provided,
  as Python provides them, and the tool loop reads the structured stream,
  which wraps `generate` for a provider that does not stream. `AiRequest`,
  `ProviderEvent`, `ProviderToolCall`, `Done` and `ProviderResponse` give
  way to Python's `AiContext`, `AiMessage` and its parts, `AiResponse`,
  `AiToolCall`, `StreamEvent` (`StreamTextDelta`, `StreamToolCall`,
  `StreamDone`) and `tool_arguments`; a provider reads a call's arguments
  and marks it `partial` or `garbled`, as Python's do. `ProviderError` is
  Python's class (`message`, `retryable`, `provider`, `status_code`,
  `context_overflow`), its cause its source. `ToolDefinition` is `AiTool`.
  The OpenAI provider names itself `OpenAIAIProvider`, Python's class
  name, in a turn's `ai_provider` and its channel's provider name, where it
  said `openai`.
- **`SqliteStore` is Python's `SQLiteStore(path='roomkit.db')`**:
  `SqliteStore::new(SqliteStoreOptions { path })` does no I/O, the file
  opening at the store's first operation, which fails
  `StoreError::Unavailable` when it cannot be opened (the next one tries
  again); a leading `~` reads as the home directory. `SqliteStore::open`
  and `OpenError` go: a file of another schema version is refused with a
  `SqliteSchemaError` as the failure's cause, Python's
  `SQLiteSchemaError`, in its words for a newer version.
- **A room's context holds the window its channels declare**, as
  Python's kit loads it (RFC §6.1 `recent_events_window`, §10.1 step 3):
  the largest window a channel bound to the room declares (an
  `AiChannel`'s `max_context_events`, a transport's none), at least 50
  while a hook is registered, at most 2,000, where every room held its
  latest 50 events. A room whose channels read no history and with no hook
  reads none, and an intelligence turn's history reads that many messages
  before its event, where it read 50: an `AiChannel` with
  `max_context_events: 80` now reads 80. A channel of your own that reads
  the room declares it with `Channel::recent_events_window`, none by
  default, as Python's.
- **`AiChannel` is Python's `AIChannel`** (`roomkit_ai`):
  `AiChannel::new(channel_id, provider, AiChannelOptions { .. })` takes
  Python's keywords and defaults (`system_prompt`, `temperature` 0.7,
  `max_tokens`, `max_context_events` 50, `tool_handler`, `tools`,
  `max_tool_rounds` 50, `tool_timeout` 30 s, `skills`, `skills_in_prompt`,
  `tool_policy`, `server_instructions`, `tool_call_intents`), and refuses a
  tool name no vendor accepts, a name two tools take and a policy pattern
  holding a bracket. The tools are Python's: `AiTool { name, description,
  parameters }`, whose `AiTool::new(name, description)` refuses a name no
  vendor accepts; a `ToolHandler` serving calls by name
  (`ToolHandler::new(|name, arguments| ..)`, called with `call`); `Tool`, a
  tool serving its own calls; `ToolEntry`, either one in `tools`;
  `ToolResult`, a text or a JSON value; `compose_tool_handlers` and
  `current_tool_call()`, the `ToolCallContext` a handler runs within. A
  handler declines a call that is not its own with
  `ToolError::UnservedToolCall`, where it was `ToolError::Unserved`. The
  channel's close closes its provider. `AiAgent`, `Tools`, `Toolset`,
  `ToolContext`, `ToolOutput` and `arguments` leave the public API.
- **`OpenAiAiProvider` is Python's `OpenAIAIProvider`**
  (`roomkit_openai`): `OpenAiAiProvider::new(OpenAiConfig::new(api_key,
  model))`, the configuration Python's `OpenAIConfig` in public fields
  (`base_url`, `max_tokens` 1024, `timeout` 30 s, `connect_timeout` 5 s,
  `include_stream_usage`, `use_max_completion_tokens`,
  `supports_custom_temperature`, `reasoning_effort`, `default_headers`,
  `extra_body`, and roomkit-rs's `api`, `vision`, `token_source`,
  `tool_namespace`). A request carries the channel's temperature, unless
  the model takes none, and its `max_tokens` or the configuration's, as
  `max_tokens` or `max_completion_tokens`. Its errors are `ProviderError`s
  read as Python's: retryable for 408, 409, 429, a 5xx, a lost connection
  and a transient message, a context overflow for
  `context_length_exceeded`. A tool name OpenAI refuses fails the request
  before it is sent. `OpenAiAgent`, `OpenAiError` and the configuration's
  builders (`with_system_prompt`, `with_history`, `with_timeout`,
  `with_reasoning_effort`, `with_api`, `with_vision`, `with_token_source`,
  `with_tool_namespace`, `shown_base_url`) go: the channel's options carry
  the prompt and the history.
- **`McpToolProvider` is Python's `MCPToolProvider`** (`roomkit_mcp`):
  `McpToolProvider::from_command(command, FromCommandOptions { args, env,
  cwd, tool_filter, label, connect_timeout })` and `from_url(url,
  FromUrlOptions { tool_filter, headers, label })` refuse, when the
  provider is built, a label that cannot name the server in an alias and a
  command, URL or header that cannot work. The host `connect`s the
  provider (Python's `async with`) and `close`s it; neither the channel's
  close nor the kit's closes it. `get_tools()`, `tool_names()`,
  `instructions()` and `as_tool_handler(AsToolHandlerOptions {
  gate_discovery })` read a connected provider, `McpError::NotConnected`
  otherwise, and its tools keep the server's names, where a label named
  them `mcp__<label>__<tool>`. Its handler runs an alias
  `mcp__<server>__<tool>` as `<tool>`, declines one naming another server
  than its label and, with `gate_discovery`, a tool the connection did not
  list; `call_tool(name, arguments, CallToolOptions { timeout })` calls any
  tool of the server, the filter left out, and answers its text, a failed
  call as `{"error": ...}`. A tool policy judges a call under its alias
  and under the tool it runs. `McpCommand`, `McpServer` and `McpHttp`
  leave the public API, with `with_label`, `with_tool_names` and
  `close_grace`.

### Added

- **`MockAiProvider`, Python's `MockAIProvider`** (`roomkit_ai`):
  `MockAiProvider::new(MockAiProviderOptions { responses, vision,
  ai_responses, streaming })` answers its texts round-robin (`Hello from
  AI` without any), or its whole responses in their place, keeps each
  context it is asked (`calls()`), and names itself `MockAIProvider`, its
  model `mock`. The scripted provider (`roomkit_ai::testing`) is hidden, a
  seam of roomkit-rs's own tests.
- **The tool loop's bounds of Appendix A.9**, as Python's `AIChannel`:
  `AiChannelOptions::tool_loop_timeout` (300 s by default; `None` or zero
  sets none) is read between rounds, the loop ending `timeout` before the
  round's calls run; `tool_loop_warn_after` (25) logs a loop still running;
  `max_empty_retries` (1) asks the model again, in a round's place, after
  an empty answer that follows a round of calls (the results are above:
  write the answer) or a call its provider could not hand over
  (`malformed_function_call`, `unexpected_tool_call`), what it wrote kept
  before the word to it. A response that ran out of room is not asked
  again. The loop's end states its deadline (`timeout_seconds`) and counts
  the rounds of calls that ran, a try not among them.
- `TransportChannel::new(channel_id, channel_type, TransportChannelOptions {
  capabilities, addressing, country_code, defaults, requires_recipient })`,
  Python's `TransportChannel`: its addresses by its addressing declaration,
  the message's provider data, metadata and idempotency key on its event, an
  SMS with media sourced MMS. With no provider in 0.x it delivers nothing.
- `ChannelAddressing`, `ChannelAddressingOptions`, `NormalizeOptions` and
  `addressing_of(channel_type)`, Python's addressing declarations and their
  table by channel type.
- `model::LoopEndMarker` and `model::LoopEndReason`, Python's
  `roomkit.models.LoopEndMarker` and its reasons.
- `SkillMetadata { name, description, license, compatibility,
  extra_metadata }`, Python's `SkillMetadata`, what a skill's frontmatter
  declares: its other keys, each read as the fields are, as texts; one the
  line reading cannot read whole is left out, the skill read all the same.
- `ConversationStore::close()`, Python's `close()`, provided with nothing
  to release: the kit's `close` calls it last, once no work can reach the
  store, as Python's releases its store (decided 2026-10-31), and a kit
  read the store then fails (`get_room`, `get_timeline`, `get_binding`,
  `list_bindings`, `list_members`) answers `RoomKitError::KitClosed`.
  `SqliteStore`'s closes its file: every later operation, of the store or
  its clones, fails (`SQLiteStore is closed`).
- `SqliteStore::search_events(query, SearchEventsOptions { room_id, limit
  })`, Python's full-text `search_events`: the events whose text holds
  every word of the query, the most relevant first, never a refused row.
- `DiscoverOptions { strict }`, Python's `discover(strict=False)`: discovery
  logs and skips each directory it cannot scan and each skill it cannot
  load, and registers the rest; the manifest's bound still fails it.
- `ToolError::Unserved`, Python's `UnservedToolCallError`: a tool declines a
  call that is not its to serve, and a SYNC `ON_TOOL_CALL` hook may serve
  it; the model reads `{"error": "No handler for tool x"}` when none does.
- `FrameworkEvent::new(type)` and `SessionStartedEvent::new(room_id,
  channel_id, channel_type, participant_id)`, Python's constructors, the
  timestamp from the system clock.

- `RoomKitError::code()` and `RoomKitError::retryable()`, Python's
  `KitClosedError.code` and `.retryable` (`kit_closed`, retryable) and
  `ProviderDeliveryError.code` and `.retryable`; `None` for an error whose
  Python class carries neither (RMK-1030).

- `roomkit-openai`: opt into stateless Responses with `OpenAiApi::Responses`,
  including streamed text, function calls, ordered encrypted reasoning replay
  within a tool loop, usage counters and terminal failure reporting. Chat
  Completions remains the default. `with_reasoning_effort` configures either API.
- `roomkit-ai` and `roomkit::transcript`: provider metadata on `Done` and `Message`
  carries a tool round's replay state without adding it to room history.

## [0.1.0] — 2026-10-27

The first release: Level 0 of the RoomKit RFC with nothing left out, on a
tokio runtime; an AI agent over any model provider, with tools, streamed
answers, skills and MCP servers over stdio; an agent on an
OpenAI-compatible endpoint; a SQLite store on the Python implementation's
schema. Nothing is published to crates.io: a service takes the crates from
the repository at the tag `v0.1.0`, as git dependencies, with Rust 1.92 or
later (README, "In a service"). The API will change before 1.0.

A service depends on `roomkit` and on the crates it needs beside it,
`roomkit-ai`, `roomkit-openai`, `roomkit-sqlite` and `roomkit-mcp`.
`roomkit-core` is the sans-IO core they run on, and no service's API: what
`roomkit` does not re-export of it may change in any 0.x release.

### Added

#### `roomkit`, the runtime

- **The kit** (`RoomKit`, on tokio). It registers and unregisters
  channels, registers hooks (`hook`, `hook_handler` for a `HookHandler`,
  whose `call` returns a `HookFuture`, `before_broadcast`, `observe`),
  sets the router, the environment, the chain-depth limit, the
  `process_timeout` and the turn backend's bounds, creates rooms
  (`create_room`, `create_room_with` `RoomOptions`), attaches channels
  (`attach`, `attach_with` `BindingOptions`), detaches, updates, mutes and
  unmutes them, moves rooms through their lifecycle, routes a message
  (`process_inbound`) or runs one into a named room (`process_inbound_in`),
  injects, commits records, updates and deletes stored events, applies room
  timers on demand (`check_timers`) or in the background (`run_timers`, a
  `TimerSweeper` whose stop ends its sweep under way), publishes framework
  events (`subscribe`), and tracks the work it runs in the background:
  `idle()` waits until none runs, `shutdown()` ends it, closes each channel
  once (`Channel::close`, RFC §6.1, whose default releases nothing) and
  refuses every later call (`KitError::ShutDown`). Its driver executes a
  run's effects concurrently, completing each as it finishes; a call keeps
  the environment it started with. `RoomKit::downgrade()` gives a
  `WeakRoomKit`, whose `upgrade()` gives the kit back while a clone of it
  lives: a hook or a channel the kit holds holds its kit so, and the kit
  and its store are freed once the host drops the last clone.
- **Its traits and errors.** `Channel`, `InboundRoomRouter` and
  `ConversationStore`; `DefaultRouter`, the RFC's routing strategy, whose
  `route` a strategy wrapping it calls; `KitError`; `StoreError`, an
  unavailable store's cause kept as its source, a `StoreFailure`, a type
  that equals only itself and reads as the error it holds, through `Deref`,
  `AsRef`, `Display` and `into_inner`, built with `new` or `From` whatever a
  boxed error is made from. The crate re-exports the core's model whole
  (`roomkit::model`) and, of its operations and rules, what a caller names:
  the results and errors of the kit's calls, the framework events, what a
  channel declares and how it writes its addresses, the routing query a
  router answers (`RouteQuery`, its sender a `RouteSender`), what a store's
  commit writes (`RoomCounters`, `TargetUpdate`), the environment, and the
  rules that read an event (`text_of`, `is_deleted`, `is_edited`,
  `turn_context`, `transcript`).
- **The store** (RFC §14.1), `InMemoryStore` by default: rooms with partial
  updates and filters, bindings, participants by room, identities linked to
  and unlinked from addresses per organization, the routing lookups by
  binding and by participant, each finding the sender by address or by
  identity, and events, a commit writing the event, the room's counters and
  an edit's or a delete's target in one transaction, an index below
  `event_count` refused, a key its source channel holds in the room refused
  (`StoreError::IdempotencyKeyTaken`, nothing written, a blank key being no
  key), the stored `delivery_results` kept by every write but the commit,
  the idempotency lookup from a channel or within the room
  (`get_event_by_idempotency_key`), hard deletes and event counts (§13.4,
  §14.3, gaps G-079, G-080); a delete's commit marks its target's edits
  deleted, and a binding stored again keeps its place, as the Python
  implementation's stores do. A history hands an edit marked deleted as an
  edit, its new content withheld (§10.3). A room's private state
  (`get_private_state`, `set_private_state`, whose defaults keep it in the
  room's metadata) and its delivery cursor (`get_delivered_index`,
  `advance_delivered_index`, a compare-and-set from the index before, or
  from any lower value when forced) are answered as Python's stores answer
  them. The store's inputs (`RoomUpdates`, `EventUpdates`, `BindingUpdate`,
  `RoomFilter`, `EventFilter`) are exhaustive on purpose: a store reads
  every field, and one that destructures them fails to compile rather than
  reading too wide when a field is added.
- **The inbound pipeline** (RFC §10.1, §10.2), for a message into a named
  room or one routed (§10.4): the sender in the channel's form; a missing
  room created with its id, its `ON_ROOM_CREATED` hooks awaited before
  `room_created` and the channel's attach, a session started on any channel
  but voice (gap G-050); an unbound channel attached unless it was detached
  (gap G-053); a routed sender's identity resolved once, the kit's router
  asked, the sender recorded on the room's binding under its lock, and the
  message routed once more when the room no longer admits them (gaps G-055,
  G-061). Then the channel's parse, the off-lock checks ordered by the
  room's admission ticket (§9.5.1, gap G-067), and the pass under the room
  lock: the due timers applied, the status gate, the idempotency key, a
  blank one, empty or whitespace only, being no key for a message, an
  injected event and a record alike (§13.4, gap G-079), the event's thread
  parent
  rooted to its thread's root, whatever the pass, the root it lands on read
  and a parent naming no event of the room dropped (§10.1 step 8, gap
  G-083), an edit's or a delete's target, the hooks, a thread parent a hook
  set rooted alike, the room read again after a hook that ran under the
  lock, the source's permissions, the commit with the room's counters and
  the event's place in the room's delivery lane. An accepted edit or delete
  fires `ON_EVENT_UPDATED` or `ON_EVENT_DELETED` once the lock is released,
  its payload the target, which the observers' filters read, and its context
  the room as the commit left it, the change the newest event of its
  history (§10.3, gap G-062). Delivery sets run in index order, one at a
  time per room; a transport delivery with no recipient is refused
  `no_recipient`, a target whose channel is no longer registered skipped or
  reported (gap G-048); responses re-enter with their own source, their
  verification cleared (gap G-035), and within their message's answers
  (gap G-073), injected events and records past the reentry budget are
  stored, a refused record emits `room_refused_event` (gap G-043), and step
  15 stores the cascade's side effects and applies the room metadata
  updates channels return (gap G-049). An `INSTRUCTION` is decided without
  being stored, delivered to the agents it names, each handed the room's
  context, which a standalone instruction's turn reads none of
  (`turn_context`), and fingerprinted on every response (gaps G-042, G-065,
  G-066). Only the framework writes the visibility `internal`: a message
  carrying it, for itself or for its answers, is refused (gaps G-041,
  G-073), a response carrying it is committed with `none`, and a hook's
  modify of an event marked `internal` keeps it (gap G-041). The hooks
  registered ASYNC on `BEFORE_BROADCAST` run as observers once the chain
  allowed the event, on the payload its SYNC hooks left (§9.2). Each ASYNC
  body is the cascade's work while it runs: a call it makes into the room
  draws one of the cascade's hook calls and is refused past the cap, so an
  observer that posts on every event stops (§9.5.1, gap G-087). A tool-call
  row reaches only the transports whose declaration takes one
  (`ChannelDeclaration::receives_tool_calls`, true by default for
  `websocket` and `webhook`): an SMS or a chat transport is never handed
  one.
- **Deadlines and callers that leave.** The kit's `process_timeout` bounds a
  call from its start to its decision, routing, the parse, the off-lock
  hooks, the ticket, the lock and the locked section, never the commit and
  what follows it (§13.6, gap G-068). A caller that stops waiting, its
  future dropped, never drops the call once anything it ran is committed:
  before the decision, it expires as one past its `process_timeout`, writes
  nothing more of its event and delivers every event it committed (gap
  G-076). Once the call is decided, its turns are cut (§10.1 "Explicit
  delivery cancellation"), as the Python implementation cuts them: a turn of
  its cascade not started never starts, one running is aborted and joined
  before the run ends, and no answer re-enters; its events are delivered to
  every other target, and the entries it adopted keep their turns. A cascade
  that fails still delivers what it committed.
- **Direct calls.** `send_event` injects an event through the pipeline from
  its lock on, without a parse or step 2: a refusal before its commit is
  raised with nothing written (§10.5, gap G-069); its `DirectEvent` may say
  where its answers go (`response_visibility`), as a message does (gap
  G-073). `commit_event` commits a record under the room lock with the next
  index and nothing else: no hook, no delivery, no activity (gap G-071), its
  thread parent rooted as every committed event's, a record stored `BLOCKED`
  included (gap G-083); it is never a replay, and a key its channel holds
  fails it as the store refuses it (gap G-079). `update_event` and
  `delete_event`, the host's changes of stored events, run under the lock,
  fire their trigger after it with the room read then, and are a no-op on a
  missing target or room; a hard delete takes its replies one level deep
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
  (`InboundError::ParticipantNotFound` otherwise), attaches the host channel
  on a room's first post in one section under the room lock, a binding made
  meanwhile kept, and is refused with nothing written in a closed room or
  one the channel was detached from, from a channel that is not a host
  channel (`InboundError::NotAHostChannel`), and past its one
  `process_timeout`; its key makes a retry a replay. A message on a host
  channel is refused before anything is read
  (`InboundError::InboundOnHostChannel`), and so is an attach of one as an
  intelligence channel (`LifecycleError::HostAsIntelligence`), a post's
  first attach of one declared so included
  (`InboundError::HostAsIntelligence`).
- **Reentrancy** (§13.5, §9.5.1 Reentrance, §10.1 step 18, gap G-070): code
  that runs inside a room's own work calls into the room without waiting
  for itself. A lifecycle change, a direct change or a record made under
  the lock takes the lock again through the caller's lease; `send_event` or
  `process_inbound_in` made under the lock or in the admission order is
  adopted by the call whose work made it and returns at its commit; a
  turn's or a delivery's call is a new arrival with its own ticket and
  `process_timeout`, drawing on the delivering cascade's budget when it
  comes into the room delivered in, refused past it first thing when it
  names that room, a routed message once step 2 kept its final room for it,
  after any re-route, and a call that creates a room, routed or named,
  charged before the room is created (a call into another existing room,
  and the cascade of a message whose room another call created first, have
  a budget of its own; no message is charged twice), and answered at its
  decision when it comes into its caller's room; a call made while a turn
  runs, into whatever room, is at least as deep as the turn's response
  (§10.6), and so are the observers of a lifecycle change a turn's tool
  makes (`ON_CHANNEL_ATTACHED`, `ON_CHANNEL_DETACHED`, `ON_CHANNEL_MUTED`,
  `ON_CHANNEL_UNMUTED`, `ON_ROOM_PAUSED`, `ON_ROOM_CLOSED`), run in the
  room's work on its budget, so an agent attaching a channel whose greeting
  it answers stops at the depth limit (RMK-1004, §8.3); work a turn or a
  delivery began starts fresh once that work is over, at depth 0 and on a
  budget of its own (gap G-070); a direct injection from the room's work
  reads its room before it draws on the budget, so a call its room refuses
  draws nothing and a caller its pass would refuse never receives an
  original (§17.2), then its key: one its channel holds makes it a replay,
  which returns the event the key committed and draws nothing, past the
  budget too, its record looking the key up again under the lock (gap
  G-081); the body of an ASYNC hook a cascade fires once an event's
  delivery set has run (`AFTER_BROADCAST`, a change's mutation trigger) is
  that cascade's work while it runs: its calls into the room draw on the
  cascade's hook calls, at least one deeper than its event, and a direct
  update or delete it makes draws one too, `MutationError::ReentryLoopCap`
  past them, nothing written (gap G-087); a hook, a parse or a route under
  the room's lock or in its admission order carries its cascade's budget,
  so that a direct change it makes is that cascade's work, drawing nothing;
  a call that would wait for what its own context holds fails
  `KitError::Reentrant`. A call's ticket is released once no section of the
  call holds the room's lock, its decision and its delivery set not waiting
  for it. `roomkit::keep_context` keeps the context in a task a hook or a
  turn spawns: the task acts for it while it runs, whether it awaits the
  task or not, as a task spawned in a Python hook does, and once the hook
  is over the task's calls are new arrivals, waiting for the room's lock its
  pass still holds.
- **The room lifecycle** (§5.1, §5.7, §7.2, §7.5): creating a room,
  attaching, detaching and updating a channel's binding, pausing, resuming,
  reopening, closing and archiving a room, and checking its timers, each
  but the creation one locked section, with their hooks, their framework
  events and their payloads (`LifecyclePayload`, `LifecycleChange`). A
  change that changes nothing writes and fires nothing (gap G-047); a
  transition from a status it does not leave from is refused (gap G-074).
  Every locked section applies the timer transitions that fell due, each at
  its instant, its framework event emitted at once and its trigger fired
  once the room's lock is released; a delivered event resumes a paused room
  (gap G-045). A channel's attach, the integrator's, step 2's or a first
  post's, its detach, mute and unmute, a binding's access, visibility and
  metadata change, and a timer's pause or close each write a record in the
  timeline, as the Python implementation does (§5.1 Lifecycle records, gap
  G-046): an event of the change's type (`CHANNEL_ATTACHED`,
  `CHANNEL_DETACHED`, `CHANNEL_MUTED`, `CHANNEL_UNMUTED`, `CHANNEL_UPDATED`,
  `SYSTEM` for a timer), the framework's source, a `SystemContent` in the
  Python implementation's form, `internal`, at the room's next index,
  committed under the room's lock with the counters, no activity, never
  delivered and running no hook of its own. A room whose status refuses
  events takes none but a timer's record of the transition itself; a room
  with three attaches stores its first message at index 3. The
  integrator's pause, resume, reopen, close and archive, and a room's
  creation, write none.
- **Authors** (§6.4, §10.1 step 12, §14.1, gap G-089): every event that
  names its sender is committed with the record of its author
  (`metadata.author`: name, rank, source), the room's register of authors
  ranking the sources whose names read alike, in case or in Unicode's
  confusables, digest for digest and rank for rank as the Python
  implementation does. The register is the room's private state, read with
  the room (`RoomContext::author_register`) and written in the event's
  commit (`RoomUpdates::private_state`), rebuilt from the timeline for a
  room that holds none, merged with one the room's metadata still holds. A
  record stored `BLOCKED` takes none and loses one it came with, and a
  direct update that writes an event's metadata keeps the record it was
  committed with. `InMemoryStore` keeps the private state apart from the
  metadata. What reads alike is read over Unicode tables generated from
  roomkit (`scripts/build-unicode-tables.py`).
- **The delivery cursor** (§5.1, §13.5, §14.1, gap G-091):
  `Room::delivered_index`, the highest index whose delivery set has run,
  `-1` before any, crosses as the Python implementation writes it; a room's
  update and an event's commit leave it as stored. The kit's delivery lane
  moves it at each index delivered, as Python's does: an event's index once
  its set has run, an index with no delivery set (a record stored
  `BLOCKED`, a lifecycle record) once committed, one index at a time.
- **A timeline read** (§14.1, "Reading a timeline"), with the Python
  implementation's names, arguments and defaults (docs/architecture.md,
  Conventions, lists how they map): `ConversationStore::list_events(room_id,
  &EventQuery)`, `offset` and `limit` (50 by default), or one of the
  cursors `after_index` and `before_index`, which exclude each other
  (`StoreError::InvalidArgument`), `newest_first` choosing the newest end
  in offset mode, the page always in ascending index order; the events an
  `EventFilter` admits, or those the room received with none, its criteria
  (types, types excluded, visibility, source channel and type, correlation
  group, participant, answered event, thread parent or top level only, a
  time range, `include_blocked`) applied before the page is cut, as is the
  exact `visibility_filter`; `get_event_count` with a filter counts the
  rows the page would serve. `RoomKit::get_timeline(room_id,
  &TimelineQuery)`, Python's `get_timeline`, reads it through the kit, and
  `OrganizationKit::get_timeline` for an organization.
  `roomkit::visible_events(context, channel_id)`, Python's, gives what a
  channel may know of a context's events (§7.5 rule 8): its own accepted
  ones and those their visibility admits, deleted content withheld, none
  for a channel the room does not bind; a page set as the context's
  `recent_events` is what a reconnecting client is sent; and
  `roomkit::visibility_allows(visibility, target_binding)`, Python's,
  whether an event of a visibility reaches a binding's channel. A cursor
  past every index bounds the page in both stores.
- **Organizations** (§17.2, gap G-077): every room operation acts for an
  organization or for none. `RoomKit::for_organization(organization_id)`
  returns an `OrganizationKit` whose calls are the kit's, made for the
  organization, and whose reads (`get_room`, `list_rooms`, `get_event`,
  `get_timeline`, `list_bindings`, `list_participants`, `get_participant`)
  see its rooms only. A room of another organization, or of none, is not
  found by its changes, its injections and its posts (`RoomNotFound`),
  before any hook runs or anything is drawn, nor by the reads of its
  records and its roster (the store's `RoomNotFound`), holds no event for
  its updates and deletes, and refuses its messages `OtherOrganization`,
  nothing written in it, its due timers included; a room it creates belongs
  to it, and its messages are routed among its rooms and identities only
  (gaps G-051, G-060). The kit itself acts for no organization.
- **The conversation a turn reads** (§6.4, §10.1.1 rules 6 and 7):
  `roomkit::transcript::conversation` gives an agent channel the room's
  latest messages before its turn's event, then the event, as `Message`s
  (built with `Message::new`) of the `User` or the `Assistant` role, a tool
  round's calls and results within the turn in the `Tool` role. Once the
  turns hold two speakers or more, or two people or more take part, each
  line of a participant's turn reads `Name: "line"`, its author's name
  ranked when it reads like another's (`Alice (2)`) or `@channel` for a
  sender with no name, a second such sender of the channel `@channel (2)`,
  speakers being counted by who they come from, and the turn's notes say how
  speakers are named. An instruction is the input, marked as the
  application's; one that stands alone reads no history and counts no one
  taking part. A copy of the runtime's marks in a participant's text is
  replaced, its letters read as the Python implementation's pattern reads
  them (any case, NFKC, confusables, a capital `I` for an `l`) and as a
  name's reading does, in any spacing, one that runs over consecutive
  messages included, and so is a mark's opening after any opening
  punctuation (`[`, `【`, `〔`, `(`…). `transcript::fence` frames text from
  outside in a block its forged closing tag cannot close (§6.4).
- **The tool hooks and the turn's host** (§9.3, §21.4). `before_tool_use` is
  SYNC and fails closed: a hook may let a call go on, rewrite its arguments,
  or block it (`ToolUse::block(reason)`), the model reading the reason as
  `{"error": "<reason>"}`, or `Tool 'x' denied by pre-execution hook.` with
  none, a hook that raises or runs past its timeout refusing it so too.
  `on_tool_call` is SYNC on a served call, each hook reading the result as
  the one before left it: it may keep it, replace it, or withhold it
  (`ToolVerdict::block(reason)`), which fails the call `blocked`, the model
  reading `{"error": "<reason>"}`, or `{"error": "blocked"}` with no reason,
  as the Python implementation writes it, or with a blank one, as a blank
  `ToolUse::Block` reason reads the fixed refusal (Python keeps a blank
  reason as written).
  `observe_tool_calls` is ASYNC and sees every call once, a refused one
  included, as tasks of the call's turn, what they send at least as deep as
  the turn was, once it is over too (§8.3). Each takes a `ToolHookSpec`
  (name, priority, timeout) and reads a `ToolCallEvent`. `RoomKit::hook()`
  refuses `BEFORE_TOOL_USE` and `ON_TOOL_CALL` (`HookTrigger::OnToolCall`),
  whose payload is a tool call (`RegistrationError::PayloadMismatch`).
  `roomkit::current_turn()` gives the `TurnHost` of the intelligence turn
  the current task runs within (§21.4 `current_turn_host()`): the turn's
  room and context, its `send_event` at the turn's depth, and the tool hooks
  the agent's loop runs.
- **Streamed responses** (§6.1, §12.2 steps 13s and 14s). An intelligence
  channel answers with a stream through `TurnHost::respond_with_stream`, a
  `ResponseStream` of `ResponseItem`s, which the kit reads once the event's
  delivery set is over: each row is stored as it ends, through
  `BEFORE_BROADCAST` once, and delivered to the other targets while the
  turn goes on; nothing of the stream is generated but as it is read. A
  chained stream, one a response's delivery asked for, draws one reentry
  pass as it is read, and past the cascade's budget is closed unread, its
  answer stored `BLOCKED` `reentry_loop_cap` (§8.3); the trigger's own
  draws none, a buffered channel one pass per event, and a tool-call row
  asks no agent. The turn's record, when no row carried it, is written on
  its last message once the turn is over, `ON_EVENT_UPDATED` firing. A
  transport that declares `ChannelDeclaration::supports_streaming_delivery`
  reads the answer as it is written, through `Channel::deliver_stream`: a
  `DeliveryStream` of `StreamItem`s, the text deltas, then each row once the
  store holds it, with its id and index (`DeliveryStream::next`); it is
  never delivered those rows again, and it is handed no row it may not
  read, nor anything more of an answer the room withdraws from it mid-turn
  (the agent muted, the channel detached, the agent's visibility narrowed
  away from it). The default reads the stream to its end, then delivers
  each row through `deliver()`. One such target per response, the first
  the rows reach; every other target receives the rows only. The reader
  sets the pace: one that stops, or a caller that leaves, leaves the text it
  was handed stored marked `cancelled`, the turn recording `cancelled`; a
  call running is let finish, one announced and not running closed
  `failed`. A stream that fails keeps the text it produced and fails the
  turn, its `ON_ERROR` category `streaming`.
- **The turn backend** (§10.6). Every intelligence turn runs on the kit's
  own turn executor, behind the in-memory backend: its request, its output,
  each item of its stream and each verb of its host cross as messages
  serialized and parsed anew, the request's events alone handed over as
  they are, written as the Python implementation's codec writes them (a
  cross-read fixture proves it both ways, `make turns-fixture`); a frame and
  a request are bounded at 16 MiB, and a reader that stops pulling holds the
  executor at a window of 64 items. A turn's `on_event()` runs in a copy of
  the execution context of the call that submitted it, its stream in that
  of the call that first read it, so reentrancy, depth and budgets hold
  across it, and `current_turn()` is the turn's in all the work it does.
  Each side beats every 2 seconds, the executor from a task of its own: a
  turn busy for as long as it likes, its thread blocked included, is never
  lost; an executor or an owner silent for 10 seconds is.
  `TurnHost::send_event` crosses as a `host_call`, and after its turn as a
  `room_call`, refused once a later turn of the agent was sent in the
  room; its errors come back as the `KitError` they were when it carries
  no data, as `KitError::Turn` (a `TurnError`) otherwise. A turn whose work
  panics fails `Panic` at once. The kit's shutdown cuts the turns first:
  each crosses as `cancel`, the tool calls it ran are reported `cancelled`
  to their observers and their ends stored `cancelled`, the text a reader
  was handed stored `cancelled`, within twice the cancel bound (10 seconds
  by default), and an inbound call running when it began answers
  `KitError::ShutDown`. `RoomKit::set_turn_timeouts` sets the bounds with
  `TurnTimeouts` (accept, heartbeat interval and timeout, cancel; the RFC's
  defaults), and `testing::TurnTap` records every message the kit's turns
  carry, for a test.
- **Test channels** (`roomkit::testing`): `RecordingTransport` and
  `EchoAgent`, for a service's tests and the examples, and `TurnTap`.

#### `roomkit-ai`, the AI channel

- **`AiAgent`** (§6.4, §6.7, §9.3, §21, Appendix A.9): the AI channel over
  any `AiProvider`, a model reached with one call, a request in
  (`AiRequest`) and the generation's events out, a `ProviderStream` of
  `ProviderEvent`s (text as it comes, the calls, the done event);
  `ProviderResponse::into_stream` serves a provider that does not stream.
  Its turn runs one tool loop, the same for every provider, and answers as
  a stream within a kit, nothing of the loop running but as it is read; a
  call starts only once the reader read past its start. The loop runs 50
  rounds and 30 seconds per call by default (`with_max_rounds`,
  `with_tool_timeout`); a round runs the first 32 calls its generation
  made, as the Python implementation does, the rest never run, are not
  stored, are left out of the assistant message the model reads next, and
  are logged at `warn`. A round's calls run concurrently, each through its
  gate in §21.1's order (a call cut short by `length`, a tool the turn does
  not carry, the `ToolPolicy`, the arguments' schema, then
  `BEFORE_TOOL_USE`) with the reference implementation's texts, then
  `ON_TOOL_CALL`. The room keeps a round's text as a MESSAGE, a
  `TOOL_CALL_START` per call, then the `TOOL_CALL_END` rows in call order;
  the last MESSAGE carries `loop_end_reason` and `ai_usage`; an event a
  tool sends is stored after its round's text and its call's start; a later
  turn replays no call. A tool that raises reads as `Tool 'x' failed (<kind>)`,
  its message for the observers alone. A provider's error keeps the text
  it produced and fails the turn. The agent takes no inbound message: one
  sent on its channel is refused, nothing entering the room, as the
  reference implementation's AI channel refuses it. Outside a kit,
  `AiAgent::on_event` returns the rows buffered.
- **Tools** (§21): `Toolset` serves many tools behind one call (`tools()`
  and an `async` `call(call, context)`, and `close`, which the agent runs
  when its kit shuts down), and `Tools` holds the host's closures, each
  with its `ToolDefinition`; `ToolOutput` is text or JSON; `ToolError`'s
  refusals and failures carry the words the model reads, and any other
  error (`ToolError::raised`, `raised_as`) reads as its kind; `arguments::<T>()`
  reads a call's arguments, refusing them as `Invalid arguments` when they
  do not fit; `ToolContext` gives a tool its room, its agent and its turn.
  `ToolPolicy` allows and denies tools by name and glob, and a tool it
  denies is never declared to the model. A name two toolsets serve is
  refused when the agent takes the second (`AiError::DuplicateTool`).
- **Skills from the host's directories** (§24, §6.4, §21.1).
  `SkillRegistry` discovers the skills of directories (`discover(&[dir])`)
  or registers one (`register`), strictly and atomically: the first skill
  that cannot be loaded fails the call with a `SkillError` naming it, and
  the registry is left as it was. A skill is a directory holding a
  `SKILL.md` whose frontmatter is read line by line, with no YAML reader,
  within §24.1's bounds (65,536 characters, 64 brackets open at once); a
  frontmatter the reader cannot read whole, and one declaring
  `allowed_tools` or `requires`, are refused. Names and descriptions follow
  the reference implementation's rules (kebab-case, 64 characters, the
  directory's name; 1,024 characters), a `SKILL.md` takes 256 KiB at most
  and the manifest 65,536 characters. A link to a skill directory is
  followed inside the directory discovery scans, refused out of it.
  `AiAgent::with_skills` gives an agent its skills: each turn's system
  prompt carries, after the agent's own, the reference implementation's
  preamble and `<available_skills>` manifest (`SkillRegistry::to_prompt_xml`),
  then the instructions of the skills active in the room; the agent serves
  `activate_skill`, which answers a skill's instructions and references the
  first time and an acknowledgement after, and `read_skill_reference`,
  which reads a file of the skill's `references/` by a name of
  `[A-Za-z0-9._-]`, resolved with its links followed and inside that
  directory, 256 KiB at most. Each file is opened, then checked through
  its handle: on Linux the path the kernel gives the open file
  (`/proc/self/fd`) must be inside, so one swapped for a link out
  meanwhile is never read, and an unreadable `/proc` refuses it; on other
  Unix systems the path resolved anew must name the handle's device and
  inode. The two tools escape the agent's tool policy, as §21.1 lists them.
  An activation counts once its call is served, for the room, four skills
  at most; a restarted agent reads the room's served activations again from
  the stored rows its turn is handed. A turn that stands alone carries
  none, and records none. No skill script runs.
- **An MCP server's instructions** (§21.2, §6.4):
  `AiAgent::with_server_instructions` carries what a server published in
  every system prompt, after the agent's own and before the skills, under
  the host's label for the server, in a `<server_instructions>` block whose
  forged closing tag is neutralised, cut at 8,192 characters
  (`AiError::ServerLabel`, `AiError::DuplicateServer`).
- **Tests** (`roomkit_ai::testing`): `ScriptedProvider` answers from a
  script of `ScriptedRound`s, which write a model's text, its calls, its
  deltas in pieces (`writing`, `in_pieces`) and a failure mid-answer
  (`failing`).

#### `roomkit-openai`, an agent on an OpenAI-compatible endpoint

- **`OpenAiAgent`** (§6.4, §6.7): `roomkit-ai`'s AI channel over the Chat
  Completions wire of an OpenAI-compatible endpoint (OpenAI's, vLLM's,
  OpenRouter's, DeepSeek's, Mistral's, a local server's), each generation
  a streamed completion (`stream`, `stream_options.include_usage`) read as
  server-sent events over reqwest, with no SSE crate, the calls' fragments
  folded by index, bounded per line (1 MiB), per event (1 MiB) and per
  answer (4 MiB). A turn sends the system prompt and the turn's
  conversation, and declares the tools the agent serves; a completion's
  members written `null` read as absent. `OpenAiConfig` names the base URL
  (with no query or fragment, refused with `OpenAiError::BaseUrl`), the API
  key (an empty one sends no `Authorization`; `Debug` never prints it), the
  model, the system prompt, how many messages a turn sends at most (all
  those among the 50 events the kit's context holds, by default) and the
  request's timeout (120 seconds); `Debug` and `shown_base_url` show the
  base URL's scheme, host and port only. `with_tools`, `with_tool_policy`,
  `with_max_rounds`, `with_tool_timeout`, `with_skills` and
  `with_server_instructions` are `AiAgent`'s. On OpenAI's own endpoint
  (`https://api.openai.com/v1`), a tool whose name is not
  `[A-Za-z0-9_-]{1,128}` is refused when the agent takes its toolset
  (`AiError::ToolName`), rather than failing every turn's request at the
  vendor; a server behind another base URL decides itself. A provider
  error, a request that fails, an answer over its bounds or one that is no
  chat completion fails the turn with an `OpenAiError`: `ON_ERROR` fires
  and the caller reads it on its result. A provider error keeps the
  provider's type, code and message, on one line, bounded, the API key
  removed; nothing else of its answer.

#### `roomkit-sqlite`, a store in one SQLite file

- **`SqliteStore`** (§14): `SqliteStore::open(path)`, a `ConversationStore`
  in one SQLite file on the Python implementation's schema, version 12, so
  that either implementation reads the other's file, as long as its channel
  types are ones Python lists (RMK-986): the same tables and indexed
  columns, each record's JSON in `data`; a record's members roomkit-rs does
  not model kept when it rewrites the record; the tables Python derives from
  the timeline kept right (idempotency keys, the full-text index, which a
  deleted message leaves, and the index sequences); the delivery cursor read
  from its column; a timeline page read from the index on `(room_id, idx)`,
  its filter's criteria on the row's columns; the tables it does not use left alone; a new file, and
  its missing directory, created at version 12 as Python creates it, and a
  file of another version refused (`OpenError`, whose `Database` variant
  holds a `StoreFailure`, so SQLite's version is no part of the crate's
  API). An identity's address under a key that is no channel type, which
  Python takes as written, is kept as a member the store does not read, and
  resolves nothing. One connection on tokio's blocking threads, in WAL mode,
  for one process. The store suite and the conformance suite run on it, and
  a cross-read test reads and rewrites a file Python wrote (`make
  sqlite-fixture`).

#### `roomkit-mcp`, the tools of MCP servers over stdio

- **`McpCommand` and `McpServer`** (§21.2, §21.1, §6.4), on `rmcp` 3.8, its
  minor version pinned. `McpCommand` starts a server, a program and its
  arguments as a list, never through a shell, its environment cleared but
  for `HOME`, `LOGNAME`, `PATH`, `SHELL`, `TERM`, `USER` and the variables
  it is given, its standard error read as it writes so it never blocks,
  each line logged at debug level. `McpCommand::connect` runs the MCP
  handshake and lists the server's tools once, following the cursor, within
  60 seconds by default (`connect_timeout`); a connection that fails
  part-way kills the process and waits for it before the error (`McpError`)
  is returned, with the last 20 lines the server wrote on its standard error
  (4 KiB at most), which end a failed handshake's or listing's message and
  are a timeout's `stderr`: a missing package, a traceback, an image being
  pulled. What a server's words make up in an error or a log line is
  bounded, its control characters, line separators and bidi controls
  replaced and each line break indented, so that it cannot drive a terminal
  or forge a log line. An
  `McpServer` is a `Toolset`: an agent serves its tools under their bare
  names, its tool policy and its bound per call applying as to any tool. A
  call answers the result's text, a note for what is not text, and a result
  that says `isError` is a failed call whose words the model reads. What a
  server sends is bounded: a message at 4 MiB, the listing at 1,000 pages
  and 512 tools, a name at 128 characters, a description at 4,096, a schema
  at 32 KiB, a result at 100,000 characters; rmcp's response cache is
  disabled, so no page of the listing stays resident.
  `McpServer::instructions` is what the server published, for
  `with_server_instructions`. On Unix the server leads a process group of
  its own, so that what it starts (the real server behind `npx` or `uvx`)
  stops with it, as the reference implementation's SDK stops it: at
  `close()` its input is closed, 2 seconds of grace by default
  (`close_grace`), then `SIGTERM` to the group and `SIGKILL` after the grace
  again; the same when the kit of the agent serving its tools shuts down,
  when its last handle is dropped and at a failed connection; one that
  exits on its own is waited for at once, its group ended. In its own group
  the server no longer receives a terminal's Ctrl+C, which a host that
  shuts its kit down on Ctrl+C covers.

#### `roomkit-core`, the sans-IO core

- **The model** (`roomkit_core::model`, RFC §5, re-exported whole as
  `roomkit::model`): rooms, events and every content type, bindings and
  permissions, capabilities, participants, identities, tasks,
  observations, inbound messages and direct events, channel outputs and
  hook results, tool calls, with their invariants checked at construction
  and their JSON form. A room's `paused_at` and timers, a visibility's
  grammar and intersection (`Visibility::intersect`, gap G-009), the
  partial updates of a room, an event and a binding (`RoomUpdates`,
  `EventUpdates`, `BindingUpdate`, which carries over what it does not
  name, §7.5 rule 6), and what a room is created and a channel attached
  with (`RoomOptions`, `BindingOptions`). A channel's type is an open name
  (`ChannelType`, with the well-known names as constants, `ChannelType::SMS`,
  `ChannelType::TEAMS`, ..., `DISCORD` and `BUZZ`, which the Python
  implementation writes), serialized as the plain name, so any name a
  shared store holds is read and written back: lowercase ASCII letters,
  digits, `_` and `-` (`ModelError::InvalidChannelType` otherwise),
  compared rather than matched. How a
  channel's senders are addressed is its declaration's
  (`ChannelDeclaration::addressing`, the type's default unless the channel
  sets another), and a declaration names its category; a channel claiming
  `system`, or `host` without being a host channel, is refused at
  registration (`ConfigError::ReservedChannelType`).
- **The types that will grow are `#[non_exhaustive]`**, so that a member
  added later breaks no caller: the options (`RoomOptions`,
  `BindingOptions`, `HookSpec`, `HookFilter`), a channel's declaration
  (`ChannelDeclaration`, `ChannelCapabilities`, `RateLimit`, `RetryPolicy`),
  every content struct and `EventContent`, `HookTrigger`, `FrameworkEvent`
  and each of its variants, the transcript's `Role` and `Message`, the
  errors, and every RFC enumeration open by nature (`EventType`,
  `EventStatus`, `RoomStatus`, `DeliveryStatus`, `TaskStatus`, the
  participant's statuses and role, `ToolCallStatus`, `ToolCallOutcome`,
  `ChannelMediaType`, `Addressing`, `Visibility`, `AgentResponsePolicy`,
  `RichFormat`, `EditSource`, `DeleteType`, `HookDecision`,
  `ConsumptionState`, `LifecycleChange`, `TransitionReason`,
  `RefusedOperation`). Build a struct from its `Default` or its constructor,
  which takes the members the RFC requires (`MediaContent::new(url,
  mime_type)`, `EditContent::new(target, new_content)`,
  `DeleteContent::new(target)`, `RetryPolicy`'s, ...), then set fields: `let
  mut options = BindingOptions::default(); options.group = true;`; an edit's
  `edit_source`, null by default, and a delete's `delete_type`, `SENDER` by
  default, are set with `with_edit_source` and `with_delete_type`. A `match`
  on one of these enumerations outside the core ends with a wildcard arm. A
  pattern on a `FrameworkEvent` variant names the members it reads and ends
  with `..`, and a driver or a test builds one with its constructor
  (`FrameworkEvent::room_paused(room_id, reason)`, one per variant, named
  after the RFC's event). An error grows by variants, never by fields. The
  records the store reads and writes (`RoomEvent`, `Room`, `ChannelBinding`,
  `Participant`, ...) and the calls' inputs (`InboundMessage`,
  `DirectEvent`, `Post`) are not marked: build them with `..base`.
- **The rules** (`roomkit_core::rules`), functions with no I/O and no
  clock: permissions and delivery planning, with solicitation and
  addressing (§7.5, §10.2, §19.3); the visibility a hook's injected event
  and a refused record keep, never wider than their event's (gaps G-031,
  G-036); answers, at any depth of their message's cascade, kept within
  where its answers may go, which each response carries as its own
  `response_visibility` within its channel's, a response's hooks never
  widening it past that and any hook narrowing an event's scope, never
  widening or clearing it (§5.2, §5.12, §9.4, gap G-073); a channel's
  history withholding a soft-deleted event's content (gap G-062);
  transcoding and the text a reader takes, a tool call's and a system
  notice's included, a composite's empty parts skipped, a location's
  coordinates in decimal with a point (`45.0`), an edit a channel cannot
  apply one `Correction: ` text (§6.6, gap G-072); the chain-depth limit and
  a cascade's reentry budget, shared by the calls that draw on it (§8.3);
  hook registrations, the off-lock and locked chains and how a chain of
  SYNC outcomes decides (§9.1–9.4); the lifecycle's transitions, which
  refuse a status they neither leave from nor lead to (gap G-074); the
  timer transitions due at an instant (gap G-045); a detach's revocation,
  kept as the Python implementation keeps it, the list
  `roomkit:detached_channels`, the former `roomkit:detached` map read and
  folded into it, and the `roomkit:` metadata namespace (gaps G-049,
  G-053); an address in its channel's form, a phone number in E.164 by the
  rules of gap G-052 and an email address lower-case, how each channel type
  is addressed, its recipient and chat keys, and what a recipient or a chat
  is, a string that is not blank or an integer, never a float, an integer
  naming the address its digits write; the key absent, null or a blank
  string is no value, and any other value is not a recipient, which a
  delivery refuses and routing reads as someone else's address: no routed
  sender is admitted into a room through it, under its lock whatever
  strategy chose the room, the record never writes over it, a `group`
  binding's is not read, at step 1 included, and a warning names the
  binding and the value's type, never the value (gaps G-051, G-057, G-075,
  G-078, RMK-965); the rooms an organization reaches (§17.2); the default
  routing strategy, its steps 1 and 3 over ACTIVE and PAUSED rooms, what
  keeps a routed room under its lock and what step 2 records of a routed
  sender (§10.4, gaps G-045, G-055, G-058, G-061); the validation of an
  edit or a delete against its target (§10.3, gap G-062); what an
  instruction must be and what its responses carry (§10.1.1, gap G-065);
  and the conversation an agent's turn reads (`rules::transcript`).
- **The effect protocol** (`roomkit_core::ops`), which `roomkit` drives:
  an `Operation` is polled for a `Step`, the notices to perform, the
  adopted entries settled, an inbound call's decision once known
  (`Decided`), and what comes `Next`; its `Effect`s each carry a `Request`
  of one family, hold (the room's lock, its delivery lane, its admission
  ticket), store, channel, hook or route, a deadline, the lock held on its
  behalf, the admission order it stands in and the reentry budget a
  delivery's calls draw on, and are answered by one `Outcome` of the same
  family, `Failed` or `DeadlinePassed`. `Operation::adopt` hands an
  operation a call its own work makes into its room, `Operation::expire`
  expires one whose caller is gone, and `Operation::cancel_turns` cuts a
  decided call's turns. Every poll reads the environment the run started
  with; `Environment` takes `&self`, `SequentialEnvironment` is shared
  through an `Arc`.

#### Examples

- **One runnable file each** (`examples/`), every one checking what it
  shows and exiting non-zero when a check fails, with no network and no
  key: the RFC's Appendix B flows (`echo_room`, `new_conversation`,
  `block_and_inject`, `agent_chain`, `advisor_joins`,
  `edit_delete_messages`), rooms and channels (`room_lifecycle`,
  `channel_mute_unmute`, `room_membership`), hooks (`hook_moderation`,
  `hook_inject_welcome`, `hook_off_lock_check`), the pipeline
  (`instruction_event`, `response_visibility`, `send_event`, `host_post`,
  `names_that_read_alike`), routing and identities (`shared_sms_number`,
  `paused_conversation`, `identity_addresses`), agents (`openai_agent`,
  against a local fake of the OpenAI wire unless `OPENAI_BASE_URL`,
  `OPENAI_API_KEY` and `OPENAI_MODEL` name an endpoint, `agent_tools`,
  `streamed_answer`, `agent_skills`, `mcp_tools`), and two servers
  (`server_agent`, and `web_chat`, which serves a page to chat with its
  agent in a browser, each page sent its conversation's past as it opens,
  read with `get_timeline` and `visible_events`).
- **The server example** (`server_agent`), the README's quickstart: an axum
  server where each WebSocket connection to
  `/conversations/{conversation}?user=...&name=...` is a participant of the
  conversation's room, written through the store, a new room made whole
  even when its first client leaves meanwhile, each text frame a message
  run into the room, and the room's deliveries sent on each of its
  connections as JSON frames, each send bounded, a client that stops
  reading dropped; a connection with `&stream=1` reads each answer as it is
  written. A refused message, a failed turn or one a stop ended is answered
  with an error frame, a message or a frame over the limit closed with
  1009, a frame at its header, a stop with 1001; the ids a connection names
  are checked (1 to 64 letters, digits, `_` or `-`, a name of 64 characters
  at most with no control character, else 400), and what a service must
  add is said in its header (TLS, authentication, the `Origin` check, who
  may create rooms, a rate limit, a cap on connections). An `OpenAiAgent`
  answers each message, with a tool that looks an order up, and a
  `SqliteStore` keeps the rooms, so that a restarted server goes on with
  each conversation's history. Its checks run offline, against the fake of
  the OpenAI wire and a temporary file: two people in one room, labelled as
  §6.4 asks, a tool call, a streamed answer, then a restart on the same
  file whose next turn reads the earlier ones and replays no call.
  `--serve [ADDRESS]` keeps it up for a person to chat with. Its WebSocket
  channel is the example's (`examples/shared/websocket.rs`), not a crate;
  `axum`, `tokio-tungstenite` and `futures-util` are dependencies of the
  examples only.

#### Repository

- **The workspace**: the `roomkit-core`, `roomkit`, `roomkit-ai`,
  `roomkit-openai`, `roomkit-sqlite` and `roomkit-mcp` crates and the
  `roomkit-examples` package; workspace lints; the gates `make all`, `make
  examples` and `make msrv`; CI for commit messages, formatting, lints,
  tests, documentation, the examples, the core's dependencies, the minimum
  Rust version, the dependency policy, and a weekly check against the
  security advisories; the architecture, roadmap, step and RFC gap
  documents. The minimum Rust version, 1.92, is written once, as the
  workspace's `rust-version`.
- **The shared conformance suite**: the scenarios of roomkit-specs'
  `conformance/`, copied at one commit (`make conformance-sync`), which
  `make test` runs through the turn backend, on the in-memory store and on
  SQLite, with a scripted model and scripted tools for the `tools/` and
  `streaming/` areas.

[Unreleased]: https://github.com/roomkit-live/roomkit-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/roomkit-live/roomkit-rs/releases/tag/v0.1.0
