# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- A turn with tools behind a `base_url` sends `reasoning_effort` only where `OpenAiConfig::supports_reasoning_effort_with_tools` says the server takes it there, as Python's does: a server that refuses it on a tool turn no longer answers 400 (RFC §6.7, RMK-1088).
- A generation's retries and its fallback provider (RFC §6.4), as Python's: `AiChannelOptions::retry_policy` tries a refusal worth another try again, before anything was handed, after a compacted replay of an overflow; `fallback_provider`, any provider as a `DynAiProvider`, is asked once the retries are spent, and its failure before it handed anything leaves the primary's error (RMK-1267).
- The JSON the kit writes as Python's `json.dumps` (a tool's answer or
  JSON result, an MCP result of parts, a structured payload, the audits'
  JSONL lines, a session's transcript) writes a float as Python's `repr`:
  `1e-05` and `2.5e-07`, where it wrote `0.00001` and `2.5e-7`
  (RMK-1262).
- An agent whose turn fails with a kit error that crosses its turn
  backend reports the error's class as `ON_ERROR`'s `error_type`,
  `ProviderDeliveryError` for a provider's refusal, its code still the
  delivery result's, as Python's `error_type_name` names it, and one that
  met the kit's close is still the close's (RFC §10.6.8, RMK-1128).
- The loop strategy (RFC §19.7.4), as Python's: `roomkit::Loop`, `orchestration::LoopOptions` and `orchestration::strategies::supervisor::WorkerStrategy` (RMK-1233).
- An MCP provider's `tool_filter` chooses among every tool the server
  lists before the 512-tool bound counts, as Python filters the listing it
  reads (RFC §21.2, RMK-1099).
- `OpenAiAiProvider` reads a failure as Python's: `generate` sends one
  completion, not streamed, on Chat Completions; an error object's `code`, `type` or `status`
  gives the failure its status, inside a stream, a failed Responses
  stream's included, or answered with 200 to a completion asked whole
  (`server_error` is 500, retried), and an error inside a stream with no
  message reads as Python's client words it; an error answer past 4 MiB
  keeps its status; a conversation message keeps its role, the system's
  included, and a message of parts goes as its blocks whatever its role
  (RFC §6.7, RMK-1087, RMK-1097, RMK-1098).
- The tool and session auditors (RFC §15.8), as Python's:
  `ToolAuditEntry`, `ToolAuditor`, `JsonlToolAuditor`, `ConsoleToolAuditor`,
  `SessionAuditEntry`, `SessionAuditor` (its `record_tool`, `tool_auditor`
  bridge and `attach(kit)`) and `JsonlSessionAuditor` in
  `roomkit::orchestration`, and `roomkit_ai::orchestration::audit_tool_handler`,
  which records each call `ok`, `failed`, `error` or `cancelled` and hands
  the channel the handler's answer unchanged; the JSONL lines and summaries
  are Python's (RMK-1233).
- A message steered into a running turn and the lines a summarizer reads
  tell nameless senders of one channel apart (`@sms`, then `@sms (2)`) and
  count speakers by source, as the conversation does, and a delegation's
  conversation block labels them so; a steered message no longer counts the
  agent's own replies, the application's instructions, tool-call records or
  blank turns as speakers (RFC §6.4, RMK-1221).
- An edit or a delete is not steered into a running turn: it waits its
  place, for no turn (RFC §10.3, RMK-1226).
- A notice, a reaction or a participant's arrival is not steered into a
  running turn either: it waits its place, for no turn, and past the depth
  limit it gets no record of an agent not asked, nor does a change (RFC
  §6.4, §8.3, RMK-1257).
- A sender's routing stamps are dropped, as Python's now are: a message
  that arrived with `_routed_to` or `_always_process` in its metadata chose
  the agent that answered it, or added one to its address; the kit drops
  both before an event's `BEFORE_BROADCAST` hooks run, with a warning, so
  only a hook, the conversation router among them, writes them (RFC §19.4,
  RMK-1233).
- A room's conversation state and the conversation router (RFC §19.2,
  §19.4), as Python's: `ConversationState` with `transition`,
  `ConversationPhase`, `PhaseTransition`, `get_conversation_state`,
  `set_conversation_state` and `save_conversation_state`, stored under the
  room metadata's `_conversation_state`; `ConversationRouter` with
  `RoutingRule` and `RoutingConditions`, whose `as_hook()` stamps
  `_routed_to` and `_always_process`, and the broadcast asks the agent
  stamped and the supervisor alone, where it asked every agent
  (RMK-1233).
- A registered channel says how it takes a room's turns,
  `RegisteredChannel::steers_room_turns` and `serializes_room_turns`, as
  a host reads Python's `kit.get_channel(id).steers_room_turns` (RFC §6.4,
  RMK-1253): a channel that steers them serializes them.
- A whole copy of a runtime mark that no closing bracket follows takes
  only the mark's own punctuation with it, as Python's: `x context
  compacted - earlier conversation summary.` reads `x [A copy of a runtime
  mark stood here: …].`, its period kept, where it was dropped (RFC §6.4,
  RMK-1231).
- An MCP part whose server named no media type is noted as Python notes
  it, `image/png` for an image, `audio` for audio, `binary` for a
  resource, where it read `unknown` (RMK-1251); a link of no type still
  reads `unknown`, as Python's now does.
- An attach's `CHANNEL_ATTACHED` record is stamped once its channel's
  `on_room_attached` answered, after what the channel wrote into the room,
  and the hooks of an attach or a detach read the room as the channel left
  it, as Python emits the record and reads the room then (RFC §5.1, §6.1,
  RMK-1246): the record took an earlier instant than the events the
  callback wrote, and the hooks a room without them.
- A channel's room callback (`on_room_attached`, `on_room_detached`) that
  sends into its room stores its event, where it failed
  `RoomKitError::ReentrantCall`: a call the run holding the room's lock
  does not adopt runs nested under that lock, on the lease of the code
  that made it, as Python's call nests under the lock its caller holds
  (RFC §6.1, §9.5.1, RMK-1245).
- `roomkit_ai::Agent`, an AI channel with an identity, and
  `roomkit::AgentIdentity`, as Python's (RFC §19.7, RMK-1244): its role,
  description, scope and language end its turns' system prompt as an
  identity block, in the language the room's conversation state sets,
  unless `identity_in_prompt` is off; its declaration carries the identity
  (`ChannelDeclaration::identity`).
- A strategy's install holds its room's lock while it runs, as Python's
  (RFC §19.7, §13.5, RMK-1247): another caller's attach waits for it and
  stays bound after the uninstall; `create_room` with a strategy runs its
  `ON_ROOM_CREATED` hooks once the strategy is installed; an install cut
  short is taken back once the writes it had decided landed; and
  `uninstall_strategy` called during an install waits for it, then takes
  it back, but from code holding the room's lock, which gets `false` at
  once.
- An MCP tool's result holding a PNG, JPEG, GIF or WebP image whose base64
  decodes reaches the model as its parts, the image an image part it
  sees, as Python's MCP handler (RMK-1242); any other image is still a
  note. Its texts hold within the 100,000 characters a text result is cut
  to, in all, and at most four of its images within 5 MiB in all, each
  image past either a note that counts as a text (the maintainer's
  choice), which Python's holds too (RMK-1249).
- A tool's end row bounds what it keeps of a text result that is a data
  URI and of a JSON result's binary blocks (image or audio `data`, `url`
  or `source.data`, a resource's `blob`), within the 512 KiB its image
  parts share, a note in place of each payload past it, as Python's
  `tool_event_payload` (RMK-1241).
- A strategy's agent given live under the id of a channel the kit holds
  that is not an intelligence channel is refused, `RoomKitError::Value`
  (`Channel 'ana' is not an intelligence channel`), nothing attached, as
  for an agent named by its id (RFC §19.7, RMK-1233): the transport was
  attached as the agent, taking its turns.
- An AI channel registers on its person's handler when a kit registers
  it, as Python's `_wire_ai_channel`, not when it is built (RMK-1201):
  with one handler shared by two channels of one id, the one a kit
  registered last owns the id's requests, and its kit's close settles
  them.
- A kit whose status bus does not subscribe at once subscribes at its
  first room activity, which awaits it, as Python's
  `_ensure_status_bus_subscribed` (RMK-1201): a status posted once that
  call is over reaches the room's `ON_STATUS_POSTED` hooks, the kit built
  in a runtime or outside one, and a subscription cut short is made again
  by the next activity. It was started in the background, a status posted
  meanwhile missed.
- A transport whose `on_event` returns a record replies as an agent does,
  as Python's `_settle_buffered_replies` merges every target's output
  (RMK-1248): its keys reach the caller's `response_metadata`, and its end
  goes under `turns` by its channel; only agents' did.
- The kit's close cuts a streaming target whose read the operation
  stopped (a delivery cancelled by its key) as it cuts any reader, as
  Python's close cancels a `deliver_stream()` at any point (RMK-1236): a
  target that never returns once handed the stream's end no longer holds
  the close for its five seconds of grace.
- An AI channel attached to a room, or detached from it, forgets what it
  kept for the room, as Python's `_forget_room` (RMK-1219, RMK-1240): an
  agent that leaves and joins again sends the author images its provider
  refused there, replays none of the images its tools returned there, and
  reads the skills active there again from the store.
- An AI turn takes the `temperature`, `max_tokens` and
  `turn_budget_tokens` its binding's metadata sets, over the channel's
  own, as Python's `_turn_settings` and `_turn_budget` (RFC §6.4,
  Appendix A.9, RMK-1237), a `null` there not set. The temperature and
  the max tokens are read as pydantic's lax mode reads them (`"0.3"`
  gives 0.3, `64.0` and `"100"` give 64 and 100, `true` gives 1); any
  other value (`"hot"`, `1.5`) fails the turn with pydantic's
  `ValidationError` text, and so does a max tokens outside `u32` (`-5`,
  which Python sends). A token budget that is not a positive integer
  (`"1200"`, `64.0`, `0`) fails the turn with Python's `ValueError`, and
  a system prompt that is no text (`5`, `0`, `false`) fails it first with
  pydantic's `ValidationError`, as Python now reads it. The binding's
  thinking, reasoning effort, response schema and cost budget come with
  those features.
- A buffered reply's end and usage go under its caller's `turns`, by its
  channel, read off its turn's record or else the last of its messages
  that names one, as
  Python's `record_buffered_reply` (RFC §6.4, RMK-1235): `{x: 1, turns:
  {bot: {loop_end_reason: completed}}}` where it read `{loop_end_reason:
  completed, x: 1}`. Every agent reply's record, a stream's first one
  included, reaches the caller without its turn's own keys
  (`loop_end_reason`, `ai_usage`, `turns`), as Python's
  `merge_channel_record`.
- A failed task's exception and a turn's error record keep the error's
  attributes, as Python's `error_record` (RMK-1239): `ErrorRecord`'s
  `attributes`, a provider error's `retryable`, `provider`, `status_code`
  and `context_overflow`, a task cut short's `reason` and `narration`, a
  turn link's `reason`, which `wait_task` hands on. A delivery failed by
  a raised error reads the `code` and the `retryable` its record
  declares, as Python's `_delivery_error`: a `ProviderError` (a
  budget-aware memory's overflow) is not retryable, where it read
  retryable. Breaking:
  `ErrorRecord` has the new public field `attributes`; build one with
  `ErrorRecord::new`, or add `attributes: Box::default()` to a struct
  literal.
- A `ProviderError` reads with its provider and the status it answered
  first, as Python's (RMK-1238): `openai (401): the provider answered HTTP
  401 ...`, `anthropic: connection reset`, so a turn's error record and
  `ON_ERROR` say which provider failed. A message that already names its
  provider as a word, in any case, and a failure with no provider read as
  given; `message` keeps the provider's own text.
- Breaking: `RoomKitError::ChannelNotRegistered` carries its message, as
  Python's `ChannelNotRegisteredError` does (RMK-1201):
  `ChannelNotRegistered { channel_id, message }`. Its text is the site's,
  where it read `channel sms not registered` everywhere: `Channel sms not
  registered` for most, `Agent channel 'worker' not registered` from
  `delegate`, and `Channel 'worker' is not registered: a strategy names by
  its id an agent the kit holds, live or by its declaration` from
  `setup_background` and `setup_task_tools`, which refuse a channel
  that is no intelligence channel first, as Python's `held_agent`:
  `Channel 'sms' is not an intelligence channel`. Match it with
  `RoomKitError::ChannelNotRegistered { channel_id, .. }` where
  `RoomKitError::ChannelNotRegistered(channel_id)` was written.
- `Channel::on_room_attached` and `Channel::on_room_detached`, as Python's
  (RFC §6.1, RMK-1219): the kit awaits them under the room's lock, the
  attach's with the stored binding before the attach is recorded or
  announced, the detach's once the binding is removed, before its hooks.
  A transport that refuses its attach fails it with
  `RoomKitError::Channel`, the binding it replaced put back; one that
  fails its detach fails the call once the detach is announced. An
  intelligence channel's run on its executor, which logs what they raise.
  Both default to doing nothing.
- Breaking: `DeliveryHandle::request_cancel` is removed; `cancel` is the
  handle's only cancel, as Python's (RMK-1204). Write
  `handle.cancel(CancelOptions::default()).await?` for
  `handle.request_cancel()` then `handle.wait().await?`: it asks the same
  cut, `caller_cancelled` by default, and returns once the cut turns are
  over; it also reaches a call the room's work adopted. Unlike the old
  pair, it gives up after its `timeout` (5 s by default) with
  `RoomKitError::Timeout`, the cut standing and `wait` joining it; and
  from the room's own work (a SYNC hook under the lock, a turn or a
  delivery in the room), its delivery not done, it fails with
  `RoomKitError::Runtime` and cancels nothing, as Python's: cancel there
  by the message's `delivery_key`, `cancel_delivery`.
- A message the room's work adopts in its admission order (an off-lock
  hook's or a parse's `process_inbound` into its room), named by its
  delivery key, is named as the run that adopted it settles it, under the
  room's lock, as Python's `_name_held` (RFC §11.7.6, RMK-1200): held for
  verification, it is held by its key before any release can reach it,
  where its caller named it once it resumed, after the lock was let go.
- A release made from a SYNC hook (`complete_verification` under the
  room's lock) places its answer in the room's lane at the release, and the
  answer leaves its place once the hook's section is over, as the RFC
  says (§11.7.6, the maintainer's choice, RMK-1200): a
  hook that then closes the room has no agent answer the released message
  (`room_refused_event`, operation `regenerate`), where the agent answered
  it while the section still ran. Python's lane runs it at the first await
  that yields, the lock still held; whether Python aligns is RMK-1243.
- A tool answers a text and its images, as Python's `ToolResult` (RFC
  §6.4, §21.4, RMK-1218): `ToolResult::Parts(vec![ToolResultPart::Text(..),
  ToolResultPart::Image(..)])`, or a JSON list of part-shaped mappings. A
  model that sees reads the parts in their round, roomkit-openai sending
  the images on a user message after the tool messages; one that does not
  reads their text, `[image]` for each image. The room's latest two inline
  images of a vision type ride each later turn's input, after its words
  and notes, each after the call that returned it, until a refusal of a
  turn's first request that may be about its content forgets them (a
  401, 402, 403, 404 or 429 keeps them). The end row keeps 512 KiB of a
  result's images, a note in place of the rest. Breaking:
  `AiToolResultPart::result` is a `ToolResult` (`AiToolResultPart` has
  Python's `as_text()` and `split_for_message()`), and `ToolResult` has a
  third variant, `Parts`, which an exhaustive match must name.
- A room takes an orchestration strategy while it lives, one at a time
  (RFC §19.7, RMK-1233): `RoomKit::install_strategy`,
  `uninstall_strategy`, `claim_room_strategy` and `room_strategy`,
  `RoomKitOptions::orchestration` and `CreateRoomOptions::orchestration`,
  as Python's. A strategy implements `Orchestration`; its agents, given
  live or by id, are attached at the install, and the uninstall takes
  back the room hooks, agents and metadata keys the install added.
  Breaking: `CreateRoomOptions` takes the kit's store type, as
  `RoomKitOptions` does, and no longer derives `PartialEq` and `Debug`
  (it implements both, a strategy compared by identity): code naming
  `CreateRoomOptions` for a kit over another store names
  `CreateRoomOptions<S>`.
- An AI channel's turn takes its binding's metadata `system_prompt` over
  its own, as Python's `_turn_settings` (RFC Appendix A.9, RMK-1208): a
  hand-back to an agent whose binding holds a persona runs on it, where
  the turn's prompt was the channel's; `null` keeps the channel's, `""`
  lifts it, the turn sending none.
- An AI channel's provider failure, its memory's context overflow
  included, is raised as Python's `ProviderError`
  (RFC §23.3 step 6, RMK-1208): a failed task's `exception`, the
  `process_inbound` result's `error`, `ON_ERROR`'s `error_type` and a
  failed delivery's code read `ProviderError`, where they read
  `ChannelError`.
- A channel's error that names its Python class (the hidden
  `ClassedError`, which roomkit-ai's provider errors are to use) keeps
  that class wherever the kit records it, as Python keeps the exception's
  type (RFC §23.3 step 6, RMK-1208): a failed task's `exception`, a turn's
  error crossing the turn backend, `ON_ERROR`'s `error_type` and a failed
  delivery's code read `ProviderError`, not `ChannelError`.
- A turn's host and its link quote the names their refusals give as
  Python's `!r` quotes them (RMK-1201): `A turn's host acts for its room
  'r1', not for 'r2'`, `No room 'r'`, `No verb 'x' is served by the room
  owner`, where they read `"r1"`, `"r"`, `"x"`, and a room named as
  another one's read unquoted.
- A delegation its caller cuts once the kit's close refuses new work
  still ends its task to its end, its completion callback included, on a
  run apart, as Python's shielded `_end_cut` (RFC §23.3, RMK-1201), where
  the task was given its result and the callback never ran.
- A delegated agent whose buffered output says it did not answer leaves
  its task failed with nothing kept, as Python's `_buffered_outcome`
  (RFC §23.3 step 6, RMK-1201): its reply is neither stored nor taken as
  the task's output, its end is not the task's, and its record still names
  its turn under `turns`, where the task completed with the reply's text.
- A keyed delivery's section holds the room lock, as Python's (RFC §22.1,
  RMK-1207): its key's lookup, its transport's check, its transport's
  parse and its commit run under the lock as one section, whatever work
  of the room its caller runs in, an off-lock hook's body included, and
  its call is nested under that lock: no admission ticket, its off-lock
  hooks run under it. A transport detached ahead of it is
  `unavailable`/`channel_unavailable`, nothing written, where a `BLOCKED`
  record was stored under the key.
- A JSON object keeps its members in the order they were written, as
  Python's dict does (RMK-1198): serde_json's `preserve_order` is on for
  the workspace. A tool's JSON result `{"total": 3, "items": [...],
  "currency": "EUR"}` reaches the model so, where it read `{"currency",
  "items", "total"}`; a call's arguments, a stored metadata map and a
  metadata record's keys keep their order too. A key removed from a map
  leaves the others in place (`shift_remove`; clippy refuses
  `Map::remove`, which swaps the last one in).
- A task's result handed back to nobody is logged as Python's
  `_log_not_delivered` logs it (RFC §15.2, RMK-1208): `Result for
  assistant in room r not delivered: unavailable (no_transport)`, a
  warning, or a debug line when the notified agent's turn failed.
- A delegated task whose worker's streamed turn ended short of its answer
  names that end, as Python's `_persist_child_stream` reads it off the
  stream's record (RFC §6.4, §23.3 step 6, RMK-1208): a turn failed after
  a round of calls keeps its last text as `output` and `loop_end_reason`
  `error`; a turn cut at `max_rounds` before any text fails `The worker's
  turn ended max_rounds before its answer`, `loop_end_reason`
  `max_rounds`, where both read no end; inline or in the background, with
  a transport shared or not, on `ON_TASK_COMPLETED` too.
- A room turn whose response streams names how it ended under `turns` in
  the caller's `response_metadata`, keyed by its channel, as Python's
  `_add_turn` (RFC §6.4, §10.1 step 18, RMK-1208): `{"turns": {"agent":
  {"loop_end_reason": "cancelled"}}}` for an agent whose streaming channel
  stopped reading it, where the result carried no `turns`.
- A delegated task cancelled, cut by a caller that stops waiting for it
  inline, or by the kit's close keeps its trace, as Python's cancelled
  reader keeps it (RFC §12.2 step 13s, §23.3, RMK-1208): its work is cut
  where it reads its turn, not aborted, so the text its turn wrote is
  stored marked `cancelled`, a row whose write was under way is kept, and
  a call left open ends `failed`, its error `cancelled` (`kit_closed` for
  the close), its tool cut, no longer running to its 30 s bound; a
  streaming target that reads no further is dropped, not waited for, and
  a delivery the worker deferred runs on. A task cancelled before it ran
  names no turn under `turns`.
- A delegation whose parent room's context the store fails to read as
  the task is announced fails with the store's error, or `RoomNotFound`
  for a room gone meanwhile, as Python's `_announce_task` raises it (RFC
  §23.3, RMK-1208): no `ON_TASK_DELEGATED`,
  no `ON_TASK_COMPLETED`, no completion callback and no status line, where
  the task ran and completed.
- An author's image goes as Python's `_inline` sends it (RFC §6.4,
  RMK-1197): a data URI is read for its data's presence and its size
  alone, its scheme, type and parameters in lower case, its data as
  written (a parameter beside `base64`, unpadded, URL-safe or wrapped
  base64 no longer read "[Image unavailable]"), a header that declares
  no type read as the media's own type; a link goes as written,
  a `..` segment, a backslash or a control character included; 5 MiB of
  bytes a `MediaResolver` gives resolve, where their base64's length read
  a byte more.
- The OpenAI provider reads an image's data URI as Python's
  `image_part_uri` (RMK-1197): whitespace and a missing padding repaired,
  the URI rebuilt from its bytes; one that does not read, URL-safe base64
  included, fails the turn before the request with `invalid image part:
  …`, not retried, and the room then reads the image as refused.
- A turn its provider failed after a round of calls reports, on
  `ON_AI_RESPONSE`, what the failed round had handed the room as its last
  segment (RFC §6.4, RMK-1199), as Python's: `["Let me look.", "Partial
  answer"]` where the report read `["Let me look."]`.
- A human-input request an AI channel's handler takes outside the kit's
  turns, as from a host's webhook or job, fires the kit's
  `ON_USER_INPUT_REQUIRED` hooks and emits `user_input_required`, as
  Python's (RFC §9.3, RMK-1201): the kit wires the channel's handler when
  it registers the channel, a shared handler's requests reaching the kit
  of their channel. It was told to nobody.
- `HumanInputHandler::wait` returns an answer accepted as its bound
  passes, as Python's (RFC §9.3, RMK-1201): `resolve()` from another
  thread could return `true` after the timeout fired and before the wait
  read the request, which the wait then reported as `RoomKitError::Timeout`,
  the answer lost.
- An AI channel compacts its context once and replays the call when its
  provider refuses it as too long for the model (RFC §6.4 "A turn
  compacted", RMK-1225), as Python's: the error's `context_overflow`,
  else its words (`is_context_overflow_message`); the history before the
  turn's input, or the older half, is summarized into the user message
  after it, a call never parted from its result, and the turn's later
  rounds build on it. A context of four messages or fewer, or one with
  nothing before its input, is not compacted: the turn fails, as it did.
- `RoomKitOptions::media_resolver` and `RoomKitOptions::attached_media`,
  Python's `RoomKit(media_resolver=..., attached_media=...)` (RFC §6.4,
  RMK-1190): `MediaResolver` gives the bytes of a reference an author
  sent, `AttachedMedia` names the media an event carries outside its
  content, and every AI channel registered on the kit reads its turns'
  events with them, the store and the broadcasts keeping the events as
  stored, an instruction standing alone reading none; a turn's references
  resolve together, as Python's `asyncio.gather`; an agent's own
  `image_resolver` comes first. A reader or a resolver that fails is
  logged and reads as none, never a failed turn.
- An AI channel remembers the author images its provider refused (RFC
  §6.4, RMK-1190), as Python's `RefusedImages`: a turn's first request
  that carried images and failed on an error not retried, of no status
  about the key, the bill, a permission, the model or the rate, before
  anything streamed, marks them refused in the room, and later turns read
  `[Image refused by the model's provider: “<name>”]` in their place.
  `ScriptedRound::with_context_overflow` classes a scripted refusal as a
  context too long for the model, or as not one.
- Delegation and human input as Python's (RFC §9.3, §19.8, §23.3,
  RMK-1192): `delegate` lets a store's failure to read the parent room
  through, no longer `RoomNotFound`; a delegated agent's buffered answers
  are kept in the child room as responses (one deeper, answering the
  description, no idempotency key, an instruction dropped); a failure its
  turn reported in the child room marks the result `error_reported`;
  `user_input_required` names the request as the tool asked it, whatever a
  hook's modify made of it; refusals quote names as Python's `!r`; a
  run's task line keyed by its task goes last; a kit built outside a
  runtime, whose bus does not subscribe at once, subscribes at its first
  call; a delegation cut once the kit closed still ends `cancelled`.
- `DeliveryHandle::cancel(CancelOptions { reason, timeout })`, Python's
  `DeliveryHandle.cancel` (RFC §10.1, RMK-1194): it cuts the call's turns
  and returns the call's result once they are over, the first reason kept;
  `RoomKitError::Timeout` past `timeout` (5 s by default), the cleanup
  going on for `wait` to join; `RoomKitError::Runtime` from the room's
  lane or under its lock, nothing cancelled.
- Identity verification's release and a delivery key's reach match
  Python's (RFC §10.1, §11.7.6, RMK-1195): a message a SYNC hook sends
  into its room is located in the room's organization before it is
  committed and, held, stays named by its key until its release; a
  release's answer cancelled before it is planned asks no agent; a
  release whose room is deleted while
  its answer waits in the lane emits `room_refused_event` with no status
  (`FrameworkEvent::RoomRefusedEvent::status` is now an `Option`), and a
  failed read of the room there runs no agent.
- The delivery queue reads as Python's (RFC §22.2, RMK-1157): a
  `DeliveryItem` read without `id` or `created_at` takes a fresh id and
  now; `deserialize_strategy` returns a `Result`, a `TypeError`
  (`RoomKitError::Type`) for params the strategy takes no such argument
  for or of the wrong kind, which the worker reports `strategy_failed`; a
  `kit.close()` from a hook of an item the in-memory worker executes
  returns, the worker neither cut nor waited for; a kit that starts on a
  backend whose worker serves a gone kit takes that worker on once the
  batch it executes is over.
- `OpenAiAiProvider` sends a user's message of parts as its text and
  image blocks in order, as Python's (RFC §6.4, RMK-1191): a caption, its
  image, then the next text, where it sent the texts joined, then every
  image.
- An AI channel's turn reads as Python's (RFC §6.4, RMK-1193): a round's
  results reach the model as one tool message, in the turn and replayed
  (the OpenAI request still writes a tool message per result); a `Cancel`
  while a round's calls are announced counts no round; with two people or
  more taking part, the turn's notes name them; notes join the last user
  message when the input reads as nothing; the turn's footprint measures
  the tools on their own schemas and counts the people note.
- An MCP tool's successful result with no content part reads `[]`, as
  Python's `text_body`, rather than its structured content's JSON
  (RFC §21.2, RMK-1193).
- A cancel waiting for a delegated task that ends as it stands goes on
  once that end is over however it stops, the close's cut included, as
  Python's (RFC §23.3, RMK-1186); a delegation announced as the kit's
  close began ends on a run of its own, which a cut caller leaves to run.
- A release's answer is refused as Python's (RFC §11.7.6, RMK-1189): a
  room that closes while the answer waits in the room's lane refuses it
  when the lane takes it, `room_refused_event` naming `regenerate`, and no
  agent runs; past `process_timeout` waiting for the room's lock, its
  `process_timeout` event carries `data.operation = "regenerate"`.
- The kit's clock reads microseconds, as Python's (RMK-1169): a message
  and a verification request opened right after it no longer read the
  same instant, so the request's window no longer takes in the message.
- A delegated turn's child room is a room for the chain's depth limit and
  its metadata, as Python's (RFC §8.3, §10.1 step 15, RMK-1188): an agent
  whose answer would reach `max_chain_depth` is not asked, its `BLOCKED`
  record and observation stored there and `chain_depth_exceeded` emitted;
  the agents' room metadata updates drop a key only the framework writes
  and stamp `updated_at`; a provider's refusal naming no error is reported
  `provider_send_failed`.
- A delegated task cut from outside ends `cancelled` once, as Python's
  (RFC §23.3, RMK-1186): an inline delegation whose caller stops waiting,
  or that the kit's close cuts, its worker's turn recorded `cancelled`; a
  delegation cut after its announce, before its run started; a task whose
  cancel's caller stops waiting, its end running on a task of its own. A
  cancel or a close that finds a task ending as it stands waits for that
  end, and returns `false`.
- A tool a `BEFORE_AI_GENERATION` hook adds to an agent with no tool of its
  own is declared too, its calls going to the tool handler, as Python's
  (RFC §6.4, RMK-1181).
- A held message's key names its delivery until its release is answered,
  as Python's (RFC §11.7.6, RMK-1182): cancelled after its call, in
  flight or while its set runs, it is released with no answer, the
  cancellation written on its hold mark so that a release in another
  process reads it; a cancel during the released answer cuts it; a
  message nothing will release is named no longer than its call, and one
  held past `release_max_age` no more. `complete_verification` called
  from a hook under the room's lock releases as Python's: its answer
  runs in the room's lane after the hook's section, where it failed
  `ReentrantCall`.
- A status's publication runs at least as deep as its poster, as
  Python's (RFC §8.3, RMK-1187): what an `ON_STATUS_POSTED` hook sends
  into the room is that deep, so a turn that posts a status, a hook that
  tells the agent again, stops at `max_chain_depth`; it ran at depth 0
  and never stopped. The hooks fire during the kit's close too, until the
  kit is sealed: the end of a task the close cuts reaches them.
- `submit_feedback`, `start_task`, `setup_background` and
  `setup_task_tools` are refused `KitClosed` once the kit's close started,
  before their arguments are read, and `cancel_task` returns `false`
  doing nothing, as Python's (RFC §12.10.4, RMK-1187).
- A channel unregistered loses the tools the kit set up for it and its
  background setup, as Python's: registered again under its id, it is set
  up afresh (RMK-1187).
- `ON_ERROR`'s payload is a message, as Python's (RFC §9.2, RMK-1183):
  its source the failed channel with the default direction, `inbound`, in
  the trigger's thread (`parent_event_id`), and a failed stream's carries
  that stream's `correlation_id`, the one its rows carry, now given by the
  operation (`ChannelRequest::ReadStream::correlation_id`). It was a
  `system` event, `outbound`, with neither.
- JSON the model reads is written as Python's `json.dumps` (RMK-1096): a
  tool's JSON value as `json.dumps(value, ensure_ascii=False)` (`", "` and
  `": "` between members), an MCP result of several parts as a JSON array
  of their texts, and the kit's refusal envelope with every character past
  ASCII escaped.
- In a delegated room a transport is shared into, the stream an agent
  answers another agent's answer with is read by the room's reader, as
  Python's (RFC §23.3 step 5, RMK-1185): it was lost, the driver told a
  read of a turn it did not hold.
- A tool a `BEFORE_AI_GENERATION` hook adds or redefines stays as the hook
  left it for the whole turn, as Python's (RFC §6.4, RMK-1180): declared
  at every round, an added one served by the tool handler, a redefined one
  checked against the schema the hook left; a tool the channel serves
  itself keeps its definition.
- A round whose text opens by replaying the text of the round of calls
  before it (a model repeating `Let me look.` after a call) is handed to
  the room from where it goes on, and `ON_AI_RESPONSE`'s segments say what
  the room saw, as Python's (RFC §6.4, RMK-1180); the model still reads its
  own text whole.
- `RoomKit::cancel_delivery(key, CancelDeliveryOptions)` cancels the
  deliveries of the messages sent with that `InboundMessage::delivery_key`,
  as Python's (RFC §10.1 "Explicit delivery cancellation", RMK-1152): the
  key names a delivery from `process_inbound`'s start until it is over, an
  adopted call's included; each delivery's turns are cut, a turn not
  started never starts, the message still delivered; a scoped cancel
  reaches only its organization's rooms, and waits for the room's read
  when the call named none; it answers how many it cancelled. A cancelled
  call, awaited or through its handle, returns once its turns are over,
  its `cancellation_reason` the reason given, the delivery going on.
- Of what a transport returns, the room keeps, as Python's, its
  `on_event()`'s tasks, observations and metadata updates, its
  `deliver()`'s metadata updates alone, and nothing of a streaming
  target's `deliver_stream()` but its failure (RFC §10.1 step 15,
  RMK-1179). Breaking: a task or an observation a transport returned from
  `deliver()` or `deliver_stream()` is no longer stored; return it from
  `on_event()`.
- A completed verification releases what its sender wrote while the
  request was open, as Python's (RFC §11.7.6, RMK-1173): with the policy's
  `release_on_verify`, the held messages of the request's window, within
  `release_max_messages` and `release_max_age`, get back the scope they
  had, `ON_EVENT_UPDATED` fires for each, and the room's agents answer the
  last one before `complete_verification` returns
  (`roomkit_core::ops::RestoreHeld`, `Inbound::rerun`).
- An AI channel reads its authors' media as Python's (RFC §6.4, RMK-1090):
  it declares text, rich content and media whatever its model sees, so the
  kit hands it the event untranscoded; each image goes after its caption,
  at its place among the message's parts; a model that does not see reads
  `[Image not shown, this model does not see images: “<name>”]`, an image
  past the bound `[Earlier image not shown: “<name>”]`, one that does not
  resolve `[Image unavailable: “<name>”]`, where it read `[Media: …]` or
  lost the image. Inline images are bounded to 5 MiB, as Python's.
- `AiChannelOptions::max_author_images` (default `Some(4)`) and
  `AiChannelOptions::describe_empty_event`, Python's (RFC §6.4, RMK-1166):
  how many of the newest author images a turn sends, and what its
  transcript says of an event that reads as nothing in it.
- `AiChannelOptions::turn_budget_tokens`, Python's (RFC §6.4, RMK-1166):
  past the billed tokens it allows (input, output and cache tokens), the
  loop ends `budget_exceeded` at its next round boundary, asks for no retry
  of an empty answer, and its end marker states the budget; zero is
  refused (`AiError::TurnBudgetTokens`).
- `roomkit_ai::memory::MockMemoryProvider` and
  `roomkit_ai::knowledge::MockKnowledgeSource` are public, Python's
  (RMK-1178), with `MockMemoryProviderOptions` and
  `MockKnowledgeSourceOptions`: what each was asked reads through
  `retrieve_calls()`, `ingest_calls()`, `clear_calls()`, `search_calls()`,
  `index_calls()` and `closed()`, Python's attributes.
- A transport shared into a delegated room is told the agent's answers,
  as Python's (RFC §23.3 step 5, RMK-1158): they are committed as a room's
  responses, through `BEFORE_BROADCAST`, the agent's right to write and
  the room's lane, a stream read by the room's reader, never the task nor
  a re-prompt; the task's output is the answer the room kept
  (`roomkit_core::ops::Inbound::answering`, `TurnResponses`).
- A delegated turn that fails fires `ON_ERROR` in its child room, as
  Python's (RFC §23.3 step 6): `generation` for a turn that raised or
  returned an error, `streaming` for a stream that failed.
- `AiChannelOptions::tool_timeouts`, Python's (RFC §21.6, RMK-1166): a
  bound per tool name, above `tool_timeout` and above a tool's own bound
  (a person's answer), `None` leaving that tool unbounded; a bound of zero
  is refused (`AiError::ToolTimeoutOf`).
- `OpenAiConfig::max_tokens` is an `Option<u32>`, `Some(1024)` by default
  as Python's `max_tokens=1024` (RMK-1177): `None` sends no output bound,
  neither `max_tokens` nor `max_completion_tokens`, and the endpoint
  decides; a turn's own bound still goes. Breaking: write
  `max_tokens: Some(n)` where `max_tokens: n` was.
- Identity verification in the inbound pipeline, Python's step 5b (RFC
  §10.1 step 5b, §11.7.3, §11.7.4, RMK-1172): with a verification policy,
  an identified sender's message is labelled `source.verification`; an
  unverified sender's fires `HookTrigger::OnVerificationRequired`, whose
  hooks block it, pass it (`HookResult::allow_unverified`) or leave it held
  from the room's agents, its transports still reached; an unverified edit
  or delete is refused; a reply that is the code of the sender's open code
  request is attempted and consumed, never stored.
  `RoomKit::sender_verification` reads the sender's verification for a
  hook.
- `roomkit_ai::current_tool_actor_id`, Python's (RFC §10.6.5, RMK-1176):
  who woke the turn a tool's call runs in; a human-input request names
  them as its `actor_id`.
- `ON_TASK_CREATED` is handed, as Python's, an internal `task_created`
  event carrying the source and content of the event the task was made
  for, the task in its metadata (RFC §10.1 step 15, RMK-1167).
- `RoomKit::delegate`'s `require_structured_result` and
  `max_result_retries`, Python's (RFC §23.3, RMK-1158): an inline run's
  worker hands its work back through `submit_result`, served in the child
  room alone, re-prompted when a turn ends without it, the missing payload
  once the retries are spent; `TurnHost::delegate` refuses both.
- `roomkit::ResultTool` and `DelegateOptions::result_tool`, Python's (RFC
  §23.3, RMK-1158): the tool a structured delegation forces in place of
  `submit_result`, its definition as JSON.
- `ChannelDeclaration::takes_owner_tools`, Python's (RFC §10.6.5):
  `setup_background` and `setup_task_tools` refuse a channel whose turns
  take no tool the owner serves (`RoomKitError::Value`).
- A task's name and a quoted text fold the information separators
  (`\x1c` to `\x1f`) as Python's `str.split()` does, so a sender cannot
  start what a reader takes for a line of its own (RFC §6.4).
- The AI loop's triggers, Python's (RFC §6.4, §9.2, RMK-1164):
  `HookTrigger::{BeforeAiGeneration, AfterToolRound, OnAiResponse}`, whose
  payloads are roomkit-ai's `AiGenerationEvent`, `ToolRoundEvent` and
  `AiResponseEvent` (with `DeclaredTool` and `response_transcript`). A
  generation hook's modify is the turn's context, its block runs no
  generation; a round's hook withdraws tools for the rest of the turn and
  adds messages the next round reads; the response reports the turn's
  segments, calls, rounds, end reason, usage and declaration. The kit
  emits `before_ai_generation` and `ai_response`.
- A round the tool loop asks the model again ends its segment, as
  Python's: its text is a message of its own, which the next round's text
  never runs on from (RFC §6.4, RMK-1073).
- `AiChannel::set_tool_handler`, Python's `tool_handler` setter
  (RMK-1091): the host's handler replaced from the next call on, the
  channel's own tools still served; `tool_handler()` answers an owned
  handler.
- `current_tool_call()` in a task a handler spawns with
  `roomkit::keep_context` reads the call, as Python's `create_task`
  copies it (RMK-1076).
- Knowledge sources and the retrieval memory, Python's (RFC §20.2,
  RMK-1165): `roomkit_ai::knowledge`'s `KnowledgeSource` and
  `KnowledgeResult`, and `RetrievalMemory`, which searches its sources for
  the turn's words and hands the best passages on as the turn's note,
  each fenced as knowledge, and indexes each event it learns.
- The inbound pipeline drops a runtime record (`runtime_record`) from a
  sender's message, as Python does: only the runtime says which text it
  wrote (RFC §6.4).
- A quoted text folds the information separators (`\x1c` to `\x1f`) as
  Python's `str.split()` does, so that none starts a line of its own in a
  summary's lines.
- An `InjectMessage` of a role the transcript does not hold reads as the
  user's, and `tool` as a tool's result, where it was dropped (RMK-1175).
- The summarizing and compacting memories, Python's (RFC §20.3, §6.4,
  RMK-1165): `SummarizingMemory` truncates the older events' long bodies,
  then has a model summarize the older events, a prior summary carried
  into the new one; `CompactingMemory` summarizes the events it trims
  past its window. Their summary reads each turn quoted on one line after
  who said it, and stands as a message of the runtime's, fenced; the
  speakers it names count toward labelling the turns. A copy of the
  summary's header in someone's text reads as a copy of a runtime mark.
- `roomkit_ai::tasks::TaskStatusTool` and `CancelTaskTool`, Python's (RFC
  §23.4, RMK-1168): `task_status` and `cancel_task` as an agent's own
  tools, reading the room and the channel of the call they serve.
- `RoomKitError::ToolNameCollision`, Python's `ToolNameCollisionError`
  (RFC §21.1, RMK-1168): `setup_background` and `setup_task_tools` refuse
  a name one of the agent's own tools takes, or one the owner serves it
  already (was `RoomKitError::Value` for the latter), and set up neither.
- `RoomKitError::Runtime`, Python's builtin `RuntimeError` (decided by the
  maintainer, RMK-1168): a setup made twice, a verb a turn's host makes
  after its turn that needs it, a human-input request its handler gave up;
  a `RuntimeError` crossing the turn backend comes back as it.
- A quoted text cut short names the blocks it holds as Python's
  `named_blocks` (RFC §6.4, RMK-1147): an opening tag as written, a closing
  tag as a model reads one, the first end bracket after each.
- Identity verification's lifecycle, Python's (RFC §11.7.1, §11.7.2,
  §11.7.5, RMK-1171): `RoomKit::request_verification`, which returns the
  request and its secret once, `complete_verification`,
  `record_failed_attempt`, `revoke_verification`, `verification_of` and
  `check_verifications`, each identity's transitions serialized;
  `VerificationPolicy` and `RoomKitOptions::verification_policy`;
  `RoomKitError::VerificationRefused`; the `verification_requested`,
  `verification_completed` and `verification_ended` framework events and
  `HookTrigger::OnVerificationCompleted` and `OnVerificationEnded`; the
  timers that end a verification and an unanswered request at their
  expiry, resumed by `RoomKit::start` and stopped by the close. A secret is
  hashed as Python hashes it (scrypt, `N = 2^15`, salted with the request's
  id, `scrypt$<hex>`), so one store serves both: new dependencies `scrypt`
  and `getrandom`.
- Human input, Python's (RFC §9.3, §10.6.5, RMK-1146):
  `roomkit::human_input`'s `HumanInputHandler`, `PendingInput`,
  `PendingInputEvent` and `PendingInputStatus`; `roomkit_ai::
  HumanInputToolHandler`, which an `AiChannel` given it as
  `AiChannelOptions::human_input_handler` serves before its tool handler,
  under its own timeout; `HookTrigger::OnUserInputRequired`, whose block
  rejects a request, and its `user_input_required` framework event;
  `RoomKit::answer_input` and `reject_input`, which reach a turn's request
  through the turn backend's `input_request`, `input_answer`,
  `input_taken` and `input_closed`, an answer that comes first held for
  `TurnTimeouts::input_hold_timeout` (60 s); `RoomKitError::
  HumanInputRejected`.
- A memory's notes and speakers, and the budget-aware memory, Python's
  (RFC §20, RMK-1165): `MemoryResult::notes`, which ride the turn's
  notes after its input, and `MemoryResult::speakers`, which count toward
  labelling the turns; a user text a memory's messages end on joins the
  user message after it. `BudgetAwareMemory`, which trims the oldest
  events to what the window leaves once the turn's footprint is paid for
  (`TurnFootprint`, `current_turn_footprint()`, measured before the memory
  reads the room), and the estimates it counts with (`estimate_tokens`,
  `estimate_message_tokens`, `estimate_event_tokens`,
  `estimate_context_tokens`, `estimate_notes_tokens`, `extract_event_text`,
  `history_budget`).
- A room's turns one after the other, and a message for the running turn,
  Python's (RFC §6.4, §21.3, RMK-1160): `AiChannelOptions::
  serialize_room_turns` and `steer_room_turns`, `Channel::
  serializes_room_turns` and `steers_room_turns`, `ChannelDeclaration`'s
  two fields; a channel's `serializes_room_turns` defaults to its
  `steers_room_turns`, so a channel that steers its room turns serializes
  them and declares them serialized, and `register_channel` refuses one
  that says it steers them but does not serialize them with
  `RoomKitError::Value`, as Python's (RMK-1253). A serialized agent's turn
  in a room takes its place as its delivery set asks it and starts,
  outside the delivery lane, once the turns before it stored what they
  said, on the room as it is then less the messages committed after it
  was asked; a waiting turn whose delivery is
  cut, or whose agent left the room, never starts; a muted agent takes no
  place. With `steer_room_turns`, a message for a room whose line holds the
  agent's one running turn is stored as any message and steered into that
  turn, under its speaker's label when several people speak; no turn of its
  own starts.
- Steering, Python's (RFC §21.3, §10.6.8, RMK-1160): `InjectMessage`,
  `Cancel`, `UpdateSystemPrompt` and `SteeringDirective` in
  `roomkit::model`, in Python's wire form; `AiChannel::steer(directive,
  SteerOptions { loop_id, room_id, turn_id })`, which the turn's loop
  reads at its start, after each round of calls, and once the model has
  answered, a message steered in meanwhile asking it again in the same
  turn, the answer then the message; a `Cancel` also stops a generation
  at its next event and a round's calls not run yet. `RoomKit::steer(
  directive, channel_id, SteerOptions { room_id, turn_id,
  organization_id })` reaches the turns the kit started, through the turn
  backend's `steer` and `steer_result`; a channel without steering takes
  a `Cancel` as its turn's cancel.
- The store's verification records (RFC §11.7, §14.1, RMK-1122), Python's:
  `Verification`, `VerificationRequest` and `VerificationRequestStatus`;
  `ConversationStore::put_verification`, `get_verification`,
  `list_verifications_in_force`, `create_verification_request`,
  `get_verification_request` and `update_verification_request`, which a
  store that keeps none refuses by name, kept by `InMemoryStore` and
  `SqliteStore` in Python's `verifications` and `verification_requests`
  tables.
- Breaking: `AiChannelOptions::temperature` and `AiContext::temperature`
  are an `f64`, 0.7 by default, as Python's `float` (RMK-1089, decided by
  the maintainer): a model that takes no temperature is the provider's to
  know, `OpenAiConfig::supports_custom_temperature`.
- `StatusBus::recent_text(RecentTextOptions { n })`, Python's (RFC §19.8,
  §6.4, RMK-1147): the bus's newest entries as an agent's context reads
  them, the agent and the action as identifiers, the detail quoted. The
  delegation's identifiers keep letters with their marks, composed first,
  as Python's `identifier`.
- `RoomKit::setup_background(agent, SetupBackgroundOptions { worker,
  hand_back })` and `RoomKit::setup_task_tools(agent)`, Python's (RFC
  §23.4, §10.6.5, RMK-1144): `work_in_background`, `task_status` and
  `cancel_task`, served by the room's owner wherever the agent's turns run.
  The turn backend carries a turn's owner tools in its request, their
  calls as `tool_call` and `tool_result`; an `AiChannel`'s turn declares
  them beside its own tools. `start_task` runs on the agent's setup's
  worker. A setup made twice is refused as Python's `RuntimeError`.
- Python's delegation ends (RFC §23.3, RMK-1158): the tasks a delegated
  turn files fire `ON_TASK_CREATED` in its child room;
  `DelegatedTaskResult::exception`, the failure itself (a
  `TaskCutShortError` for a worker cut short), which `TurnHost::wait_task`
  reads beside the result; the kit's close ends each background task its
  runner holds `cancelled`, its observers told, nothing handed back.
- `AiChannel` reads its history through a memory (RFC §20), Python's:
  `roomkit_ai::memory::{MemoryProvider, MemoryResult, SlidingWindowMemory}`
  and `AiChannelOptions::memory`, the default a `SlidingWindowMemory` of
  `max_context_events`. Its window holds the turn's own event, so a turn
  reads one message fewer before its input, and none of the agent's tool
  rows (RMK-1072). A memory's messages come before the history, it learns
  every event the channel is handed but its own, a tool row or an
  instruction, and the channel's close closes it.
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
  partial failure's `total` counts the deliveries and the failures, not an
  agent's turn that did not fail (RMK-1095); a turn whose frames end or
  repeat before its output or in its stream reads as a lost executor, as
  Python's (RMK-1106).
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
