# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- An install whose initial room snapshot fails keeps its claim until `uninstall_strategy` releases it, as Python does (RFC §19.7).

- A returned `PendingInput` and its clones keep observing resolution, rejection and timeout through `status()`, `result()` and `reject_reason()`; `PendingInputOptions` preserves constructor defaults and serialized fields stay flat (RMK-1637).

- Human-input close waits for its notifications to finish; AI channels warn once about human tools never offered to the model, as Python does (RFC §9.3).

- `work_in_background` keeps the turn's agent name and task boundary when stored metadata changes; `start_task` with an empty `hand_back` uses the agent's configured mode, as Python does (RFC §21.4, §23.4; RMK-1568).

- Task results and `task_status` name discussion agents by their handles; tool answers use the turn's room snapshot and leave status entries keyed by channel id (RFC §23.4; RMK-1567).

- A channel's final observers may start during shutdown and are cut before the store is released, without reopening public admission or extending the rights of turns and writes (RFC §12.10.4; RMK-1616).
- Shutdown no longer reports a cut streaming-capability query as a provider failure, or a cut task hand-back as a delivery warning. Completed task results and the recipient's partial answer are retained (RFC §12.10.4; RMK-1616).

- A closing observer drops its own answer still awaiting a cursor claim, so the answer leaves no run suspended after the store closes (RFC §12.10.4; RMK-1616).

- A delegated task's turn no longer overflows a 2 MB stack in a debug build: the kit boxes a call's run, a spawned task and a turn's observer before wrapping them, so each layer moves a pointer rather than the whole future (RMK-1616).

- A queued delivery cut by close returns without waiting on the active set’s cursor read or advancing past an undelivered index (RFC §12.10.4; RMK-1616).
- `roomkit-anthropic` adds the native Messages provider: streamed text, images, signed reasoning, tools, structured answers, prompt caching, model discovery and per-turn credentials. Anthropic's deferred tools keep declarations stable across rounds and later turns; a fallback receives the tools already revealed (RMK-1633, RMK-1306).

- A call whose model writes the intent property without its leading underscore, `roomkit_intent`, has it taken as its intent instead of refused as an unknown argument; a tool that declares that name keeps it (RFC §6.4, `tool_intent::INTENT_ALIAS`). `tool_intent::take_intent` takes whether the tool owns that name.

- Named inbound calls check their organization and spend their current work budget before rate limiting or address callbacks, retaining that charge across adoption and scope reads (RFC §9.5.1, §10.1, RMK-1560, RMK-1616).

- A named inbound call refused at the reentry cap keeps its blocked trace even when its channel is unregistered (RFC §9.5.1, RMK-1616).

- Each turn observer lends its depth only while its own body runs: a task it leaves starts a new chain once that body ends, even while a sibling observer keeps the parent turn alive (RFC §8.3, §9.5.1, RMK-1616).

- A delivery lane retries its completed indexes' failed cursor writes before waiting on another worker, preventing a completed message from being reported as skipped and unblocking the next worker's delivery (RFC §13.5, RMK-1616).

- `DelegateOptions` and `DelegateHandlerOptions` take `max_result_chars`, how much of a worker's output its relayed result carries, 4,000 by default.

- Calls into a room keep one pre-commit deadline across preparatory reads and adoption; a closing kit refuses `deliver` and `record_failed_attempt`, and `cancel_delivery` leaves its close reason intact (RFC §13.6, §12.10.4, RMK-1616).

- A strategy cancelling its own turn leaves no inbound error, as Python does; transport failures remain reported (RFC §10.2, RMK-1614).

- Timer transitions preserve concurrent activity, finish their reports after caller cancellation, and give both hooks the final room context; response passes on a deleted room report its refusal (RFC §5.1, RMK-1608).

- Closing or archiving a room clears its pause timestamp; archiving an open room records its close timestamp, preserving an existing one, as Python does. Shared conformance also checks all room transitions and runs named timer checks with their organization scope (RFC §5.1, RMK-1607).

- rmcp 3.9: RUSTSEC-2026-0462, unbounded buffering of a Streamable HTTP response. The kit's own HTTP client already bounded a response at 4 MiB.
- `OpenAiConfig::thinking_field` names the assistant message field a server reads a model's earlier reasoning from (`reasoning_content` on Z.ai's GLM), as Python's `OpenAIConfig.thinking_field`. Absent by default: strict servers refuse a field they do not know.
- SYNC hooks outside BEFORE_BROADCAST reject injected events with a warning naming the hook and trigger, including verification and turn hooks, while preserving their decision (RFC §9.4, RMK-1604).

- A BEFORE_DELIVER hook that returns injected events now logs their rejection, naming the hook and trigger, as Python does (RFC §9.4, RMK-1603).

- Cancelling a delivery reaches its active turn before the caller resumes; a reentrant delivery keeps its cancellation when adopted, and a cut tool round stays out of the next turn’s history (RFC §10.1, RMK-1549, RMK-1601, RMK-1602).

- Observer hooks outside the inbound driver start in firing order on every runtime, while their bodies still finish independently (RFC §9.2, RMK-1550, RMK-1563).

- A streamed answer reads up to 256 MiB, envelopes included, where it was cut at 4 MiB: a model that wraps each token of its reasoning in some 200 bytes of JSON failed the turn after about 20,000 tokens (`the provider's answer is longer than 4194304 bytes`). An answer read whole keeps its 4 MiB.
- A record replacing an agent answer at the depth limit keeps the trigger's response scope, including `none`, as Python does (RFC §8.5, RMK-1600).

- A read-only, muted agent consumes its stream and audits its answers as `source_read_only`, as RFC §7.5 requires; its turns still run in order (RMK-1600).

- A refused call under a name vendors do not take is not replayed, as Python's (RFC §6.4, RMK-1592): a model's invented call named `multi_tool_use.parallel`, or with an empty name, made every later request of the room a 400 until the round left the memory's window.
- `testing::idle` waits for scheduled status publications and the work their hooks start, so conformance reads no longer race a background worker's terminal status (RMK-1548); the bus's posting and close behavior stay unchanged.
- A hook returning a MODIFY without a payload keeps its tasks, observations and metadata, drops its injections, and follows its fail-open or fail-closed policy; a fail-closed result names `hook_invalid_result`, as Python's (RFC §9.3, RMK-1591).
- A routed first message in a process that follows a discussion another process installed reads that discussion's names, as Python's (RFC §19.7.5 rules 4 and 16, RMK-1476): a `process_inbound` naming no room, which the router sends to a room this process had not read, joined the room's discussion only once its context was read, after the call had taken its hooks, so the agent it named was not asked and the message went to `everyone` or the dispatch policy; the call now fires the room's hooks it lacks once its context read finds this process in the room's discussion, one another call of this process joined meanwhile included, so two such messages taken at once both read names. A `send_event` or `post` into a room the process had not read reads them too.
- A round of tool calls publishes `TOOL_CALL_START` and `TOOL_CALL_END` on the room's realtime, as Python's (RFC §6.4, §8.4, RMK-1119): the start with each call's id, tool, arguments and intent before they run, the end with each result's first 500 characters, its status (`completed` or `failed`), its intent and the round's `duration_ms`, once the round's end rows are stored and handed to its reader; a round cut before then publishes each call's end, its own for the end rows its reader asked past and aborted for the others, so a call whose tool answered once the turn was cut ends aborted, as its stored row reads it cancelled. A round a `Cancel` keeps from running, and a turn whose answers do not reach the room, publish none. They published nothing, so a live surface saw a turn's reasoning and never its tool calls.
- `AiChannel::active_turns` counts the turns the channel runs, each from its first round to its end, a cut or a failure included, as Python's (RMK-1462): it answered 0, the trait's default, so a caller that waited for zero before closing a channel closed it under a turn still running.
- A `SKILL.md` that declares the Agent Skills format's `allowed-tools` (hyphenated, unlike the gating `allowed_tools`) loads, the key kept in `extra_metadata` as written, as Python's (RFC §24.2, RMK-1579): it was refused, and a strict `discover` failed the whole directory.
- The skills' preamble ends with Python's note, "Note: Script execution is not available in this environment.", where it said the skill loader does not execute scripts and asked the model to use the host's tools (RFC §24.3, RMK-1389).
- Under Tool Search, `list_tools` lists `read_stored_result` once a round declares it, as Python's (RFC §21.1, RMK-1485); `find_tools` never names it, as it never names the agent's own tools.
- A summarizing or compacting memory's summarizer reads a person whose name reads like a label of an agent's own turns (`You`, `assistant`) with `(a participant)` after the name, as Python's and as a worker's brief (RFC §6.4, §20.3, RMK-1435).
- **Security:** a copy of a runtime mark a memory's message holds over two adjacent text parts is replaced, as Python's (RFC §6.4, RMK-1574): the run is read back to back, as the model reads it, and made one part when it reads otherwise than each part cleaned alone; each part was cleaned alone, so `[Conver` then `sation summary — earlier messages compacted]` reached a provider that joins the parts as the summary's header.
- The structured copy an `ON_TOOL_CALL` hook leaves is on the call's `TOOL_CALL_END` row, as in Python (RFC §9.3): a surface can render it while the model reads the result alone.
- `AiChannelOptions::tool_call_intents_only`, Python's `tool_call_intents_only` (RFC §6.4): the tools that ask for a call's intent; the others are shown as they are.
- A discussion's turn is cut by what another process stores while it runs, and its process keeps the lease meanwhile, as Python's (RFC §19.7.5 rules 1, 12 and 16, RMK-1233): once a second while a turn runs, the process giving it reads the stored queue; a turn whose agent another process set to listen, or which a configuration installed since no longer holds, is cut as a `Cancel` does, and one whose discussion another process uninstalled is cut with its end recorded nowhere, its process leaving the discussion after it, where each ran to its end. The lease is renewed before it expires, where a turn longer than the lease's 15 seconds let another process give a turn beside it.
- A discussion whose installed configuration another install replaced gives no turn from that process, and a process installs a discussion over the one it installed, as Python's (RFC §19.7.5 rules 1 and 16, RMK-1233): a process whose host installed agents another install replaced routes and queues by the configuration the room stores, tells an instruction reaching it dropped, and gives up the lease and the instructions it held, where it went on giving turns by its old configuration. `install_strategy` with a `Discussion` in a room holding the discussion this process installed takes its place, its queue kept, where it was refused (`holds a strategy already`); a replacing install detaches the agents earlier installs attached that it lets go, where it refused them as intelligence channels the discussion does not hold. A turn under way in a process when another's install replaces its configuration runs to its end when the new configuration keeps its agent.
- `MemoryProvider::compact` and `CompactOptions`, Python's `MemoryProvider.compact` (RFC §6.4, §20.1, HITL cb3751f9, RMK-1533): after each tool round it ran, an AI channel hands its memory the history before the turn's input and the round's usage, and a history it returns stands for the old one in the next requests, the input whole and the earlier rounds' long results stored for `read_stored_result`; `None` by default, which changes nothing. `SummarizingMemory` compacts past `tier2_ratio` of its `max_context_tokens`, every counter of the round counted, by summarizing the older messages; the other memories, `HandoffMemoryProvider` included, hand the call to the memory they wrap.
- A user text a memory's messages end on, before no user message, is a user message of its own, its text alone, as Python's (RFC §20.2, RMK-1533): it kept the memory's message, its provenance included, so a `SummarizingMemory` read a summary set before an assistant's message as a prior summary at its next compaction, where Python reads it as one of the messages.
- An auto-delegating `Supervisor` whose first pass streams reads its two passes as two turns, as Python's (RFC §19.7.3, §10.6.1, §9.2, RMK-1287): the first pass's tool rows carry a correlation id of their own and the second pass's rows and answer another, where both carried one; a first pass that fails is logged once at its own level (a provider's 400 a warning, where it was a debug line) and fires `ON_ERROR` with its rows' id. A worker that raises once the first pass handed on a task (its channel unregistered) fails the supervisor's turn as a turn that raised: `ON_ERROR` `generation` with no correlation id, the supervisor's delivery failed in the call's `delivery_results`, reported (`broadcast_partial_failure`, `delivery_failed`) and recorded on the message before the call returns, and the line "Processing target <supervisor> failed" logged once, where it fired `streaming` with the read's id and no delivery failed. Not yet as Python's: those reports follow `event_processed`, where Python's come before it (RMK-1569).
- In a room held by a discussion, a background task names its agent by the discussion's handle, as Python's (RFC §23.4, §19.7.5 rule 4, RMK-1557): `start_task` writes its worker's brief for `@<handle>` and the `work_in_background` tool's answer says the result comes back to the handle, where both used the channel id (`DelegatedTask::description` read `@dev` where Python's reads `@ops`).
- `SupervisorOptions::async_delivery`, Python's `Supervisor(async_delivery=True)` (RFC §19.7.3, §23.3, RMK-1273): with a `strategy`, a `delegate_workers` call answers at once, `dispatched`, pointing the supervisor at `task_status` only when it was given it, and runs the team in the background; the room is freed and its answer forgotten before the workers' results are handed back to the supervisor, an instruction addressed to it at the depth of the turn that called, each worker's output bounded and set apart as data, so a dispatch in answer to them runs the team again; the run posts one `pipeline` entry as `orchestration`, failed when no worker completed, a step was not validated, the run failed (its message on the bus, never told to the model) or nobody heard the hand-back. A call while the room's team still runs answers `already_running`. The kit's close cuts the run, its workers' tasks ended cancelled and their entries posted before the run's, as Python's. With `auto_delegate` too, the team serves a realtime voice session, which this implementation does not have: the supervisor is not attached, no router asks for it, and nothing delegates.
- `RoomKitOptions::persistence_policy` and `PersistencePolicy`, Python's (RFC §14.3, RMK-1125): the event types the kit's commits store, `persist_types` naming the only ones and `exclude_types` never stored, winning over it. An event of a type kept out is delivered as any event, its hooks and `AFTER_BROADCAST` firing, and stored nowhere: it takes no index, moves none of the room's counters, is none of the call's `response_events`, and a streamed row kept out is handed to no reader of the stream; its author record still joins the room's register of authors. An edit or a delete kept out still writes its target alone, a delete marking its edits deleted, its mutation trigger firing; a lifecycle record kept out is not written, its change and its timer transition announced all the same. A hook's injected event, a record stored blocked and a record `commit_event` commits are stored whatever the policy.
- A streaming channel is handed the turn's end last, as Python's (RFC §6.4, HITL 577ecd24, RMK-1537): a turn that ends on a tool call (cut at its round limit while the model only calls tools) writes its record on the call's end row once its stream ended, so the target read rows that said nothing of how the turn ended and a terminal showed it completed. `deliver_stream` is now handed `StreamItem::LoopEnd(LoopEndMarker)` as the stream's last item, before the response's failure when it fails; the default `deliver_stream` skips it, and `StreamItem` stays `#[non_exhaustive]`.
- `RoomKit::set_room_timers` and `RoomKit::set_agent_response_policy`, as Python's (RFC §5.1, §19.3.1, RMK-1125): each writes the room under its lock and returns it, the timers keeping the room's last activity unless they name one, a policy the room holds leaving it as it is, a room missing or another organization's refused `RoomNotFound`. `RoomUpdates` carries the timers' bounds and the policy for them.
- `RoomKitOptions::inbound_rate_limit`, Python's `inbound_rate_limit` (RFC §10.1, RMK-1125): a bucket of tokens per channel, full at first, refilled at the limit's rate; a message past it is refused before anything is read, written or traced, blocked with the reason `rate_limited`.
- A channel shared into a child room is attached with its parent binding's metadata, as Python's (RFC §23.3 step 3, RMK-1341): it was attached without it and updated after, which wrote an extra `channel_updated` record in the child room and handed the channel's `on_room_attached` a binding without its metadata.
- A person whose name reads like a label of an agent's own turns (`You`, `assistant`) is labelled `(a participant)` in the room's conversation a delegation hands its worker, as Python's and as the discussion's policy reads it (RFC §6.4, §23.3 step 4, RMK-1435).
- **Security:** a `delegate_task` call's task is set apart in `<task>`, under `Your task, as the delegating agent wrote it:`, once a block follows it in its worker's brief, as Python's (RFC §6.4, HITL 90c4e28a, RMK-1418): a task its model wrote could imitate the block after it, such as the room's conversation with a line in someone else's name. A task with no block after it is still the worker's instruction as written. The task of a `work_in_background` call or of `start_task` is set apart the same way, under `Your task, as it was asked:`, before the runtime's text and the room's conversation that follow it (HITL 3737a1a2).
- A generation cut after reasoning alone is tried again, as Python's (RFC §6.4, HITL ea119829, RMK-1542): a reasoning delta counted as an answer under way, so a stream cut after it (a lost connection, a 503) failed the turn with no retry and no fallback. Until its first text, call or end, a failure worth another try is now tried again under `retry_policy`, then the fallback provider is asked, as before anything was handed; a fallback that reasoned before it failed leaves the primary's error. A streaming reader and the thinking subscribers have read the abandoned reasoning, its window closed at the failure; the round replays and reports only the reasoning of the try that answered.
- On a multi-thread runtime, a status bus publishes its posts in the order they were made, a direct change's `ON_EVENT_DELETED` hooks start in the order of the ids it returned, and framework events reach their handlers in the order they were emitted, as Python's (RFC §8.2, §10.3, §19.8, RMK-1529): each post's publication, each observer hook the driver fires (a pipeline's or a direct change's `ASYNC` hooks) and each event's handlers ran as a task of its own that the runtime's workers started in any order, so `recent()` could read a cut worker's `failed` entry before its `pending` one, a projection over the timeline see a reply removed before its root, and a handler read `session_started` before the `hook_timeout` of the hook it waited for. Each of these starts in its order; once started, each runs at its own pace. Not yet for the observer hooks the kit fires outside the driver, `ON_STATUS_POSTED` and `ON_DELIVERY_STATUS` among them (RMK-1550). The shared conformance suite now runs on a four-worker runtime too.
- A tool that changes state starts the turn's repeat counts again, as Python's (RFC §6.4, HITL a3a67086, 3368d06c, 3b16cc44, RMK-1541): `AiTool::changes_state`, false by default, declares a tool whose call changes what later calls read; once such a call is served, the turn's counts of identical calls and identical answers start again, the call's own count kept, so a test rerun after each edit runs where its third run was refused, and the same change made three times is still refused. The refusal of a call repeated with identical arguments now ends "Use the results you already have, or try genuinely different arguments." and the identical-answer note "Use it and move on.": neither asks the model to answer now. BREAKING for code that builds an `AiTool` field by field: it names `changes_state` or ends on `..AiTool::new(..)?`.
- A delivery set the room's lane drops before it ran, the kit closing or another worker having skipped its index, stores what its event's hooks gave at its drop, as Python's (RFC §10.1 step 15, RMK-1514): it was stored at the end of the cascade the set belonged to, and an adopted call's handle answered `kit_closed` before that, so a hook waiting on it read the room's tasks without the one its call's hook gave; the handle now answers once that task is stored and its `ON_TASK_CREATED` observers started. The kit's close also drops at once a set a call holds queued behind another of its sets still delivering, as Python's sealed lane drops every queued entry, the set placed first in an idle lane running from its placement (HITL 0924fad6): its write came after the close's grace, cut, so a transport stuck past the grace lost that set's task.
- **Security:** `SqliteStore::create_identity` of an identity naming an organization refuses `IdentityNotFound` over a stored record of its id the model cannot read, as Python's and as `link_address` and `unlink_address` (RFC §17.2, HITL cc6072ea, RMK-1538): it failed `Value("Identity <id> has a record the model cannot read")`, which told a caller of another organization that the record under that id cannot be read, where a readable record of another organization reads `IdentityNotFound`; nothing is written. An identity of no organization keeps the fixed error.
- **Security:** `SqliteStore`'s and `PostgresStore`'s `link_address` and `unlink_address` with an `organization_id` refuse `IdentityNotFound` for an identity record the model cannot read, as Python's and as `RoomKit::resolve_participant` (RFC §17.2, HITL 7cb63f4f, RMK-1530): `SqliteStore` failed `Value("Identity <id> has a record the model cannot read")`, which told a caller of another organization that the id exists; `PostgresStore`, where only a caller of the identity's own organization read it, now answers the same. Without an organization the fixed error stays.
- `PostgresStore` and `PostgresKnowledgeSource` cancel on the server the statement of a call its caller cuts, as Python's asyncpg cancels it and as `PostgresAdvisoryLockManager` already cancelled a lock's wait (HITL 3ebe4596, RMK-1398): an `update_room` or an `index` dropped while a row lock held it (a task aborted, a `timeout`, the kit's `process_timeout`) went on, wrote once the lock went (`metadata` `cut: true`, `Refund cancelled`), and its connection went back to the pool still busy, so a store of one connection kept its next call waiting behind it; the cut connection's statement is now cancelled and the connection closed, its place in the pool kept until the cancel request is sent, and the pool opens another. The cancel leaves once the call is dropped, so the statement ends a moment later, where Python's caller has its hand back once it ended. For a pool handed in, whose TLS the crate cannot read, a cancel request asks for TLS when the connection's mode asks for it, checking no certificate where asyncpg's checks as its connection does (HITL f281e902), then goes in clear if that fails, never under `sslmode=require`; the lock manager's went in clear only, and a lock's release used no pool's TLS.
- A float whose shortest form has 16 or 17 digits, its value exactly halfway between two of them that read back, is written with the even last digit, as Python's `repr` and `json.dumps` write it (RMK-1525): `-1548782250018927.25` reads `-1548782250018927.2` in the kit's JSON (a model's tool arguments, a memory's replay), not `...927.3`.
- A deferred call's handle waits for the call's own delivery, not for a call its off-lock hook made into its room and did not wait for, as Python's (RFC §9.5.1, RMK-1518): `DeliveryHandle::wait` of a message sent with `defer_delivery` read the end of that call's whole cascade, to the end of its stream, and, once the kit's close cut it, `kit_closed`; `done()` is true once the message's own delivery is over. Not yet for a message an agent answers, or one whose blocking hook leaves tasks or observations (RMK-1519).
- The record of a delivery set with a failure is written under the room's lock as asked of the lock itself, as Python's `_record_failed_deliveries` (RFC §5.13, §14.3, §13.5, RMK-1522): a set a call runs while it holds the room's lock (a keyed publication's section, a call run on a lease), such as the set of an event a keyed publication's locked hook sent into the room, recorded its failure at once inside that section, its lock answered as the section's own, so a later hook of the section read the event recorded; it now waits for the section to let the lock go, as Python's set waits in the room's lane. Not yet as Python's for a call run on a lease, whose record the driver grants at once on that lease once the call lets its hand go (RMK-1526).
- A call's arguments go back to the model, within a turn and replayed, as Python's `json.dumps` writes them (HITL 9f5e61ba, RMK-1508): `{"q": "caf\u00e9", "n": 42}`, `", "` and `": "` between items, every character outside printable ASCII as `\uXXXX`, a float as Python's `repr` (`roomkit_ai::dumps`), on the chat wire and in a Responses replay, where they were compact UTF-8, `{"q":"café","n":42}`.
- A delegated turn's room metadata updates are written on its child room under the room's lock, once its tasks and observations are, and before its answers are committed, as Python's `_broadcast_and_collect` writes them through `_persist_side_effects` (RFC §23.3 step 5, §10.1 step 15, RMK-1521): a call holding the child room's lock meanwhile, a locked `BEFORE_BROADCAST` hook writing the room's metadata, writes first and the turn's updates follow, where the turn wrote them at once, in parallel with its tasks, its observations and its answers, so a key the hook wrote under the lock was left with the hook's value. The stored tasks' `ON_TASK_CREATED` observers, the turns' failures' `ON_ERROR` and the answers' commit follow the writes, in that order, as Python's.
- An adopted call answers once the events that a call nested in its section, or in the section of a pass its answer waits for, injected are committed, and an adopted message held for its sender once its challenge is, an adopted message carrying their failed release, as Python's (RFC §9.5.1, §13.5, RMK-1516): it answered at its commit, or at its hold, with no error, and the event that adopted it carried the failure. Not yet as Python's: a held message waits for its challenge's commit, not its delivery, and one nested under a lock answers at its hold (RMK-1523).
- A call returns once its own cascade is over, though an entry its off-lock hook or its parse adopted still delivers or reads its answer's stream, as Python's (RFC §9.5.1, RMK-1512): a message that hook blocked, or let through without waiting for the call it made, waited for that call's whole cascade, to the end of its stream.
- A delivery set's room metadata updates are written under the room's lock, once its tasks and observations are, as Python's `_write_metadata_updates` (RFC §10.1 step 15, RMK-1511): a call holding the lock meanwhile, a locked `BEFORE_BROADCAST` hook writing the room's metadata, writes first and the set's updates follow, where the set wrote them at once, in parallel with its tasks and observations, so a key the hook wrote under the lock was left with the hook's value. A lock or a release that fails is logged as a failed write, `Failed to write the room metadata updates ['stage']`.
- A rerun (a regeneration, a held message's release) whose room cannot be read once the room's lane takes it logs the failure at ERROR, as Python's lane logs a plan that raised (RFC §15.2, RMK-1513): `Delivery execution failed for room r1 index None` on the `roomkit::lanes` target, where it was a WARN `reading the room of a rerun once its turn came failed; no agent runs`. The call's error and the agents that do not run are unchanged.
- A rerun (a regeneration, a held message's release) whose room cannot be read once the room's lane takes it stores nothing of the agents the chain-depth limit stops, as Python's lane runs none of a failed plan's aftermath (RFC §10.1, §11.7.6, RMK-1502): its set kept those agents not asked, so a rerun past the limit whose room's read failed stored the depth-limit record (`event_chain_depth_limit`) and its observation for an agent that never ran.
- A held message's release logs its agent's failed turn at the failure's own level, as Python's (RFC §15.2, §11.7.6, HITL 36e60a22, RMK-1500): a 404 or a 5xx while the room's agents answer a released message is an ERROR `stream consumption (no targets) of ai1 for room r1 failed (provider=mock, status=404): ...`, as a `send_event`'s, where it was a DEBUG line kept for a caller that receives the error, though the release reads no result.
- A side effect the store fails to write is logged and the call goes on, as Python's (RFC §10.1 step 15, §15.2, HITL bdb62789, RMK-1498): each task and observation is written on its own, one the store fails, or whose write panics, logged at ERROR (`Failed to persist task t1`, `Failed to persist observation o1`) and the others written, and only a task stored fires `ON_TASK_CREATED`; room metadata updates the store fails are logged as `Failed to write the room metadata updates ['owner', 'stage']`, the keys sorted; a room gone meanwhile, whose write Python's store answers `None`, and a write the kit's close cut before it started log a DEBUG line only. The first write that failed was the call's `InboundResult.error`, the tasks and observations after it unwritten and none of the write's tasks announced. A delegated turn's side effects are written and logged so too, where a failed write of its metadata updates was a WARN `a delegated turn's side effects were not stored` without its keys.
- An adopted call's handle waits for the streams its cut left unread, as any deferred call's, as Python's (RFC §10.1, HITL 3c028a3e, RMK-1442): its `cancel` and `wait` wait for their closing, each one's end, `done()` false meanwhile; `cancel` returns `RoomKitError::Timeout` at its `timeout` when that closing does not come, and every wait reads `Timeout` once a turn among them is over without its executor's end; they returned the call as over at once, the closing going on in a task nobody waited for.
- An adopted call cut, by its handle or by the kit's close, while a turn whose stream it read is over without its executor's end reads `RoomKitError::Timeout`, as any deferred call's, as Python's (RFC §10.6.8, HITL 3c028a3e, RMK-1510): its `cancel` returns `Timeout` once that turn's cancel bound passed, well within its own `timeout`, and so does every `wait`; it returned the call as cancelled, or `kit_closed`.
- A call an off-lock hook or a parse adopts answers once the events its own hooks injected, and theirs, are committed, each in a section of its own and apart from the room's delivery lane, as Python's (RFC §9.4, §9.5.1, §13.5, HITL b779fc14, RMK-1443): it answered at its own commit. An adopted message carries the first failed release of those sections as its error, which went on the event that adopted it; an adopted `send_event` or `post` returns its event, the failure its caller's, as before. An entry nested under a lock still answers at its commit, what it injected committed after the section, as Python's.
- A call that ended before its streaming target stopped reading keeps its outcome, as Python's (RFC §10.6.4, §12.2 step 13s, RMK-1509): a target that returns between a round's end rows leaves each call that had ended by then closed `served`, or `refused` for a second call under a held id, where it read `cancelled`. The executor sends each call's end the channel told (`TurnHost::call_ended`) as `tool_ended` before its next item, as Python's executor sends a call's `ended`, not only before a failure and at the turn's end.
- A call that ended before its turn was cut keeps its outcome, as Python's (RFC §10.6.4, §12.2 step 13s, HITL e151b3f4, RMK-1362): its end row reads `served`, or `refused` for a second call under a held id, where every call open at the cut read `cancelled` though `ON_TOOL_CALL` had reported it as it ended. roomkit-ai tells each call's end as it ends (the hidden `TurnHost::call_ended`), the executor sends it as `tool_ended` before the turn's end, and the owner closes the call with it at a cut, a steering cut's too.
- A streaming target that fails once the kit's close or `cancel_task` cut the turn it read fires `ON_ERROR`, as Python's target whose `finally` fails (RFC §12.2 step 13s, HITL 267527a7, RMK-1330): left to end alone, it was only logged, and a cut task's read lost it with the task's work. A failure that is the kit's own close (a call back into the kit it refused) fires none there, as Python's `ended_by_close`; a response's own failure pending at a task's cut fires none, as Python's. The call reads the cut as before.
- BREAKING: an agent keeps the application's instruction its turn read, and its later turns replay it where the turn read it, as Python's (RFC §10.1.1, HITL 5e89048c, 25b07f2a, 4ffcc5e0, RMK-1407): an instruction was the turn's input, then absent from every later turn's history, so the next request no longer opened with the previous one. An `AiChannel`'s turn opened by an instruction that does not stand alone now hands the kit its text first, and the kit stores it as the agent's own row, `EventType::InstructionRead` (`"instruction_read"`), a record no member receives as `commit_event` writes one (no hook reads it, no channel is delivered it, no reader of the stream is handed it, it is no activity and the turn's record never lands on it), visible to the agent alone and never read by another agent's history; its later turns replay it, marked as the application's, before what that turn wrote, the rounds of a turn whose instruction opened the room included. `list_events()` returns the row and the room's indexes and `event_count` count it; a turn's stream carries it across the turn backend (`instruction_read`), so owner and executors run the same version. An emergency compaction's summary quotes it as `[application]`, never as a user.
- BREAKING: `BudgetAwareMemory`, `CompactingMemory` and `SummarizingMemory` charge each replayed round of tool calls, and each replayed instruction, to the history event it reads with, and keep or cut a turn whole, as Python's `history_event_costs` (RFC §20, HITL 4ffcc5e0, RMK-1370): `TurnFootprint::replayed`, `(index, turn, tokens)` per piece, no longer in `input_tokens`. Reserved up front, the rounds cut the very messages they followed: a 16,000-token window kept 2 questions of 12. `TurnFootprint` is no longer `Copy`.
- BREAKING: a later turn replays each earlier round of tool calls as its turn sent it, as Python's (RFC §6.4, HITL 093113a1, 3c7ef9a2, RMK-1370): a turn's history replayed the last 8 calls, the 3 most recent whole up to 6,000 characters and the older ones as a 120-character preview, without refusals, calls nothing served nor the channel's own tools (`find_tools`, `activate_skill`, `read_stored_result`), a reused id renamed (`c1_5`), and a round's text as an assistant message apart from its calls: each new call rewrote the history, and a server that caches a prompt's prefix reprocessed it at every turn's first request. Every round the history holds now reads whole, each call its turn ended under the id the model gave it, with the result it read, but one cancelled before it answered, and a round's text and calls as the one assistant message the turn sent: a turn's first request opens with the previous turn's last one, then its answer. A turn reads every row of the channel's calls from the room's message before its history's first on, page by page, beside the latest 60 its tool memory and its skills' activations read; `roomkit_core::rules::transcript::conversation` and `conversation_with_memory` take no `replayed` predicate, and `replayed_rounds` is gone, replaced by `earlier_turns`, which returns a `Vec<Replayed>`: each round of tool calls and each instruction a turn read, with its index and its turn.
- An event's AFTER_BROADCAST hooks read stored the tasks and observations its BEFORE_BROADCAST hooks gave and its delivery set's targets returned, the observation the set makes of an agent it did not ask at the chain-depth limit (stored even when a closed room refuses that agent's record, as Python's), and the room metadata updates its targets returned, as Python's (RFC §10.1 steps 15 and 16, RMK-1454): each set writes them once it is over, then its tasks' `ON_TASK_CREATED` observers start, then its event's `AFTER_BROADCAST` hooks, where the whole call's were written once its cascade was over, after every `AFTER_BROADCAST`; a hook calling `list_tasks` from `AFTER_BROADCAST` read `[]`. What no set stores, a blocked record's, an event's whose set delivered nothing and a discussion's stopped turn's observation, is still written at the cascade's end.
- **Security:** `RoomKit::resolve_participant` with an `organization_id` refuses `IdentityNotFound` for an identity record the model cannot read, as Python's (RFC §17.2, HITL 3f36d6c8, RMK-1504): it failed `Value("Identity <id> has a record the model cannot read")`, which told a caller of another organization that the id exists. Without an organization the fixed error stays.
- `PostgresStore` reads a JSONB column that holds a JSON string as the value its text holds, as Python's store reads it (HITL f9737c41, RMK-1503): the rooms', events', bindings', participants', identities', tasks' and observations' columns and the verifications' records, where a row written so by hand failed its read (`Unavailable`, `Value` for an identity, an event's source read empty); `link_address` and `unlink_address` change such an identity's addresses and write them as an object. A string holding no JSON text fails the read, as Python's. A merge that keeps what the model does not read (a room's `timers`, a binding's `capabilities`, `rate_limit` and `retry_policy`, a verification's record) merges into the value such a string holds, where it wrote the string and the object as one array, which neither implementation reads; over a string holding no JSON text it fails, where Python's write replaces it. An identity's replacement keeps the members the string holds that the model does not read; `update_event` reads such a string's content and delivery results, and takes one holding no JSON text for none. A room's private state is read once, as Python's.
- `CompactingMemory` and `SummarizingMemory` leave the turn its room in the window, as Python's (RFC §20, HITL 7dc490cd, RMK-450): they sized the history against the whole `max_context_tokens`, the model's window, and now read the turn's footprint (`current_turn_footprint()`) as `BudgetAwareMemory` does. With a 32,000-token window, a 12,000-token system prompt, 8,000 tokens of tools and a 6,000-token reply, the request and its reply fit the window, unless the latest events a memory always keeps do not. `CompactingMemory` keeps the events that fit the window less the measured input, the current event, the inner memory's messages and notes and the reply (the larger of its safety margin and the turn's `max_tokens`), less the summary's share; `SummarizingMemory`'s tiers are fractions of the window less the measured input, the turn's `max_tokens` and the current event, and its last cut keeps the history within that.
- **Security:** a tag's separator in another form no longer closes its block, as Python's (RFC §6.4, HITL 6004cc75, eb4462f6, RMK-1320): `fence` read a name's letters in every form but its separator only as written, so `</project‐instructions>` (U+2010), `</project_instructions>` or `</projectinstructions>` in a text fenced in `project-instructions` was kept as written, which a model can read as the block's end. Where a name holds a separator between two letters, any run of dashes (Unicode 16.0's Dash_Punctuation, U+FE31 aside, which reads as `l`; the minus sign; what reads as a dash or a minus, such as U+2043 or U+30FC), underscores (the double low line among them) and spaces in any of their forms, or none, is now read as it; a character that reads as several letters is read across a separator left out (`</previouﬆasks>`); an honest mention of such a form gets the underscore too (`</serverinstructions_>`).
- A regeneration's refusal carries the room lock's failed release as its `error`, as Python's (RFC §13.5, RMK-1443): refused `room_closed` at its choice or in the room's lane, or `trigger_moved`, where it was left out. `process_timeout` bounds a regeneration's read of the room under the lock too, and a held message's release's, as Python's (RFC §13.6, §11.7.6): past it a regeneration is refused `process_timeout`, a release's `process_timeout` names `regenerate`, no agent runs and the lock is released, where the read went on unbounded.
- A turn's compaction stores the long results of its older rounds, as Python's (RFC §6.4 "A turn compacted", §21.5, RMK-1228): each result over 2000 characters of the rounds between the turn's input and the half reaches the model as a placeholder previewing it in 1000 characters, its whole text stored for `read_stored_result`; a skill's instructions, a page read back and a result already stored stay as they are, and a result of parts keeps its images. A turn with nothing before its input compacts when it stores a result and its call is replayed, where the compaction was refused (`nothing left to compact before the turn's input`) and the turn failed.
- A tool's outcome past `AiChannelOptions::evict_threshold_tokens` (5000 estimated tokens by default) reaches the model as a placeholder previewing its first and last lines, its whole text stored in the room for `read_stored_result` to page through or search, as Python's (RFC §21.5, RMK-1228): a result, a refusal, a failure, a hook's block or override, the text of a result of parts (its images kept); never a skill's instructions. The hooks and observers read the whole outcome, the model and the end row the placeholder. `read_stored_result` is declared last from a turn's first round whenever it declares a tool or its room stores a result, escapes the tool policy, is never hidden, and refuses an id its room does not hold; a generation hook sees it once the room stores a result, and may withdraw it. A host's tool of that name is refused, as a skill tool's is.
- A failed turn logs one line, as Python's `log_failure` (RFC §15.2, §15.3, HITL 99aa8bd9, RMK-1125): a stream's failure as `streaming delivery of ai1 to ws1 for room r1 failed (provider=mock, status=503): mock (503): model gone`, or `stream consumption (no targets) of ...` with no streaming target, at ERROR for a 404 or a 5xx, WARN for another provider error, WARN `... failed: <error>` for a turn cut short, ERROR for any other failure; a provider's error at DEBUG when the call's caller still waits for the error with no streaming target (a waiting `process_inbound`, a regeneration), and for a delegated room's reader, rendered or not. A delegated task's failure is logged as `Inline task <id> failed ...` or `Task <id> failed ...`, and another agent's in its child room as `In <room>, b's turn failed ...` (or `another agent's turn` when whose it was cannot be told), by the same rule. A stream's failure was a WARN `the streaming channel failed on a response that failed` whatever its status, its streaming channel and its caller; a delegated task's a WARN `a delegated task failed` without its status; another agent's buffered failure was not logged. A streaming channel raising another error than the response's failure has it logged as `streaming channel ws1 handling the failed response of ai1 in room r1 failed`.
- A call the room's work adopts has a `process_timeout` of its own and follows its caller, as Python's (RFC §9.5.1, §13.6, HITL 35d1efa7, RMK-1472): one a hook's task makes beside the event's check is refused at its own deadline and reports its `process_timeout`, where it ended unreported at the event's, and commits when its hook lets it before then; one a later pass's hook makes, after the event's decision, is bounded too, where it waited for its hooks' timeouts; one a hook awaits ends with the hook's body before its commit, nothing stored, at the hook's own timeout and when the event's caller leaves, where it went on and committed. The event is refused at its deadline while a call its hook's task made holds the room's lock, where it waited for that call's end; a call a locked hook's task nests under the lock still holds the event back, at most until the event's deadline, and is refused with it, unreported. The event is answered at its deadline too when it passes in its off-lock check while an event its call injected holds the room's lock, where it waited for that event's pass.
- `RoomKit::regenerate_response` and `RoomKit::regenerate_target`, as Python's (RFC §13.6, RMK-1267): the room's agents answer its last message again, chosen under the room's lock, without storing it again or handing it to a transport; `RegenerateResponseOptions::trigger_id` refuses a choice that moved `trigger_moved`, and a room whose status refuses new events is refused `room_closed` first. In a room a discussion holds the call fails `RoomKitError::Value` until stream I ports it (HITL 785a43bb, RMK-1474). A regeneration's turn, and the turn of a held message once released, reads the messages stored after its message, up to the room as the regeneration or the release read it under the lock, the message last, as Python's context does (they read only up to their message). A turn on the room's first message reads no message committed after its pass, as a turn on a later message already did, where it read every message since.
- BREAKING: `FrameworkEvent::RoomRefusedEvent`'s `event_id` and `event_type` are `Option`s, as Python's: a regeneration refused in a room that holds no message to answer names none.
- A `<think>` an answer writes stays in the answer, as Python's (RFC §6.7, HITL 9cf6483f, 241215eb, RMK-1327): on Chat Completions only a block that opens a generation, after nothing but spaces (U+0020), tabs, line feeds and carriage returns, is reasoning, one per generation (each round of a tool loop is one), where a `<think>` anywhere began reasoning: `The parser reads the <think> tag. Everything after is lost.` was stored, delivered and sent back as `The parser reads the `.
- **Security:** `PostgresStore` quotes no identity row the model cannot read, as Python's and as `SqliteStore` (HITL 70fef50d, RMK-1431): `get_identity` fails `StoreError::Value("Identity <id> has a record the model cannot read")`, where a JSONB column of another shape failed `Unavailable` quoting it; `link_address` and `unlink_address` read the whole row and refuse such a one so, where they took some and failed on others; `resolve_identity` returns `None` and logs a warning naming the id as the row holds it, an empty one included, where it failed. A member of an identity's addresses under a key that is no channel type's name must be a list of strings, as Python's model reads it, on `SqliteStore` too, where it was left out unread; `PostgresStore::create_identity` drops another, so that a replacement repairs the row, as Python's. `SqliteStore` logs its warning of such a record under `roomkit.store.sqlite`, Python's logger, where it used `roomkit::store::sqlite`. `RoomKit::resolve_participant` checks the room's organization and the participant before it tells an identity's read failed, as Python: a caller of another organization, or naming a participant the room lacks, reads `RoomNotFound` or `ParticipantNotFound`, where it read that the identity exists and cannot be read.
- The ACP agent channel, as Python's (RFC §6.4, HITL 1bd56827, RMK-1440): `roomkit_acp::AcpChannel`, an external coding agent as a room's intelligence channel, spawned (`command`) or reached through an `AcpTransport` of the host's, one connection per channel and one session per room (its room and scope `room` in `_meta`), each message prompted with its event's id, the agent's text streamed back as the turn's answer, the turn's record (`acp.protocol_version`, `prompt_returned`, `stop_reason`, `interrupted`) on the result and the message, a failed prompt the `acp` provider's error, ON_AI_RESPONSE with the agent's counters unaltered, and `acp_event_text`; `Channel::active_turns`, 0 by default. A turn's end under `turns` reads an `acp` record, as Python's `recorded_turn_end`. An instruction is prompted under the runtime's mark (a standalone one, in a session of its own, comes with lot 8); outside a kit the turn is read whole (HITL c2e5eb96); `info` reports `sdk_version` null (HITL c7b3c7a3). Its tools, thinking, permissions, session setup, catch-up, discussion and the usage's provenance come with stream L's next lots.
- BREAKING: `AiResponseEvent::loop_end_reason` is an `Option<String>`, Python's `LoopEndReason | str`, so that an ACP turn's end reaches ON_AI_RESPONSE as its record names it (`max_tokens`, `refusal`), as Python's (HITL e680d98d, RMK-1440): an AI channel writes its `LoopEndReason` as text, `Some("completed")` where it wrote `Some(LoopEndReason::Completed)`.
- A discussion served by several processes (RFC §19.7.5 rule 16), as Python's: a process that did not install the discussion its room stores follows it on each context it builds, and before a `process_inbound` naming the room (HITL c2f6fe5e, RMK-1476 for a routed message): it asks no agent at broadcast, reads names and queues by the stored configuration, gives no turn and tells an instruction dropped, and leaves it once the room stores none; an install there takes it over, its queue kept; `uninstall_strategy` from it uninstalls the discussion for every process, the agents its installs attached, now stored with it, detached; `claim_room_strategy` reads the room first, and refuses another strategy a room holding a discussion; `RoomKit::room_strategy_is_current` (`RoomStrategyIsCurrentOptions`, the kind a `TypeId`, HITL c25287ea), an uninstall another process made left at once; `Orchestration` is `Any`, to tell a `Discussion`; `testing::idle` rouses each discussion's driver first (RMK-1233).
- A response schema on the Responses API (RFC §6.7, HITL 78f2cb96, RMK-1433): sent strictly as the request's `text.format`, with Chat Completions' defaults, the answer checked before its end as on Chat (a refusal part fails `refusal`, an answer cut for `max_output_tokens` `truncated`, one a content filter stopped `refusal`, prose `invalid_json`). A refusal part still reads as the answer's text, and the end now names it on `metadata["refusal"]`, as Chat's does. `examples/ai_response_schema.rs` runs the triage on Responses too.
- A message holding both a call and its result renders on Chat Completions as the assistant's round that called, its result left out, as Python's (HITL 9c8d354c, RMK-1408), where it rendered the tool message and lost the call; a call round is the assistant's whatever the message's role, and carries a dialect's reasoning field only when the assistant wrote it. The Responses request, built from the same messages, sends such a round's `function_call` where it sent the result, and a call in another role's message, which it lost or refused, as the assistant's.
- BREAKING: a provider's refusal before any request marks no author image refused, as Python's (RFC §6.4, HITL fb46b635, d91b8137, RMK-1436): `ProviderError::before_request` and `ProviderErrorOptions::before_request` (Python's `before_request=False`), set by an unsupported `ResponseSchemaError`, a response schema outside the portable subset, OpenAI's refusal of a model or of tools its endpoint does not serve, and a tool name a vendor refuses; only the error raised is read, not one it was raised from (a schema outside the subset is still found anywhere in the chain, HITL 17bfa822). A turn's error record carries it among the error's attributes. A `ProviderErrorOptions` written field by field takes the new field.
- BREAKING: a DeepSeek usage count the server leaves out stays unknown, as Python's (RFC §6.4, HITL e2e71208, RMK-1441): `{"completion_tokens": 50, "prompt_cache_hit_tokens": 600}` reads `{"output_tokens": 50, "cache_read_input_tokens": 600}`, where it read `input_tokens: 0`; an omitted completion reads no `output_tokens`, where it read 0.
- A call an event's admission adopted, not committed when the event's deadline passes, ends with the work that made it, as Python's (RFC §9.5.1, §13.6, RMK-1443): the event is refused `process_timeout`, the call's hook dropped, nothing of the call stored and only the event's `process_timeout` reported, deferred, awaited, nested under a locked hook or made from the message's parse, a record past the reentry budget waiting for the room lock among them, where it went on, its hooks running to their own timeouts, and committed. A call a hook's task makes and the hook never awaits ends there too, its caller refused `RoomKitError::ProcessTimeout`. A call committed by then goes on, as before, and the event's caller gone before its deadline cuts none of them, as before.
- The client side of the Agent Client Protocol, as Python's (RFC §6.4, HITL 0e261e51, 3592a76c, 3f858b12, RMK-1437): the new crate `roomkit-acp`, its types the `agent-client-protocol-schema` crate's, with `AcpTransport` and `StdioAcpTransport` (an agent spawned without a shell, its environment the Python SDK's defaults plus `env` and the `inherit_env` names, stopped as the SDK stops it, killed when dropped), `ClientSideConnection`, JSON-RPC 2.0 over a byte stream, `AcpConnection`, `AcpClient` and `AcpError`; the agent's file system, terminal, elicitation and extension requests are refused `method_not_found`. The channel comes next.
- A call's cut closes each stream its turns answered that no read took and waits for its end, as Python's cascade cleanup does (RFC §10.1, §10.6.8, RMK-1442): with an executor that never ends that turn, `DeliveryHandle::cancel` returns `RoomKitError::Timeout` at its `timeout` and the handle is not `done`, a later `cancel` times out the same and `wait` waits, and an awaited call cut by its key waits too, where each returned the call as over and the stream was closed only with the run, unwaited.
- An answer streams to the first transport the room lists among those it reaches that stream, as Python's (RFC §12.2 step 11s, RMK-1125): with two streaming transports bound `st2` then `st1`, it streamed to `st1`, the first by its id, and delivered `st2` the answer whole.
- A call's result carries a room lock's failed release as its `error`, as Python's (RFC §13.5, RMK-1443): a call refused `process_timeout` under the lock, awaited, and deferred at its decision and on its handle's wait; and a message whose step 2 attached its channel under a lock whose release failed, committed or refused, a call an event's admission adopted among them, committed or held for its sender, where the error was `None`.
- A Chat Completions answer names its model on `metadata["model"]`, the one the server says answered, else the one the configuration asked for, whole and at a stream's end, as Python's `answered_by` (RMK-1434). A round goes back to the model without it, nor its refusal, as Python's round message carries no metadata: a generation hook read them on the round's message, where Python's reads none.
- A Chat completion with no choice is an empty answer that reports its usage, as Python's (RMK-1434), where it failed `Malformed` and its usage was lost. A stream answered with such a completion's JSON in place of its events still fails.
- A streamed call id that is no text reads as Python's (RMK-1372): a nameless fragment opening another call under an id read as false (`0`, `false`, `[]`, `{}`) is a call with no name, for the loop to refuse, where it ran as a second call to the held tool; any id that is no text on a call named under no id of the server's, or a number or `true` on an entry with no function, changes nothing, where the read failed `Malformed`.
- A provider's refusal of a response schema outside the portable subset marks no author image refused and forgets none of the room's replayed tool images, as Python's, which refuses such a schema where it is written (HITL 17bfa822, RMK-1436): a `BEFORE_AI_GENERATION` hook writing `{"type": "string"}` on a turn whose first request would have carried a person's images left them read as refused by the provider in the room's later turns.
- The owner's tools of a turn are declared in the order the owner set them up, as Python's (RFC §10.6.5, RMK-1358): a tool whose call waits by design (`delegate_task`) was declared after the bounded ones (`work_in_background`) whatever their order.
- A call an event's admission adopted outlives the event's deadline, as Python's (RFC §9.5.1, §13.6, RMK-1432): the event is refused `process_timeout` on time, whether its deadline passes in its off-lock check or under the room's lock, and the adopted call's answer, its stream still read or its turn still to answer, is stored and its handle reads it, where the answer was refused `cascade_stopped` or dropped and the refused call waited for it. The refused call's delivery key names no delivery once it returned, and a deferred one's handle waits for nothing.
- A deferred call cut while a turn whose stream it read is over for its owner without its executor's end times out, as Python's (RFC §10.6.8, RMK-1328): its cancel bound passed or its link failed, `DeliveryHandle::cancel` and every `wait` after it return `RoomKitError::Timeout`, cut by a cancel, its key or the kit's close, where they returned the call as over. An awaited call, and a turn cut before its output, read their result as before.
- A Chat Completions usage counted as Python's (RMK-1427): its `reasoning_tokens` (`completion_tokens_details`), a detail of the output, on OpenAI's shape and DeepSeek's; its cache writes (`prompt_tokens_details.cache_write_tokens`, else a top-level `cache_write_tokens`) as `cache_creation_input_tokens`, taken off `input_tokens`. A counter of another shape there is left unread. A count the server leaves out stays unknown (RMK-1029).
- `PostgresStore`, `PostgresAdvisoryLockManager` and `PostgresKnowledgeSource` read a connection string, and what it leaves out, as Python's asyncpg (HITL fa9dd934, RMK-1438): with no host anywhere, the Unix sockets of `/run/postgresql`, `/var/run/postgresql`, `/tmp` and `/private/tmp`, then `localhost`, where only `localhost` was tried; a host list in the netloc, the `host` and `port` parameters, or `PGHOST` and `PGPORT`, tried in its order, a host that cannot be reached or refuses TLS passed for the next and a server's refusal or a connection lost ending the list (one lost on the TLS request too, which asyncpg passes, HITL 22f38d15), where `?host=h1,h2` and `PGHOST=h1,h2` named one host and `PGPORT` was not read beside a host the netloc names; with no password, the first line of `~/.pgpass`, or of the file `PGPASSFILE` or `passfile` names, that matches, read at each new connection, a file the group or others can access warned of and not read, an empty `PGPASSWORD` a password; the user's name from `LOGNAME`, `USER`, `LNAME` or `USERNAME` after `PGUSER`, in Python's order, else the account database's (HITL 7b637a02); asyncpg's reading of the file kept, its escapes and its refusal of a short line included (HITL 8142aaf2). A connection is given 60 s whichever hosts it tries, a server that accepts and never answers included, where one hung. The string is cut as Python's `urlsplit` and `parse_qs` cut it: a `#` ends it, `+` in a parameter is a space and its name's escapes are read, a parameter with no value is left out and one with no `=`, or an empty field, refused, a bracket that holds no IPv6 address at the start of the host part refused, a raw `/` in a password ends the netloc there. `whoami`, already built for tokio-postgres, becomes a dependency of `roomkit-postgres`.
- BREAKING: `hostaddr` in a connection string goes to the server as a setting, which refuses it, as Python's asyncpg sends it (HITL ddc33146, RMK-1438): `postgresql://u:pw@/db?hostaddr=127.0.0.1` connected to that address, where it now fails `init`; beside a socket's directory named as the host (`?host=/run/postgresql&hostaddr=…`), it had the connection go over TCP to that address with no TLS whatever `sslmode` said, its authentication included (the password itself under cleartext authentication).
- BREAKING: the parameters asyncpg does not read go to the server as settings, as Python's asyncpg sends them (HITL 22f38d15, RMK-1456): `connect_timeout`, `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_retries`, `channel_binding` and `load_balance_hosts`, which the client read, now fail `init` with the server's `unrecognized configuration parameter`, and `tcp_user_timeout` is the server's own setting of the session, on the server's socket and in milliseconds, where the client read it in seconds for its own socket (`tcp_user_timeout=30`, 30 s then, is 30 ms now). As asyncpg's, the connection asks no TCP keepalive, where the client's default asked one after two hours.
- BREAKING: a message never commits into a room whose binding, made while the room was created for it, names someone else, as Python's (RFC §10.1 step 2, HITL 9a2e6d19, ef0ef858, RMK-1396): once its `ON_ROOM_CREATED` hooks are over, a room step 2 created for a routed message is held to the check of any routed room, so a sender recorded there meanwhile (a custom router answering one missing id, or step 3 once a hook attached the channel) keeps the room and the first message is routed once more. A hook's binding naming no one records the sender, or takes the name `system` for a message with no address; when no room opened for a message is still its own, it is refused `room_taken`, nothing stored. A room the caller names is unchanged.
- A call an event's admission adopts (an off-lock hook's body, a parse) runs its whole cascade ahead of the event, as Python's (RFC §9.5.1, RMK-1322, RMK-1329): its handle's wait, from that work or later, reads its turns' ends and its answers, which commit before the event when that work waits, where it read only its delivery set's report; its handle's cancel and its delivery key cut its turns until that cascade is over, the result reading the reason, where they did nothing once its set ran; and one the kit's close finds under way reads `kit_closed`, as the call that adopted it.
- A `PostgresStore` operation that panics ends its `store.<operation>` span with `error`, as Python's `_query_span` ends one on any exception, where the span stayed open; one its caller drops unfinished, or panics beside, stays as Python leaves a cancelled one's (HITL 69e3f386, RMK-1399).
- **Security:** `SqliteStore` quotes no identity record the model cannot read, as Python's (HITL 4ee4d298, 47b7dfc6, 2e66d6ef, RMK-1410): `resolve_identity` of an address whose identity's record is unreadable, or no UTF-8, returns `None` and logs a warning naming the id alone, where it failed `Unavailable` quoting the record's error; `get_identity`, `create_identity`, `link_address` and `unlink_address` fail `StoreError::Value("Identity <id> has a record the model cannot read")`, Python's `ValueError`, and the kit's `resolve_participant` `RoomKitError::Value` with the same text, where they quoted it; an address such a holder keeps stays refused, `AddressLinked`, also when its record is no UTF-8, where that one failed `Unavailable`. `tracing` becomes a dependency of `roomkit-sqlite`.
- A discussion's dispatch policy (RFC §19.7.5 rule 18), as Python's (HITL 7a38cb9c): `DispatchPolicy`, `DispatchCandidate`, `DispatchTurn`, `DispatchDecision`, `DispatchDecisionEvent`, `MockDispatchPolicy`, `HookTrigger::OnDispatchDecision`, `DiscussionOptions::dispatch` and `dispatch_timeout`: a person's message that names no one waits for the policy, decided off the room's lock before the next turn, the agents picked queued at its place, a failure or a decision past its bound asking what the message asks with no policy (RMK-1233).
- Response schemas, as Python's (RFC §6.7, Appendix A.9, RMK-1355): `AiContext::response_schema`, `check_portable_schema` and `schema_mismatch` (Python's portable subset and words), `ResponseSchemaError` and its `ResponseSchemaFailure`, the provider's `supports_response_schema` and `supports_response_schema_with_tools`, `MockAiProviderOptions::response_schema` and `response_schema_with_tools`; `AiChannelOptions`, `AiChannelTurnConfig` and a binding's `response_schema`, the answer's text held until checked and a loop stopped short failing `truncated`; on Chat Completions a strict `json_schema` response format, the answer and the stream checked, `OpenAiConfig` and the three vendors' `supports_response_schema` and `supports_response_schema_with_tools` (on for OpenAI and Kimi, off for DeepSeek and MiniMax), and a Chat answer's refusal, streamed or not, on its end's `metadata["refusal"]`. A generation hook's schema is the turn's. The Responses API carries one as its `text.format` (HITL 78f2cb96, RMK-1433, above).
- The DeepSeek, MiniMax and Kimi providers, as Python's (RFC §6.4, HITL 59acce09, RMK-1409, RMK-1361): `roomkit_openai::deepseek::DeepSeekAiProvider`, `minimax::MiniMaxAiProvider` and `kimi::KimiAiProvider`, with their configs, on the vendor's API or behind a base URL; nothing on `OpenAiConfig`. A round's reasoning goes back in `reasoning_content` for as long as a turn lasts, empty on DeepSeek and a single space on Kimi for a round with none; DeepSeek's `thinking` object and cache counters; MiniMax's and Kimi's floors and levels; their catalogues, Python's.
- The `delegate_task` tool, as Python's (RFC §23.4, RMK-1209): `roomkit_ai::tasks::{setup_delegation, build_delegate_tool, DELEGATE_TOOL}` and `roomkit::tasks::{DelegateHandler, CompletedTaskCache}`. A call's `context` and the agent's own previous tasks reach the worker quoted as data; `max_conversation`, 0 by default, gives the tool a `conversation` argument, held to it, what both the calling agent and the notified channel may read. A channel that may not read is handed no conversation by `delegate` either, and U+2028 and U+2029 between a tag's letters are neutralised in every block (RMK-1363).
- A Discussion's host calls and memory (RFC §19.7.5), as Python's: `RoomKit::listen_only` and `talk_again` (`ListenOnlyOptions`, `TalkAgainOptions`), the turn of an agent set to listen cut as a `Cancel` does, or its delivery cancelled when it has not started; each message handed to the memory of each agent it reaches but its author, through the new `Channel::on_room_event` and the turn backend's `committed_event`; an agent's streamed silence never handed to the streaming target, a start that could have been it held back to 256 characters; an install telling each instruction it drops from a stored queue (RMK-1233).
- BREAKING: routing reads a binding's recipient as the channel answers it, as Python's (RFC §5.7, §10.4, HITL e7dc9966, RMK-1129): its `recipient_address`, else the value under its `recipient_key`, at step 3, under the lock, on an attach and a metadata update, on a hook's binding in a room opened with no address and for a delivery status, where a channel reading its recipient under a key it does not declare was ignored and a stranger entered its room. The record names that recipient. The hidden `Channel::address_config` is gone, a kit's channels writing their addresses by their own methods, and `AddressForm` gains `answered_recipient`.
- An answer's text published as it is written (RFC §6.4), as Python's: `AiChannelOptions::publish_text_deltas`, `text_coalesce_ms` and `text_coalesce_chars`, one `TEXT_DELTA` per window, for a turn read as a delegated task's trace only: a turn `BEFORE_BROADCAST` judges publishes none (RMK-1360).
- BREAKING: a message routed under the name `system`, or a chat of that name, is refused, as Python's (RFC §10.1 step 2, HITL 6200e78d, RMK-1339): `blocked`, reason `reserved_sender`, nothing written, no room opened, no router asked. Under the room lock the name spared a sender on a channel whose senders choose their id step 3's conditions. A host writing under that name names its room.
- BREAKING: a routed message with no address enters a room only through a `group` binding, and the room it opens admits no other sender, as Python's (RFC §5.7, §10.4, HITL 8ea30e4a, 474148bb, RMK-1339): its binding names `system`, which no sender is, until the integrator gives it a recipient, where unnamed it passed step 3 and the next sender's agent read what it left.
- **BREAKING:** a model's reasoning reaches no Chat Completions request, as Python's (RFC §6.4, RMK-1369): a `<think>` block in the content emptied a server's prompt cache at each step of a turn. `AiThinkingPart`, the hooks and the reader of a stream still read the reasoning, a `<think>` in a model's output is still read out of its text, and Responses still replays its reasoning items.
- A stream nobody read opens no `llm.generate`, as Python's: a muted agent's, or one a cut left unread, where the span opened with the response and ended `cancelled` under `framework.broadcast` (HITL 0baec919, RMK-1384).
- A replay gives each call its own result, as Python's (RFC §6.4): a round's results pair to its calls first in, first out per id, so a later turn no longer reads a refused call with the result of the one that held its id (RMK-1373).
- A turn that wrote no message records on its last row, as Python's: its record rides its last message, else its last row, the call's end, a strategy's first pass handing it on (RMK-1344).
- A cut stores what its turn leaves at once and gives the room's line up: only its caller waits for a streaming target still working past the cut, so a target stuck there no longer holds the next message of an agent that serializes or steers its room's turns. Python's blocked `finally` holds it still, the target's defect. Unlike Python's, a target that fails once it flushed leaves what the cut stored, as a cut's, its failure its caller's error, and a call's end may reach it while it flushes (RMK-1343, HITL d406164d, fcaf1ecd).
- An address held by a row no read serves is free, as Python's (HITL 2bd8fc23, RMK-1376): `PostgresStore` and `SqliteStore` refused an address for a row whose identity is of another organization, which `SqliteStore::create_identity` of v0.1.0 could leave; `create_identity` and `link_address` now take such a row over, and an address an identity of the organization holds stays refused. Nothing moves at startup.
- `store.<operation>` spans on `PostgresStore`, as Python's (HITL d95a5592, 70079f74, RMK-1383): the kit gives its provider to its store through `ConversationStore::set_telemetry`, Python's `store._telemetry`, and each of Python's 37 operations opens its span, `store.operation` and `store.table` set, under the current span; an error reads its class, its text if `record_sensitive_data` asks.
- What a turn standing alone activates counts for the room, as Python's: its next ordinary turn carries the rules, and it reads the room's stored rows first as any turn does (RMK-1356).
- A Discussion installs (RFC §19.7.5), as Python's: `Orchestration` for `Discussion`, its turns given one at a time under the room's lock, routing, silence, `max_turns`, `done`, a depth stop recorded with its own limit, `ON_SPEAK_QUEUE` in order, `RoomKit::speak_queue` (`SpeakQueueOptions`), and a room binding another intelligence channel, a voice channel or a router refused, `HookEngine::has_router_hook` reading a router's closure by its type (HITL 88bbe70b).
- A discussion's stored key and a room's conversation state are the framework's, as Python's (HITL 9247da98, RMK-1337): `create_room`, `update_room_metadata` and a channel's `metadata_updates` drop `_discussion`, `_speak_queue` and `_conversation_state` with a warning, `save_conversation_state` staying the way to set a state (RFC §19.2). A stored handle counts only for an agent and if it can name one, a stored discussion the constructor would refuse reads as none, with a warning, and a person whose name reads like a handle or like `all` is addressed by no name.
- `PostgresStore` takes a room's locks in one order on its other writes, as Python's (RMK-1394): `delete_event` and `write_change_target` hold the room before the message and its edits, which `delete_room` takes in the order it finds them; `dedupe_event_indices` and the boot that rewrites versions 3 and 4 hold the rooms before their events; an add holds the room in the statement that takes its sequence, so that a room deleted and created again under it leaves it nothing taken with nothing held. The server killed one of each pair as a deadlock. `delete_event` and `write_change_target` take one more statement and wait, for theirs, on the room's other writes, and they on them; an add takes one statement fewer; a missing room is refused as before. Left open: a room id created again under live writes, with no sequence yet for a moment, can still have an add, a commit or a deletion killed (HITL d49b8b14: kept). On a room a kit serves, an index `add_event` takes at or past the room's next, or any `add_event_auto_index` takes, opens a hole the next delivery waits `delivery_gap_timeout` to skip, as their docs now say (HITL ab765c3d).
- `PostgresStore`'s writes of an event take a room's locks in one order, as Python's (HITL a8c2f7bd, 0affbb3b, RMK-1365): `add_event` and `add_event_auto_index` hold the room `FOR NO KEY UPDATE` before its sequence, as `commit_event` holds it first, so that a call of each on one room is no deadlock the server kills one of, and a stream of adds starves no commit; one more statement, a missing room refused as before.
- `PostgresKnowledgeSource`, as Python's (HITL f3eb441b, RMK-1366): a full-text keyword search for a `RetrievalMemory`, in `roomkit-postgres`, which now depends on `roomkit-ai`, on Python's table `knowledge_documents`, so that processes of both implementations share its documents.
- Telemetry's spans left, as Python's: `framework.detached`, what a caller answered at its decision did not wait for; `roomkit.llm.ttfb_ms`, a generation's time to first token; a turn's request names its span (`trace`, RFC §10.6.9). `framework.route` reads the room the kit created, `framework.broadcast` counts its plan's targets and ends with its last target, and a call refused before its injection opens no `framework.send_event` (RMK-1301).
- The anti-loop guard (RFC §6.4), as Python's: a call repeated with identical arguments in a turn is refused at its third (its second for `find_tools` and `list_tools`), a call its gate refuses counted too; a model that re-issues it three more times gets one last generation, whose calls never run, and the turn ends `force_stopped`; a tool answering the same text a third time has it noted for the model (RMK-1307).
- An identity resolves in its own organization only, as Python's (HITL 0f96a597, 6928c418): `PostgresStore`'s `resolve_identity` compares the row's organization with its identity's, and a row the move to per-organization addresses left unscoped moves to its identity's organization, once (schema version row 5 on Postgres, file version 14 on SQLite), or goes when the identity no longer declares the address; `unlink_address` removes the identity's rows for the address in every scope. None and the empty string are one organization on every store, as Python's.
- A round's reasoning goes back as it came (RFC §6.4), as Python's: `AiThinkingPart` and `AiContentPart::Thinking`, `AiResponse::thinking_signature` and `thinking_parts`, `thinking_parts_of`, `ThinkingBlocks`, `RoundTranscript`, `round_parts`; a wrapped response hands each block, the loop replays each block where it came among the round's text and calls, a call's metadata with it, within the turn and never stored; Chat Completions sends it inline as `<think>` (RMK-1316).
- A close that finds a first activity waiting for the kit's subscription to its bus, as Python's: a message past its door reads the close's cut (`blocked`, `kit_closed`, nothing stored, no handle), where it could fail with `KitClosed`; a room or event change ends its work within the grace, where it was cut at its room's lock; `update_event` and `delete_event` subscribe only for a hook that reads their context (RMK-1255).
- A turn cut while a call runs closes its row with the arguments its gate fixed, as its report says, as Python's: `tool_args` crosses before the turn's end.
- A tool's argument wider than 64 bits, written or quoted, is of its declared integer type and the call runs, as Python's: `find_tools`' `max_results` bounded as any; the tool reads a float, a difference the language forces (RMK-1308).
- A response's calls read as Python's: `ToolCallSlots` for a stream, whole calls on one index two calls, a second call to one tool included, calls in the order they came in; an entry with no function is no call, and a whole answer's id that is no text reads as text.
- A second call of a round under an id the first holds is refused before any gate and reported as its own, as Python's: `Tool call 'c1' has not had its result yet`, the first running on and a cut still reporting it; a result names its own call's tool. A provider reading its wire gives two calls two ids first, with `roomkit_ai::CallIds`, Python's (RMK-1308).
- A turn declares what the room's owner set up after the host's tools and before a person's and the skills', as Python's (RMK-1357).
- A turn's own skills (RFC §24.3), as Python's: `AiChannelTurnConfig::skills` gives a turn a registry in place of the channel's, its manifest, its activation and its references, the room's active skills kept per room and one the registry lacks leaving them; a channel given a registry keeps the skill tools' names, `run_skill_script`'s and an empty registry's included, against a turn's tools, a hook's and an owner's, and one built without ignores a turn's with a warning; a prompt with no base writes each skill block after a blank line, as Python's, and one with nothing to write stays none (RMK-1293).
- `HandoffHandler::send_greeting` (RFC §19.6), as Python's, its text path: the room's active agent says the line the handler's greeting map gives it, as that agent's message; no active agent, no line or no channel named, nothing is said; a room missing is not found (RMK-1314).
- `AiProvider::list_models`, the models an endpoint reports live, as Python's: `OpenAiAiProvider` asks `GET /models`, the catalogue's metadata backfilled and its speech models tagged (`speech_tags`, `with_speech_tags`, `TRANSCRIPTION_CAPABILITY`, `SPEECH_CAPABILITY`); any other provider answers its catalogue (RMK-1278).
- `OpenAiConfig::max_retries`, the retries Python's openai SDK takes on its own, 0 by default: a connection failure, a timeout or a status worth it, after the wait the endpoint asks or the SDK's backoff (RMK-1269).
- A provider's stream is read to its end, as Python's: a failure after its done event fails the turn as a stream under way (no retry, no compaction, no fallback), what the done event reported counting on the turn's usage (RMK-1271).
- A delivery set with a failure is recorded on its event under the room's lock, every target's outcome, as Python's: a set that all succeeded, a response's, a greeting's, a hook's injected event's and one the kit's close cut write nothing, and a record that fails is logged (RMK-1117). A turn the close cuts is no failed delivery of its call, as Python's.
- `update_binding_metadata` names its correspondent as an attach does, as Python's: the recipient and the correspondent in the channel's form, a binding naming no one taking its recipient (RMK-1124).
- Under the lock, a routed message's check reads the channel's received events a page at a time, up to the first other sender, and holds a message with no address to step 3's conditions, as Python's (RMK-1127).
- The seams a discussion uses (RFC §19.7.5), as Python's, inert outside one: `HookTrigger::OnSpeakQueue` takes a `SpeakQueueEvent`; a room that stores a discussion asks no agent at broadcast, a bound agent is no unavailable target; an instruction to one of its agents goes to the discussion, one to a discussion that is over is blocked `discussion_over`; a turn reads its event at its place with its notes.
- Each generation of a turn tells what it used, as Python's: `StreamItem::Usage(UsageMarker)`, in its order among the text and the rows, which the default `deliver_stream` skips, read ahead as a delta across the turn backend. A turn cut once it began records what the markers told as a finished turn does, its end `cancelled`; a generation in flight is not counted, and `ai_usage_complete` is then false (RMK-1333).
- `LoopEndMarker`'s `last_usage`, `usage_complete`, `model` and `provider` cross on its own item as Python's do, no more in a `record` frame, and stay off a caller's record (RMK-1056).
- The discussion's rules and its constructor (RFC §19.7.5), as Python's: `roomkit::Discussion`, `DiscussionOptions`, `SpeakQueue`, `SpeakQueueChange`, `SpeakQueueEvent`. A discussion is built and read: its handles and refusals are Python's; it cannot be installed yet.
- A Postgres store and the lock across processes, as Python's: the new crate `roomkit-postgres`, `PostgresStore` (`init`, `migrate`, `dedupe_event_indices`, `drop_legacy_parent_index`, `event_from_row`, `PostgresSchemaError`) on Python's tables, and `PostgresAdvisoryLockManager` on advisory locks whose keys are Python's, so that processes of both implementations share one database.
- `create_identity` never crosses an organization, as Python's (HITL b5e932a6): an id another organization holds is refused `StoreError::IdentityNotFound`, nothing written, on every store, and a replacement removes what the identity no longer holds.
- A kit warns when a store other processes may share is paired with `InMemoryLockManager`, as Python's (RFC §13.5, RMK-1291).
- A handoff to a human holds the room for them (RFC §19.6), as Python's: `ConversationState::human_hold`; while it holds, the router asks no agent and no supervisor, the message still reaching the room's transports, an address naming an agent still asking it and the supervisor beside it, `ON_HANDOFF` naming the human; a handoff to an agent is refused, by the tool and by `handle`, and only the host's transition to an agent takes it off (RMK-1233).
- A `ConversationPipeline`'s or a router's install refused for an agent whose handoff is set up already, named twice, or whose tools hold the handoff's name leaves no hook and no tool behind, as Python's (RMK-1233).
- A delivery cancelled, a held task cut or the kit's close leaves a streaming target reading its stream to end, what it does past the stream's end standing for Python's `finally`: no more 50 ms grace nor hard 5 s bound, `CancelOptions::timeout` bounds the waiter only, the close its grace, `DeliveryHandle::wait` reads the whole record, a target past its stream's end is dropped where it waits, `DeliveryStream::cut` tells a cut's end from the response's, and the default `deliver_stream` delivers nothing (RMK-1326).
- `ResponseMetadata::with_value` loses no other thread's write and ends when its change calls it on its own key (RMK-1326).
- **Breaking:** `InboundResult::response_metadata` is a `ResponseMetadata`, as Python's: a deferred call's `DeliveryHandle::wait` fills the record the call returned at its commit, and readers take `get()`, a copy, in place of indexing a map (RMK-1321).
- A muted agent's stream is closed unread, as Python's, a supervisor's second pass included: its reply is never generated, its tools never run, nothing of its record reaches the caller, its side effects stay, where the stream was read and its rows blocked (RFC §7.5, §10.6.4).
- `AiChannel::new` names a person's tool under a host's name before a definition the person gave twice, as Python's (RMK-1290).
- A turn's live record (RFC §6.7, §10.6.4), as Python's: `ResponseMetadata`, `roomkit_ai::current_response_metadata`, `AiContext::response_metadata`, and `ChannelOutput::response_metadata` now a record shared with its writers: what a memory, a generation hook, a `BEFORE_TOOL_USE` hook or a tool handler writes rides the messages stored after it and reaches the caller's result once the stream is read, a deferred wait, a reader's stop and a failed turn included; a value written under `turns` stays off every message (RMK-1254, RMK-1248).
- A blank stretch reaches no reader as text, as Python's now does: `ON_AI_RESPONSE`'s segments skip it and a buffering channel's `deliver_stream` delivers no text for it; a blank reads as Python's `str.strip` reads it, `\x1c` to `\x1f` included (RMK-1300).
- A binding's `enable_thinking` written otherwise than `false` (`"off"`, `"no"`, `0`) turns off a budget a less specific level set, as `false` does, the value read before it is compared, as Python's now does (RFC §6.7, Appendix A.9).
- A model's reasoning for the room's observers (RFC §6.7, §9.2), as Python's: `HookTrigger::OnAiThinking` and `ThinkingEvent` fired as each block opens, `THINKING_START`, `THINKING_DELTA` and `THINKING_END` ephemeral events, each block closed by the text, the round's end, a failure or a stop, coalesced by `AiChannelOptions::thinking_coalesce_ms` and `thinking_coalesce_chars`, their text only with `publish_thinking_text`, a subscriber otherwise reading the reasoning's length and round alone; `AiResponseEvent::thinking` reads every round's (RMK-1052).
- The pipeline (RFC §19.5, §19.7.1), as Python's, in text: `roomkit::Pipeline`, `ConversationPipeline` and `PipelineStage`, each agent handing off to the next, routed by its phase, its handoff tool naming the agents it can reach and its handler keeping to the stages (RMK-1233).
- A handoff's target is an agent of the room, a human or a known one: a transport the room binds is not found. Under allowed transitions and a phase map, a target is led only to a phase it holds, a human or a known target the map does not name excepted, and the caller holds the room's phase by the same rule; one the map does not name also passes as the room's active agent, or its default agent while none is active, as Python's (RMK-1233).
- Telemetry, as Python's: `roomkit::telemetry` (`TelemetryProvider`, `SpanKind`, `Attr`, `Span`, `TelemetryConfig`, `get_current_span`, the Noop, Mock and Console providers), and `OpenTelemetryProvider` behind the cargo feature `opentelemetry`, off by default; `RoomKitOptions::telemetry` and `kit.telemetry()`; the kit's spans of an inbound, its route and identity steps, a send, a hook, a broadcast, a delegation, an AI channel's generation and its tool calls, under Python's names and attributes, and the close ends and exports them. A span holds no address and no error's text unless `TelemetryConfig::record_sensitive_data` asks for them, as Python's (HITL a5171c2d) (RMK-1213).
- A turn's reasoning settings (RFC §6.7, Appendix A.9), as Python's: `AiChannelOptions` and `AiChannelTurnConfig`'s `thinking_budget` and `enable_thinking` reach `AiContext`, a binding's metadata over the turn's configuration over the channel's, a false turning off a budget a less specific level set; on OpenAI's own endpoint a budget of 0 or a false sends the model's floor (RMK-1052).
- A model's reasoning in the stream a channel reads (RFC §6.1, §6.7), as Python's: `StreamEvent::ThinkingDelta(StreamThinkingDelta)`, `AiResponse::thinking`, `ResponseItem::Thinking` and `StreamItem::Thinking`, in arrival order among the text and the rows, never stored, and skipped by the default `deliver_stream`. `OpenAiAiProvider` reads `reasoning_content`, `reasoning` and `<think>` tags on Chat Completions, and a reasoning item's text on Responses (RMK-1052).
- `OpenAiAiProvider` hands what it read before a stream's failure, a chunk of reasoning or text read in the same bytes as the error included, where it dropped it, reading the stream as never under way: a turn's author image is then no longer refused (RMK-1234).
- The handoff and the swarm (RFC §19.6, §19.7), as Python's: `roomkit::HandoffHandler`, `HandoffRequest`, `HandoffResult`, `ConversationRouter::install`, `roomkit::Swarm`; `roomkit_ai::setup_handoff`, `roomkit_ai::orchestration::{HANDOFF_TOOL, build_handoff_tool, HandoffMemoryProvider}`; `HookTrigger::OnHandoff`, `OnHandoffRejected`, `OnPhaseTransition`. An agent hands the room to another through `handoff_conversation`: its state moves under the room's lock, the timeline records it, the status bus says it; an event marked `_orchestration_internal` asks no agent, the mark dropped, with a warning, from what arrives through `process_inbound`, and a copy of the handoff's marks in a model's input is replaced (RMK-1233).
- An agent's greeting speaks first, as Python's: `RoomKit::send_greeting` stores an agent's greeting and delivers it on the room's lane, with no hook and no check of its right to write, and an `Agent` with a `greeting` and `auto_greet` greets the room a text session starts in before the message goes on; a session's start now awaits its hooks named with a leading `_`, within `process_timeout`. A voice or realtime session's greeting comes with voice (RMK-1264).
- Tool Search (RFC §6.4, §21.1), as Python's: `AiChannelOptions::tool_search`, `tool_search_pinned`, `tool_search_threshold`, `tool_search_threshold_pct`, `tool_search_miss_hint` and `tool_search_threshold_tokens`, the served `find_tools` and `list_tools` (`TOOL_FIND_TOOLS`, `TOOL_LIST_TOOLS`, `TOOL_SEARCH_INFRA_TOOL_NAMES`) and `AiTool::tags`, read from a binding's tools and a FastMCP server's: past its thresholds a turn declares its pinned tools and the discovery tools, a tool found or used stays declared for the room's next turns, and `ON_AI_RESPONSE` reports the union of the rounds with why each tool was declared. A tool literal now names `tags`, and a host tool may not take either name unless `tool_search` is `Some(false)` (RMK-1053).
- A turn's own configuration (RFC Appendix A.9, per-turn settings), as Python's: `AiChannelOptions::config_provider` returns an `AiChannelTurnConfig` at the start of each turn, its system prompt, temperature, max tokens, reasoning effort and budgets under the binding's metadata and over the channel's (a `None` not set), its tools replacing the binding's, its `tool_handler` serving the host's tools for that turn alone; the tools a binding's metadata declares under `tools` are now declared, before the channel's own, served by its handler (RMK-1289).
- A streamed answer asks every transport it reaches whether it streams in the
  room (`supports_streaming_delivery_for`), as Python's, where it asked only
  those whose `supports_streaming_delivery` was true at registration: a
  channel that streams once a client connects now streams (RMK-1126).
- A turn's cost budget (RFC §6.4), as Python's: `AiChannelOptions::turn_budget_usd`, each generation priced as one response at the model's catalogue price and the fallback's at the primary's (a fallback priced otherwise logged once per pair), a binding's metadata `turn_budget_usd` over the channel's (a `null` keeps it), a model with no price refused at construction and per turn (`AiError::UnpricedModel`, Python's `ValueError`), the turn's end stating `budget_usd`; `DynAiProvider::dyn_catalog_entry`. The token budget counts the image tokens too, Python's six billed counters, where it counted four
- A `Supervisor` that does not wait for its workers (RFC §19.7.3), as Python's: `SupervisorOptions::wait_for_result` (`true` by default); `false` with no strategy, a `delegate_to_<worker>` call answers `delegated` with the task's id at once and the worker runs in the background, its outcome handed back to the supervisor as an instruction addressed to it, at the depth of the turn that called; a second call for a worker still on the room's task answers `already_running`; a worker past its task timeout is cancelled and told as not completed; the kit's close ends the run cancelled (RMK-1233).
- On the Responses API, `OpenAiAiProvider` sends the turn's effort over its configured one, and on OpenAI's own endpoint an effort of `none` as the model's floor, as on Chat Completions; a turn with tools sends it as any turn does, the tool-turn rule being Chat Completions' (RFC §6.7, HITL 78d888bf).
- A turn's own reasoning effort (RFC §6.7), as Python's: `AiChannelOptions::reasoning_effort` and `AiContext::reasoning_effort`, a binding's metadata `reasoning_effort` over the channel's (a `null` keeps it, a value that is no text fails the turn with pydantic's `ValidationError`); `OpenAiAiProvider` sends the turn's effort over its configured one (RMK-1088).
- On OpenAI's own endpoint, a Chat Completions request reads the effort from the catalogue, as Python's: a turn with tools sends `none` to a model tagged `tools_reasoning_none` (GPT-5.4 and later), even unset, and the effort to one tagged `tools_reasoning_effort`, the switch deciding for the rest; an effort of `none` goes as the model's least (`minimal` on GPT-5, `low` on o3); a model Chat Completions does not serve, or tools for one that takes none there, fail before the request; an effort a tool turn leaves out unsaid is warned of once (RFC §6.7, RMK-1088).
- The model catalogue (RFC §6.7), as Python's: `ModelInfo`, `ModelPricing` and its `cost_for(usage)`, `Date`, and `AiProvider::available_models`, `catalog_entry` and `context_window`; `OpenAiAiProvider` embeds Python's OpenAI catalogue, copied by `make models-sync` from one roomkit commit, which `make models-check` holds it to (RMK-1088).
- `post()` speaks as the own record a speaker's id names on the host
  channel (RFC §10.5, §11.2), as Python's reads it through
  `People.named_on`, where it refused it as naming nobody (RMK-1163).
- A change's mutation trigger and a delivery set read the room as Python's
  `committed_in` holds it: the window the pass read and the committed
  event after it, 51 events for a window of 50 where the trigger read 50,
  at most 2000 where a delivery set at the ceiling held 2001 (RMK-1130).
- `register_channel` of an agent whose human-input handler is closed whole
  fails with the handler's `RoomKitError::Runtime`, as Python's raises,
  where it registered the channel with no owner of its requests
  (RMK-1255).
- A parallel team's first error cuts the other workers (RFC §19.7.3, §19.7.4), as Python's: a `Supervisor` with the parallel strategy, and a `Loop`'s parallel reviews, whose delegation raises answer with that error without waiting for the other delegations, each started then cut, posting its end; the Loop's parallel reviews waited for every reviewer (RMK-1279).
- A lock manager (RFC §13.5), as Python's: `RoomKitOptions::lock_manager`
  takes any `RoomLockManager` (`locked(&room_id)` a `RoomLock` guard,
  released by `release().await` or its drop), `InMemoryLockManager` by
  default, `kit.lock_manager()` the one given; a release that fails leaves
  what the section committed, reported on the result's `error`.
  `delivery_claim_lock_manager` takes the rooms' delivery claims, the lock
  manager by default, so kits sharing one share their claims (RMK-1153).
  A manager other than the in-memory one gets the claims' and the
  verifications' keys led by a NUL character, as Python's, and
  `create_room` refuses a room id holding one with `RoomKitError::Value`;
  a room the kit did not make (written to the store straight) takes no
  room lock under such an id, whatever the manager, the call failing with
  `RoomKitError::Value`, as a manager's own `Value` now does (RMK-1281).
- A room id takes no identity's verification lock (RFC §13.5), as
  Python's: the verification locks are mutexes of their own, apart from
  the room locks, so a section of the room `verification::alice` no longer
  holds alice's verification (RMK-1281).
- Delivery lanes across processes (RFC §13.5), as Python's: a delivery
  set waits for the room's cursor to pass the indexes before its own that
  another process committed; a hole no lane runs is skipped after
  `RoomKitOptions::delivery_gap_timeout` (30 s), the kit emitting
  `delivery_skipped`, and a set whose index another lane skipped is
  dropped, `delivery_stale` (RMK-1153).
- A name the kit quotes as Python's `repr` (a rename's record, an error's
  text, a skill's or a memory's message) escapes every character Python's
  `str.isprintable` refuses, as Python does: `'Bob\xa0Smith'` and
  `'\u202e'`, where it wrote the no-break space and the right-to-left
  override raw; one helper over a table generated from Python's Unicode
  data replaces four copies (RMK-1163, RMK-1255). A call for a tool no
  handler serves is declined naming it so too, `tool 'x' is not served
  here` (a person's tools, an MCP provider, a channel's handler, the task
  tools), where it wrote `"x"` from the `Debug` form.
- A turn with tools behind a `base_url` sends `reasoning_effort` only where `OpenAiConfig::supports_reasoning_effort_with_tools` says the server takes it there, as Python's does: a server that refuses it on a tool turn no longer answers 400; set the switch to `Some(true)` where the server takes it (RFC §6.7, RMK-1088).
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
- The supervisor strategy (RFC §19.7.3): `roomkit::Supervisor` and `orchestration::SupervisorOptions`, its per-worker tools waiting for their worker and its `delegate_workers` team, supervised when sequential, as Python's, and its framework-driven delegation (`auto_delegate`, `refine_task`, `refine_instruction`, `delegation_message`): the supervisor's turns in the room run the team, a first pass turning the message into its task, then the supervisor answers from their results; not yet `async_delivery`. A strategy's delegation tool is not cut by its channel's default bound per call (RMK-1233).
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
