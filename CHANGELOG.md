# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it
publishes a release.

## [Unreleased]

### Added

- Repository foundation: the `roomkit-core` and `roomkit` crates, workspace
  lints, the `make all` gate, CI, and the architecture, roadmap, step 1 and
  RFC gap documents.
- `roomkit_core::model`: the data model of RFC §5 (rooms, events and every
  content type, bindings and permissions, capabilities, participants, tasks,
  observations, inbound messages, channel outputs, hook results), with its
  invariants checked at construction and its JSON form.
- `roomkit_core::rules`: the RFC's rules as pure functions: permissions and
  delivery planning with solicitation and addressing (§7.5, §10.2, §19.3),
  the visibility of what a source writes (§7.3), transcoding (§6.6), the
  chain-depth limit and reentry budget (§8.3), hook registration checks and
  the SYNC hook chain (§9.1–9.4).
- `roomkit_core::ops`: the effect protocol (`Operation`, `Effect`, `Outcome`,
  `Notice`) and the operations run through it: `Inbound` (RFC §10.1 and
  §10.2: the locked commit pass, hooks, permissions, broadcast, reentry,
  chain depth, injected events, framework events), `CreateRoom`,
  `AttachChannel` and `CloseRoom`.
- `roomkit`: the tokio runtime. `RoomKit` registers channels and hooks,
  creates, attaches and closes rooms, and runs inbound messages; the
  `Channel` and `ConversationStore` traits, `InMemoryStore`, framework
  event subscription, and `roomkit::testing` (`RecordingTransport`,
  `EchoAgent`).
- Examples `echo_room`, `block_and_inject` and `agent_chain`, after the RFC's
  Appendix B.1, B.2 and B.4.
- The room lifecycle (RFC §5.1, §7.2, §7.5 rules 6 and 7): the core
  operations `DetachChannel`, `UpdateBinding`, `PauseRoom`, `ResumeRoom`,
  `ReopenRoom`, `ArchiveRoom` and `CheckTimers`, with their hooks
  (`ON_CHANNEL_DETACHED`, `ON_CHANNEL_MUTED`, `ON_CHANNEL_UNMUTED`,
  `ON_ROOM_PAUSED`) and framework events (`room_channel_detached`,
  `room_paused`, `room_archived`, `channel_unregistered`); the rules
  `lifecycle`, `timers` and `attachment`; `Room.paused_at`, `RoomUpdates` and
  `BindingUpdate`.
- Room timers: every locked section applies the transitions that fell due
  before it decides, each at the instant it fell due; `RoomKit::check_timers`
  and the background `RoomKit::run_timers` apply them to silent rooms. A
  delivered event resumes a paused room.
- `RoomKit`: `create_room_with`, `detach`, `update_binding`, `mute`, `unmute`,
  `pause_room`, `resume_room`, `reopen_room`, `archive_room`,
  `unregister_channel`; the store's `delete_binding` and `list_rooms` with a
  `RoomFilter`.
- The room metadata updates a channel returns are applied; the keys under
  `roomkit:` are the implementation's and refused to integrator writes.
- Example `advisor_joins`, after the RFC's Appendix B.5.
- A named room (RFC §10.1 steps 1 to 3, §5.7, §17.2): the sender is written
  in its channel's form before anything reads it, a phone number in E.164 and
  an email address lower-case without its display name (`rules::addresses`).
  A missing room is created with its id in the caller's organization,
  `ON_ROOM_CREATED`'s hooks are awaited, the channel is attached with a
  binding naming the sender and its recipient, and a channel other than
  `VOICE` and `REALTIME_VOICE` fires `ON_SESSION_STARTED` and emits
  `session_started`. A room that exists has its unbound channel attached,
  unless it was detached. A room of another organization fails the message
  with nothing written (`rules::organizations`,
  `InboundError::OtherOrganization`).
- Recipients (RFC §10.2 step 3d): a channel's declaration names the binding
  metadata key it delivers to, and its country code.
  `ChannelDeclaration::new` gives a channel its type's key by default
  (Appendix A): `phone_number` for SMS, MMS, RCS, WhatsApp and WhatsApp
  Personal, `email_address` for email, `facebook_user_id` for Messenger,
  `teams_conversation_id` for Teams, `telegram_chat_id` for Telegram; none
  for a channel that delivers to a destination of its own (WebSocket,
  webhook, voice). The integrator may declare another key, or none.
- The protocol's `EnterLane`, `AwaitLane`, `LeaveLane` and `RunObservers`
  requests, `Outcome::LaneTurn`, `HookTrigger::OnSessionStarted` and
  `FrameworkEvent::SessionStarted`.
- Participants in the store (`create_participant`, `get_participant`,
  `update_participant`, `delete_participant`, `list_participants`), and in
  every room context.
- `RoomKit::process_inbound_for`, for a caller acting for an organization;
  `ChannelDeclaration::new` and `ChannelDeclaration::binding`.

- Inbound routing (RFC §10.4, §10.1 step 2): a message that names no room
  is routed. `InboundTarget::Routed`; the kit's router is asked through the
  protocol's `Route` request with a `RouteQuery` (the channel, the sender
  in the channel's form, the metadata, the caller's organization, the
  sender's address and identity, the channel's address form), after the core
  reads the sender's identity once (`ResolveIdentity`). A routed sender is
  recorded on the room's binding under the room lock before step 3, through
  `BindingUpdate`; a binding naming someone else by then, or a room no
  longer ACTIVE, routes the message once more, then into a new room. A
  router's failure fails the message with nothing written
  (`InboundError::Routing`).
- `rules::routing`: the default strategy over the rooms a channel reaches
  (the sender's newest conversation by binding, then by participant; else
  the one bound room when it admits the sender; else none), deterministic
  whatever the order rooms are read in, comparing addresses in the
  channel's form; the routing address; what step 2 records.
- `roomkit::InboundRoomRouter`, `DefaultRouter` and `RouterError`;
  `RoomKit::set_router`.
- Chat channels: a channel's declaration names the metadata key a message
  carries its chat under (`ChannelDeclaration::chat_key`, `chat_id` for
  Telegram, `conversation_id` for Teams and `group_jid` for a WhatsApp
  Personal group by default). A chat
  message is routed by its chat, and a room created for it binds the chat
  as correspondent and recipient, so its answers are delivered instead of
  refused `no_recipient`. `rules::addresses::AddressForm` and
  `ChannelDeclaration::form`.
- Identities (RFC §5.6): `model::Identity`, and the store's
  `create_identity`, `get_identity`, `resolve_identity` and `link_address`,
  scoped by organization, one identity per address within one. The store's
  `find_room_id_by_binding` and `find_room_id_by_participant`, and
  `RoomFilter::channel_id`.
- `Environment::new_room_id`, `RoomId::from_u128`; `SequentialEnvironment`
  numbers rooms `new_room_1`, `new_room_2`, ...
- `BindingUpdate::participant_id` and `BindingUpdate::is_empty`.
- Example `new_conversation`, after the RFC's Appendix B.1 with two senders.
- Edits and deletes (RFC §10.3, §10.1 steps 8a and 12): every pass, the
  inbound message's, a response's or a hook's injected event, checks an
  `EDIT` or a `DELETE` against its target under the room lock
  (`rules::edits`). The target is a received event of the room, neither
  deleted nor itself an edit or a delete; a sender's change comes from the
  target's author (the same participant on their channel, or the same sender
  id on the same channel, an agent's response being its channel's); an
  `ADMIN` delete from an OWNER or an AGENT on their channel; a `SYSTEM`
  change from a SYSTEM channel or a hook, which may make no other. An edit
  or a delete held anywhere but as an `EDIT`'s or a `DELETE`'s whole content
  (an edit's new content, a composite's part) is refused. A refused
  change stores nothing, and the inbound result reports
  `target_event_not_found`, `identity_required_for_edit`, `not_authorized`
  or `not_original_author`. An accepted change updates its target in its own
  commit (the new content and `metadata.edited`, or `metadata.deleted`),
  reaches no channel that could not see the target, and fires
  `ON_EVENT_UPDATED` or `ON_EVENT_DELETED` with the target as the commit left
  it once the lock is released; a blocked one changes and fires nothing.
- The host's direct calls (RFC §10.3): `RoomKit::update_event`, which
  replaces the fields an `EventUpdates` names and adds no marker, and
  `RoomKit::delete_event`, a hard delete that takes the event's thread
  replies by default, and always the edits and deletes of what it removes,
  and returns the removed ids; both under the room lock,
  firing their trigger after it. A missing target is a no-op. The core's
  `UpdateEvent` and `DeleteEvent`, `MutationError` and `KitError::Mutation`.
- The store's `get_event`, `update_event`, `delete_event` and
  `get_event_count` with an `EventFilter`, and `StoreError::EventNotFound`. A
  hard delete leaves the room's counters as they are: an index is never
  given again, and `get_event_count` then counts fewer rows than
  `event_count`.
- `model::EventUpdates`; the protocol's `GetEvent`, `UpdateEvent` and
  `DeleteEvent` requests, `Outcome::Event`, `Outcome::Deleted` and
  `TargetUpdate`; `HookTrigger::OnEventUpdated` and
  `HookTrigger::OnEventDeleted`; `rules::edits::is_edited`, `is_deleted` and `holds_change`;
  `RecordingTransport::with_capabilities`.
- Example `edit_and_delete`, after the RFC's Appendix B.6 and B.7.
- Instructions (RFC §10.1.1): a message of type `INSTRUCTION` makes the
  agents it names speak without being stored. It is refused before anything
  is read, `blocked` with `instruction_unaddressed` when its address names no
  registered intelligence channel and `instruction_not_idempotent` when it
  carries an idempotency key. It is visible to intelligence channels only
  and carries the caller's metadata and `metadata.standalone`, the typed
  field's word. Its hooks may block it or change its text, never its
  visibility, its address or its `standalone` word. Its pass commits
  nothing: no index is consumed and no counter moves; a blocked instruction
  is not stored, and `event_blocked` is emitted. `InboundResult.event` is
  the uncommitted instruction. Every response it produces records
  `metadata.instruction = {sha256, length}`, never the text, which the
  response's hooks cannot remove or replace, and a standalone instruction's
  turn is handed no history. A hook's injected
  instruction and a channel's response typed as one are dropped with a
  warning (gaps G-042, G-066).
- `rules::instructions`: the refusals (`InstructionRefusal`), the stamp,
  what a modify keeps, a standalone turn's context, and the responses'
  `Fingerprint`. `roomkit-core` depends on `sha2` for it.
- Off-lock checks (RFC §9.5.1, §10.1 step 5a): a SYNC `BEFORE_BROADCAST`
  hook registered with `needs_lock = false` runs before the room lock, on
  the room as step 2 left it. The event takes the room's admission ticket
  before its first off-lock hook and waits for every earlier ticket after
  the last; step 9 then applies their outcome after the status, idempotency
  and edit gates, running only the hooks that need the lock, and the side
  effects of both phases are collected once. An event no off-lock hook fires
  for takes no ticket; a response and a hook's injected event run every hook
  under the lock. An off-lock hook's failure is reported before the lock.
  The ticket is released on every path; one that cannot be taken or awaited
  fails the call with `InboundError::Ticket` (gap G-067).
- The protocol's `TakeTicket`, `AwaitTicket` and `ReleaseTicket` requests,
  `Outcome::Ticketed`, `Outcome::Admitted` and `Outcome::TicketReleased`;
  the runtime's ticket queue, which drops a cancelled call's ticket.
  `HookRegistry::off_lock_checks`, `HookRegistry::locked_chain` and
  `HookChain::take_incidents`.
- The processing timeout (RFC §13.6, §10.4): `KitConfig::process_timeout`
  and `RoomKit::set_process_timeout`, off by default, bound a call from its
  start to its decision: routing, the parse, the off-lock hooks, the
  ticket's turn, the room lock and the locked section up to the commit.
  Past it nothing of the event is written, the hooks' injected events and
  side effects are dropped, what the call holds is released,
  `process_timeout` is emitted (`FrameworkEvent::ProcessTimeout`, naming
  the channel before the room lock and the event under it), and the result
  is `blocked` with the reason `process_timeout`. The commit and what
  follows it are never bounded; a hook's own timeout is still `hook_timeout`
  and `hook_error` (gap G-068).
- `Effect::deadline` and `Outcome::DeadlinePassed`: an effect may carry the
  instant past which the driver gives up on it; the tokio driver waits for
  it on tokio's clock, whatever clock the kit has.
- Direct injection (RFC §10.5): `RoomKit::send_event(room_id, DirectEvent)`
  and `Inbound::inject` run an event through the inbound pipeline without a
  channel's parse: its off-lock check and ticket, its hooks, its source's
  permissions, an edit's or a delete's target, its idempotency key (per room
  and channel), its commit, delivery and reentry passes. Its source is the
  channel it names, `INBOUND`, or `OUTBOUND` for an intelligence channel. It
  returns the committed event, a record stored `BLOCKED` included; a refusal
  before the commit is raised with nothing written,
  `InboundError::Refused { reason }` (`room_closed`, `process_timeout`, an
  edit's or a delete's refusal, an instruction's). A missing room is not
  created, and an unbound channel is not attached (gap G-069).
  `model::DirectEvent`.
- A record outside the pipeline (RFC §10.5): `RoomKit::commit_event(room_id,
  event, organization_id)` and the core's `CommitRecord` commit a record
  under the room lock with the next index, and nothing else: no hook, no
  delivery, no framework event. It is refused for another room's id before
  anything is read, not found in another organization's room, and refused
  by a status that refuses events (`MutationError::OtherRoom`,
  `MutationError::Refused`). It moves the room's counters, not its activity
  (gap G-071).
- The reentrant lock (RFC §13.5, §9.5.1 Reentrance, §10.1 step 18): code
  that runs inside a room's own work calls into the room without waiting for
  itself. A lifecycle change, a direct mutation or `commit_event` made from a
  hook under the lock takes the lock again at once, within the hook's
  execution context, a tokio task-local the driver scopes into every call it
  makes. `send_event` or `process_inbound` naming the room, made under its
  lock or in its admission order (an off-lock hook's body, the message's
  parse), is adopted by the call whose work made it: from a locked hook it
  commits ahead of the event being decided, which takes the next index;
  from the admission order it commits beside the running work; either way
  its caller receives it at its commit. One made by a turn, a delivery or
  an awaited `ON_ROOM_CREATED` hook is a new arrival, a call of its own;
  one a turn or a delivery makes into its own room is answered at its
  decision, its delivery waiting behind its caller's. A call that would
  wait for what its own context holds fails with `KitError::Reentrant` (gap
  G-070).
- The protocol's adoption: `Operation::adopt(entry, origin)`, `Entry`,
  `EntryId`, `Step::settled` and `Settled`; `Effect::locked`, the room lock
  an operation holds on a request's behalf, and `Effect::admitted`, the
  room in whose admission order a request stands; `Step::decided` and
  `Decided`, an inbound call's decision, published once, before its
  deliveries.
- `roomkit::keep_context(future)`: a task a hook or a turn spawns and
  awaits as its own keeps the call's execution context (RFC §13.5).
  `KitError::Reentrant` and `KitError::Abandoned`.
- `ConversationStore::unlink_address` and `Identity::unlink`: an address is
  moved from one identity to another by an unlink, then a link, since
  `link_address` refuses an address another identity of the organization
  uses (RFC §5.6, §14.1, gap G-060).

### Changed

- The implementation's room metadata namespace is `roomkit:`, the
  revocations of detached channels `roomkit:detached` (they were
  `roomkit.` and `roomkit.detached`); a key written `roomkit.` is the
  integrator's (gap G-049). No release wrote the old keys, so no store
  holds a revocation under them.
- A Telegram binding's recipient key is `telegram_chat_id` (it was
  `chat_id`), and a channel's default chat key is its type's own instead of
  its recipient key: `chat_id` for Telegram, `conversation_id` for Teams
  (it was `teams_conversation_id`), `group_jid` for WhatsApp Personal (it
  had none), so a WhatsApp Personal group message is routed by its group
  (gap G-051).
- A hook's injected event is visible to the targets the room binds only,
  `none` when it binds none (it named every target the hook gave); the hooks
  of its own pass may narrow that visibility, never widen it, and its
  delivery reaches only the targets it admits. A blocked one, or one stored
  past the reentry budget, keeps that visibility (gaps G-031, G-036).
  `rules::permissions::targets_visibility` and `injected_visibility`.
- A response that claims another channel's source gets a fresh source, the
  responder's, keeping nothing of the claimed one (participant, provider,
  ids, raw payload, verification); one under its own source keeps its other
  fields (gap G-035).
- A phone number written without `+` on a channel that declares no country
  code is kept as written; 11 to 15 digits were read as international. With
  a country code declared, digits that start with it are read as written
  with it only past ten digits, a shorter number taking the code as a
  national one: `3931234567` on a channel declaring `39` is
  `+393931234567` (gap G-052).
- A delivery set does not call a target whose channel is not registered,
  its binding left in the room: a transport is skipped, logged at debug
  level, and an intelligence channel is reported `delivery_failed` with the
  error `channel_not_registered`; each was called and its failure reported
  (gap G-048). A driver answers an `on_event()` or a `deliver()` of a
  channel it no longer holds `Failed` with the error type
  `ops::CHANNEL_NOT_REGISTERED`.
- `RoomKit::create_room` and `create_room_with` wait for the room's
  `ON_ROOM_CREATED` hooks before they return, as the inbound pipeline's
  creation does; they fired them without waiting (gap G-050).
- `RoomKit::update_event` and `delete_event` on a room that does not exist
  are a no-op, `None` and no id, as for an event the room does not hold;
  they failed with `MutationError::RoomNotFound` (gap G-063).
- `RoomKit::send_event` on a channel the room does not bind is refused,
  `InboundError::Refused` with the reason `source_unbound` and nothing
  written; its event was stored `BLOCKED` `source_unbound` (gap G-069).
- `RoomKit::send_event_for(organization_id, room_id, event)` injects for a
  caller acting for an organization: a room of another organization, or of
  none, refuses the event with nothing written,
  `InboundError::OtherOrganization` (RFC §17.2, gap G-069).
  `Inbound::inject(config, room_id, organization_id, event)` and
  `Entry::Direct` carry the organization.
- A sender who writes again after a timer paused their room returns to it,
  and the message resumes it: routing's step 1 reads ACTIVE and PAUSED
  rooms, the newest winning. Step 3 reads ACTIVE rooms and the PAUSED rooms
  bound to the channel as a group, so a paused group room takes its group's
  next message, and any other PAUSED room takes in no one new; one ACTIVE
  room and one PAUSED group room bound to the channel are two, and give a
  new room. Under the room lock, a routed room found PAUSED stays the
  message's when it is the sender's conversation by step 1 or bound to the
  channel as a group, and the message is routed once more otherwise; it was
  routed once more whenever the room was not ACTIVE (gaps G-045, G-055).
  `rules::routing::considered` gives way to `step_one_considers` and
  `step_three_considers`, which take a `Candidate`; `conversation_through`,
  answering a `Through`, tells a sender's conversation for step 1, and
  `paused_room_takes` tells the lock what steps 1 and 3 would take.
  `DefaultRouter::candidates` reads the PAUSED rooms too.
- A channel's response that carries the visibility `internal` is committed
  with `none`, stored and shown to no channel, and a warning is logged; it
  was committed with `internal`, the mark only the framework writes (gap
  G-041).
- The `roomkit` driver completes each effect as it finishes and polls the
  operation between completions, a lock, a lane or a ticket's turn waited
  for beside the calls, instead of waiting for a step's every effect.
  `Next::Waiting` is answered by waiting for the next completion.
- A pass that ran a hook under the room lock reads the room again once its
  hooks are over (`LoadContext`): the event takes the room's next index then,
  a room a hook closed refuses it with nothing stored, and the source is
  checked on the bindings as the hooks left them. A pass that ran no such
  hook decides on its gate's read. `Request::LoadBindings` and
  `Outcome::Bindings` are removed.
- A record stored `BLOCKED` keeps the visibility step 12 would have given it
  when its source is bound (gap G-036); a record a closed room refuses
  emits `room_refused_event` (gap G-043).
- An inbound message carrying the visibility `internal` is refused,
  `InboundError::InternalVisibility`, and a hook's modify that writes it is
  put back to the original's (gap G-041).
- A message whose idempotency key replays an event no longer clears the
  lane, which may hold an adopted entry's delivery set.
- A cascade that fails or passes its deadline still delivers every event it
  committed: the delivery sets already queued run before the call ends
  (RFC §13.6); no pass runs any more. They were dropped.
- A pass that ran a hook under the room lock checks an edit's or a delete's
  target again before its commit.

- `metadata.instruction` is the pipeline's alone: a parser's, a caller's or
  a channel's key is stripped from an inbound, an injected event and a
  response that does not answer an instruction, and a hook's modify keeps
  the pipeline's fingerprint only, so a hook that redacts the key elsewhere
  stays obeyed (gap G-065).
- `Effect` carries a `deadline`, and `Request` the ticket requests, which
  every driver answers.
- A channel's history never carries a soft-deleted event's content: it is
  handed with an empty text and no raw payload, and an edit of it with its
  new content withheld; hooks and the store keep the record whole (gap
  G-062).
- A routed sender is recorded only where the room, as its lock finds it,
  still admits them by step 3: a room that has heard someone else on the
  channel meanwhile routes the message once more (gap G-055).
- `RoomKit::process_inbound(message, room_id)` and
  `RoomKit::process_inbound_for(organization_id, message, room_id)` take the
  RFC's order, the room optional: `None` routes the message.
- `RoomFilter::matches` takes the room's bindings and participants.
- An `EDIT` or a `DELETE` message is processed instead of refused
  `InboundError::Unsupported`.
- An `INSTRUCTION` message is processed instead of refused
  `InboundError::Unsupported`, which is removed: nothing returns it any
  more.
- `ConversationStore::commit(event, counters, target)` and `Request::Commit`
  carry what an accepted edit or delete writes on its target, in the same
  transaction. `InMemoryStore` refuses an index below the room's
  `event_count` (it compared with the timeline's length), so a hard-deleted
  index is never reused.
- A `BEFORE_BROADCAST` modify keeps an edit's or a delete's target, its
  `edit_source` and its `delete_type`; it never turns an event into an edit
  or a delete, nor an edit or a delete into another event, nor slips one
  into an edit's new content or a composite.
- `StoreError::IndexTaken` reads "index N is not room R's next index": the
  commit's index is not the room's next one.
- `Environment` requires `new_room_id`.
- A room step 2 creates for a chat message names the chat on its binding;
  a chat message without its chat still names neither.
- `rules::addresses::correspondent` is gone: the binding of a created room is
  written by `rules::routing::recording`.
- `ConversationStore::update_room` is a partial update,
  `update_room(room_id, updates)`, and `Request::UpdateRoom` carries
  `RoomUpdates`; a commit's `RoomCounters` carries the resume of a paused
  room.
- `room_closed` carries a `reason`, `timer` when the close timer closed the
  room; the pass of a hook's injected event that a closed room refuses emits
  `room_refused_event` with `operation = reentry`.
- `Inbound::new(config, target, organization_id, message)` takes an
  `InboundTarget` and the organization the caller acts for. A message naming
  a missing room creates it instead of failing.
- The room's delivery lane is an effect, taken at the commit (RFC §10.2):
  an event takes its place under the room lock, between its `Commit` and
  the release (`EnterLane`), its delivery set waits for the place's turn
  (`AwaitLane`) and leaves it once over (`LeaveLane`), each naming the
  event; an instruction takes its place at its decision. The runtime keeps
  one queue of places per room. Calls into one room overlap until the room
  lock, commit in lock order and deliver in index order, and another call's
  commit may land between a trigger and its responses; the lane was a
  per-room mutex held for the whole call (gap G-070).
- `source_unbound` marks the event of a channel detached from the room: step
  2 attaches any other unbound channel.
- The default `handle_inbound` names the sender as the event source's
  `participant_id`.
- Two calls into one room run their off-lock checks at the same time and
  commit in ticket order, whichever check finishes first; an event no
  off-lock hook fires for may commit during another call's check. Two
  checks of one room never overlapped (gap G-067).
- `process_timeout` no longer counts the wait for an earlier call's
  cascade: a call behind a slow delivery commits within its bound, and its
  delivery waits after the commit. A call into a busy room was refused past
  its bound (gap G-068).
- A call that an agent's turn or a delivery makes into a room is a new
  arrival: its own operation, admission ticket and `process_timeout`, its
  off-lock hooks run off the lock; it was adopted beside the running work.
  It draws on the reentry budget of the cascade that delivers, past which a
  message is refused with no event and an injected event stored `BLOCKED`,
  both with `reentry_loop_cap`, so a transport that calls into its room on
  every delivery stops at the cap. `Inbound::drawing_on(budget)`,
  `Effect::budget`; `ReentryBudget` is a shared count, its clones drawing on
  the same passes. Into its caller's own room it is answered at its
  decision, `send_event` returning the committed event with no delivery
  results, its delivery run after its caller's set and its end logged; a
  message a turn routes back into its room is answered so too, where it
  failed `KitError::Reentrant`. A `send_event` from an awaited
  `ON_ROOM_CREATED` hook waits for its delivery and reports it (gap G-070).
- A call made within an agent's turn is at least as deep as the turn's
  response, so an agent that injects into its room on every turn, through
  any channel, stops at the chain-depth limit (RFC §10.6, §8.3, gap G-070).
- An inbound call runs on a task of its own, within its caller's execution
  context; a caller that stops waiting aborts it. `KitError::Stalled` also
  reports a run the runtime cancelled, as it does when it shuts down.
- A lane request that fails is logged and the event delivered all the same
  (RFC §13.6); a failed leave no longer sets the result's `error`.
  `InboundError::Lane` is removed.
- `ChannelDeclaration` has `recipient_key` and `country_code`;
  `RecordingTransport` declares no recipient key, its record being its
  destination.
- A transport delivery whose binding holds nothing under its channel's
  recipient key is refused `no_recipient` before `deliver()` is called: a
  failed delivery result of code `no_recipient`, not retryable, and
  `delivery_failed` for an event that entered through the inbound pipeline.
  Its `on_event()` is still called.
- A channel attached to an existing room at step 2 names no recipient (RFC
  §10.1 step 2: with a room id given, only a room step 2 creates records
  its sender; a routed message records it, with routing). An integrator
  that names the room of an SMS, email or other addressed conversation
  gives the binding its recipient when it attaches the channel
  (`attach_with`, `phone_number` = the customer's number): otherwise its
  deliveries are refused `no_recipient`.
