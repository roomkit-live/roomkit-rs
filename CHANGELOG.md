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
  `roomkit.` are the implementation's and refused to integrator writes.
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
  `teams_conversation_id` for Teams, `chat_id` for Telegram; none for a
  channel that delivers to a destination of its own (WebSocket, webhook,
  voice). The integrator may declare another key, or none.
- The protocol's `EnterLane`, `LeaveLane` and `RunObservers` requests,
  `HookTrigger::OnSessionStarted` and `FrameworkEvent::SessionStarted`.
- Participants in the store (`create_participant`, `get_participant`,
  `update_participant`, `delete_participant`, `list_participants`), and in
  every room context.
- `RoomKit::process_inbound_for`, for a caller acting for an organization;
  `ChannelDeclaration::new` and `ChannelDeclaration::binding`.

### Changed

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
- The room's delivery lane is an effect: an inbound operation enters it once
  the room is known and leaves it on every path, and the driver holds it in
  between.
- `source_unbound` marks the event of a channel detached from the room: step
  2 attaches any other unbound channel.
- The default `handle_inbound` names the sender as the event source's
  `participant_id`.
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
