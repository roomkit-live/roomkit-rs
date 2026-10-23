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
  Telegram and `teams_conversation_id` for Teams by default). A chat
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

### Changed

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
