# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it
publishes a release.

## [Unreleased]

Nothing is published yet: this section lists what the first release brings,
as it stands.

### Added

- **Repository.** The `roomkit-core` and `roomkit` crates and the
  `roomkit-examples` package; workspace lints; the gates `make all`, `make
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
  where its answers may go, a response's hooks never widening it past that
  (§5.12, gap G-073); a channel's history withholding a soft-deleted event's
  content (gap G-062); transcoding and the text a reader takes, a tool
  call's and a system notice's included, a composite's empty parts skipped
  (§6.6, gap G-072); the
  chain-depth limit and a cascade's reentry budget, shared by the calls that
  draw on it (§8.3); hook registrations, the off-lock and locked chains and
  how a chain of SYNC outcomes decides (§9.1–9.4); the lifecycle's
  transitions, which refuse a status they neither leave from nor lead to
  (gap G-074); the timer transitions due at an instant (gap G-045); a
  detach's revocation and the `roomkit:` metadata namespace (gaps G-049,
  G-053); an address in its channel's form, a phone number in E.164 by the
  rules of gap G-052 and an email address lower-case, how each channel
  type is addressed, its recipient and chat keys, and what a recipient is
  (gaps G-051, G-057, G-075); the rooms an organization reaches (§17.2);
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
  idempotency key, an edit's or a delete's target, the hooks, the room
  read again after a hook that ran under the lock, the source's
  permissions, the commit with the room's counters and the event's place in
  the room's delivery lane. Delivery sets run in index order, one at a
  time per room; a transport delivery with no recipient is refused
  `no_recipient`, a target whose channel is no longer registered skipped
  or reported (gap G-048); responses re-enter with their own source, their
  verification cleared (gap G-035), and within their message's answers
  (gap G-073), injected events and records past the reentry budget are
  stored, a refused record emits
  `room_refused_event` (gap G-043), and step 15 stores the cascade's side
  effects and applies the room metadata updates channels return (gap
  G-049). An `INSTRUCTION` is decided without being stored, delivered to
  the agents it names and fingerprinted on every response (gaps G-042,
  G-065, G-066). Only the framework writes the visibility `internal`: a
  message carrying it, for itself or for its answers, is refused (gaps
  G-041, G-073), a response carrying it is committed with
  `none`, and a hook's modify of an event marked `internal` keeps it (gap
  G-041).
- **Deadlines and callers that leave.** The kit's `process_timeout` bounds
  a call from its start to its decision, routing, the parse, the off-lock
  hooks, the ticket, the lock and the locked section, never the commit and
  what follows it (§13.6, gap G-068). A caller that stops waiting, its
  future dropped, never drops the call once anything it ran is committed:
  decided, the call delivers its cascade; before, it expires as one past its
  `process_timeout`, writes nothing more of its event and delivers every
  event it committed (gap G-076). A cascade that fails still delivers what
  it committed.
- **Direct calls.** `send_event` injects an event through the pipeline
  from its lock on, without a parse or step 2: a refusal before its commit
  is raised with nothing written (§10.5, gap G-069); its `DirectEvent` may
  say where its answers go (`response_visibility`), as a message does (gap
  G-073). `commit_event` commits
  a record under the room lock with the next index and nothing else: no
  hook, no delivery, no activity (gap G-071). `update_event` and
  `delete_event`, the host's changes of stored events, run under the lock,
  fire their trigger after it, and are a no-op on a missing target or
  room; a hard delete takes its thread and the changes of what it removes,
  leaving the counters as they are (§10.3, gaps G-063, G-064).
- **Reentrancy** (§13.5, §9.5.1 Reentrance, §10.1 step 18, gap G-070): code
  that runs inside a room's own work calls into the room without waiting
  for itself. A lifecycle change, a direct change or a record made under the
  lock takes the lock again through the caller's lease; `send_event` or
  `process_inbound_in` made under the lock or in the admission order is
  adopted by the call whose work made it and returns at its commit; a
  turn's or a delivery's call is a new arrival with its own ticket and
  `process_timeout`, drawing on the delivering cascade's budget when it
  comes into the room delivered in, a routed message once routing places
  it there (a call into another room has a budget of its own), and
  answered at its decision when it comes into its caller's
  room; a call made while a turn runs, into whatever room, is at least as
  deep as the turn's response (§10.6); work a turn or a delivery began
  starts fresh once that work is over, at depth 0 and on a budget of its
  own (gap G-070); a
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
- **Organizations** (§17.2, decision 2 of step 2b, gap G-077): every room
  operation acts for an organization or for none.
  `RoomKit::for_organization(organization_id)` returns an `OrganizationKit`
  whose calls are the kit's, made for the organization, and whose reads
  (`get_room`, `list_rooms`, `get_event`, `list_events`, `list_bindings`,
  `list_participants`, `get_participant`) see its rooms only. A room of
  another organization, or of none, is not found by its changes, holds no
  event for its updates and deletes, and refuses its messages and
  injections `OtherOrganization`, nothing written in it, its due timers
  included; a room it creates belongs to it, and its messages are routed
  among its rooms and identities only (gaps G-051, G-060). The kit itself
  acts for no organization.
- **The runtime** (`roomkit`), on tokio. `RoomKit` registers and
  unregisters channels, registers hooks (`hook`, `hook_handler`,
  `before_broadcast`, `observe`), sets the
  router, the environment, the chain-depth limit and the
  `process_timeout`, creates rooms (`create_room`, `create_room_with`
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
  crate's `default_route`, the default strategy's decision, `InMemoryStore`, `StoreError` (an unavailable store's cause
  kept as its source), `KitError`, and the testing helpers
  `RecordingTransport` and `EchoAgent`. The crate re-exports the core's
  model whole and, of its operations and rules, only what a caller names.
- **The store** (§14.1): rooms with partial updates and filters, bindings,
  participants by room, identities linked to and unlinked from addresses
  per organization, the routing lookups by binding and by participant,
  each finding the sender by address or by identity, and
  events, a commit writing the event, the room's counters and an edit's or
  a delete's target in one transaction, an index below `event_count`
  refused, the idempotency lookup from a channel or within the room
  (`get_event_by_idempotency_key`), hard deletes and event counts.
- **Examples**, one runnable file each, every one checking what it shows:
  the RFC's Appendix B flows (`echo_room`, `new_conversation`,
  `block_and_inject`, `agent_chain`, `advisor_joins`,
  `edit_delete_messages`), rooms and channels (`room_lifecycle`,
  `channel_mute_unmute`, `room_membership`), hooks (`hook_moderation`,
  `hook_inject_welcome`, `hook_off_lock_check`), the pipeline
  (`instruction_event`, `response_visibility`, `send_event`), and routing and
  identities (`shared_sms_number`, `paused_conversation`,
  `identity_addresses`).
