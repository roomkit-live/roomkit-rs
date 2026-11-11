-- Every name this batch looks up in the catalogs, or drops, is the one of the
-- schema this session writes to (current_schema()): two deployments may share
-- a database, one per schema, and through search_path a namesake of the
-- other's would be read for ours, or dropped.
--
-- Migration: idx_participants_channel must NOT be unique.
-- Multiple participants can share the same channel in group rooms.
-- Dropping/recreating an INDEX is non-destructive to data.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
        AND indexname = 'idx_participants_channel'
        AND indexdef LIKE '%UNIQUE%'
    ) THEN
        EXECUTE format('DROP INDEX %I.%I', current_schema(), 'idx_participants_channel');
        EXECUTE format(
            'CREATE INDEX %I ON %I.%I (room_id, channel_id)',
            'idx_participants_channel', current_schema(), 'participants'
        );
        RAISE NOTICE 'Converted idx_participants_channel from UNIQUE to regular index';
    END IF;
END $$;

-- Migration: upgrade idx_events_room_index to UNIQUE(room_id, index) so a
-- duplicate index (e.g. from two processes assigning concurrently) is rejected
-- rather than silently persisted (RFC §8.1 / §14.3). Best-effort in init(): if
-- the table already holds duplicate (room_id, index) rows (from a prior
-- release's races), the upgrade is skipped with a warning so startup still
-- succeeds — deduplicate, then recreate the index UNIQUE. The inner block's
-- rollback on failure preserves the existing non-unique index.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
        AND indexname = 'idx_events_room_index'
        AND indexdef NOT LIKE '%UNIQUE%'
    ) THEN
        BEGIN
            EXECUTE format('DROP INDEX %I.%I', current_schema(), 'idx_events_room_index');
            EXECUTE format(
                'CREATE UNIQUE INDEX %I ON %I.%I (room_id, index)',
                'idx_events_room_index', current_schema(), 'events'
            );
            RAISE NOTICE 'Upgraded idx_events_room_index to UNIQUE(room_id, index)';
        EXCEPTION WHEN unique_violation THEN
            RAISE WARNING 'idx_events_room_index NOT upgraded to UNIQUE: duplicate '
                '(room_id, index) rows exist. Deduplicate the events table, then '
                'recreate the index UNIQUE. Multi-process index safety is NOT '
                'enforced until then.';
        END;
    END IF;
END $$;

-- rooms
CREATE TABLE IF NOT EXISTS rooms (
    id              TEXT PRIMARY KEY,
    organization_id TEXT,
    status          TEXT NOT NULL DEFAULT 'active',
    event_count     INTEGER NOT NULL DEFAULT 0,
    latest_index    INTEGER,
    delivered_index INTEGER NOT NULL DEFAULT -1,
    metadata        JSONB NOT NULL DEFAULT '{}',
    timers          JSONB NOT NULL DEFAULT '{}',
    -- What an agent's own output solicits here (RFC §19.3.1).
    agent_response_policy TEXT NOT NULL DEFAULT 'agent_chain',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- When the room was paused (RFC §5.1); NULL unless paused.
    paused_at       TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ
);

-- Additive migration for pre-existing deployments: the delivery-lane
-- cursor (RFC §10.1 step 14). -1 = no delivery set executed yet. The
-- backfill runs ONLY when the column is first created: a pre-lane
-- deployment delivered under the room lock, so every stored event is
-- delivered and the cursor starts at latest_index — without it, a busy
-- room's first laned delivery would wait out a delivery_gap_timeout
-- against a purely historical hole. Guarded (not re-run per boot):
-- re-applying it against a live multi-worker deployment would declare
-- an in-flight first delivery lost. Empty rooms keep -1.
--
-- The lookup is scoped to the schema this session actually writes to:
-- information_schema.columns spans every schema the role can see, so an
-- unqualified match would read a namesake `rooms` table from another
-- schema and skip the migration our tables need. The guard is safe against
-- a concurrent boot because PostgresStore.init() runs this DDL under the
-- migration advisory lock.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'rooms'
          AND column_name = 'delivered_index'
    ) THEN
        ALTER TABLE rooms ADD COLUMN delivered_index INTEGER NOT NULL DEFAULT -1;
        UPDATE rooms SET delivered_index = latest_index WHERE event_count > 0;
    END IF;
END $$;
-- latest_index is NULL while a room has no event (RFC §5.1): an empty room
-- stored 0, which reads as the first event's index. Guarded on the column's
-- nullability, so it runs once and no later boot scans the table.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'rooms'
          AND column_name = 'latest_index'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE rooms ALTER COLUMN latest_index DROP NOT NULL;
        ALTER TABLE rooms ALTER COLUMN latest_index DROP DEFAULT;
        UPDATE rooms SET latest_index = NULL WHERE event_count = 0;
    END IF;
END $$;
-- The instant a room paused (RFC §5.1), which its close timer counts from.
-- Added only when missing, as for delivered_index: a room stored paused
-- before it takes its last modification, which the pause was, once; no later
-- boot scans the table.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'rooms'
          AND column_name = 'paused_at'
    ) THEN
        ALTER TABLE rooms ADD COLUMN paused_at TIMESTAMPTZ;
        UPDATE rooms SET paused_at = updated_at WHERE status = 'paused';
    END IF;
END $$;
-- Additive: the default IS the behaviour every existing room ran under, so
-- there is nothing to backfill.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'rooms' AND column_name = 'agent_response_policy') THEN
        ALTER TABLE rooms ADD COLUMN agent_response_policy TEXT NOT NULL DEFAULT 'agent_chain';
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_rooms_org')) IS NULL THEN
        CREATE INDEX idx_rooms_org ON rooms (organization_id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_rooms_status')) IS NULL THEN
        CREATE INDEX idx_rooms_status ON rooms (status);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_rooms_updated')) IS NULL THEN
        CREATE INDEX idx_rooms_updated ON rooms (updated_at DESC);
    END IF;
END $$;

-- events
CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    room_id             TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    type                TEXT NOT NULL DEFAULT 'message',
    content             JSONB NOT NULL,
    source_channel_id   TEXT NOT NULL,
    source_channel_type TEXT NOT NULL,
    source_direction    TEXT NOT NULL DEFAULT 'inbound',
    source_participant_id TEXT,
    source_provider     TEXT,
    source_extra        JSONB NOT NULL DEFAULT '{}',
    status              TEXT NOT NULL DEFAULT 'pending',
    visibility          TEXT NOT NULL DEFAULT 'all',
    -- Intelligence channels asked to act (RFC §19.3). NULL = unaddressed,
    -- which is not the same as the empty array (addressed to nobody), so the
    -- column is nullable rather than defaulting to '{}'.
    addressed_to        TEXT[],
    response_visibility TEXT,
    index               INTEGER NOT NULL DEFAULT 0,
    chain_depth         INTEGER NOT NULL DEFAULT 0,
    correlation_id      TEXT,
    parent_event_id     TEXT,
    -- The event this one answers (RFC §8.5); NULL for an event that answers none.
    responds_to         TEXT,
    idempotency_key     TEXT,
    blocked_by          TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}',
    channel_data        JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The outcome of the event's delivery set, written once it has run.
    delivery_results    JSONB NOT NULL DEFAULT '{}'
);
-- Columns that arrived after events existed, each additive, with nothing to
-- backfill: addressing (an existing row is unaddressed, NULL, the behaviour it
-- was stored under), the event an answer answers (RFC §8.5: no event stored
-- before it named one), a delivery set's outcome (RFC §10.3, §14.3: an
-- existing row recorded none, '{}').
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'events' AND column_name = 'addressed_to') THEN
        ALTER TABLE events ADD COLUMN addressed_to TEXT[];
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'events' AND column_name = 'responds_to') THEN
        ALTER TABLE events ADD COLUMN responds_to TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'events' AND column_name = 'delivery_results') THEN
        ALTER TABLE events ADD COLUMN delivery_results JSONB NOT NULL DEFAULT '{}';
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_room_index')) IS NULL THEN
        CREATE UNIQUE INDEX idx_events_room_index ON events (room_id, index);
    END IF;
END $$;
-- Composite idx_events_parent_index on (parent_event_id, index): thread-reply
-- pagination filters by parent_event_id then reads forward ORDER BY index, so
-- the second column lets Postgres return the page pre-sorted instead of
-- sorting the whole thread. The leading column still serves plain
-- parent_event_id lookups. A distinct name (not the older single-column
-- idx_events_parent) so init() creates it on databases that predate the
-- composite; PostgresStore.drop_legacy_parent_index() removes the old one.
-- idx_events_room_responds_to finds a request's answers: the events of a
-- room that answer a given event.
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_room_created')) IS NULL THEN
        CREATE INDEX idx_events_room_created ON events (room_id, created_at);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_room_type')) IS NULL THEN
        CREATE INDEX idx_events_room_type ON events (room_id, type);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_correlation')) IS NULL THEN
        CREATE INDEX idx_events_correlation ON events (correlation_id) WHERE correlation_id IS NOT NULL;
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_parent_index')) IS NULL THEN
        CREATE INDEX idx_events_parent_index ON events (parent_event_id, index) WHERE parent_event_id IS NOT NULL;
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_source')) IS NULL THEN
        CREATE INDEX idx_events_source ON events (source_channel_id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_room_responds_to')) IS NULL THEN
        CREATE INDEX idx_events_room_responds_to ON events (room_id, responds_to) WHERE responds_to IS NOT NULL;
    END IF;
END $$;
-- An idempotency key is unique per room and per source channel (RFC §13.4):
-- the per-room index gives way to one that names the channel. Keys unique
-- per room are unique per room and channel, so the new index always builds.
-- The index names below are looked up in the schema this session writes to.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema() AND indexname = 'idx_events_idempotency'
    ) THEN
        EXECUTE format('DROP INDEX %I.%I', current_schema(), 'idx_events_idempotency');
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_idempotency_channel')) IS NULL THEN
        CREATE UNIQUE INDEX idx_events_idempotency_channel ON events (room_id, source_channel_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
    END IF;
END $$;
-- A proactive delivery's key is looked up within its room, whatever channel
-- committed it (RFC §22.1).
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_events_room_idempotency')) IS NULL THEN
        CREATE INDEX idx_events_room_idempotency ON events (room_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
    END IF;
END $$;

-- Per-room high-water marks make indexes monotonic even when the latest
-- event is deleted. Every write keeps them since the table exists (an insert
-- observes its index, a commit reserves the next), so the backfill from the
-- rooms and events of a deployment that predates it runs in the boot that
-- creates it, and only then: run at every boot it wrote every row, waiting on
-- any commit holding one, and every commit then waited behind it.
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'event_sequences')) IS NULL THEN
        CREATE TABLE event_sequences (
            room_id    TEXT PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
            next_index INTEGER NOT NULL DEFAULT 0 CHECK (next_index >= 0)
        );
        INSERT INTO event_sequences(room_id, next_index)
        SELECT r.id,
               GREATEST(
                   CASE WHEN r.event_count > 0 THEN r.latest_index + 1 ELSE 0 END,
                   COALESCE(MAX(e.index) + 1, 0)
               )
        FROM rooms r
        LEFT JOIN events e ON e.room_id = r.id
        GROUP BY r.id, r.event_count, r.latest_index;
    END IF;
END $$;

-- bindings
CREATE TABLE IF NOT EXISTS bindings (
    channel_id    TEXT NOT NULL,
    room_id       TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    channel_type  TEXT NOT NULL,
    category      TEXT NOT NULL DEFAULT 'transport',
    direction     TEXT NOT NULL DEFAULT 'bidirectional',
    access        TEXT NOT NULL DEFAULT 'read_write',
    muted         BOOLEAN NOT NULL DEFAULT FALSE,
    output_muted  BOOLEAN NOT NULL DEFAULT FALSE,
    visibility    TEXT DEFAULT 'all',
    participant_id TEXT,
    last_read_index INTEGER,
    attached_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    capabilities  JSONB NOT NULL DEFAULT '{}',
    metadata      JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (room_id, channel_id)
);

-- Nullable policies preserve defaults for existing deployments, and retain
-- existing binding rows. A group binding admits every sender (RFC §10.4);
-- existing rows are not one. Not named "group": a reserved word in SQL.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'bindings' AND column_name = 'rate_limit') THEN
        ALTER TABLE bindings ADD COLUMN rate_limit JSONB;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'bindings' AND column_name = 'retry_policy') THEN
        ALTER TABLE bindings ADD COLUMN retry_policy JSONB;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'bindings' AND column_name = 'is_group') THEN
        ALTER TABLE bindings ADD COLUMN is_group BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;
END $$;
-- Attachment order survives updates, tied timestamps, and pool restarts.
-- Existing rows have no insertion ordinal: recover timestamp order, breaking
-- historical ties by room/channel id. New rows take a sequence value; an
-- upsert keeps its original value and a reattachment takes a new one.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'bindings'
        AND column_name = 'attachment_order') THEN
        ALTER TABLE bindings ADD COLUMN attachment_order BIGSERIAL;
        WITH ordered AS (
            SELECT room_id, channel_id,
                row_number() OVER (ORDER BY attached_at, room_id, channel_id) AS ordinal
            FROM bindings
        )
        UPDATE bindings b SET attachment_order = ordered.ordinal
        FROM ordered WHERE b.room_id = ordered.room_id AND b.channel_id = ordered.channel_id;
    END IF;
END $$;

-- Routing reads the binding of one channel naming one sender (RFC §10.4): on
-- a number shared by many correspondents every binding has the same channel.
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_bindings_channel_id')) IS NULL THEN
        CREATE INDEX idx_bindings_channel_id ON bindings (channel_id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_bindings_channel_participant')) IS NULL THEN
        CREATE INDEX idx_bindings_channel_participant ON bindings (channel_id, participant_id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_bindings_attachment_order')) IS NULL THEN
        CREATE INDEX idx_bindings_attachment_order ON bindings (room_id, attachment_order);
    END IF;
END $$;

-- participants
CREATE TABLE IF NOT EXISTS participants (
    id             TEXT NOT NULL,
    room_id        TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    channel_id     TEXT NOT NULL,
    display_name   TEXT,
    role           TEXT NOT NULL DEFAULT 'member',
    status         TEXT NOT NULL DEFAULT 'active',
    identification TEXT NOT NULL DEFAULT 'pending',
    identity_id    TEXT,
    external_id    TEXT,
    joined_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ,
    resolved_by    TEXT,
    metadata       JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (room_id, id)
);
-- The channels a participant has been reached through (RFC 5.5), primary
-- included and first. The UPDATE is an idempotent data repair for rows created
-- before the column existed, and for any old writer that stored an empty or
-- out-of-order list. It never discards a channel already recorded.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'participants' AND column_name = 'connected_via') THEN
        ALTER TABLE participants ADD COLUMN connected_via TEXT[] NOT NULL DEFAULT '{}';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'participants' AND column_name = 'candidates') THEN
        ALTER TABLE participants ADD COLUMN candidates TEXT[];
    END IF;
END $$;
UPDATE participants
SET connected_via = ARRAY[channel_id] || array_remove(connected_via, channel_id)
WHERE connected_via[1] IS DISTINCT FROM channel_id;
-- Routing reads the rooms a sender is a participant of, and names a
-- participant by its identity too (RFC §10.4 step 1).
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_participants_channel')) IS NULL THEN
        CREATE INDEX idx_participants_channel ON participants (room_id, channel_id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_participants_id')) IS NULL THEN
        CREATE INDEX idx_participants_id ON participants (id);
    END IF;
    IF to_regclass(format('%I.%I', current_schema(), 'idx_participants_identity')) IS NULL THEN
        CREATE INDEX idx_participants_identity ON participants (identity_id);
    END IF;
END $$;

-- identities
CREATE TABLE IF NOT EXISTS identities (
    id                TEXT PRIMARY KEY,
    organization_id   TEXT,
    display_name      TEXT,
    email             TEXT,
    channel_addresses JSONB NOT NULL DEFAULT '{}',
    metadata          JSONB NOT NULL DEFAULT '{}'
);

-- What an identity holds beside its addresses (RFC §5.6). Additive; an existing
-- row was stored without them, and reads as having none.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'identities' AND column_name = 'phone') THEN
        ALTER TABLE identities ADD COLUMN phone TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'identities' AND column_name = 'external_id') THEN
        ALTER TABLE identities ADD COLUMN external_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'identities' AND column_name = 'external_ids') THEN
        ALTER TABLE identities ADD COLUMN external_ids JSONB NOT NULL DEFAULT '{}';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS identity_addresses (
    channel_type    TEXT NOT NULL,
    address         TEXT NOT NULL,
    identity_id     TEXT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
    organization_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (channel_type, address, organization_id)
);

-- An address is unique WITHIN an organization, not globally (RFC §17.2): the
-- same number belongs to one person at one tenant and someone else at another.
-- Existing rows carry the empty string, the unscoped tenant, which preserves
-- what they resolved to before. The key is rebuilt only when it is still the
-- two-column one, so init() stays idempotent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'identity_addresses' AND column_name = 'organization_id') THEN
        ALTER TABLE identity_addresses ADD COLUMN organization_id TEXT NOT NULL DEFAULT '';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = to_regclass(format('%I.%I', current_schema(), 'identity_addresses'))
          AND i.indisprimary
          AND array_length(i.indkey::int[], 1) = 2
    ) THEN
        ALTER TABLE identity_addresses DROP CONSTRAINT identity_addresses_pkey;
        ALTER TABLE identity_addresses
            ADD PRIMARY KEY (channel_type, address, organization_id);
    END IF;
END $$;

-- Identity verification (RFC §11.7). Additive: new tables only. The record
-- itself is a JSONB document; the columns are what lookups filter on.
CREATE TABLE IF NOT EXISTS verifications (
    identity_id     TEXT NOT NULL,
    organization_id TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL,
    data            JSONB NOT NULL,
    PRIMARY KEY (identity_id, organization_id)
);

CREATE TABLE IF NOT EXISTS verification_requests (
    id              TEXT PRIMARY KEY,
    identity_id     TEXT NOT NULL,
    organization_id TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL,
    data            JSONB NOT NULL
);

-- tasks
CREATE TABLE IF NOT EXISTS tasks (
    id          TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    title       TEXT NOT NULL,
    description TEXT,
    assigned_to TEXT,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata    JSONB NOT NULL DEFAULT '{}'
);
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_tasks_room_id')) IS NULL THEN
        CREATE INDEX idx_tasks_room_id ON tasks (room_id);
    END IF;
END $$;

-- observations
CREATE TABLE IF NOT EXISTS observations (
    id          TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    channel_id  TEXT,
    content     TEXT NOT NULL,
    category    TEXT,
    confidence  DOUBLE PRECISION,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata    JSONB NOT NULL DEFAULT '{}'
);
-- A confidence reads as written: a REAL read 0.42 back as 0.41999998688697815.
-- Through its text, the shortest digits that name the single, so that a value
-- stored before reads as written too. Guarded on the column's type, so it runs
-- once and no later boot rewrites the table.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'observations'
          AND column_name = 'confidence'
          AND data_type = 'real'
    ) THEN
        ALTER TABLE observations ALTER COLUMN confidence TYPE DOUBLE PRECISION
            USING confidence::text::double precision;
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass(format('%I.%I', current_schema(), 'idx_observations_room_id')) IS NULL THEN
        CREATE INDEX idx_observations_room_id ON observations (room_id);
    END IF;
END $$;

-- read tracking
CREATE TABLE IF NOT EXISTS read_markers (
    room_id    TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    event_index INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, channel_id)
);

-- Proactive voice injection reservations, independent of queue acknowledgement.
CREATE TABLE IF NOT EXISTS voice_deliveries (
    room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    outcome JSONB,
    PRIMARY KEY (room_id, key_hash)
);

-- A room's private state (RFC §14.1), apart from its metadata, which clients
-- may read: the keys the runtime keeps for itself, the register of authors
-- among them (§10.1 step 12), whose digests name the room's senders to
-- whoever tries every phone number or name.
CREATE TABLE IF NOT EXISTS private_state (
    room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    key     TEXT NOT NULL,
    value   JSONB NOT NULL,
    PRIMARY KEY (room_id, key)
);

-- schema version
CREATE TABLE IF NOT EXISTS schema_version (
    version INT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_version (version)
    SELECT 2 WHERE NOT EXISTS (SELECT 1 FROM schema_version WHERE version >= 2);

-- An event's visibility is stamped at its commit (RFC §7.3): its own
-- intersected with its source binding's. An event stored 'all' before read its
-- source binding's when history was rebuilt; it takes that binding's now,
-- once, so the stored field is the whole answer and its readers stay the
-- same. A message held for verification records the visibility its release
-- restores; a recorded 'all' takes the binding's too. Version 3 records the
-- rewrite as done. The rooms first, in one order: a commit with a target holds
-- its room, then that event, and version 4 below writes rooms.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_version WHERE version >= 3) THEN
        PERFORM 1 FROM rooms WHERE id IN (
            SELECT e.room_id FROM events e JOIN bindings b
                ON b.room_id = e.room_id AND b.channel_id = e.source_channel_id
            WHERE COALESCE(b.visibility, 'all') <> 'all'
              AND (e.visibility = 'all'
                   OR e.metadata -> 'roomkit:verification_held' ->> 'visibility' = 'all')
        ) ORDER BY id FOR NO KEY UPDATE;
        UPDATE events e SET visibility = b.visibility
        FROM bindings b
        WHERE b.room_id = e.room_id
          AND b.channel_id = e.source_channel_id
          AND e.visibility = 'all'
          AND COALESCE(b.visibility, 'all') <> 'all';
        UPDATE events e SET metadata = jsonb_set(
            e.metadata, '{roomkit:verification_held,visibility}', to_jsonb(b.visibility))
        FROM bindings b
        WHERE b.room_id = e.room_id
          AND b.channel_id = e.source_channel_id
          AND e.metadata -> 'roomkit:verification_held' ->> 'visibility' = 'all'
          AND COALESCE(b.visibility, 'all') <> 'all';
        INSERT INTO schema_version (version) VALUES (3);
    END IF;
END $$;

-- A room's register of authors leaves its metadata for its private state, once;
-- one that is not an object is dropped, as its readers read it absent. The
-- metadata loses only the copy the table now holds: one a process from
-- before writes meanwhile, or wrote since a move, stays to be merged at the
-- room's next turn that names its sender (a register only adds), never
-- dropped with the ranks it gave. Version 4 records the move as done.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_version WHERE version >= 4) THEN
        INSERT INTO private_state (room_id, key, value)
            SELECT id, 'author_register', metadata -> 'author_register' FROM rooms
            WHERE jsonb_typeof(metadata -> 'author_register') = 'object'
            ON CONFLICT (room_id, key) DO NOTHING;
        UPDATE rooms r SET metadata = r.metadata - 'author_register'
            FROM private_state p
            WHERE p.room_id = r.id AND p.key = 'author_register'
              AND r.metadata -> 'author_register' = p.value;
        UPDATE rooms SET metadata = metadata - 'author_register'
            WHERE jsonb_typeof(metadata -> 'author_register') <> 'object';
        INSERT INTO schema_version (version) VALUES (4);
    END IF;
END $$;

-- An address the move to per-organization addresses left unscoped ('') for an
-- identity of an organization moves to that organization, once, if the identity
-- still declares it: a read serves an identity in its own organization only (RFC
-- §17.2), and its sender stays identified there. One whose organization holds the
-- address already yields to that row, whose identity keeps it. One the identity no
-- longer declares, a link since revoked, is not restored: it goes, and the address
-- is free. A row of another organization is left: no read serves it, and
-- a write of that organization takes it over. Version 5 records the rewrite as done.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_version WHERE version >= 5) THEN
        -- Whoever holds these identities first: an older process's unlink in flight
        -- has written the declaration read below.
        PERFORM 1 FROM identities i JOIN identity_addresses ia ON i.id = ia.identity_id
            WHERE ia.organization_id = '' AND COALESCE(i.organization_id, '') <> ''
            FOR SHARE OF i;
        DELETE FROM identity_addresses ia USING identities i, identity_addresses held
            WHERE i.id = ia.identity_id
              AND ia.organization_id = ''
              AND COALESCE(i.organization_id, '') <> ''
              AND held.channel_type = ia.channel_type
              AND held.address = ia.address
              AND held.organization_id = i.organization_id;
        UPDATE identity_addresses ia SET organization_id = i.organization_id
            FROM identities i
            WHERE i.id = ia.identity_id
              AND ia.organization_id = ''
              AND COALESCE(i.organization_id, '') <> ''
              AND jsonb_typeof(i.channel_addresses -> ia.channel_type) = 'array'
              AND i.channel_addresses -> ia.channel_type ? ia.address;
        DELETE FROM identity_addresses ia USING identities i
            WHERE i.id = ia.identity_id
              AND ia.organization_id = ''
              AND COALESCE(i.organization_id, '') <> '';
        INSERT INTO schema_version (version) VALUES (5);
    END IF;
END $$;
