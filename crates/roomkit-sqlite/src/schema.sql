
CREATE TABLE IF NOT EXISTS rooms(
    id TEXT PRIMARY KEY,
    created_ts REAL NOT NULL,
    organization_id TEXT,
    status TEXT NOT NULL,
    delivered_index INTEGER NOT NULL DEFAULT -1,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
    id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    type TEXT NOT NULL,
    visibility TEXT NOT NULL,
    source_channel_id TEXT,
    source_channel_type TEXT,
    participant_id TEXT,
    correlation_id TEXT,
    parent_event_id TEXT,
    idempotency_key TEXT,
    created_ts REAL NOT NULL,
    data TEXT NOT NULL,
    responds_to TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_room ON events(room_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_room_idx ON events(room_id, idx);
CREATE INDEX IF NOT EXISTS idx_events_room_parent ON events(room_id, parent_event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency_channel
    ON events(room_id, source_channel_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS event_sequences(
    room_id TEXT PRIMARY KEY,
    next_index INTEGER NOT NULL DEFAULT 0 CHECK(next_index >= 0)
);
CREATE TABLE IF NOT EXISTS bindings(
    room_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    participant_id TEXT,
    channel_type TEXT,
    data TEXT NOT NULL,
    PRIMARY KEY(room_id, channel_id)
);
CREATE INDEX IF NOT EXISTS idx_bindings_channel ON bindings(channel_id);
CREATE INDEX IF NOT EXISTS idx_bindings_channel_participant
    ON bindings(channel_id, participant_id);
CREATE TABLE IF NOT EXISTS participants(
    room_id TEXT NOT NULL,
    id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY(room_id, id)
);
CREATE INDEX IF NOT EXISTS idx_participants_id ON participants(id);
CREATE INDEX IF NOT EXISTS idx_participants_identity
    ON participants(json_extract(data, '$.identity_id'));
CREATE TABLE IF NOT EXISTS identities(
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS identity_addresses(
    channel_type TEXT NOT NULL,
    address TEXT NOT NULL,
    identity_id TEXT NOT NULL,
    organization_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(channel_type, address, organization_id)
);
CREATE TABLE IF NOT EXISTS verifications(
    identity_id TEXT NOT NULL,
    organization_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY(identity_id, organization_id)
);
CREATE TABLE IF NOT EXISTS verification_requests(
    id TEXT PRIMARY KEY,
    identity_id TEXT NOT NULL,
    organization_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks(
    id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL,
    status TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_room ON tasks(room_id);
CREATE TABLE IF NOT EXISTS observations(
    id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observations_room ON observations(room_id);
CREATE TABLE IF NOT EXISTS read_markers(
    room_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    PRIMARY KEY(room_id, channel_id)
);
CREATE TABLE IF NOT EXISTS voice_deliveries(
    room_id TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY(room_id, key_hash)
);
CREATE TABLE IF NOT EXISTS idempotency(
    room_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    key TEXT NOT NULL,
    PRIMARY KEY(room_id, channel_id, key)
);
CREATE INDEX IF NOT EXISTS idx_idempotency_room_key ON idempotency(room_id, key);
CREATE TABLE IF NOT EXISTS private_state(
    room_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(room_id, key)
);
CREATE INDEX IF NOT EXISTS idx_events_room_idempotency
    ON events(room_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE VIRTUAL TABLE IF NOT EXISTS events_fts
    USING fts5(text, event_id UNINDEXED, room_id UNINDEXED);
