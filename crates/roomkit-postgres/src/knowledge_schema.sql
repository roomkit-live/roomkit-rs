
CREATE TABLE IF NOT EXISTS knowledge_documents (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    room_id TEXT,
    source TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}',
    tsv tsvector,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_knowledge_tsv ON knowledge_documents USING GIN(tsv);
CREATE INDEX IF NOT EXISTS idx_knowledge_room ON knowledge_documents(room_id);
