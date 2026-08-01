CREATE TABLE IF NOT EXISTS ttl_keeper (
    object_id TEXT PRIMARY KEY,
    expiration_policy INTEGER NOT NULL DEFAULT 0,
    duration INTEGER,
    created_at INTEGER NOT NULL,
    expires_at INTEGER
);
