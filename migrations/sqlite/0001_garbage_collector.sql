CREATE TABLE IF NOT EXISTS garbage_collector (
    object_id TEXT NOT NULL PRIMARY KEY,
    expires_at INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_garbage_collector_expires_at
    ON garbage_collector (expires_at);
