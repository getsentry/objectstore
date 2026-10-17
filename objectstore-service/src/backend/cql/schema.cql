-- Run in the pre-created keyspace. Change the table name if table_name differs.
-- All mutations must use LWT. Do not write directly with ordinary INSERT/UPDATE.
CREATE TABLE IF NOT EXISTS objects (
    namespace text,
    path text,
    kind tinyint,
    revision uuid,
    expires_at bigint,
    metadata text,
    payload blob,
    redirect text,
    PRIMARY KEY ((namespace, path))
) WITH default_time_to_live = 0;
