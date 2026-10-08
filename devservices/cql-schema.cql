-- Schema for the CQL backend (Apache Cassandra and ScyllaDB).
--
-- `NetworkTopologyStrategy` keeps this valid on both databases. On ScyllaDB, it
-- creates a tablets keyspace, which supports lightweight transactions from 2025.4.
CREATE KEYSPACE IF NOT EXISTS objectstore
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS objectstore.objectstore (
  key text PRIMARY KEY,
  kind tinyint,
  metadata blob,
  payload blob,
  redirect text,
  expires_at timestamp,
  rev uuid
);
