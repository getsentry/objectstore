//! CQL backend for high-volume, low-latency storage of small objects.
//!
//! Works with both [Apache Cassandra] (5.0+) and [ScyllaDB] (2025.4+), using the
//! [`scylla`] driver.
//!
//! # Table Format
//!
//! Each object lives in its own partition, keyed by the object's storage path. A row holds
//! either an **object**, a redirect **tombstone**, or an **upload marker**, distinguished by
//! the `kind` column:
//!
//! | Column       | Type        | Content                                               |
//! |--------------|-------------|-------------------------------------------------------|
//! | `key`        | `text`      | Storage path (primary key)                            |
//! | `kind`       | `tinyint`   | `0` object, `1` tombstone, `2` upload marker          |
//! | `metadata`   | `blob`      | [`Metadata`] JSON (objects only)                      |
//! | `payload`    | `blob`      | Compressed payload bytes (objects only)               |
//! | `redirect`   | `text`      | Long-term storage path (tombstones only)              |
//! | `expires_at` | `timestamp` | Logical expiration deadline, `null` if not expiring   |
//! | `rev`        | `uuid`      | Revision, regenerated on every write                  |
//!
//! Every write sets all columns, using `null` for those that do not apply to the row's kind,
//! so that an overwrite never leaves stale cells behind. Upload markers use keys in a separate
//! `uploads` namespace, so they never collide with objects.
//!
//! # Conditional Writes
//!
//! All writes are lightweight transactions (LWT). CQL conditions cannot express the
//! disjunctions that tombstone-aware operations need, such as "absent, inline, or expired
//! tombstone". Instead, every conditional operation reads the row, checks its precondition
//! locally, and then writes with `IF rev = ?` on the revision it observed. A missing row
//! has a `null` revision, which the same condition matches. If another writer changed the
//! row in between, the write is not applied and the operation starts over.
//!
//! Writes are never mixed with non-LWT writes on the same row. The two use different
//! timestamp sources, so mixing them can reorder writes under clock skew.
//!
//! Rows are only ever written with `UPDATE`, never `INSERT`. An `INSERT` also writes a row
//! marker whose TTL can outlive the cells of later updates, leaving behind empty rows.
//!
//! # Expiration
//!
//! The `expires_at` column is authoritative: reads compare it against the operation's access
//! time. Additionally, rows are written with a TTL so the database can reclaim them. The TTL
//! extends an hour past the deadline to absorb clock skew between objectstore and the
//! database. Deadlines too far in the future for the database to represent are written
//! without a TTL; such rows still expire logically, but are only reclaimed once overwritten
//! or deleted.
//!
//! [Apache Cassandra]: https://cassandra.apache.org/
//! [ScyllaDB]: https://www.scylladb.com/

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::TryStreamExt;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;
use objectstore_types::time::Timestamp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::frame::types::{Consistency, SerialConsistency};
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::response::query_result::QueryResult;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlTimestamp, CqlValue, Row};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use crate::backend::common::{
    self, Backend, DeleteResponse, ExpiryUpdate, GetResponse, HighVolumeBackend, MetadataResponse,
    PutResponse, SetExpiryResponse, TieredGet, TieredMetadata, TieredUpdate, TieredWrite,
    Tombstone,
};
use crate::change_stream::{
    ChangeStream, ChangeStreamFactory, CostTrackerStreamConfig, flush_change_stream,
};
use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::id::ObjectId;
use crate::stream::{ChunkedBytes, ClientStream};

/// Configuration for [`CqlBackend`].
///
/// Stores objects in a CQL database, either [Apache Cassandra] 5.0+ or [ScyllaDB] 2025.4+.
///
/// **Note**: The keyspace and table must be created before starting the server. For example:
///
/// ```cql
/// CREATE KEYSPACE objectstore
///   WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
///
/// CREATE TABLE objectstore.objectstore (
///   key text PRIMARY KEY,
///   kind tinyint,
///   metadata blob,
///   payload blob,
///   redirect text,
///   expires_at timestamp,
///   rev uuid
/// );
/// ```
///
/// On ScyllaDB before 2025.4, lightweight transactions are not supported on tablets keyspaces;
/// create the keyspace with `AND tablets = {'enabled': false}` there.
///
/// [Apache Cassandra]: https://cassandra.apache.org/
/// [ScyllaDB]: https://www.scylladb.com/
///
/// # Example
///
/// ```yaml
/// storage:
///   type: cql
///   nodes: ["cassandra-1:9042", "cassandra-2:9042"]
///   keyspace: objectstore
///   table_name: objectstore
///   local_datacenter: us-east1
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CqlConfig {
    /// Contact points used to discover the cluster, as `host:port`.
    ///
    /// The driver discovers the remaining nodes of the cluster from these.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE=cql`
    /// - `OS__STORAGE__NODES=[cassandra-1:9042,cassandra-2:9042]`
    pub nodes: Vec<String>,

    /// Keyspace containing the table.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__KEYSPACE=objectstore`
    pub keyspace: String,

    /// Table name.
    ///
    /// The table must exist before starting the server.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TABLE_NAME=objectstore`
    pub table_name: String,

    /// Datacenter to prefer when routing requests.
    ///
    /// Requests go to replicas in this datacenter first. Set this in multi-datacenter
    /// deployments, together with local consistency levels.
    ///
    /// # Default
    ///
    /// `None` (all datacenters are treated equally)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__LOCAL_DATACENTER=us-east1` (optional)
    #[serde(default)]
    pub local_datacenter: Option<String>,

    /// Username for password authentication.
    ///
    /// # Default
    ///
    /// `None` (no authentication)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__USERNAME=objectstore` (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password for password authentication.
    ///
    /// Required if [`username`](Self::username) is set. Never serialized.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__PASSWORD=...` (optional)
    #[serde(default, skip_serializing)]
    pub password: Option<CqlPassword>,

    /// Encrypts connections with TLS.
    ///
    /// # Default
    ///
    /// `None` (plaintext connections)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TLS__CA_CERT=/etc/objectstore/ca.pem` (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<CqlTlsConfig>,

    /// Timeout for a single request, including the driver's retries.
    ///
    /// # Default
    ///
    /// `2s`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__REQUEST_TIMEOUT=2s`
    /// - `OS__STORAGE__HIGH_VOLUME__REQUEST_TIMEOUT=2s` (tiered storage)
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,

    /// Consistency level for reads and the commit phase of writes.
    ///
    /// # Default
    ///
    /// `local_quorum`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__CONSISTENCY=local_quorum`
    #[serde(default)]
    pub consistency: CqlConsistency,

    /// Consistency level for the Paxos phase of conditional writes.
    ///
    /// `local_serial` is only linearizable within one datacenter. Use `serial` if objects
    /// can be written concurrently from multiple datacenters.
    ///
    /// # Default
    ///
    /// `local_serial`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__SERIAL_CONSISTENCY=local_serial`
    #[serde(default)]
    pub serial_consistency: CqlSerialConsistency,

    /// Reports what this backend stores, for per-usecase cost attribution.
    ///
    /// # Default
    ///
    /// `None`, which disables reporting for this backend.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__COGS__SHARED_RESOURCE_ID=cql_objectstore`
    /// - `OS__STORAGE__COGS__SAMPLE_RATE=1.0` (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(2)
}

/// A password for [`CqlConfig`], redacted from debug output.
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct CqlPassword(String);

impl CqlPassword {
    /// Creates a password from its plaintext value.
    pub fn new(password: impl Into<String>) -> Self {
        Self(password.into())
    }
}

impl fmt::Debug for CqlPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

/// TLS settings for [`CqlConfig`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CqlTlsConfig {
    /// PEM file with the certificate authorities that sign the nodes' certificates.
    ///
    /// # Default
    ///
    /// `None` (uses the system's root certificates)
    pub ca_cert: Option<PathBuf>,

    /// PEM file with the client certificate chain, for mutual TLS.
    ///
    /// Must be set together with [`client_key`](Self::client_key).
    pub client_cert: Option<PathBuf>,

    /// PEM file with the client private key, for mutual TLS.
    pub client_key: Option<PathBuf>,

    /// Whether to check that node certificates are valid for the nodes' addresses.
    ///
    /// The driver connects to nodes by IP address, so with verification enabled, node
    /// certificates must list those IP addresses. Disabling this still verifies the
    /// certificate chain against [`ca_cert`](Self::ca_cert).
    ///
    /// # Default
    ///
    /// `true`
    #[serde(default = "default_verify_hostname")]
    pub verify_hostname: bool,
}

fn default_verify_hostname() -> bool {
    true
}

/// Consistency level of [`CqlConfig::consistency`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CqlConsistency {
    /// A single replica.
    One,
    /// A single replica in the local datacenter.
    LocalOne,
    /// A majority of all replicas.
    Quorum,
    /// A majority of replicas in the local datacenter.
    #[default]
    LocalQuorum,
    /// A majority of replicas in each datacenter.
    EachQuorum,
    /// All replicas.
    All,
}

impl From<CqlConsistency> for Consistency {
    fn from(value: CqlConsistency) -> Self {
        match value {
            CqlConsistency::One => Consistency::One,
            CqlConsistency::LocalOne => Consistency::LocalOne,
            CqlConsistency::Quorum => Consistency::Quorum,
            CqlConsistency::LocalQuorum => Consistency::LocalQuorum,
            CqlConsistency::EachQuorum => Consistency::EachQuorum,
            CqlConsistency::All => Consistency::All,
        }
    }
}

/// Serial consistency level of [`CqlConfig::serial_consistency`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CqlSerialConsistency {
    /// Paxos across all datacenters.
    Serial,
    /// Paxos within the local datacenter.
    #[default]
    LocalSerial,
}

impl From<CqlSerialConsistency> for SerialConsistency {
    fn from(value: CqlSerialConsistency) -> Self {
        match value {
            CqlSerialConsistency::Serial => SerialConsistency::Serial,
            CqlSerialConsistency::LocalSerial => SerialConsistency::LocalSerial,
        }
    }
}

/// How many times to retry a conditional write that lost a race before giving up.
const CAS_RETRY_COUNT: usize = 3;

/// How long a row outlives its deadline before the database reclaims it.
///
/// Reads enforce the deadline themselves, so this only needs to cover clock skew between
/// objectstore and the database, and operations with an `access_time` in the past.
const TTL_GRACE: Duration = Duration::from_hours(1);

/// The longest TTL that Cassandra and ScyllaDB accept: 20 years.
const MAX_TTL_SECS: u64 = 20 * 365 * 24 * 60 * 60;

/// The latest expiration time Cassandra can store with its default storage format, in Unix
/// seconds, minus a day of margin for clock skew.
///
/// Cassandra stores expiration times as 32-bit seconds unless `storage_compatibility_mode` is
/// `NONE`, and rejects writes that would expire after 2038-01-19.
const MAX_EXPIRATION_SECS: u64 = i32::MAX as u64 - 86_400;

/// `kind` of a row holding an object.
const KIND_OBJECT: i8 = 0;
/// `kind` of a row holding a redirect tombstone.
const KIND_TOMBSTONE: i8 = 1;
/// `kind` of a row holding an upload marker.
const KIND_UPLOAD: i8 = 2;

/// Prepared statements used by [`CqlBackend`].
struct Statements {
    /// Reads all columns except the payload.
    select_metadata: PreparedStatement,
    /// Reads all columns.
    select_full: PreparedStatement,
    /// Overwrites all columns if the revision matches.
    write: PreparedStatement,
    /// Deletes the row if the revision matches.
    delete_revision: PreparedStatement,
    /// Deletes the row if it exists.
    delete_existing: PreparedStatement,
    /// Deletes a live upload marker.
    delete_marker: PreparedStatement,
}

impl Statements {
    async fn prepare(session: &Session, table: &str) -> anyhow::Result<Self> {
        let mut select_metadata = session
            .prepare(format!(
                "SELECT kind, metadata, redirect, expires_at, rev FROM {table} WHERE key = ?"
            ))
            .await?;
        select_metadata.set_is_idempotent(true);

        let mut select_full = session
            .prepare(format!(
                "SELECT kind, metadata, redirect, expires_at, rev, payload FROM {table} WHERE key = ?"
            ))
            .await?;
        select_full.set_is_idempotent(true);

        let write = session
            .prepare(format!(
                "UPDATE {table} USING TTL ? \
                 SET kind = ?, metadata = ?, payload = ?, redirect = ?, expires_at = ?, rev = ? \
                 WHERE key = ? IF rev = ?"
            ))
            .await?;
        let delete_revision = session
            .prepare(format!("DELETE FROM {table} WHERE key = ? IF rev = ?"))
            .await?;
        let delete_existing = session
            .prepare(format!("DELETE FROM {table} WHERE key = ? IF EXISTS"))
            .await?;
        let delete_marker = session
            .prepare(format!(
                "DELETE FROM {table} WHERE key = ? IF kind = ? AND expires_at >= ?"
            ))
            .await?;

        Ok(Self {
            select_metadata,
            select_full,
            write,
            delete_revision,
            delete_existing,
            delete_marker,
        })
    }
}

/// The contents of a row, interpreted according to its `kind`.
enum Entry {
    /// An object with its metadata and payload.
    ///
    /// The payload is empty if the row was read without it.
    Object { metadata: Metadata, payload: Bytes },
    /// A redirect tombstone pointing to the long-term backend.
    Tombstone(Tombstone),
    /// A marker for an ongoing resumable upload.
    UploadMarker,
}

/// A row read from the table, regardless of whether it has expired.
struct StoredRow {
    entry: Entry,
    expires_at: Option<Timestamp>,
    rev: Uuid,
}

/// The columns read by [`Statements::select_metadata`].
type MetadataColumns = (
    Option<i8>,
    Option<Bytes>,
    Option<String>,
    Option<CqlTimestamp>,
    Option<Uuid>,
);

/// The columns read by [`Statements::select_full`].
type FullColumns = (
    Option<i8>,
    Option<Bytes>,
    Option<String>,
    Option<CqlTimestamp>,
    Option<Uuid>,
    Option<Bytes>,
);

impl StoredRow {
    /// Interprets raw columns, returning `None` for a row without a revision.
    ///
    /// Only rows written by this backend are expected; every write sets a revision.
    fn from_columns(columns: FullColumns) -> Result<Option<Self>> {
        let (kind, metadata, redirect, expires_at, rev, payload) = columns;
        let Some(rev) = rev else {
            return Ok(None);
        };

        let expires_at = expires_at.map(from_cql_timestamp).transpose()?;
        let entry = match kind {
            Some(KIND_OBJECT) => {
                let mut metadata: Metadata = match metadata {
                    Some(json) => serde_json::from_slice(&json)
                        .context(ErrorKind::CorruptData, "decoding CQL object metadata")?,
                    None => Metadata::default(),
                };
                metadata.time_expires = expires_at;
                Entry::Object {
                    metadata,
                    payload: payload.unwrap_or_default(),
                }
            }
            Some(KIND_TOMBSTONE) => {
                let path = redirect.unwrap_or_default();
                let target = ObjectId::from_storage_path(&path).ok_or_else(|| {
                    Error::new(ErrorKind::CorruptData, "parsing CQL redirect target")
                })?;
                Entry::Tombstone(Tombstone {
                    target,
                    time_expires: expires_at,
                })
            }
            Some(KIND_UPLOAD) => Entry::UploadMarker,
            _ => return Err(Error::new(ErrorKind::CorruptData, "unknown CQL row kind")),
        };

        Ok(Some(Self {
            entry,
            expires_at,
            rev,
        }))
    }

    /// Returns `true` if the row's deadline is strictly earlier than `access_time`.
    fn is_expired(&self, access_time: Timestamp) -> bool {
        self.expires_at
            .is_some_and(|deadline| deadline < access_time)
    }

    /// Returns the tombstone in this row if it is still live at `access_time`.
    fn live_tombstone(&self, access_time: Timestamp) -> Option<&Tombstone> {
        match &self.entry {
            Entry::Tombstone(tombstone) if !self.is_expired(access_time) => Some(tombstone),
            _ => None,
        }
    }
}

/// The values written by [`Statements::write`], apart from the key and revisions.
struct NewRow {
    kind: i8,
    metadata: Option<Bytes>,
    payload: Option<Bytes>,
    redirect: Option<String>,
    expires_at: Option<Timestamp>,
}

impl NewRow {
    /// Builds an object row, recording the payload size in the metadata.
    fn object(metadata: &Metadata, payload: Bytes) -> Result<Self> {
        let mut metadata = metadata.clone();
        metadata.size = Some(payload.len());
        let json = serde_json::to_vec(&metadata)
            .context(ErrorKind::Internal, "encoding CQL object metadata")?;

        Ok(Self {
            kind: KIND_OBJECT,
            metadata: Some(json.into()),
            payload: Some(payload),
            redirect: None,
            expires_at: metadata.time_expires,
        })
    }

    /// Builds a tombstone row.
    fn tombstone(tombstone: &Tombstone) -> Self {
        Self {
            kind: KIND_TOMBSTONE,
            metadata: None,
            payload: None,
            redirect: Some(tombstone.target.as_storage_path().to_string()),
            expires_at: tombstone.time_expires,
        }
    }

    /// Builds an upload marker row.
    fn upload_marker(time_expires: Timestamp) -> Self {
        Self {
            kind: KIND_UPLOAD,
            metadata: None,
            payload: None,
            redirect: None,
            expires_at: Some(time_expires),
        }
    }

    /// Approximates the bytes the row occupies, as its key plus every value written.
    ///
    /// Does not include the database's own overhead.
    fn size(&self, key: &str) -> u64 {
        let values = self.metadata.as_ref().map_or(0, Bytes::len)
            + self.payload.as_ref().map_or(0, Bytes::len)
            + self.redirect.as_ref().map_or(0, String::len);
        (key.len() + values) as u64
    }
}

/// Converts a [`Timestamp`] into a CQL `timestamp` value.
fn to_cql_timestamp(timestamp: Timestamp) -> CqlTimestamp {
    // INVARIANT: The maximum timestamp in milliseconds fits easily into an i64.
    CqlTimestamp(timestamp.as_secs() as i64 * 1000)
}

/// Converts a CQL `timestamp` value into a [`Timestamp`], rounding up to whole seconds.
fn from_cql_timestamp(timestamp: CqlTimestamp) -> Result<Timestamp> {
    timestamp
        .0
        .checked_mul(1000)
        .and_then(|micros| Timestamp::from_unix_micros(micros).ok())
        .ok_or_else(|| Error::new(ErrorKind::CorruptData, "decoding CQL expiration timestamp"))
}

/// Returns the TTL in seconds for a row expiring at `deadline`, or `0` for no TTL.
///
/// The TTL extends [`TTL_GRACE`] past the deadline and is at least one second, since a TTL of
/// zero disables expiration. Deadlines beyond what the database can store get no TTL.
fn ttl_secs(deadline: Option<Timestamp>, now: Timestamp) -> i32 {
    let Some(deadline) = deadline else {
        return 0;
    };

    let remaining = deadline.checked_duration_since(now).unwrap_or_default();
    let ttl = remaining.saturating_add(TTL_GRACE).as_secs().max(1);
    if ttl > MAX_TTL_SECS || now.as_secs().saturating_add(ttl) > MAX_EXPIRATION_SECS {
        return 0;
    }

    // INVARIANT: `ttl` is at most `MAX_TTL_SECS`, which fits into an i32.
    ttl as i32
}

/// Reads the `[applied]` column of a conditional statement's result.
///
/// Cassandra and ScyllaDB return different additional columns, so only the first column
/// is inspected.
fn applied(result: QueryResult) -> Result<bool> {
    let row = result
        .into_rows_result()
        .context(ErrorKind::BackendFailure, "reading CQL conditional result")?
        .first_row::<Row>()
        .context(ErrorKind::BackendFailure, "reading CQL conditional result")?;

    match row.columns.first() {
        Some(Some(CqlValue::Boolean(applied))) => Ok(*applied),
        _ => Err(Error::new(
            ErrorKind::BackendFailure,
            "missing [applied] column in CQL conditional result",
        )),
    }
}

/// Classifies a driver error into an [`ErrorKind`].
fn error_kind(error: &ExecutionError) -> ErrorKind {
    match error {
        ExecutionError::RequestTimeout(_) => ErrorKind::BackendTimeout,
        ExecutionError::EmptyPlan | ExecutionError::ConnectionPoolError(_) => {
            ErrorKind::BackendUnavailable
        }
        ExecutionError::LastAttemptError(attempt) => match attempt {
            RequestAttemptError::DbError(db_error, _) => match db_error {
                DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. } => {
                    ErrorKind::BackendTimeout
                }
                DbError::Unavailable { .. } | DbError::IsBootstrapping => {
                    ErrorKind::BackendUnavailable
                }
                DbError::Overloaded | DbError::RateLimitReached { .. } => {
                    ErrorKind::BackendRateLimited
                }
                _ => ErrorKind::BackendFailure,
            },
            RequestAttemptError::BrokenConnectionError(_)
            | RequestAttemptError::UnableToAllocStreamId => ErrorKind::BackendUnavailable,
            _ => ErrorKind::BackendFailure,
        },
        _ => ErrorKind::BackendFailure,
    }
}

/// Checks that `name` is a plain CQL identifier that can be interpolated into statements.
fn validate_identifier(name: &str, what: &str) -> anyhow::Result<()> {
    let mut chars = name.chars();
    let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= 48;
    anyhow::ensure!(
        valid,
        "invalid CQL {what} {name:?}: must be a letter followed by up to 47 letters, digits, or underscores"
    );
    Ok(())
}

/// Builds the rustls client configuration from [`CqlTlsConfig`].
fn tls_client_config(config: &CqlTlsConfig) -> anyhow::Result<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    match &config.ca_cert {
        Some(path) => {
            for cert in CertificateDer::pem_file_iter(path)? {
                roots.add(cert?)?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            let (added, _ignored) = roots.add_parsable_certificates(native.certs);
            anyhow::ensure!(added > 0, "no usable system root certificates found");
        }
    }

    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;

    let roots = Arc::new(roots);
    let builder = if config.verify_hostname {
        builder.with_root_certificates(roots)
    } else {
        let inner =
            rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider).build()?;
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(IgnoreHostnameVerifier(inner)))
    };

    let client_config = match (&config.client_cert, &config.client_key) {
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
            builder.with_client_auth_cert(chain, PrivateKeyDer::from_pem_file(key)?)?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => anyhow::bail!("CQL TLS client_cert and client_key must be set together"),
    };

    Ok(Arc::new(client_config))
}

/// Verifies server certificates against trusted roots, ignoring the server name.
#[derive(Debug)]
struct IgnoreHostnameVerifier(Arc<rustls::client::WebPkiServerVerifier>);

impl rustls::client::danger::ServerCertVerifier for IgnoreHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error as TlsError};

        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(TlsError::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(rustls::client::danger::ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// CQL storage backend for high-volume, low-latency object storage.
pub struct CqlBackend {
    session: Session,
    statements: Statements,
    table: String,
    change_stream: Arc<dyn ChangeStream>,
}

impl fmt::Debug for CqlBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CqlBackend")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl CqlBackend {
    /// Creates a new [`CqlBackend`] from the given `config`.
    ///
    /// Connects to the cluster and prepares all statements, which fails if the keyspace or
    /// table does not exist.
    pub async fn new(config: CqlConfig, streams: &ChangeStreamFactory) -> anyhow::Result<Self> {
        let CqlConfig {
            nodes,
            keyspace,
            table_name,
            local_datacenter,
            username,
            password,
            tls,
            request_timeout,
            consistency,
            serial_consistency,
            cogs,
        } = config;

        validate_identifier(&keyspace, "keyspace")?;
        validate_identifier(&table_name, "table name")?;
        anyhow::ensure!(!nodes.is_empty(), "at least one CQL node is required");

        let mut load_balancing = DefaultPolicy::builder().token_aware(true);
        if let Some(datacenter) = local_datacenter {
            load_balancing = load_balancing.prefer_datacenter(datacenter);
        }
        let profile = ExecutionProfile::builder()
            .consistency(consistency.into())
            .serial_consistency(Some(serial_consistency.into()))
            .request_timeout(Some(request_timeout))
            .load_balancing_policy(load_balancing.build())
            .build();

        let mut builder = SessionBuilder::new()
            .known_nodes(&nodes)
            .default_execution_profile_handle(profile.into_handle());
        match (username, password) {
            (Some(username), Some(CqlPassword(password))) => {
                builder = builder.user(username, password);
            }
            (None, None) => {}
            _ => anyhow::bail!("CQL username and password must be set together"),
        }
        if let Some(tls) = tls {
            builder = builder.tls_context(Some(tls_client_config(&tls)?));
        }

        let session = builder.build().await?;
        let table = format!("{keyspace}.{table_name}");
        let statements = Statements::prepare(&session, &table).await?;

        Ok(Self {
            session,
            statements,
            table,
            change_stream: streams.build(cogs.as_ref()),
        })
    }

    /// Executes a prepared statement, recording failures.
    async fn execute(
        &self,
        statement: &PreparedStatement,
        values: impl SerializeRow,
        action: &'static str,
    ) -> Result<QueryResult> {
        let span = tracing::debug_span!("cql.request", action);
        let result = self
            .session
            .execute_unpaged(statement, values)
            .instrument(span)
            .await;

        result.map_err(|error| {
            objectstore_metrics::count!("cql.failures", action = action);
            let kind = error_kind(&error);
            Error::with_context(kind, format!("running CQL {action}"), error)
        })
    }

    /// Reads the row at `key`, including expired rows.
    ///
    /// Skips the payload unless `with_payload` is set.
    #[tracing::instrument(level = "debug", fields(action), skip_all)]
    async fn read_row(
        &self,
        key: &str,
        with_payload: bool,
        action: &'static str,
    ) -> Result<Option<StoredRow>> {
        let columns = if with_payload {
            let result = self
                .execute(&self.statements.select_full, (key,), action)
                .await?;
            result
                .into_rows_result()
                .context(ErrorKind::BackendFailure, "reading CQL row")?
                .maybe_first_row::<FullColumns>()
                .context(ErrorKind::BackendFailure, "reading CQL row")?
        } else {
            let result = self
                .execute(&self.statements.select_metadata, (key,), action)
                .await?;
            result
                .into_rows_result()
                .context(ErrorKind::BackendFailure, "reading CQL row")?
                .maybe_first_row::<MetadataColumns>()
                .context(ErrorKind::BackendFailure, "reading CQL row")?
                .map(|(kind, metadata, redirect, expires_at, rev)| {
                    (kind, metadata, redirect, expires_at, rev, None)
                })
        };

        match columns {
            Some(columns) => StoredRow::from_columns(columns),
            None => Ok(None),
        }
    }

    /// Writes `row` at `key` if the row's revision is still `observed`.
    ///
    /// `observed = None` requires the row to be absent. Returns whether the write was applied.
    ///
    /// The outcome of a failed write is unknown, as it may have been applied before the error.
    /// The row is then read back: if it carries the revision of this write, the write is
    /// reported as applied; otherwise the original error is returned.
    async fn write_if(
        &self,
        key: &str,
        observed: Option<Uuid>,
        row: &NewRow,
        action: &'static str,
    ) -> Result<bool> {
        let rev = Uuid::new_v4();
        let values = (
            ttl_secs(row.expires_at, Timestamp::now()),
            row.kind,
            &row.metadata,
            &row.payload,
            &row.redirect,
            row.expires_at.map(to_cql_timestamp),
            rev,
            key,
            observed,
        );

        match self.execute(&self.statements.write, values, action).await {
            Ok(result) => applied(result),
            Err(error) => match self.read_row(key, false, action).await {
                Ok(Some(current)) if current.rev == rev => Ok(true),
                _ => Err(error),
            },
        }
    }

    /// Deletes the row at `key` if its revision is still `observed`.
    ///
    /// Returns whether the delete was applied. If the delete fails and the row is absent
    /// afterwards, it is reported as applied.
    async fn delete_if(&self, key: &str, observed: Uuid, action: &'static str) -> Result<bool> {
        let statement = &self.statements.delete_revision;
        match self.execute(statement, (key, observed), action).await {
            Ok(result) => applied(result),
            Err(error) => match self.read_row(key, false, action).await {
                Ok(None) => Ok(true),
                _ => Err(error),
            },
        }
    }
}

/// Error for an upload marker found where an object or tombstone was expected.
fn unexpected_upload_marker() -> Error {
    Error::new(
        ErrorKind::CorruptData,
        "found CQL upload marker in object namespace",
    )
}

#[async_trait::async_trait]
impl Backend for CqlBackend {
    fn name(&self) -> &'static str {
        "cql"
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        mut stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to CQL backend");
        let key = id.as_storage_path().to_string();

        let mut payload = ChunkedBytes::new(0);
        while let Some(chunk) = stream.try_next().await? {
            payload.push(chunk);
        }
        let row = NewRow::object(metadata, payload.into_bytes())?;

        for _ in 0..CAS_RETRY_COUNT {
            let observed = self.read_row(&key, false, "put").await?;
            if self
                .write_if(&key, observed.map(|row| row.rev), &row, "put")
                .await?
            {
                self.change_stream
                    .write(id, row.size(&key), metadata.time_expires);
                return Ok(());
            }
        }

        Err(Error::new(ErrorKind::Internal, "CQL put race exhausted"))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        match self.get_tiered_object(id, access_time, range).await? {
            TieredGet::Object(metadata, content_range, payload) => {
                Ok(Some((metadata, content_range, payload)))
            }
            TieredGet::Tombstone(_) => Err(ErrorKind::UnexpectedTombstone.into()),
            TieredGet::NotFound => Ok(None),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        match self.get_tiered_metadata(id, access_time).await? {
            TieredMetadata::Object(metadata) => Ok(Some(metadata)),
            TieredMetadata::Tombstone(_) => Err(ErrorKind::UnexpectedTombstone.into()),
            TieredMetadata::NotFound => Ok(None),
        }
    }

    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        self.compare_and_update(id, None, TieredUpdate::SetExpiry(target), access_time)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from CQL backend");
        let key = id.as_storage_path().to_string();

        let statement = &self.statements.delete_existing;
        if let Err(error) = self.execute(statement, (&key,), "delete").await {
            // The delete may have been applied before the error; it succeeded if the row is gone.
            if !matches!(self.read_row(&key, false, "delete").await, Ok(None)) {
                return Err(error);
            }
        }
        self.change_stream.delete(id);

        Ok(())
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

#[async_trait::async_trait]
impl HighVolumeBackend for CqlBackend {
    async fn create_upload_marker(
        &self,
        revision: &ObjectId,
        time_expires: Timestamp,
    ) -> Result<()> {
        let key = revision.as_upload_path().to_string();
        let row = NewRow::upload_marker(time_expires);
        if !self
            .write_if(&key, None, &row, "create_upload_marker")
            .await?
        {
            return Err(Error::new(
                ErrorKind::Internal,
                "CQL upload marker already exists",
            ));
        }
        Ok(())
    }

    async fn has_upload_marker(&self, revision: &ObjectId, access_time: Timestamp) -> Result<bool> {
        let key = revision.as_upload_path().to_string();
        let row = self.read_row(&key, false, "has_upload_marker").await?;
        Ok(row.is_some_and(|row| {
            matches!(row.entry, Entry::UploadMarker) && !row.is_expired(access_time)
        }))
    }

    async fn delete_upload_marker(
        &self,
        revision: &ObjectId,
        access_time: Timestamp,
    ) -> Result<bool> {
        let key = revision.as_upload_path().to_string();
        let values = (&key, KIND_UPLOAD, to_cql_timestamp(access_time));
        let result = self
            .execute(
                &self.statements.delete_marker,
                values,
                "delete_upload_marker",
            )
            .await?;
        applied(result)
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        objectstore_log::debug!("Conditional put to CQL backend");
        let key = id.as_storage_path().to_string();
        let row = NewRow::object(metadata, payload)?;

        for _ in 0..CAS_RETRY_COUNT {
            let observed = self.read_row(&key, false, "put_non_tombstone").await?;
            if let Some(tombstone) = observed
                .as_ref()
                .and_then(|row| row.live_tombstone(access_time))
            {
                return Ok(Some(tombstone.clone()));
            }

            let rev = observed.map(|row| row.rev);
            if self.write_if(&key, rev, &row, "put_non_tombstone").await? {
                self.change_stream
                    .write(id, row.size(&key), metadata.time_expires);
                return Ok(None);
            }
        }

        Err(Error::new(ErrorKind::Internal, "CQL put race exhausted"))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<TieredGet> {
        objectstore_log::debug!("Reading from CQL backend");
        let key = id.as_storage_path().to_string();

        let row = self.read_row(&key, true, "get_tiered_object").await?;
        let Some(row) = row.filter(|row| !row.is_expired(access_time)) else {
            return Ok(TieredGet::NotFound);
        };

        Ok(match row.entry {
            Entry::Object {
                mut metadata,
                payload,
            } => {
                if metadata.size.is_none() {
                    metadata.size = Some(payload.len());
                }
                let (content_range, payload) = common::apply_range(payload, range)?;
                TieredGet::Object(metadata, content_range, crate::stream::single(payload))
            }
            Entry::Tombstone(tombstone) => TieredGet::Tombstone(tombstone),
            Entry::UploadMarker => return Err(unexpected_upload_marker()),
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_tiered_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<TieredMetadata> {
        objectstore_log::debug!("Reading metadata from CQL backend");
        let key = id.as_storage_path().to_string();

        let row = self.read_row(&key, false, "get_tiered_metadata").await?;
        let Some(row) = row.filter(|row| !row.is_expired(access_time)) else {
            return Ok(TieredMetadata::NotFound);
        };

        Ok(match row.entry {
            Entry::Object { metadata, .. } => TieredMetadata::Object(metadata),
            Entry::Tombstone(tombstone) => TieredMetadata::Tombstone(tombstone),
            Entry::UploadMarker => return Err(unexpected_upload_marker()),
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_non_tombstone(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        objectstore_log::debug!("Conditional delete from CQL backend");
        let key = id.as_storage_path().to_string();

        for _ in 0..CAS_RETRY_COUNT {
            // Expired rows are deleted too, to reclaim their storage right away.
            let Some(observed) = self.read_row(&key, false, "delete_non_tombstone").await? else {
                return Ok(None);
            };
            if let Some(tombstone) = observed.live_tombstone(access_time) {
                return Ok(Some(tombstone.clone()));
            }

            if self
                .delete_if(&key, observed.rev, "delete_non_tombstone")
                .await?
            {
                self.change_stream.delete(id);
                return Ok(None);
            }
        }

        Err(Error::new(ErrorKind::Internal, "CQL delete race exhausted"))
    }

    #[tracing::instrument(level = "debug", skip(self, write))]
    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
        access_time: Timestamp,
    ) -> Result<bool> {
        objectstore_log::debug!("CAS put to CQL backend");
        let key = id.as_storage_path().to_string();

        let row = match &write {
            TieredWrite::Tombstone(tombstone) => Some(NewRow::tombstone(tombstone)),
            TieredWrite::Object(metadata, payload) => {
                Some(NewRow::object(metadata, payload.clone())?)
            }
            TieredWrite::Delete => None,
        };

        for _ in 0..CAS_RETRY_COUNT {
            let observed = self.read_row(&key, false, "compare_and_write").await?;
            let live_target = observed
                .as_ref()
                .and_then(|row| row.live_tombstone(access_time))
                .map(|tombstone| &tombstone.target);

            // The write proceeds if the row is in the expected state, or already in the state
            // the write would produce. This makes retries of the same operation succeed.
            let matches = match (current, write.target()) {
                (Some(old), Some(new)) => live_target.is_some_and(|t| t == old || t == new),
                (Some(target), None) | (None, Some(target)) => {
                    live_target.is_none_or(|t| t == target)
                }
                (None, None) => live_target.is_none(),
            };
            if !matches {
                return Ok(false);
            }

            let observed_rev = observed.map(|row| row.rev);
            let applied = match (&row, observed_rev) {
                (Some(row), rev) => self.write_if(&key, rev, row, "compare_and_write").await?,
                (None, Some(rev)) => self.delete_if(&key, rev, "compare_and_write").await?,
                // Deleting an absent row: nothing to do.
                (None, None) => return Ok(true),
            };

            if applied {
                match &row {
                    Some(row) => self.change_stream.write(id, row.size(&key), row.expires_at),
                    None => self.change_stream.delete(id),
                }
                return Ok(true);
            }
        }

        Err(Error::new(ErrorKind::Internal, "CQL CAS race exhausted"))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn compare_and_update(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        update: TieredUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let TieredUpdate::SetExpiry(expiry_target) = update;
        let key = id.as_storage_path().to_string();

        // Inline extension needs metadata and payload from the same read, since the TTL of
        // every cell has to be rewritten.
        let row = self.read_row(&key, true, "set_expiry").await?;
        let Some(row) = row.filter(|row| !row.is_expired(access_time)) else {
            return Ok(SetExpiryResponse::NotFound);
        };

        let (expire_at, new_row) = match row.entry {
            Entry::Object { metadata, payload } => {
                if current.is_some() {
                    return Ok(SetExpiryResponse::Rejected); // wrong row kind
                }
                let Some(old_expiry) = metadata.time_expires else {
                    return Ok(SetExpiryResponse::Rejected);
                };
                let Some(expire_at) = expiry_target.resolve(metadata.time_created, access_time)?
                else {
                    return Ok(SetExpiryResponse::Rejected);
                };
                if old_expiry >= expire_at {
                    return Ok(SetExpiryResponse::Satisfied(expire_at)); // already satisfied
                }

                let mut metadata = metadata;
                metadata.expiration_policy = common::extended_expiration_policy(
                    metadata.expiration_policy,
                    metadata.time_created,
                    old_expiry,
                    expire_at,
                )?;
                metadata.time_expires = Some(expire_at);
                (expire_at, NewRow::object(&metadata, payload)?)
            }
            Entry::Tombstone(tombstone) => {
                let Some(expected) = current else {
                    return Ok(SetExpiryResponse::Rejected); // wrong row kind
                };
                let Some(old_expiry) = tombstone.time_expires else {
                    return Ok(SetExpiryResponse::Rejected);
                };
                if tombstone.target != *expected {
                    return Ok(SetExpiryResponse::Rejected); // wrong target
                }
                let Some(expire_at) = expiry_target.resolve(None, access_time)? else {
                    return Ok(SetExpiryResponse::Rejected);
                };
                if old_expiry >= expire_at {
                    return Ok(SetExpiryResponse::Satisfied(expire_at)); // already satisfied
                }

                let tombstone = Tombstone {
                    target: tombstone.target,
                    time_expires: Some(expire_at),
                };
                (expire_at, NewRow::tombstone(&tombstone))
            }
            Entry::UploadMarker => return Err(unexpected_upload_marker()),
        };

        let applied = self
            .write_if(&key, Some(row.rev), &new_row, "set_expiry")
            .await?;

        Ok(if applied {
            self.change_stream.update(id, Some(expire_at));
            SetExpiryResponse::Satisfied(expire_at)
        } else {
            SetExpiryResponse::Rejected
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::{OpType, test_utils::DummyProducer};
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::ExpiryTarget;
    use crate::id::ObjectContext;
    use crate::stream;

    // NB: Most of these tests require a Cassandra or ScyllaDB node with the devservices schema.
    // CI runs them against both. Set `OS_TEST_CQL_NODES` to point them at a different node.

    fn test_config() -> CqlConfig {
        let nodes = std::env::var("OS_TEST_CQL_NODES").unwrap_or_else(|_| "localhost:8088".into());
        CqlConfig {
            nodes: nodes.split(',').map(str::to_owned).collect(),
            keyspace: "objectstore".into(),
            table_name: "objectstore".into(),
            local_datacenter: None,
            username: None,
            password: None,
            tls: None,
            request_timeout: Duration::from_secs(10),
            consistency: CqlConsistency::default(),
            serial_consistency: CqlSerialConsistency::default(),
            cogs: None,
        }
    }

    async fn create_test_backend() -> Result<CqlBackend> {
        CqlBackend::new(test_config(), &ChangeStreamFactory::default()).await
    }

    #[cfg(feature = "storage-cogs")]
    async fn create_test_backend_with_change_stream() -> Result<(CqlBackend, DummyProducer)> {
        let (streams, producer) = crate::change_stream::dummy_factory();
        let config = CqlConfig {
            cogs: Some(CostTrackerStreamConfig {
                shared_resource_id: "cql_objectstore".into(),
                sample_rate: 1.0,
            }),
            ..test_config()
        };

        Ok((CqlBackend::new(config, &streams).await?, producer))
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    /// Overwrites the row at `key` regardless of its current state.
    async fn overwrite(backend: &CqlBackend, key: &str, row: &NewRow) -> Result<()> {
        let observed = backend.read_row(key, false, "test-setup").await?;
        let applied = backend
            .write_if(key, observed.map(|row| row.rev), row, "test-setup")
            .await?;
        anyhow::ensure!(applied, "test setup lost a race");
        Ok(())
    }

    async fn create_object(
        backend: &CqlBackend,
        id: &ObjectId,
        metadata: &Metadata,
        payload: &[u8],
        now: Timestamp,
    ) -> Result<()> {
        // Resolve `time_expires` from `now` (as `from_insert_headers` does) unless the test set
        // it explicitly, so the row has an expiration to persist.
        let mut metadata = metadata.clone();
        if metadata.time_expires.is_none() {
            metadata.time_expires = metadata.expiration_policy.expires_in().map(|ttl| now + ttl);
        }
        let row = NewRow::object(&metadata, Bytes::copy_from_slice(payload))?;
        overwrite(backend, &id.as_storage_path().to_string(), &row).await
    }

    async fn create_tombstone(
        backend: &CqlBackend,
        id: &ObjectId,
        tombstone: &Tombstone,
    ) -> Result<()> {
        let row = NewRow::tombstone(tombstone);
        overwrite(backend, &id.as_storage_path().to_string(), &row).await
    }

    /// Reads the remaining TTL of the `rev` column, which every row sets.
    async fn read_ttl(backend: &CqlBackend, key: &str) -> Result<Option<i32>> {
        let query = format!("SELECT TTL(rev) FROM {} WHERE key = ?", backend.table);
        let result = backend.session.query_unpaged(query, (key,)).await?;
        Ok(result.into_rows_result()?.single_row::<(Option<i32>,)>()?.0)
    }

    // --- Section 1: Object Operations ---

    /// Verifies the full roundtrip: put → get_object (payload + metadata) → get_metadata (metadata).
    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            time_created: Some(Timestamp::now()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let (obj_meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(payload, b"hello, world");
        assert_eq!(obj_meta.content_type, metadata.content_type);
        assert_eq!(obj_meta.custom, metadata.custom);
        assert_eq!(obj_meta.size, Some(payload.len()));

        let head_meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
        assert_eq!(head_meta.content_type, metadata.content_type);
        assert_eq!(head_meta.custom, metadata.custom);
        assert_eq!(head_meta.size, Some(payload.len()));

        Ok(())
    }

    /// Verifies that a server-resolved `time_expires` is persisted verbatim, not recomputed.
    #[tokio::test]
    async fn test_time_expires_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let ttl = Duration::from_hours(2 * 24);
        let expires = Timestamp::now() + ttl;
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(ttl),
            time_expires: Some(expires),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"data", Timestamp::now()).await?;

        let meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
        assert_eq!(meta.time_expires, Some(expires));

        Ok(())
    }

    /// Verifies that absent rows return None or succeed silently for all read/delete operations.
    #[tokio::test]
    async fn test_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );
        assert!(backend.get_metadata(&id, Timestamp::now()).await?.is_none());
        backend.delete_object(&id, Timestamp::now()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let first_metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };
        create_object(&backend, &id, &first_metadata, b"hello", Timestamp::now()).await?;

        let second_metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };
        backend
            .put_object(
                &id,
                &second_metadata,
                stream::single("world"),
                Timestamp::now(),
            )
            .await?;

        let (meta, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(payload, b"world");
        assert_eq!(meta.custom, second_metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();
        create_object(&backend, &id, &metadata, b"hello", Timestamp::now()).await?;
        backend.delete_object(&id, Timestamp::now()).await?;

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        Ok(())
    }

    /// Backend reads are side-effect-free; explicit extension preserves payload.
    #[tokio::test]
    async fn test_set_expiry() -> Result<()> {
        for is_ttl in [false, true] {
            let backend = create_test_backend().await?;
            let tti = Duration::from_hours(2 * 24);
            let mut metadata = Metadata {
                expiration_policy: if is_ttl {
                    ExpirationPolicy::TimeToLive(tti)
                } else {
                    ExpirationPolicy::TimeToIdle(tti)
                },
                ..Default::default()
            };

            // Backdate `now` so the written expiry (past_now + tti) is stale but not expired.
            let past_now = Timestamp::now() - tti + Duration::from_mins(1);

            let id = make_id();
            metadata.time_created = Some(past_now);
            metadata.time_expires = Some(past_now + tti);
            create_object(&backend, &id, &metadata, b"hello, world", past_now).await?;

            let (observed, _, _) = backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .unwrap();
            let observed_expiry = observed.time_expires.unwrap();
            assert_eq!(
                backend
                    .get_metadata(&id, Timestamp::now())
                    .await?
                    .unwrap()
                    .time_expires,
                Some(observed_expiry),
                "backend reads must not renew TTI"
            );

            let requested = past_now + tti + tti;
            for target in [
                ExpiryTarget::At(requested),
                ExpiryTarget::FromCreation(tti + tti),
            ] {
                assert_eq!(
                    backend
                        .set_expiry(&id, target.into(), Timestamp::now())
                        .await?,
                    SetExpiryResponse::Satisfied(requested)
                );
            }
            assert_eq!(
                backend
                    .get_metadata(&id, Timestamp::now())
                    .await?
                    .unwrap()
                    .time_expires,
                Some(requested)
            );
            let (updated, _, stream) = backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .unwrap();
            assert_eq!(
                updated.expiration_policy,
                if is_ttl {
                    ExpirationPolicy::TimeToLive(tti + tti)
                } else {
                    ExpirationPolicy::TimeToIdle(tti)
                }
            );
            let payload = stream::read_to_vec(stream).await?;
            assert_eq!(payload, b"hello, world");

            // The extension also extends the TTL of every cell, including the payload.
            let key = id.as_storage_path().to_string();
            let ttl = read_ttl(&backend, &key)
                .await?
                .expect("row must have a TTL");
            assert!(u64::try_from(ttl)? > tti.as_secs());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_expiry_outcomes() -> Result<()> {
        let backend = create_test_backend().await?;
        let access_time = Timestamp::now();
        let deadline = access_time + Duration::from_hours(1);
        for (expiry, expected) in [
            (None, SetExpiryResponse::Rejected),
            (
                Some(access_time - Duration::from_secs(1)),
                SetExpiryResponse::NotFound,
            ),
            (Some(deadline), SetExpiryResponse::Rejected),
        ] {
            let id = make_id();
            let metadata = Metadata {
                time_expires: expiry,
                ..Default::default()
            };
            create_object(&backend, &id, &metadata, b"payload", access_time).await?;
            assert_eq!(
                backend
                    .set_expiry(
                        &id,
                        ExpiryTarget::FromCreation(Duration::from_hours(2)).into(),
                        access_time
                    )
                    .await?,
                expected
            );
        }
        Ok(())
    }

    /// A conditional write based on a stale revision must not overwrite a newer row.
    #[tokio::test]
    async fn test_expiry_conflict() -> Result<()> {
        let backend = create_test_backend().await?;
        let missing = make_id();
        assert_eq!(
            backend
                .set_expiry(
                    &missing,
                    ExpiryTarget::At(Timestamp::now() + Duration::from_hours(2)).into(),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::NotFound
        );

        let id = make_id();
        let key = id.as_storage_path().to_string();
        let observed_expiry = Timestamp::now() + Duration::from_hours(1);
        let original = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(observed_expiry),
            ..Default::default()
        };
        create_object(&backend, &id, &original, b"original", Timestamp::now()).await?;
        let observed = backend.read_row(&key, false, "test").await?.unwrap();

        let mut extended = original.clone();
        extended.time_expires = Some(observed_expiry + Duration::from_hours(1));
        let extension = NewRow::object(&extended, Bytes::from_static(b"original"))?;

        let mut replacement = original.clone();
        replacement.time_expires = Some(observed_expiry + Duration::from_mins(1));
        create_object(
            &backend,
            &id,
            &replacement,
            b"replacement",
            Timestamp::now(),
        )
        .await?;
        assert!(
            !backend
                .write_if(&key, Some(observed.rev), &extension, "test-expiry-conflict")
                .await?
        );
        let (_, _, payload) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(stream::read_to_vec(payload).await?, b"replacement");
        Ok(())
    }

    #[tokio::test]
    async fn test_redirect_expiry() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let target = ObjectId::random(id.context().clone());
        let wrong_target = ObjectId::random(id.context().clone());
        let old_expiry = Timestamp::now() + Duration::from_hours(1);
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: Some(old_expiry),
            },
        )
        .await?;

        let later = old_expiry + Duration::from_hours(2);
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&wrong_target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(later).into()),
                    Timestamp::now(),
                )
                .await?,
            SetExpiryResponse::Rejected
        );
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(later).into()),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::Satisfied(later)
        );
        let requested = old_expiry + Duration::from_mins(30);
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(requested).into()),
                    Timestamp::now(),
                )
                .await?,
            SetExpiryResponse::Satisfied(requested)
        );
        let TieredMetadata::Tombstone(tombstone) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(tombstone.time_expires, Some(later));
        Ok(())
    }

    // --- Section 2: Expiration ---

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world", Timestamp::now()).await?;

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world", Timestamp::now()).await?;

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        Ok(())
    }

    /// Rows get a TTL past their deadline, and none if they do not expire.
    #[tokio::test]
    async fn test_row_ttl() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let ttl = Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(ttl),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"data", Timestamp::now()).await?;
        let row_ttl = read_ttl(&backend, &id.as_storage_path().to_string())
            .await?
            .expect("expiring rows must have a TTL");
        let expected = (ttl + TTL_GRACE).as_secs();
        let row_ttl = u64::try_from(row_ttl)?;
        assert!(
            (expected - 10..=expected).contains(&row_ttl),
            "TTL {row_ttl} must cover the deadline plus grace"
        );

        let id = make_id();
        create_object(
            &backend,
            &id,
            &Metadata::default(),
            b"data",
            Timestamp::now(),
        )
        .await?;
        assert_eq!(
            read_ttl(&backend, &id.as_storage_path().to_string()).await?,
            None,
            "rows without expiration must not have a TTL"
        );

        Ok(())
    }

    /// Deadlines the database cannot represent are stored without a TTL, but still expire.
    #[tokio::test]
    async fn test_distant_deadline() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let deadline = Timestamp::now() + Duration::from_hours(30 * 365 * 24);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(30 * 365 * 24)),
            time_expires: Some(deadline),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"data", Timestamp::now()).await?;

        let key = id.as_storage_path().to_string();
        assert_eq!(read_ttl(&backend, &key).await?, None);
        let meta = backend.get_metadata(&id, Timestamp::now()).await?.unwrap();
        assert_eq!(meta.time_expires, Some(deadline));
        let after_deadline = deadline + Duration::from_secs(1);
        assert!(backend.get_metadata(&id, after_deadline).await?.is_none());

        Ok(())
    }

    #[test]
    fn ttl_computation() {
        let now = Timestamp::from_unix_secs(1_800_000_000).unwrap();
        let grace = TTL_GRACE.as_secs() as i32;

        assert_eq!(ttl_secs(None, now), 0);
        assert_eq!(ttl_secs(Some(now), now), grace);
        assert_eq!(
            ttl_secs(Some(now + Duration::from_secs(60)), now),
            grace + 60
        );
        // Past deadlines still get a TTL; zero would disable expiration.
        assert_eq!(
            ttl_secs(Some(now - Duration::from_hours(2)), now),
            grace,
            "past deadlines expire after the grace period"
        );

        // Too far in the future for Cassandra's 32-bit expiration times.
        let limit = Timestamp::from_unix_secs(MAX_EXPIRATION_SECS).unwrap();
        let within = limit - TTL_GRACE - Duration::from_secs(1);
        assert!(ttl_secs(Some(within), now) > 0);
        assert_eq!(ttl_secs(Some(limit), now), 0);

        // Too long for the maximum TTL, even when the expiration time is representable.
        let early = Timestamp::from_unix_secs(1_000_000).unwrap();
        let too_long = early + Duration::from_secs(MAX_TTL_SECS);
        assert_eq!(ttl_secs(Some(too_long), early), 0);
    }

    #[test]
    fn timestamp_conversion() {
        let timestamp = Timestamp::from_unix_secs(1_800_000_000).unwrap();
        let cql = to_cql_timestamp(timestamp);
        assert_eq!(cql.0, 1_800_000_000_000);
        assert_eq!(from_cql_timestamp(cql).unwrap(), timestamp);

        // Fractional seconds round up, like all timestamps.
        let fractional = CqlTimestamp(1_800_000_000_001);
        assert_eq!(
            from_cql_timestamp(fractional).unwrap(),
            timestamp + Duration::from_secs(1)
        );
        assert!(from_cql_timestamp(CqlTimestamp(-1)).is_err());
    }

    // --- Section 3: Tiered Operations ---

    /// Covers all three row states for `get_tiered_object` and `get_tiered_metadata`.
    ///
    /// - **empty**: both return NotFound.
    /// - **object**: put_object, both return the Object variant with correct payload/metadata.
    /// - **tombstone**: CAS-write with a distinct `lt_id`, both return the Tombstone variant
    ///   with `target == lt_id`.
    #[tokio::test]
    async fn test_tiered_get() -> Result<()> {
        let backend = create_test_backend().await?;

        // empty
        let id = make_id();
        assert!(matches!(
            backend
                .get_tiered_object(&id, Timestamp::now(), None)
                .await?,
            TieredGet::NotFound
        ));
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::NotFound
        ));

        // object
        let id = make_id();
        let put_meta = Metadata {
            content_type: "text/plain".into(),
            custom: BTreeMap::from_iter([("k".into(), "v".into())]),
            ..Default::default()
        };
        create_object(&backend, &id, &put_meta, b"payload", Timestamp::now()).await?;

        let TieredGet::Object(obj_meta, _, obj_stream) = backend
            .get_tiered_object(&id, Timestamp::now(), None)
            .await?
        else {
            panic!("expected TieredGet::Object");
        };
        let obj_payload = stream::read_to_vec(obj_stream).await?;
        assert_eq!(obj_payload, b"payload");
        assert_eq!(obj_meta.content_type, put_meta.content_type);
        assert_eq!(obj_meta.custom, put_meta.custom);

        let TieredMetadata::Object(head_meta) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected TieredMetadata::Object");
        };
        assert_eq!(head_meta.content_type, put_meta.content_type);
        assert_eq!(head_meta.custom, put_meta.custom);

        // tombstone
        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let tombstone = Tombstone {
            target: lt_id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &hv_id, &tombstone).await?;

        match backend
            .get_tiered_object(&hv_id, Timestamp::now(), None)
            .await?
        {
            TieredGet::Tombstone(get_t) => assert_eq!(get_t.target, lt_id),
            other => panic!("expected TieredGet::Tombstone, got {other:?}"),
        }
        match backend
            .get_tiered_metadata(&hv_id, Timestamp::now())
            .await?
        {
            TieredMetadata::Tombstone(meta_t) => assert_eq!(meta_t.target, lt_id),
            other => panic!("expected TieredMetadata::Tombstone, got {other:?}"),
        }

        Ok(())
    }

    /// Covers all three row states for `put_non_tombstone`.
    ///
    /// - **empty**: returns None, object is readable.
    /// - **object**: overwrites with new payload, returns None.
    /// - **tombstone**: returns Some(Tombstone) with the correct target; tombstone still intact.
    #[tokio::test]
    async fn test_put_non_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        // empty: put_non_tombstone on absent row succeeds and makes object readable.
        let id = make_id();
        let metadata = Metadata::default();
        let result = backend
            .put_non_tombstone(
                &id,
                &metadata,
                Bytes::from_static(b"first"),
                Timestamp::now(),
            )
            .await?;
        assert_eq!(result, None, "expected None on empty row");
        let (_, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"first");

        // object: put_non_tombstone on existing object replaces payload, returns None.
        let id = make_id();
        create_object(&backend, &id, &metadata, b"old", Timestamp::now()).await?;
        let result = backend
            .put_non_tombstone(&id, &metadata, Bytes::from_static(b"new"), Timestamp::now())
            .await?;
        assert_eq!(result, None, "expected None when overwriting object");
        let (_, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"new");

        // tombstone: put_non_tombstone returns Some(Tombstone) and leaves tombstone intact.
        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let tombstone = Tombstone {
            target: lt_id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &hv_id, &tombstone).await?;
        let result = backend
            .put_non_tombstone(&hv_id, &metadata, Bytes::new(), Timestamp::now())
            .await?;
        let returned = result.expect("expected Some(Tombstone) when row is a tombstone");
        assert_eq!(returned.target, lt_id);
        assert!(
            matches!(
                backend
                    .get_tiered_metadata(&hv_id, Timestamp::now())
                    .await?,
                TieredMetadata::Tombstone(_)
            ),
            "tombstone must still exist after put_non_tombstone"
        );

        Ok(())
    }

    /// Covers all three row states for `delete_non_tombstone`.
    ///
    /// - **empty**: returns None.
    /// - **object**: returns None, row gone.
    /// - **tombstone**: returns Some(Tombstone) with correct target; tombstone still intact.
    #[tokio::test]
    async fn test_delete_non_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        // empty
        let id = make_id();
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None
        );

        // object
        let id = make_id();
        let metadata = Metadata::default();
        create_object(&backend, &id, &metadata, b"hello, world", Timestamp::now()).await?;
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None
        );
        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        // tombstone
        let id = make_id();
        let tombstone = Tombstone {
            target: id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &id, &tombstone).await?;
        let tombstone = backend
            .delete_non_tombstone(&id, Timestamp::now())
            .await?
            .expect("expected Some(tombstone)");
        assert_eq!(tombstone.target, id, "tombstone target must be returned");
        assert!(
            matches!(
                backend.get_tiered_metadata(&id, Timestamp::now()).await?,
                TieredMetadata::Tombstone(_)
            ),
            "tombstone must still exist after delete_non_tombstone"
        );

        Ok(())
    }

    /// Upload markers are live until their deadline and can be consumed exactly once.
    #[tokio::test]
    async fn test_upload_markers() -> Result<()> {
        let backend = create_test_backend().await?;

        let revision = make_id();
        let now = Timestamp::now();
        let deadline = now + Duration::from_hours(1);
        assert!(!backend.has_upload_marker(&revision, now).await?);
        assert!(!backend.delete_upload_marker(&revision, now).await?);

        backend.create_upload_marker(&revision, deadline).await?;
        assert!(backend.has_upload_marker(&revision, now).await?);
        assert!(backend.has_upload_marker(&revision, deadline).await?);
        let after_deadline = deadline + Duration::from_secs(1);
        assert!(!backend.has_upload_marker(&revision, after_deadline).await?);
        assert!(
            !backend
                .delete_upload_marker(&revision, after_deadline)
                .await?,
            "expired markers must not be consumed"
        );

        // Markers live outside the object namespace.
        assert!(matches!(
            backend.get_tiered_metadata(&revision, now).await?,
            TieredMetadata::NotFound
        ));

        assert!(backend.delete_upload_marker(&revision, now).await?);
        assert!(!backend.delete_upload_marker(&revision, now).await?);
        assert!(!backend.has_upload_marker(&revision, now).await?);

        Ok(())
    }

    // --- Section 4: Compare-and-Write ---

    /// Creating a tombstone on an empty row succeeds; a retry of the same CAS also succeeds.
    #[tokio::test]
    async fn test_cas_create_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let time_expires = Some(Timestamp::now() + Duration::from_hours(1));
        let tombstone = Tombstone {
            target: lt_id.clone(),
            time_expires,
        };

        // First create succeeds.
        let committed = backend
            .compare_and_write(
                &hv_id,
                None,
                TieredWrite::Tombstone(tombstone.clone()),
                Timestamp::now(),
            )
            .await?;
        assert!(committed, "expected CAS success on empty row");

        // Tiered reads must see the tombstone with the correct target and deadline.
        let TieredMetadata::Tombstone(t) = backend
            .get_tiered_metadata(&hv_id, Timestamp::now())
            .await?
        else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(t.target, lt_id, "target must round-trip");
        assert_eq!(t.time_expires, time_expires);
        match backend
            .get_tiered_object(&hv_id, Timestamp::now(), None)
            .await?
        {
            TieredGet::Tombstone(t) => assert_eq!(t.target, lt_id, "target must round-trip"),
            other => panic!("expected TieredGet::Tombstone, got {other:?}"),
        }

        // Non-tiered reads must error rather than leak tombstone data.
        assert!(
            backend
                .get_object(&hv_id, Timestamp::now(), None)
                .await
                .is_err_and(|error| error.kind() == ErrorKind::UnexpectedTombstone)
        );
        assert!(
            backend
                .get_metadata(&hv_id, Timestamp::now())
                .await
                .is_err_and(|error| error.kind() == ErrorKind::UnexpectedTombstone)
        );

        // Idempotent retry: retry with the same target succeeds
        let second = backend
            .compare_and_write(
                &hv_id,
                None,
                TieredWrite::Tombstone(tombstone),
                Timestamp::now(),
            )
            .await?;
        assert!(second, "idempotent retry");

        Ok(())
    }

    /// Swapping a tombstone target: wrong expected → false, correct expected → true.
    #[tokio::test]
    async fn test_cas_swap_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let hv_id = make_id();
        let old_lt_id = ObjectId::random(hv_id.context().clone());
        let wrong_lt_id = ObjectId::random(hv_id.context().clone());
        let new_lt_id = ObjectId::random(hv_id.context().clone());

        let tombstone = Tombstone {
            target: old_lt_id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &hv_id, &tombstone).await?;

        // Wrong target: CAS fails, tombstone unchanged.
        let write = TieredWrite::Tombstone(Tombstone {
            target: new_lt_id.clone(),
            time_expires: None,
        });
        let swapped = backend
            .compare_and_write(&hv_id, Some(&wrong_lt_id), write.clone(), Timestamp::now())
            .await?;
        assert!(!swapped, "expected CAS failure due to wrong target");
        match backend
            .get_tiered_metadata(&hv_id, Timestamp::now())
            .await?
        {
            TieredMetadata::Tombstone(t) => assert_eq!(t.target, old_lt_id),
            other => panic!("expected tombstone, got {other:?}"),
        }

        // Correct target: CAS succeeds, target updated.
        let swapped = backend
            .compare_and_write(&hv_id, Some(&old_lt_id), write.clone(), Timestamp::now())
            .await?;
        assert!(swapped, "expected CAS success with correct target");
        match backend
            .get_tiered_metadata(&hv_id, Timestamp::now())
            .await?
        {
            TieredMetadata::Tombstone(t) => assert_eq!(t.target, new_lt_id),
            other => panic!("expected tombstone, got {other:?}"),
        }

        // Idempotent retry: same A→B swap returns true.
        let retry = backend
            .compare_and_write(&hv_id, Some(&old_lt_id), write, Timestamp::now())
            .await?;
        assert!(retry, "idempotent retry");

        Ok(())
    }

    /// Swapping a tombstone for inline object data: wrong expected → false, correct → true.
    #[tokio::test]
    async fn test_cas_swap_inline() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let lt_id = ObjectId::random(id.context().clone());
        let wrong_id = ObjectId::random(id.context().clone());

        let tombstone = Tombstone {
            target: lt_id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &id, &tombstone).await?;

        // Wrong target: CAS fails, tombstone intact.
        let write = TieredWrite::Object(Metadata::default(), Bytes::new());
        let swapped = backend
            .compare_and_write(&id, Some(&wrong_id), write, Timestamp::now())
            .await?;
        assert!(!swapped, "expected CAS failure with wrong target");
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        // Correct target: CAS succeeds, row becomes an inline object.
        let payload = Bytes::from_static(b"hello inline");
        let write = TieredWrite::Object(Metadata::default(), payload.clone());
        let swapped = backend
            .compare_and_write(&id, Some(&lt_id), write.clone(), Timestamp::now())
            .await?;
        assert!(swapped, "expected CAS success with correct target");
        let TieredGet::Object(_, _, stream) = backend
            .get_tiered_object(&id, Timestamp::now(), None)
            .await?
        else {
            panic!("expected inline object after swap");
        };
        assert_eq!(&stream::read_to_vec(stream).await?, payload.as_ref());

        // Idempotent retry: row is already inline (no tombstone), same CAS returns true.
        let retry = backend
            .compare_and_write(&id, Some(&lt_id), write, Timestamp::now())
            .await?;
        assert!(retry, "idempotent retry");

        Ok(())
    }

    /// CAS-write an object onto an empty row (expected=None, write=Object) succeeds.
    #[tokio::test]
    async fn test_cas_create_object_on_empty_row() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let payload = Bytes::from_static(b"cas object");
        let write = TieredWrite::Object(Metadata::default(), payload.clone());
        let committed = backend
            .compare_and_write(&id, None, write, Timestamp::now())
            .await?;
        assert!(committed, "expected CAS success on empty row");

        let TieredGet::Object(_, _, stream) = backend
            .get_tiered_object(&id, Timestamp::now(), None)
            .await?
        else {
            panic!("expected Object after CAS-create");
        };
        assert_eq!(&stream::read_to_vec(stream).await?, payload.as_ref());

        Ok(())
    }

    /// CAS-delete: wrong expected → false; correct expected → true, row gone.
    #[tokio::test]
    async fn test_cas_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let lt_id = ObjectId::random(id.context().clone());
        let wrong_id = ObjectId::random(id.context().clone());

        let tombstone = Tombstone {
            target: lt_id.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &id, &tombstone).await?;

        // Wrong target: fails, row preserved.
        let deleted = backend
            .compare_and_write(&id, Some(&wrong_id), TieredWrite::Delete, Timestamp::now())
            .await?;
        assert!(!deleted, "expected CAS failure with wrong target");
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        // Correct target: succeeds, row gone.
        let deleted = backend
            .compare_and_write(&id, Some(&lt_id), TieredWrite::Delete, Timestamp::now())
            .await?;
        assert!(deleted, "expected CAS delete success");
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::NotFound
        ));

        // Idempotent retry: row is already absent (no tombstone), same delete returns true.
        let retry = backend
            .compare_and_write(&id, Some(&lt_id), TieredWrite::Delete, Timestamp::now())
            .await?;
        assert!(retry, "idempotent retry");

        // Inline object replaced tombstone: Safe to delete since it is an idempotent operation.
        let id2 = make_id();
        let fake_lt_id = ObjectId::random(id2.context().clone());
        let metadata = Metadata::default();
        create_object(&backend, &id2, &metadata, b"data", Timestamp::now()).await?;
        let deleted = backend
            .compare_and_write(
                &id2,
                Some(&fake_lt_id),
                TieredWrite::Delete,
                Timestamp::now(),
            )
            .await?;
        assert!(deleted, "expected idempotent deletion");

        Ok(())
    }

    /// Concurrent tombstone creations on the same row: exactly one wins.
    #[tokio::test]
    async fn test_cas_concurrent_single_winner() -> Result<()> {
        let backend = Arc::new(create_test_backend().await?);
        let id = make_id();

        let tasks = (0..8).map(|_| {
            let backend = Arc::clone(&backend);
            let id = id.clone();
            tokio::spawn(async move {
                let tombstone = Tombstone {
                    target: ObjectId::random(id.context().clone()),
                    time_expires: None,
                };
                let target = tombstone.target.clone();
                let written = backend
                    .compare_and_write(
                        &id,
                        None,
                        TieredWrite::Tombstone(tombstone),
                        Timestamp::now(),
                    )
                    .await;
                (target, written)
            })
        });

        let mut winners = Vec::new();
        for task in futures::future::join_all(tasks).await {
            let (target, written) = task?;
            // Losers may exhaust their retries under contention, which is not a win either.
            if written.unwrap_or(false) {
                winners.push(target);
            }
        }

        assert_eq!(winners.len(), 1, "exactly one writer must win");
        let TieredMetadata::Tombstone(tombstone) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(tombstone.target, winners[0]);

        Ok(())
    }

    /// Overwriting a row with a different kind leaves no stale columns behind.
    #[tokio::test]
    async fn test_overwrite_clears_columns() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let key = id.as_storage_path().to_string();
        create_object(
            &backend,
            &id,
            &Metadata::default(),
            b"data",
            Timestamp::now(),
        )
        .await?;
        let lt_id = ObjectId::random(id.context().clone());
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: lt_id,
                time_expires: None,
            },
        )
        .await?;

        let query = format!(
            "SELECT metadata, payload FROM {} WHERE key = ?",
            backend.table
        );
        let result = backend.session.query_unpaged(query, (&key,)).await?;
        let (metadata, payload) = result
            .into_rows_result()?
            .single_row::<(Option<Bytes>, Option<Bytes>)>()?;
        assert_eq!(metadata, None);
        assert_eq!(payload, None);

        Ok(())
    }

    // --- Section 5: Expired Tombstone Handling ---

    /// CAS with `current=None` must succeed when the row holds an expired
    /// tombstone. The physical row still exists but is logically gone.
    #[tokio::test]
    async fn test_cas_create_tombstone_over_expired() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let old_lt_id = ObjectId::random(id.context().clone());
        let old_tombstone = Tombstone {
            target: old_lt_id,
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
        };
        create_tombstone(&backend, &id, &old_tombstone).await?;

        let new_lt_id = ObjectId::random(id.context().clone());
        let new_tombstone = Tombstone {
            target: new_lt_id.clone(),
            time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
        };
        let committed = backend
            .compare_and_write(
                &id,
                None,
                TieredWrite::Tombstone(new_tombstone),
                Timestamp::now(),
            )
            .await?;
        assert!(
            committed,
            "CAS with current=None must succeed over an expired tombstone"
        );

        let TieredMetadata::Tombstone(t) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected new tombstone to be readable");
        };
        assert_eq!(t.target, new_lt_id);

        Ok(())
    }

    /// `put_non_tombstone` must succeed when the row holds only an expired
    /// tombstone — the expired row is logically absent.
    #[tokio::test]
    async fn test_put_non_tombstone_over_expired() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let lt_id = ObjectId::random(id.context().clone());
        let tombstone = Tombstone {
            target: lt_id,
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
        };
        create_tombstone(&backend, &id, &tombstone).await?;

        let result = backend
            .put_non_tombstone(
                &id,
                &Metadata::default(),
                Bytes::from_static(b"data"),
                Timestamp::now(),
            )
            .await?;
        assert_eq!(
            result, None,
            "put_non_tombstone must succeed (return None) over an expired tombstone"
        );

        let (_, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"data");

        Ok(())
    }

    // --- Range Request Tests ---

    async fn put_range_test_object(backend: &CqlBackend) -> Result<ObjectId> {
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            ..Default::default()
        };
        let payload = b"Hello, range requests!";
        backend
            .put_object(
                &id,
                &metadata,
                stream::single(payload.as_slice()),
                Timestamp::now(),
            )
            .await?;
        Ok(id)
    }

    #[tokio::test]
    async fn get_object_range_bounded() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Bounded(7, 11)))
            .await?
            .unwrap();
        let data = stream::read_to_vec(stream).await?;
        assert_eq!(&data, b"range");

        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 11);
        assert_eq!(content_range.total, 22);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_from() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::From(7)))
            .await?
            .unwrap();
        let data = stream::read_to_vec(stream).await?;
        assert_eq!(&data, b"range requests!");

        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 21);
        assert_eq!(content_range.total, 22);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_last() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Last(9)))
            .await?
            .unwrap();
        let data = stream::read_to_vec(stream).await?;
        assert_eq!(&data, b"requests!");

        let content_range = content_range.unwrap();
        assert_eq!(content_range.start, 13);
        assert_eq!(content_range.end, 21);
        assert_eq!(content_range.total, 22);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_unsatisfiable() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        match backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::From(100)))
            .await
        {
            Err(error) if matches!(error.kind(), ErrorKind::RangeNotSatisfiable { total: 22 }) => {}
            Ok(_) => panic!("expected RangeNotSatisfiable, got Ok"),
            Err(e) => panic!("expected RangeNotSatisfiable, got {e:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn get_object_no_range_returns_full_payload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .unwrap();
        let data = stream::read_to_vec(stream).await?;
        assert_eq!(&data, b"Hello, range requests!");
        assert!(content_range.is_none());

        Ok(())
    }

    // --- Configuration ---

    #[test]
    fn identifier_validation() {
        assert!(validate_identifier("objectstore", "table").is_ok());
        assert!(validate_identifier("object_store_2", "table").is_ok());
        assert!(validate_identifier("", "table").is_err());
        assert!(validate_identifier("2objects", "table").is_err());
        assert!(validate_identifier("objects; DROP TABLE x", "table").is_err());
        assert!(validate_identifier("ks.table", "table").is_err());
        assert!(validate_identifier(&"a".repeat(49), "table").is_err());
    }

    #[test]
    fn row_size_counts_the_key_and_every_value() {
        let key = "attachments/org.1/objects/abc";
        let row = NewRow::object(&Metadata::default(), Bytes::from_static(b"0123456789")).unwrap();

        // `NewRow::object` stamps the size into the metadata before serializing it.
        let stamped = Metadata {
            size: Some(10),
            ..Default::default()
        };
        let metadata_len = serde_json::to_vec(&stamped).unwrap().len();
        assert_eq!(row.size(key), (key.len() + 10 + metadata_len) as u64);

        let tombstone = NewRow::tombstone(&Tombstone {
            target: ObjectId::from_storage_path("attachments/org.1/objects/abc/0199").unwrap(),
            time_expires: None,
        });
        assert!(tombstone.size(key) > key.len() as u64);
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/cql-tls")
            .join(name)
    }

    #[test]
    fn tls_config() {
        let mut config = CqlTlsConfig {
            ca_cert: Some(fixture("ca.pem")),
            client_cert: None,
            client_key: None,
            verify_hostname: true,
        };
        tls_client_config(&config).unwrap();

        config.verify_hostname = false;
        tls_client_config(&config).unwrap();

        config.client_cert = Some(fixture("client.pem"));
        assert!(
            tls_client_config(&config).is_err(),
            "a client certificate requires a key"
        );
        config.client_key = Some(fixture("client.key"));
        tls_client_config(&config).unwrap();

        config.ca_cert = Some(fixture("missing.pem"));
        assert!(tls_client_config(&config).is_err());
    }

    #[test]
    fn password_is_redacted() {
        let password = CqlPassword::new("hunter2");
        assert_eq!(format!("{password:?}"), "[redacted]");
    }

    // --- Change Stream ---

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_writes_and_deletes() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(3600)),
            time_expires: Some(Timestamp::now() + Duration::from_secs(3600)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<crate::stream::ClientError>(b"hello".to_vec()),
                Timestamp::now(),
            )
            .await?;
        backend.delete_object(&id, Timestamp::now()).await?;

        let records = producer.records();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].op_type, OpType::Write);
        assert_eq!(records[0].app_feature, "testing");
        assert_eq!(records[0].shared_resource_id, "cql_objectstore");
        // Key plus payload plus metadata, so strictly more than the payload alone.
        assert!(records[0].size.unwrap() > b"hello".len() as u64);
        assert!(records[0].expiration_time.is_some());

        assert_eq!(records[1].op_type, OpType::Delete);
        assert_eq!(records[1].size, None);
        assert_eq!(records[1].record_id, records[0].record_id);

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn delete_non_tombstone_reclaims_expired_rows() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;

        // An expired tombstone is past its lifetime: reclaimed, not handed to the caller.
        let id = make_id();
        let tombstone = Tombstone {
            target: ObjectId::random(id.context().clone()),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
        };
        create_tombstone(&backend, &id, &tombstone).await?;
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None,
            "an expired tombstone must not be returned to the caller"
        );
        let records = producer.records();
        assert_eq!(records.len(), 1, "the expired tombstone must be reclaimed");
        assert_eq!(records[0].op_type, OpType::Delete);

        // The same holds for an object row (expired or otherwise).
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"gone", Timestamp::now()).await?;
        producer.clear();
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None
        );
        let records = producer.records();
        assert_eq!(records.len(), 1, "the object row must be reclaimed");
        assert_eq!(records[0].op_type, OpType::Delete);

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_tombstone_rows() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let target = ObjectId::random(id.context().clone());

        let tombstone = Tombstone {
            target,
            time_expires: Some(Timestamp::now() + Duration::from_secs(3600)),
        };
        let written = backend
            .compare_and_write(
                &id,
                None,
                TieredWrite::Tombstone(tombstone),
                Timestamp::now(),
            )
            .await?;
        assert!(written);

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert!(
            records[0].size.unwrap() > 0,
            "tombstone rows occupy storage and must not report zero"
        );
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_expiry_extension_as_an_update() -> Result<()> {
        let (backend, producer) = create_test_backend_with_change_stream().await?;
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_expires: Some(Timestamp::now() + Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single::<crate::stream::ClientError>(b"hello".to_vec()),
                Timestamp::now(),
            )
            .await?;
        producer.clear();

        backend
            .set_expiry(
                &id,
                ExpiryTarget::At(Timestamp::now() + Duration::from_secs(3600)).into(),
                Timestamp::now(),
            )
            .await?;

        let records = producer.records();
        assert_eq!(records.len(), 1, "expected exactly one extension report");
        assert_eq!(records[0].op_type, OpType::Update);
        assert_eq!(
            records[0].size, None,
            "an extension does not change the size"
        );
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }
}
