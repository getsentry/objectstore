//! Cassandra and Scylla storage using portable CQL and lightweight transactions.
//!
//! # Schema and consistency
//!
//! Pre-create a keyspace and apply `cql/schema.cql` in it before starting Objectstore.
//! Each `(namespace, path)` partition holds one inline object, redirect, or upload marker.
//! Objects and markers occupy separate namespaces. Every mutation is a lightweight
//! transaction (LWT) conditioned on a fresh revision UUID. Ordinary CQL writes must not be
//! mixed with these transactions. Conditional UPDATE creates rows without INSERT liveness
//! markers, so no immortal primary-key-only rows survive expiration.
//!
//! Reads use LOCAL_SERIAL; writes use LOCAL_SERIAL consensus and LOCAL_QUORUM commits.
//! All clients for a keyspace must use the same datacenter. Routing never fails over to another
//! datacenter. LWT introduces extra round trips, including on reads. Mutations are neither
//! automatically replayed nor speculatively executed; ambiguous failures remain errors.
//!
//! # Expiration
//!
//! The absolute deadline is checked against the operation's access time, with equality still
//! live. Native per-cell TTL removes expired data; disk reclamation occurs later during
//! compaction. All live columns share one TTL, and renewal rewrites the entire row (including
//! payload). Scylla's non-portable per-row TTL extension is not used. Application and database
//! clocks must be synchronized. Deadlines must precede `2038-01-19T03:14:06Z`, and native TTL
//! must not exceed 630720000 seconds. Unsupported deadlines are rejected, never clamped.
//!
//! # Connections
//!
//! Optional username/password authentication and rustls TLS are supported. A TLS CA bundle
//! enables certificate verification; node certificates must include node IP addresses in their
//! subject alternative names, as required by the driver. Client certificates are not supported.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt as _;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;
use objectstore_types::time::Timestamp;
use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::policies::host_filter::DcHostFilter;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::{CqlValue, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::common::{
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

const OBJECTS: &str = "objects";
const UPLOADS: &str = "uploads";
const CAS_ATTEMPTS: usize = 3;
const MAX_TTL: u64 = 630_720_000;
const MAX_EXPIRATION: u64 = 2_147_483_646;

/// Connection configuration for [`CqlBackend`].
///
/// The keyspace and table must already exist. All clients must use the same local datacenter.
///
/// ```yaml
/// storage:
///   type: cql
///   nodes: [localhost:9042]
///   keyspace: objectstore
///   table_name: objects
///   local_datacenter: datacenter1
/// ```
#[derive(Clone, Deserialize, Serialize)]
pub struct CqlConfig {
    /// Nonempty list of contact points, optionally including port (default 9042).
    pub nodes: Vec<String>,
    /// Pre-created keyspace, using a lowercase CQL identifier.
    pub keyspace: String,
    /// Pre-created table, using a lowercase CQL identifier.
    pub table_name: String,
    /// Datacenter to which all requests and connections are restricted.
    pub local_datacenter: String,
    /// Optional username. Must be configured together with `password`.
    pub username: Option<String>,
    /// Optional password. Redacted from debug output.
    pub password: Option<String>,
    /// PEM CA bundle enabling verified TLS. `None` selects plaintext transport.
    pub tls_ca_bundle: Option<PathBuf>,
    /// Timeout for each CQL request. Defaults to five seconds.
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Optional per-backend storage cost attribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

impl fmt::Debug for CqlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CqlConfig")
            .field("nodes", &self.nodes)
            .field("keyspace", &self.keyspace)
            .field("table_name", &self.table_name)
            .field("local_datacenter", &self.local_datacenter)
            .field("username", &self.username.as_ref().map(|_| "[redacted]"))
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("tls_ca_bundle", &self.tls_ca_bundle)
            .field("request_timeout", &self.request_timeout)
            .field("cogs", &self.cogs)
            .finish()
    }
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(5)
}

impl CqlConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.nodes.is_empty() && self.nodes.iter().all(|s| !s.trim().is_empty()),
            "CQL nodes must not be empty"
        );
        for identifier in [&self.keyspace, &self.table_name] {
            ensure!(
                valid_identifier(identifier),
                "invalid CQL keyspace/table identifier"
            );
        }
        ensure!(
            !self.local_datacenter.trim().is_empty(),
            "CQL local_datacenter is required"
        );
        ensure!(
            self.username.is_some() == self.password.is_some(),
            "CQL username and password must be supplied together"
        );
        ensure!(
            !self.request_timeout.is_zero(),
            "CQL request_timeout must be positive"
        );
        Ok(())
    }
}

fn valid_identifier(value: &str) -> bool {
    // Lowercase names avoid CQL's implicit case folding. Values are always bound separately.
    !value.is_empty()
        && value.len() <= 48
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}

/// High-volume storage backed by Cassandra or Scylla.
pub struct CqlBackend {
    session: Session,
    read: PreparedStatement,
    read_metadata: PreparedStatement,
    write: PreparedStatement,
    delete: PreparedStatement,
    change_stream: Arc<dyn ChangeStream>,
}

impl fmt::Debug for CqlBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CqlBackend").finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum Record {
    // Metadata-only reads leave payload empty; only full reads can be used for renewal.
    Object(Metadata, Bytes),
    Redirect(Tombstone),
    Upload(Timestamp),
}

impl Record {
    fn expires_at(&self) -> Option<Timestamp> {
        match self {
            Self::Object(metadata, _) => metadata.time_expires,
            Self::Redirect(t) => t.time_expires,
            Self::Upload(deadline) => Some(*deadline),
        }
    }

    fn live(&self, now: Timestamp) -> bool {
        self.expires_at().is_none_or(|deadline| deadline >= now)
    }

    fn redirect(&self, now: Timestamp) -> Option<&Tombstone> {
        match self {
            Self::Redirect(t) if self.live(now) => Some(t),
            _ => None,
        }
    }
}

struct StoredRecord {
    revision: Uuid,
    record: Record,
}

// Both SELECTs have this prefix. The full SELECT appends the payload column.
type Head = (i8, Uuid, Option<i64>, Option<String>, Option<String>);
type FullRow = (
    i8,
    Uuid,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<Vec<u8>>,
);

fn corrupt(message: &'static str) -> Error {
    Error::new(ErrorKind::CorruptData, message)
}

/// Converts logical expiry to native TTL, including the logical deadline second.
fn native_ttl(deadline: Option<Timestamp>, now_seconds: u64) -> Result<i32> {
    let Some(deadline) = deadline else {
        return Ok(0);
    };
    if deadline.as_secs() >= MAX_EXPIRATION {
        return Err(Error::new(
            ErrorKind::InvalidMetadata,
            "CQL expiration must precede 2038-01-19T03:14:06Z",
        ));
    }
    let ttl = (deadline.as_secs() + 1).saturating_sub(now_seconds).max(1);
    if ttl > MAX_TTL {
        return Err(Error::new(
            ErrorKind::InvalidMetadata,
            "CQL TTL exceeds 630720000 seconds",
        ));
    }
    Ok(ttl as i32)
}

fn wall_clock_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_secs()
}

fn execution_error(error: ExecutionError) -> Error {
    use RequestAttemptError as Attempt;
    let kind = match &error {
        ExecutionError::RequestTimeout(_) => ErrorKind::BackendTimeout,
        ExecutionError::EmptyPlan | ExecutionError::ConnectionPoolError(_) => {
            ErrorKind::BackendUnavailable
        }
        ExecutionError::LastAttemptError(Attempt::BrokenConnectionError(_)) => {
            ErrorKind::BackendUnavailable
        }
        ExecutionError::LastAttemptError(Attempt::DbError(db, _)) => match db {
            DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. } => ErrorKind::BackendTimeout,
            DbError::Unavailable { .. } | DbError::IsBootstrapping => ErrorKind::BackendUnavailable,
            DbError::Overloaded | DbError::RateLimitReached { .. } => ErrorKind::BackendRateLimited,
            _ => ErrorKind::BackendFailure,
        },
        _ => ErrorKind::BackendFailure,
    };
    Error::with_source(kind, error)
}

fn applied_value(value: Option<&Option<CqlValue>>) -> Result<bool> {
    match value {
        Some(Some(CqlValue::Boolean(applied))) => Ok(*applied),
        _ => Err(corrupt("missing or invalid CQL [applied] result")),
    }
}

fn applied(result: QueryResult) -> Result<bool> {
    let rows = result.into_rows_result().kind(ErrorKind::CorruptData)?;
    let column = rows
        .column_specs()
        .iter()
        .position(|c| c.name() == "[applied]")
        .ok_or_else(|| corrupt("missing CQL [applied] column"))?;
    let row = rows.single_row::<Row>().kind(ErrorKind::CorruptData)?;
    // Cassandra returns only [applied] on success; Scylla also returns old columns.
    applied_value(row.columns.get(column))
}

impl CqlBackend {
    /// Connects to the configured datacenter and prepares statements against an existing table.
    ///
    /// Returns an error for invalid configuration, TLS material, connection failure, or a missing
    /// or incompatible schema. Does not create or migrate production tables.
    pub async fn new(config: CqlConfig, streams: &ChangeStreamFactory) -> anyhow::Result<Self> {
        config.validate()?;
        let policy = DefaultPolicy::builder()
            .prefer_datacenter(config.local_datacenter.clone())
            .permit_dc_failover(false)
            .build();
        let profile = ExecutionProfile::builder()
            .consistency(Consistency::LocalQuorum)
            .serial_consistency(Some(SerialConsistency::LocalSerial))
            .request_timeout(Some(config.request_timeout))
            .load_balancing_policy(policy)
            .retry_policy(Arc::new(FallthroughRetryPolicy))
            .speculative_execution_policy(None)
            .build()
            .into_handle();
        let mut builder = SessionBuilder::new()
            .known_nodes(&config.nodes)
            .host_filter(Arc::new(DcHostFilter::new(config.local_datacenter.clone())))
            .default_execution_profile_handle(profile);
        if let (Some(user), Some(password)) = (&config.username, &config.password) {
            builder = builder.user(user, password);
        }
        if let Some(path) = &config.tls_ca_bundle {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(path).with_context(|| "read CQL CA bundle")? {
                roots.add(cert.with_context(|| "parse CQL CA certificate")?)?;
            }
            ensure!(!roots.is_empty(), "CQL CA bundle contains no certificates");
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
            builder = builder.tls_context(Some(Arc::new(tls)));
        }
        let session = builder
            .build()
            .await
            .with_context(|| "connect to CQL backend")?;
        // Identifiers are validated above and quoted to also permit reserved words.
        let table = format!("\"{}\".\"{}\"", config.keyspace, config.table_name);
        let columns = "kind, revision, expires_at, metadata, redirect";
        let mut read = session
            .prepare(format!(
                "SELECT {columns}, payload FROM {table} WHERE namespace = ? AND path = ?"
            ))
            .await?;
        let mut read_metadata = session
            .prepare(format!(
                "SELECT {columns} FROM {table} WHERE namespace = ? AND path = ?"
            ))
            .await?;
        read.set_consistency(Consistency::LocalSerial);
        read_metadata.set_consistency(Consistency::LocalSerial);
        let write = session.prepare(format!("UPDATE {table} USING TTL ? SET kind = ?, revision = ?, expires_at = ?, metadata = ?, redirect = ?, payload = ? WHERE namespace = ? AND path = ? IF revision = ?")).await?;
        let delete = session
            .prepare(format!(
                "DELETE FROM {table} WHERE namespace = ? AND path = ? IF revision = ?"
            ))
            .await?;
        Ok(Self {
            session,
            read,
            read_metadata,
            write,
            delete,
            change_stream: streams.build(config.cogs.as_ref()),
        })
    }

    async fn read_record(
        &self,
        namespace: &str,
        id: &ObjectId,
        payload: bool,
    ) -> Result<Option<StoredRecord>> {
        let path = id.as_storage_path().to_string();
        let query = if payload {
            &self.read
        } else {
            &self.read_metadata
        };
        let result = self
            .session
            .execute_unpaged(query, (namespace, &path))
            .await
            .map_err(execution_error)?;
        let rows = result.into_rows_result().kind(ErrorKind::CorruptData)?;
        let row = if payload {
            rows.maybe_first_row::<FullRow>()
                .kind(ErrorKind::CorruptData)?
        } else {
            rows.maybe_first_row::<Head>()
                .kind(ErrorKind::CorruptData)?
                .map(|(kind, revision, expiry, metadata, redirect)| {
                    (kind, revision, expiry, metadata, redirect, None)
                })
        };
        let Some((kind, revision, expiry, metadata, redirect, bytes)) = row else {
            return Ok(None);
        };
        let expiry = expiry
            .map(|seconds| {
                Timestamp::from_unix_secs(
                    seconds
                        .try_into()
                        .map_err(|_| corrupt("negative CQL expiry"))?,
                )
                .map_err(|_| corrupt("invalid CQL expiry"))
            })
            .transpose()?;
        let record = match (namespace, kind) {
            (OBJECTS, 0) => {
                if redirect.is_some() || (payload && bytes.is_none()) {
                    return Err(corrupt("invalid inline CQL row"));
                }
                let mut metadata: Metadata =
                    serde_json::from_str(&metadata.ok_or_else(|| corrupt("missing CQL metadata"))?)
                        .kind(ErrorKind::CorruptData)?;
                metadata.time_expires = expiry;
                Record::Object(metadata, bytes.unwrap_or_default().into())
            }
            (OBJECTS, 1) => {
                if metadata.is_some() || bytes.is_some() {
                    return Err(corrupt("invalid CQL redirect row"));
                }
                let target = redirect
                    .as_deref()
                    .and_then(ObjectId::from_storage_path)
                    .ok_or_else(|| corrupt("invalid CQL redirect target"))?;
                Record::Redirect(Tombstone {
                    target,
                    time_expires: expiry,
                })
            }
            (UPLOADS, 2) => {
                Record::Upload(expiry.ok_or_else(|| corrupt("missing upload marker expiry"))?)
            }
            _ => return Err(corrupt("invalid CQL row kind")),
        };
        Ok(Some(StoredRecord { revision, record }))
    }

    /// Writes all cells under one TTL and revision condition. `None` expects absence.
    async fn write_record(
        &self,
        namespace: &str,
        id: &ObjectId,
        expected: Option<Uuid>,
        record: &Record,
        expiry_update: bool,
    ) -> Result<bool> {
        let path = id.as_storage_path().to_string();
        let expiry = record.expires_at();
        let ttl = native_ttl(expiry, wall_clock_seconds())?;
        let (kind, metadata, redirect, payload) = match record {
            Record::Object(metadata, payload) => {
                let mut metadata = metadata.clone();
                metadata.size = Some(payload.len());
                (
                    0_i8,
                    Some(serde_json::to_string(&metadata).kind(ErrorKind::InvalidMetadata)?),
                    None,
                    Some(payload.as_ref()),
                )
            }
            Record::Redirect(t) => (1, None, Some(t.target.as_storage_path().to_string()), None),
            Record::Upload(_) => (2, None, None, None),
        };
        let size = (namespace.len()
            + path.len()
            + 1
            + 16
            + expiry.map_or(0, |_| 8)
            + metadata.as_ref().map_or(0, String::len)
            + redirect.as_ref().map_or(0, String::len)
            + payload.map_or(0, <[u8]>::len)) as u64;
        let result = self
            .session
            .execute_unpaged(
                &self.write,
                (
                    ttl,
                    kind,
                    Uuid::new_v4(),
                    expiry.map(|t| t.as_secs() as i64),
                    metadata.as_deref(),
                    redirect.as_deref(),
                    payload,
                    namespace,
                    &path,
                    expected,
                ),
            )
            .await
            .map_err(execution_error)?;
        let applied = applied(result)?;
        if applied && namespace == OBJECTS {
            if expiry_update {
                self.change_stream.update(id, expiry);
            } else {
                self.change_stream.write(id, size, expiry);
            }
        }
        Ok(applied)
    }

    async fn delete_record(&self, namespace: &str, id: &ObjectId, revision: Uuid) -> Result<bool> {
        let path = id.as_storage_path().to_string();
        let result = self
            .session
            .execute_unpaged(&self.delete, (namespace, &path, revision))
            .await
            .map_err(execution_error)?;
        let applied = applied(result)?;
        if applied && namespace == OBJECTS {
            self.change_stream.delete(id);
        }
        Ok(applied)
    }

    async fn replace(&self, namespace: &str, id: &ObjectId, record: &Record) -> Result<()> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.read_record(namespace, id, false).await?;
            if self
                .write_record(namespace, id, current.map(|r| r.revision), record, false)
                .await?
            {
                return Ok(());
            }
        }
        Err(contention())
    }
}

fn contention() -> Error {
    Error::new(
        ErrorKind::BackendFailure,
        "CQL conditional mutation contention exhausted",
    )
}

#[async_trait::async_trait]
impl Backend for CqlBackend {
    fn name(&self) -> &'static str {
        "cql"
    }

    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        mut stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        let mut payload = ChunkedBytes::new(0);
        while let Some(chunk) = stream.try_next().await? {
            payload.push(chunk);
        }
        self.replace(
            OBJECTS,
            id,
            &Record::Object(metadata.clone(), payload.into_bytes()),
        )
        .await
    }

    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        match self.get_tiered_object(id, access_time, range).await? {
            TieredGet::Object(m, r, p) => Ok(Some((m, r, p))),
            TieredGet::NotFound => Ok(None),
            TieredGet::Tombstone(_) => Err(ErrorKind::UnexpectedTombstone.into()),
        }
    }

    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        match self.get_tiered_metadata(id, access_time).await? {
            TieredMetadata::Object(m) => Ok(Some(m)),
            TieredMetadata::NotFound => Ok(None),
            TieredMetadata::Tombstone(_) => Err(ErrorKind::UnexpectedTombstone.into()),
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

    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
        for _ in 0..CAS_ATTEMPTS {
            let Some(current) = self.read_record(OBJECTS, id, false).await? else {
                return Ok(());
            };
            if self.delete_record(OBJECTS, id, current.revision).await? {
                return Ok(());
            }
        }
        Err(contention())
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
        self.replace(UPLOADS, revision, &Record::Upload(time_expires))
            .await
    }

    async fn has_upload_marker(&self, revision: &ObjectId, access_time: Timestamp) -> Result<bool> {
        Ok(self
            .read_record(UPLOADS, revision, false)
            .await?
            .is_some_and(|r| r.record.live(access_time)))
    }

    async fn delete_upload_marker(
        &self,
        revision: &ObjectId,
        access_time: Timestamp,
    ) -> Result<bool> {
        let Some(row) = self.read_record(UPLOADS, revision, false).await? else {
            return Ok(false);
        };
        if !row.record.live(access_time) {
            return Ok(false);
        }
        self.delete_record(UPLOADS, revision, row.revision).await
    }

    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        let record = Record::Object(metadata.clone(), payload);
        for _ in 0..CAS_ATTEMPTS {
            let row = self.read_record(OBJECTS, id, false).await?;
            if let Some(t) = row.as_ref().and_then(|r| r.record.redirect(access_time)) {
                return Ok(Some(t.clone()));
            }
            if self
                .write_record(OBJECTS, id, row.map(|r| r.revision), &record, false)
                .await?
            {
                return Ok(None);
            }
        }
        Err(contention())
    }

    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<TieredGet> {
        let Some(row) = self.read_record(OBJECTS, id, true).await? else {
            return Ok(TieredGet::NotFound);
        };
        if !row.record.live(access_time) {
            return Ok(TieredGet::NotFound);
        }
        match row.record {
            Record::Object(metadata, payload) => {
                let total = payload.len() as u64;
                let (range, payload) = match range {
                    None => (None, payload),
                    Some(range) => {
                        let range = range
                            .resolve(total)
                            .ok_or(ErrorKind::RangeNotSatisfiable { total })?;
                        let bytes = payload.slice(range.start as usize..=range.end as usize);
                        (Some(range), bytes)
                    }
                };
                Ok(TieredGet::Object(
                    metadata,
                    range,
                    crate::stream::single(payload),
                ))
            }
            Record::Redirect(t) => Ok(TieredGet::Tombstone(t)),
            Record::Upload(_) => Err(corrupt("upload marker in object namespace")),
        }
    }

    async fn get_tiered_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<TieredMetadata> {
        let Some(row) = self.read_record(OBJECTS, id, false).await? else {
            return Ok(TieredMetadata::NotFound);
        };
        if !row.record.live(access_time) {
            return Ok(TieredMetadata::NotFound);
        }
        match row.record {
            Record::Object(m, _) => Ok(TieredMetadata::Object(m)),
            Record::Redirect(t) => Ok(TieredMetadata::Tombstone(t)),
            Record::Upload(_) => Err(corrupt("upload marker in object namespace")),
        }
    }

    async fn delete_non_tombstone(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        for _ in 0..CAS_ATTEMPTS {
            let Some(row) = self.read_record(OBJECTS, id, false).await? else {
                return Ok(None);
            };
            if let Some(t) = row.record.redirect(access_time) {
                return Ok(Some(t.clone()));
            }
            if self.delete_record(OBJECTS, id, row.revision).await? {
                return Ok(None);
            }
        }
        Err(contention())
    }

    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
        access_time: Timestamp,
    ) -> Result<bool> {
        let next_target = write.target().cloned();
        let record = match write {
            TieredWrite::Object(m, p) => Some(Record::Object(m, p)),
            TieredWrite::Tombstone(t) => Some(Record::Redirect(t)),
            TieredWrite::Delete => None,
        };
        for _ in 0..CAS_ATTEMPTS {
            let row = self.read_record(OBJECTS, id, false).await?;
            let actual = row
                .as_ref()
                .and_then(|r| r.record.redirect(access_time))
                .map(|t| &t.target);
            if actual != current {
                return Ok(actual == next_target.as_ref());
            }
            let applied = match (&record, row) {
                (Some(record), row) => {
                    self.write_record(OBJECTS, id, row.map(|r| r.revision), record, false)
                        .await?
                }
                (None, Some(row)) => self.delete_record(OBJECTS, id, row.revision).await?,
                (None, None) => true,
            };
            if applied {
                return Ok(true);
            }
        }
        Err(contention())
    }

    async fn compare_and_update(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        update: TieredUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let Some(mut row) = self.read_record(OBJECTS, id, true).await? else {
            return Ok(SetExpiryResponse::NotFound);
        };
        if !row.record.live(access_time) {
            return Ok(SetExpiryResponse::NotFound);
        }
        let created = match &row.record {
            Record::Object(m, _) if current.is_none() => m.time_created,
            Record::Redirect(t) if current == Some(&t.target) => None,
            _ => return Ok(SetExpiryResponse::Rejected),
        };
        let Some(old_expiry) = row.record.expires_at() else {
            return Ok(SetExpiryResponse::Rejected);
        };
        let TieredUpdate::SetExpiry(target) = update;
        let Some(deadline) = target.resolve(created, access_time)? else {
            return Ok(SetExpiryResponse::Rejected);
        };
        native_ttl(Some(deadline), wall_clock_seconds())?;
        if old_expiry >= deadline {
            return Ok(SetExpiryResponse::Satisfied(deadline));
        }
        match &mut row.record {
            Record::Object(m, _) => {
                m.expiration_policy = common::extended_expiration_policy(
                    m.expiration_policy,
                    m.time_created,
                    old_expiry,
                    deadline,
                )?;
                m.time_expires = Some(deadline);
            }
            Record::Redirect(t) => t.time_expires = Some(deadline),
            Record::Upload(_) => unreachable!(),
        }
        Ok(
            if self
                .write_record(OBJECTS, id, Some(row.revision), &row.record, true)
                .await?
            {
                SetExpiryResponse::Satisfied(deadline)
            } else {
                SetExpiryResponse::Rejected
            },
        )
    }
}

#[cfg(test)]
mod tests;
