//! BigTable backend for high-volume, low-latency storage of small objects.
//!
//! # Row Format
//!
//! Each row key is the object's storage path. A row contains either an **object** or a
//! **tombstone** — never both. The two layouts are mutually exclusive and distinguished by
//! column presence:
//!
//! | Column | Family | Content | Present when |
//! |--------|--------|---------|--------------|
//! | `p` | `fg`/`fm` | Compressed payload bytes | Object row only |
//! | `m` | `fg`/`fm` | [`Metadata`] JSON | Object row only |
//! | `r` | `fg`/`fm` | Redirect sentinel (`b""`) | Tombstone row only |
//! | `t` | `fg`/`fm` | [`TombstoneMeta`] JSON | Tombstone row only |
//!
//! `p`/`m` and `r`/`t` are mutually exclusive. Every write begins with a `DeleteFromRow`
//! mutation that clears all columns before writing the new cells, so mixed rows cannot exist.
//!
//! ## Legacy Tombstone Format
//!
//! Tombstones written before the `r`/`t` column layout used the object-row format with an
//! empty `p` column and `"is_redirect_tombstone": true` in the `m` JSON. Both formats are
//! supported for reading. A `bigtable.legacy_tombstone_read` metric is emitted on each legacy
//! read. Legacy tombstones expire naturally by TTL/GC; TTI bumps transparently upgrade them
//! to the new format.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bigtable_rs::bigtable::{BigTableConnection, Error as BigTableError, RowCell};
use bigtable_rs::google::bigtable::v2::{self, mutation};
use futures_util::TryStreamExt;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use serde::{Deserialize, Serialize};
use tonic::Code;

use bytes::Bytes;

use crate::backend::common::{
    Backend, ConditionalOutcome, DeleteResponse, GetResponse, HighVolumeBackend, MetadataResponse,
    PutResponse, TieredGet, TieredMetadata, Tombstone,
};
use crate::error::{Error, Result};
use crate::gcp_auth::PrefetchingTokenProvider;
use crate::id::ObjectId;
use crate::stream::{ChunkedBytes, ClientStream};

/// Configuration for [`BigTableBackend`].
///
/// Stores objects in [Google Cloud Bigtable], a NoSQL wide-column database optimized for
/// high-throughput, low-latency workloads with small objects. Authentication uses Application
/// Default Credentials (ADC).
///
/// **Note**: The table must be pre-created with the following column families:
/// - `fg`: timestamp-based garbage collection (`maxage=1s`)
/// - `fm`: manual garbage collection (`no GC policy`)
///
/// [Google Cloud Bigtable]: https://cloud.google.com/bigtable
///
/// # Example
///
/// ```yaml
/// storage:
///   type: bigtable
///   project_id: my-project
///   instance_name: objectstore
///   table_name: objectstore
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BigTableConfig {
    /// Optional custom Bigtable endpoint.
    ///
    /// Useful for testing with emulators. If `None`, uses the default Bigtable endpoint.
    ///
    /// # Default
    ///
    /// `None` (uses default Bigtable endpoint)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE=bigtable`
    /// - `OS__STORAGE__ENDPOINT=localhost:8086` (optional)
    pub endpoint: Option<String>,

    /// GCP project ID.
    ///
    /// The Google project ID (not project number) containing the Bigtable instance.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__PROJECT_ID=my-project`
    pub project_id: String,

    /// Bigtable instance name.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__INSTANCE_NAME=my-instance`
    pub instance_name: String,

    /// Bigtable table name.
    ///
    /// The table must exist before starting the server.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TABLE_NAME=objectstore`
    pub table_name: String,

    /// Optional number of connections to maintain to Bigtable.
    ///
    /// # Default
    ///
    /// `None` (defaults to 1)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__CONNECTIONS=16` (optional)
    pub connections: Option<usize>,
}

/// Connection timeout used for the initial connection to Bigtable.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum age for connections (GRPC channels) to Bigtable, after which they will be swapped with
/// new ones in the background.
/// This is intended to avoid latency spikes that could occur every hour or so, when the server
/// closes long standing connections ([source](https://web.archive.org/web/20260211140930/https://docs.cloud.google.com/bigtable/docs/performance#cold-starts:~:text=return%20an%20error.-,Cold%20start,-at%20client%20initialization)).
/// `tonic` already handles reconnections transparently, but lazily, meaning that the first requests
/// that attempt to use a certain channel after the server has closed it will pay the cost of the
/// reconnection, resulting in increased latency for those requests.
const MAX_CHANNEL_AGE: Option<Duration> = Some(Duration::from_mins(50));
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day
/// Permission scopes required for accessing the BigTable data API.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/bigtable.data"];

/// How often to retry failed requests.
const REQUEST_RETRY_COUNT: usize = 2;

/// Column that stores the raw payload (compressed).
const COLUMN_PAYLOAD: &[u8] = b"p";
/// Column that stores metadata in JSON.
const COLUMN_METADATA: &[u8] = b"m";
/// Column that stores the redirect sentinel for tombstone rows (`b""` for now; a GCS key later).
const COLUMN_REDIRECT: &[u8] = b"r";
/// Column that stores [`TombstoneMeta`] JSON for tombstone rows.
const COLUMN_TOMBSTONE_META: &[u8] = b"t";

/// Column family that uses timestamp-based garbage collection.
///
/// We require a GC rule on this family to automatically delete rows.
/// See: <https://cloud.google.com/bigtable/docs/gc-cell-level>
const FAMILY_GC: &str = "fg";
/// Column family that uses manual garbage collection.
const FAMILY_MANUAL: &str = "fm";

/// BigTable storage backend for high-volume, low-latency object storage.
pub struct BigTableBackend {
    bigtable: BigTableConnection,

    instance_path: String,
    table_path: String,
    table_name: String,
}

impl fmt::Debug for BigTableBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigTableBackend")
            .field("instance_path", &self.instance_path)
            .field("table_path", &self.table_path)
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

fn is_retryable(error: &BigTableError) -> bool {
    match error {
        // Transient errors on auth token refresh
        BigTableError::GCPAuthError(_) => true,
        // Transient GRPC network failures
        BigTableError::TransportError(_) => true,
        // These could also indicate transient network failures
        BigTableError::IoError(_) => true,
        BigTableError::TimeoutError(_) => true,

        // See https://docs.cloud.google.com/bigtable/docs/status-codes
        BigTableError::RpcError(status) => match status.code() {
            // Generic retriable status
            Code::Unavailable => true,
            // Timeouts
            Code::Cancelled => true,
            Code::DeadlineExceeded => true,
            // Token might have refreshed too late
            Code::Unauthenticated => true,
            // Unspecified, attempt to retry anyways
            Code::Aborted => true,
            Code::Internal => true,
            Code::FailedPrecondition => true,
            Code::Unknown => true,
            _ => false,
        },
        _ => false,
    }
}

/// Creates a row filter that matches a single column by exact qualifier.
fn column_filter(column: &[u8]) -> v2::RowFilter {
    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::ColumnQualifierRegexFilter(
            [b"^", column, b"$"].concat(),
        )),
    }
}

/// Creates a row filter that matches any tombstone row, new- or legacy-format.
///
/// New format: presence of the `r` column.
/// Legacy format: `is_redirect_tombstone: true` in the `m` column JSON.
///
/// Used by [`BigTableBackend::put_non_tombstone`] and [`BigTableBackend::delete_non_tombstone`]
/// as the `CheckAndMutate` predicate. After legacy tombstones expire naturally, this simplifies
/// to just `column_filter(COLUMN_REDIRECT)`.
fn tombstone_predicate() -> v2::RowFilter {
    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Interleave(
            v2::row_filter::Interleave {
                filters: vec![
                    // Current: redirect column is present.
                    column_filter(COLUMN_REDIRECT),
                    // Legacy: Metadata starts with `is_redirect_tombstone``.
                    v2::RowFilter {
                        filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
                            filters: vec![
                                column_filter(COLUMN_METADATA),
                                v2::RowFilter {
                                    filter: Some(v2::row_filter::Filter::ValueRegexFilter(
                                        b"^\\{\"is_redirect_tombstone\":true[,}].*".to_vec(),
                                    )),
                                },
                            ],
                        })),
                    },
                ],
            },
        )),
    }
}

/// Creates a row filter that reads all non-payload columns (`m`, `r`, `t`).
///
/// Used by metadata-only reads to avoid fetching the (potentially large) payload column
/// while still being able to detect both new- and legacy-format tombstones.
fn metadata_filter() -> v2::RowFilter {
    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::ColumnQualifierRegexFilter(
            b"^[mrt]$".to_vec(),
        )),
    }
}

/// Builds the three mutations that write an object row: clear existing data,
/// then set the payload and metadata cells.
///
/// Used by both [`BigTableBackend::put_row`] (unconditional write) and
/// [`BigTableBackend::put_non_tombstone`] (conditional write).
fn build_write_mutations(
    metadata: &Metadata,
    payload: Vec<u8>,
    now: SystemTime,
) -> Result<[mutation::Mutation; 3]> {
    let (family, timestamp_micros) = match metadata.expiration_policy {
        ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
        ExpirationPolicy::TimeToLive(ttl) => (FAMILY_GC, ttl_to_micros(ttl, now)?),
        ExpirationPolicy::TimeToIdle(tti) => (FAMILY_GC, ttl_to_micros(tti, now)?),
    };

    let metadata_bytes = serde_json::to_vec(metadata).map_err(|cause| Error::Serde {
        context: "failed to serialize metadata".to_string(),
        cause,
    })?;

    Ok([
        // NB: We explicitly delete the row to clear metadata on overwrite.
        mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {}),
        mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_PAYLOAD.to_owned(),
            timestamp_micros,
            value: payload,
        }),
        mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_METADATA.to_owned(),
            timestamp_micros,
            value: metadata_bytes,
        }),
    ])
}

/// Metadata carried by tombstone rows in the `t` (tombstone-meta) column.
///
/// Tombstone-specific metadata evolves independently of object [`Metadata`]. Only fields
/// that are meaningful on tombstones are included here.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct TombstoneMeta {
    /// Expiration policy for this tombstone.
    ///
    /// Skipped during serialization when set to [`ExpirationPolicy::Manual`].
    #[serde(default, skip_serializing_if = "ExpirationPolicy::is_manual")]
    expiration_policy: ExpirationPolicy,
}

/// Builds the three mutations that write a tombstone row: clear existing data,
/// then set the redirect sentinel and tombstone-meta cells.
///
/// Used by both [`BigTableBackend::put_tombstone_row`] (unconditional write) and the
/// TTI bump path in tiered reads.
fn build_tombstone_mutations(
    tombstone: &Tombstone,
    now: SystemTime,
) -> Result<[mutation::Mutation; 3]> {
    let (family, timestamp_micros) = match tombstone.expiration_policy {
        ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
        ExpirationPolicy::TimeToLive(ttl) => (FAMILY_GC, ttl_to_micros(ttl, now)?),
        ExpirationPolicy::TimeToIdle(tti) => (FAMILY_GC, ttl_to_micros(tti, now)?),
    };

    let tombstone_meta = TombstoneMeta {
        expiration_policy: tombstone.expiration_policy,
    };
    let metadata_bytes = serde_json::to_vec(&tombstone_meta).map_err(|cause| Error::Serde {
        context: "failed to serialize tombstone meta".to_string(),
        cause,
    })?;

    Ok([
        mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {}),
        mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_REDIRECT.to_owned(),
            timestamp_micros,
            value: b"".to_vec(),
        }),
        mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_TOMBSTONE_META.to_owned(),
            timestamp_micros,
            value: metadata_bytes,
        }),
    ])
}

/// Subset of [`Metadata`] that indicates a row is a tombstone instead of a real object.
///
/// Used to construct [`RowData`].
#[derive(Debug, Deserialize)]
struct LegacyTombstoneMeta {
    /// Internal redirect tombstone marker.
    ///
    /// When `true`, this object is a legacy tombstone. This implies:
    ///  - the payload is empty
    ///  - metadata other than the expiration policy is not meaningful
    ///  - the `r` and `t` columns are not present
    #[serde(default)]
    is_redirect_tombstone: bool,

    /// Expiration policy for this tombstone.
    #[serde(default)]
    expiration_policy: ExpirationPolicy,
}

/// Parsed data from a BigTable row's cells.
enum RowData {
    /// A regular object row with payload and metadata.
    Object {
        metadata: Metadata,
        payload: Vec<u8>,
    },
    /// A tombstone row indicating the real payload lives on the long-term backend.
    Tombstone {
        meta: TombstoneMeta,
        time_expires: Option<SystemTime>,
    },
}

impl RowData {
    /// Parses a set of row cells into a [`RowData`].
    ///
    /// New-format tombstones are identified by the presence of the `r` column.
    /// Legacy tombstones (written before the column migration) are identified by
    /// `is_redirect_tombstone: true` in the `m` column JSON; a
    /// `bigtable.legacy_tombstone_read` metric is emitted on each such read.
    fn from_cells(cells: Vec<RowCell>) -> Result<Self> {
        let mut metadata_opt: Option<Metadata> = None;
        let mut tombstone_meta_opt: Option<TombstoneMeta> = None;
        let mut redirect_detected = false;
        let mut expire_at = None;
        let mut payload = Vec::new();

        for cell in cells {
            // NB: All cells are written with the same timestamp; last write is safe.
            expire_at = micros_to_time(cell.timestamp_micros);

            match cell.qualifier.as_slice() {
                COLUMN_REDIRECT => {
                    redirect_detected = true;
                }
                COLUMN_PAYLOAD => {
                    payload = cell.value;
                }
                COLUMN_TOMBSTONE_META => {
                    tombstone_meta_opt =
                        Some(serde_json::from_slice(&cell.value).map_err(|cause| {
                            Error::Serde {
                                context: "failed to deserialize tombstone meta".to_string(),
                                cause,
                            }
                        })?);
                }
                COLUMN_METADATA => {
                    if let Ok(legacy_meta) =
                        serde_json::from_slice::<LegacyTombstoneMeta>(&cell.value)
                        && legacy_meta.is_redirect_tombstone
                    {
                        redirect_detected = true;
                        objectstore_metrics::count!("bigtable.legacy_tombstone_read");
                        tombstone_meta_opt = Some(TombstoneMeta {
                            expiration_policy: legacy_meta.expiration_policy,
                        });
                        continue;
                    }

                    metadata_opt = Some(serde_json::from_slice(&cell.value).map_err(|cause| {
                        Error::Serde {
                            context: "failed to deserialize metadata".to_string(),
                            cause,
                        }
                    })?);
                }
                _ => {}
            }
        }

        Ok(if redirect_detected {
            RowData::Tombstone {
                meta: tombstone_meta_opt.unwrap_or_default(),
                time_expires: expire_at,
            }
        } else {
            // Metadata may have been skipped during read - payload-only read for TTI bump.
            let mut metadata = metadata_opt.unwrap_or_default();
            metadata.time_expires = expire_at;
            RowData::Object { metadata, payload }
        })
    }

    /// Returns the expiration policy for this row, regardless of variant.
    fn expiration_policy(&self) -> ExpirationPolicy {
        match self {
            RowData::Object { metadata, .. } => metadata.expiration_policy,
            RowData::Tombstone { meta, .. } => meta.expiration_policy,
        }
    }

    /// Returns the resolved expiration timestamp for this row, regardless of variant.
    fn time_expires(&self) -> Option<SystemTime> {
        match self {
            RowData::Object { metadata, .. } => metadata.time_expires,
            RowData::Tombstone { time_expires, .. } => *time_expires,
        }
    }

    /// Returns `true` if this row is expired as of the given `time`.
    ///
    /// Only applies to rows with an expiration policy set.
    fn expires_before(&self, time: SystemTime) -> bool {
        self.expiration_policy().is_timeout() && self.time_expires().is_some_and(|ts| ts < time)
    }

    /// Returns `true` if this row's TTI deadline should be bumped.
    fn needs_tti_bump(&self) -> bool {
        matches!(
            self.expiration_policy(),
            ExpirationPolicy::TimeToIdle(tti) if self.expires_before(SystemTime::now() + tti - TTI_DEBOUNCE)
        )
    }
}

impl BigTableBackend {
    /// Creates a new [`BigTableBackend`] from the given `config`.
    ///
    /// Pass an `endpoint` in the config to connect to a local emulator; omit it to use real GCP
    /// credentials. `connections` controls the gRPC connection pool size (defaults to 1).
    pub async fn new(config: BigTableConfig) -> anyhow::Result<Self> {
        let BigTableConfig {
            endpoint,
            project_id,
            instance_name,
            table_name,
            connections,
        } = config;

        let bigtable = if let Some(ref endpoint) = endpoint {
            BigTableConnection::new_with_emulator(
                endpoint,
                &project_id,
                &instance_name,
                false, // is_read_only
                Some(CONNECT_TIMEOUT),
            )?
        } else {
            let token_provider = PrefetchingTokenProvider::gcp_auth(TOKEN_SCOPES).await?;
            BigTableConnection::new_with_managed_transport(
                &project_id,
                &instance_name,
                false, // is_read_only
                Some(CONNECT_TIMEOUT),
                Arc::new(token_provider),
                connections.unwrap_or(1),
                true, // prime_channels
                None, // app_profile_id
                MAX_CHANNEL_AGE,
            )
            .await?
        };

        let client = bigtable.client();

        Ok(Self {
            bigtable,
            instance_path: format!("projects/{project_id}/instances/{instance_name}"),
            table_path: client.get_full_table_name(&table_name),
            table_name,
        })
    }

    /// Retries a BigTable RPC on transient errors.
    async fn with_retry<T, F>(&self, action: &'static str, f: impl Fn() -> F) -> Result<T>
    where
        F: Future<Output = Result<T, BigTableError>> + Send,
    {
        let mut retry_count = 0usize;

        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if retry_count >= REQUEST_RETRY_COUNT || !is_retryable(&e) {
                        objectstore_metrics::count!("bigtable.failures", action = action);
                        return Err(Error::Generic {
                            context: format!("Bigtable: `{action}` failed"),
                            cause: Some(Box::new(e)),
                        });
                    }
                    retry_count += 1;
                    objectstore_metrics::count!("bigtable.retries", action = action);
                    tracing::warn!(
                        retry_count,
                        action,
                        error = &e as &dyn std::error::Error,
                        "Retrying request"
                    );
                }
            }
        }
    }

    /// Reads a single row by key, returning parsed row data.
    ///
    /// Returns `None` if the row is absent or has expired.
    async fn read_row(
        &self,
        path: &[u8],
        filter: Option<v2::RowFilter>,
        action: &'static str,
    ) -> Result<Option<RowData>> {
        let request = v2::ReadRowsRequest {
            table_name: self.table_path.clone(),
            rows: Some(v2::RowSet {
                row_keys: vec![path.to_owned()],
                row_ranges: vec![],
            }),
            filter,
            rows_limit: 1,
            ..Default::default()
        };

        let response = self
            .with_retry(action, || async {
                self.bigtable.client().read_rows(request.clone()).await
            })
            .await?;
        debug_assert!(response.len() <= 1, "Expected at most one row");

        let Some((_, cells)) = response.into_iter().next() else {
            tracing::debug!("Object not found");
            return Ok(None);
        };

        let row = RowData::from_cells(cells)?;
        Ok(if row.expires_before(SystemTime::now()) {
            None
        } else {
            Some(row)
        })
    }

    async fn mutate<I>(
        &self,
        path: Vec<u8>,
        mutations: I,
        action: &'static str,
    ) -> Result<v2::MutateRowResponse>
    where
        I: IntoIterator<Item = mutation::Mutation>,
    {
        let mutations = mutations
            .into_iter()
            .map(|m| v2::Mutation { mutation: Some(m) })
            .collect();
        let request = v2::MutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path,
            mutations,
            ..Default::default()
        };

        let response = self
            .with_retry(action, || async {
                self.bigtable.client().mutate_row(request.clone()).await
            })
            .await?;

        Ok(response.into_inner())
    }

    async fn put_row(
        &self,
        path: Vec<u8>,
        metadata: &Metadata,
        payload: Vec<u8>,
        action: &'static str,
    ) -> Result<v2::MutateRowResponse> {
        let mutations = build_write_mutations(metadata, payload, SystemTime::now())?;
        self.mutate(path, mutations, action).await
    }

    async fn put_tombstone_row(
        &self,
        path: Vec<u8>,
        tombstone: &Tombstone,
        action: &'static str,
    ) -> Result<v2::MutateRowResponse> {
        let mutations = build_tombstone_mutations(tombstone, SystemTime::now())?;
        self.mutate(path, mutations, action).await
    }
}

#[async_trait::async_trait]
impl Backend for BigTableBackend {
    fn name(&self) -> &'static str {
        "bigtable"
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        mut stream: ClientStream,
    ) -> Result<PutResponse> {
        tracing::debug!("Writing to Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let mut payload = ChunkedBytes::new(0);
        while let Some(chunk) = stream.try_next().await? {
            payload.push(chunk);
        }

        self.put_row(path, metadata, payload.into_bytes().into(), "put")
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        tracing::debug!("Reading from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let Some(row) = self.read_row(&path, None, "get").await? else {
            return Ok(None);
        };

        let needs_bump = row.needs_tti_bump();
        match row {
            RowData::Tombstone { .. } => Err(Error::UnexpectedTombstone),
            RowData::Object { metadata, payload } => {
                if needs_bump {
                    // TODO: Schedule into background persistently so this doesn't get lost on restarts
                    let _ = self
                        .put_row(path, &metadata, payload.clone(), "tti-bump")
                        .await;
                }
                let mut metadata = metadata;
                metadata.size = Some(payload.len());
                let stream = crate::stream::single(payload);
                Ok(Some((metadata, stream)))
            }
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        tracing::debug!("Reading metadata from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        // Read metadata and tombstone columns — skip the (potentially large) payload.
        // NB: `metadata.size` will not be populated since the payload is not fetched.
        let row_opt = self
            .read_row(&path, Some(metadata_filter()), "get_metadata")
            .await?;
        let Some(row) = row_opt else {
            return Ok(None);
        };

        let needs_bump = row.needs_tti_bump();
        match row {
            RowData::Tombstone { .. } => Err(Error::UnexpectedTombstone),
            RowData::Object { metadata, .. } => {
                // Conditional TTI bump: read the payload only when a bump is actually needed.
                if needs_bump {
                    // TODO: Schedule into background persistently so this doesn't get lost on restarts
                    // Best-effort — failures here should not fail the metadata read.
                    let payload_read = self
                        .read_row(&path, Some(column_filter(COLUMN_PAYLOAD)), "tti-bump")
                        .await;
                    if let Ok(Some(RowData::Object { payload, .. })) = payload_read {
                        let _ = self.put_row(path, &metadata, payload, "tti-bump").await;
                    }
                }
                Ok(Some(metadata))
            }
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        tracing::debug!("Deleting from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = [mutation::Mutation::DeleteFromRow(
            mutation::DeleteFromRow {},
        )];
        self.mutate(path, mutations, "delete").await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl HighVolumeBackend for BigTableBackend {
    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<ConditionalOutcome> {
        tracing::debug!("Conditional put to Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let false_mutations = build_write_mutations(metadata, payload.to_vec(), SystemTime::now())?
            .into_iter()
            .map(|m| v2::Mutation { mutation: Some(m) })
            .collect();

        // Predicate: tombstone present → true_mutations (no-op), skip write.
        // Tombstone absent → false_mutations fire the write.
        let request = v2::CheckAndMutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path,
            predicate_filter: Some(tombstone_predicate()),
            true_mutations: vec![], // Tombstone matched → skip write.
            false_mutations,        // No tombstone → write the object.
            ..Default::default()
        };

        let is_tombstone = self
            .with_retry("put_non_tombstone", || async {
                self.bigtable
                    .client()
                    .check_and_mutate_row(request.clone())
                    .await
            })
            .await?
            .predicate_matched;

        if is_tombstone {
            Ok(ConditionalOutcome::Tombstone)
        } else {
            Ok(ConditionalOutcome::Executed)
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<ConditionalOutcome> {
        tracing::debug!("Conditional delete from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let delete_mutation = v2::Mutation {
            mutation: Some(mutation::Mutation::DeleteFromRow(
                mutation::DeleteFromRow {},
            )),
        };

        // Predicate: tombstone present → true_mutations (no-op), leave intact.
        // Tombstone absent → false_mutations fire the delete.
        let request = v2::CheckAndMutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path,
            predicate_filter: Some(tombstone_predicate()),
            true_mutations: vec![], // Tombstone matched → leave intact (no mutations).
            false_mutations: vec![delete_mutation], // Not a tombstone → delete the row.
            ..Default::default()
        };

        let is_tombstone = self
            .with_retry("delete_non_tombstone", || async {
                self.bigtable
                    .client()
                    .check_and_mutate_row(request.clone())
                    .await
            })
            .await?
            .predicate_matched;

        if is_tombstone {
            Ok(ConditionalOutcome::Tombstone)
        } else {
            Ok(ConditionalOutcome::Executed)
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn create_tombstone(&self, id: &ObjectId, tombstone: Tombstone) -> Result<()> {
        tracing::debug!("Writing tombstone to Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();
        self.put_tombstone_row(path, &tombstone, "create_tombstone")
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet> {
        tracing::debug!("Reading from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let Some(row) = self.read_row(&path, None, "get_tiered_object").await? else {
            return Ok(TieredGet::NotFound);
        };

        let needs_bump = row.needs_tti_bump();
        match row {
            RowData::Tombstone { meta, .. } => {
                if needs_bump {
                    // TODO: Schedule into background persistently so this doesn't get lost on restarts
                    let tombstone = Tombstone {
                        expiration_policy: meta.expiration_policy,
                    };
                    let _ = self.put_tombstone_row(path, &tombstone, "tti-bump").await;
                }
                Ok(TieredGet::Tombstone(Tombstone {
                    expiration_policy: meta.expiration_policy,
                }))
            }
            RowData::Object { metadata, payload } => {
                if needs_bump {
                    // TODO: Schedule into background persistently so this doesn't get lost on restarts
                    let _ = self
                        .put_row(path, &metadata, payload.clone(), "tti-bump")
                        .await;
                }
                let mut metadata = metadata;
                metadata.size = Some(payload.len());
                Ok(TieredGet::Object(metadata, crate::stream::single(payload)))
            }
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
        tracing::debug!("Reading metadata from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        // Read metadata and tombstone columns — skip the (potentially large) payload.
        // NB: `metadata.size` will not be populated since the payload is not fetched.
        let row_opt = self
            .read_row(&path, Some(metadata_filter()), "get_tiered_metadata")
            .await?;
        let Some(row) = row_opt else {
            return Ok(TieredMetadata::NotFound);
        };

        let needs_bump = row.needs_tti_bump();
        match row {
            RowData::Tombstone { meta, .. } => {
                if needs_bump {
                    // Tombstone has no payload — bump without a separate payload read.
                    let tombstone = Tombstone {
                        expiration_policy: meta.expiration_policy,
                    };
                    let _ = self.put_tombstone_row(path, &tombstone, "tti-bump").await;
                }
                Ok(TieredMetadata::Tombstone(Tombstone {
                    expiration_policy: meta.expiration_policy,
                }))
            }
            RowData::Object { metadata, .. } => {
                // Conditional TTI bump: read the payload only when a bump is actually needed.
                if needs_bump {
                    // TODO: Schedule into background persistently so this doesn't get lost on restarts
                    // Best-effort — failures here should not fail the metadata read.
                    let payload_read = self
                        .read_row(&path, Some(column_filter(COLUMN_PAYLOAD)), "tti-bump")
                        .await;
                    if let Ok(Some(RowData::Object { payload, .. })) = payload_read {
                        let _ = self.put_row(path, &metadata, payload, "tti-bump").await;
                    }
                }
                Ok(TieredMetadata::Object(metadata))
            }
        }
    }
}

/// Converts the given TTL duration to a microsecond-precision unix timestamp.
///
/// The TTL is anchored at the provided `from` timestamp, which defaults to `SystemTime::now()`. As
/// required by BigTable, the resulting timestamp has millisecond precision, with the last digits at
/// 0.
fn ttl_to_micros(ttl: Duration, from: SystemTime) -> Result<i64> {
    let deadline = from.checked_add(ttl).ok_or_else(|| Error::Generic {
        context: format!(
            "TTL duration overflow: {} plus {}s cannot be represented as SystemTime",
            humantime::format_rfc3339_seconds(from),
            ttl.as_secs()
        ),
        cause: None,
    })?;
    let millis = deadline
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| Error::Generic {
            context: format!(
                "unable to get duration since UNIX_EPOCH for SystemTime {}",
                humantime::format_rfc3339_seconds(deadline)
            ),
            cause: Some(Box::new(e)),
        })?
        .as_millis();
    (millis * 1000).try_into().map_err(|e| Error::Generic {
        context: format!("failed to convert {}ms to i64 microseconds", millis),
        cause: Some(Box::new(e)),
    })
}

/// Converts a microsecond-precision unix timestamp to a `SystemTime`.
fn micros_to_time(micros: i64) -> Option<SystemTime> {
    let micros = u64::try_from(micros).ok()?;
    let duration = Duration::from_micros(micros);
    SystemTime::UNIX_EPOCH.checked_add(duration)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;
    use crate::stream;

    // NB: Not run most of these tests, you need to have a BigTable emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<BigTableBackend> {
        BigTableBackend::new(BigTableConfig {
            endpoint: Some("localhost:8086".into()),
            project_id: "testing".into(),
            instance_name: "objectstore".into(),
            table_name: "objectstore".into(),
            connections: None,
        })
        .await
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            time_created: Some(SystemTime::now()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend.delete_object(&id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        backend.delete_object(&id).await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_returns_metadata() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_metadata(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_tombstone_returns_error() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend
            .create_tombstone(
                &id,
                Tombstone {
                    expiration_policy: ExpirationPolicy::Manual,
                },
            )
            .await?;

        let result = backend.get_metadata(&id).await;
        assert!(
            matches!(result, Err(Error::UnexpectedTombstone)),
            "expected UnexpectedTombstone, got {result:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_bumps_tti() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // Manually rewrite the row with a timestamp that will trigger a bump.
        // The bump condition is: expire_at < now + tti - TTI_DEBOUNCE.
        // Set the expiry to just under the threshold but still in the future
        // (so it doesn't get filtered as expired).
        let path = id.as_storage_path().to_string().into_bytes();
        let old_deadline = SystemTime::now() + tti - TTI_DEBOUNCE - Duration::from_secs(60);
        let old_micros = old_deadline
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            * 1000;

        let mutations = [
            mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {}),
            mutation::Mutation::SetCell(mutation::SetCell {
                family_name: FAMILY_GC.to_owned(),
                column_qualifier: COLUMN_PAYLOAD.to_owned(),
                timestamp_micros: old_micros,
                value: b"hello, world".to_vec(),
            }),
            mutation::Mutation::SetCell(mutation::SetCell {
                family_name: FAMILY_GC.to_owned(),
                column_qualifier: COLUMN_METADATA.to_owned(),
                timestamp_micros: old_micros,
                value: serde_json::to_vec(&metadata).unwrap(),
            }),
        ];
        backend
            .mutate(path.clone(), mutations, "test-setup")
            .await?;

        // First get_metadata sees the old timestamp and triggers a TTI bump.
        let pre_meta = backend.get_metadata(&id).await?.unwrap();
        let pre_expiry = pre_meta.time_expires.unwrap();

        // Second get_metadata sees the bumped timestamp.
        let post_meta = backend.get_metadata(&id).await?.unwrap();
        let post_expiry = post_meta.time_expires.unwrap();
        assert!(
            post_expiry > pre_expiry,
            "TTI bump should have extended the expiry: {pre_expiry:?} -> {post_expiry:?}"
        );

        // Verify the payload is still intact after the bump.
        let (_, stream) = backend.get_object(&id).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(&payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_does_not_bump_fresh_tti() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // A freshly written object has time_expires ≈ now + 2d, which is well outside
        // the bump window (now + 2d - 1d = now + 1d). No bump should occur.
        let first = backend.get_metadata(&id).await?.unwrap();
        let first_expiry = first.time_expires.unwrap();

        let second = backend.get_metadata(&id).await?.unwrap();
        let second_expiry = second.time_expires.unwrap();

        assert_eq!(
            first_expiry, second_expiry,
            "Fresh TTI object should not have its expiry bumped"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_real_object() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, ConditionalOutcome::Executed);

        let get_result = backend.get_object(&id).await?;
        assert!(get_result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend
            .create_tombstone(
                &id,
                Tombstone {
                    expiration_policy: ExpirationPolicy::Manual,
                },
            )
            .await?;

        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, ConditionalOutcome::Tombstone);

        // Tombstone should still exist — delete_non_tombstone leaves it intact.
        let get_result = backend.get_tiered_metadata(&id).await?;
        assert!(
            matches!(get_result, TieredMetadata::Tombstone(_)),
            "tombstone should still exist after delete_non_tombstone"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, ConditionalOutcome::Executed);

        Ok(())
    }

    #[tokio::test]
    async fn test_hv_get_not_found() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        assert!(matches!(
            backend.get_tiered_object(&id).await?,
            TieredGet::NotFound
        ));
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::NotFound
        ));

        Ok(())
    }

    /// Legacy tombstones (written before the `r` column migration) are detected by all read
    /// paths and conditional predicates.
    ///
    /// Uses `Manual` expiration so `timestamp_micros = -1` (server-assigned ≈ write time) does
    /// not trigger immediate expiry.
    #[tokio::test]
    async fn test_legacy_tombstone_compat() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();

        // The legacy regex predicate requires `is_redirect_tombstone` to be the first JSON field,
        // so construct the bytes manually (serde_json sorts keys alphabetically).
        backend
            .mutate(
                id.as_storage_path().to_string().into_bytes(),
                [mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: FAMILY_MANUAL.to_owned(),
                    column_qualifier: COLUMN_METADATA.to_owned(),
                    timestamp_micros: -1,
                    value: b"{\"is_redirect_tombstone\":true}".to_vec(),
                })],
                "test-setup",
            )
            .await?;

        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected tombstone");
        };
        assert_eq!(t.expiration_policy, ExpirationPolicy::Manual);
        assert!(matches!(
            backend.get_tiered_object(&id).await?,
            TieredGet::Tombstone(_)
        ));
        assert_eq!(
            backend
                .put_non_tombstone(&id, &Metadata::default(), bytes::Bytes::new())
                .await?,
            ConditionalOutcome::Tombstone,
        );
        assert_eq!(
            backend.delete_non_tombstone(&id).await?,
            ConditionalOutcome::Tombstone,
        );

        Ok(())
    }

    /// Legacy tombstones with a `TimeToLive` expiration policy have the policy correctly
    /// deserialized from the `m` column JSON.
    ///
    /// A future cell timestamp (now + TTL) is required so `expires_before` does not immediately
    /// filter the row: the cell timestamp doubles as the GC expiry time.
    #[tokio::test]
    async fn test_legacy_tombstone_expiration_policy() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();

        let ttl = Duration::from_secs(2 * 24 * 3600);
        let secs = ttl.as_secs();
        let future_micros = (SystemTime::now() + ttl)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            * 1000;
        let meta = format!(
            r#"{{"is_redirect_tombstone":true,"expiration_policy":{{"TimeToLive":{{"secs":{secs},"nanos":0}}}}}}"#
        );

        backend
            .mutate(
                id.as_storage_path().to_string().into_bytes(),
                [mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: FAMILY_GC.to_owned(),
                    column_qualifier: COLUMN_METADATA.to_owned(),
                    timestamp_micros: future_micros,
                    value: meta.into_bytes(),
                })],
                "test-setup",
            )
            .await?;

        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(t.expiration_policy, ExpirationPolicy::TimeToLive(ttl),);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_tombstone_round_trip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let expiration_policy = ExpirationPolicy::TimeToLive(Duration::from_secs(3600));
        backend
            .create_tombstone(&id, Tombstone { expiration_policy })
            .await?;

        // Both hv methods must surface the tombstone with the correct expiration_policy.
        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected TieredMetadataResponse::Tombstone");
        };
        assert_eq!(t.expiration_policy, expiration_policy);
        assert!(matches!(
            backend.get_tiered_object(&id).await?,
            TieredGet::Tombstone(_)
        ));

        // Legacy get_object / get_metadata must error rather than leak tombstone data.
        assert!(matches!(
            backend.get_object(&id).await,
            Err(Error::UnexpectedTombstone)
        ));
        assert!(matches!(
            backend.get_metadata(&id).await,
            Err(Error::UnexpectedTombstone)
        ));

        Ok(())
    }
}
