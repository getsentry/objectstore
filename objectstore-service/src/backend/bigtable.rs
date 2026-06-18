//! BigTable backend for high-volume, low-latency storage of small objects.
//!
//! # Row Format
//!
//! Each row key is the object's storage path. A row contains either an **object** or a
//! **tombstone** — never both. The two layouts are mutually exclusive and distinguished by
//! column presence:
//!
//! | Column | Family    | Content                     | Present when       |
//! |--------|-----------|-----------------------------|--------------------|
//! | `p`    | `fg`/`fm` | Compressed payload bytes    | Object row only    |
//! | `m`    | `fg`/`fm` | [`Metadata`] JSON           | Object row only    |
//! | `r`    | `fg`/`fm` | Redirect path to LT storage | Tombstone row only |
//! | `t`    | `fg`/`fm` | [`Tombstone`] metadata JSON | Tombstone row only |
//!
//! The `r` column signals a tombstone row: its **value** is the long-term `ObjectId`
//! serialized via `as_storage_path()`. Callers can resolve the LT object directly from the
//! `r` value without reconstructing it from the row key.
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
use bytes::Bytes;
use futures_util::TryStreamExt;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};
use serde::{Deserialize, Serialize};
use tonic::Code;

use crate::backend::common::{
    Backend, DeleteResponse, GetResponse, HighVolumeBackend, MetadataResponse, PutResponse,
    TieredGet, TieredMetadata, TieredWrite, Tombstone,
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
const TTI_DEBOUNCE: Duration = Duration::from_hours(24);
/// Permission scopes required for accessing the BigTable data API.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/bigtable.data"];

/// How often to retry failed requests.
const REQUEST_RETRY_COUNT: usize = 2;
/// How many times to retry a CAS mutation before giving up and returning an error.
const CAS_RETRY_COUNT: usize = 3;

/// Column that stores the raw payload (compressed).
const COLUMN_PAYLOAD: &[u8] = b"p";
/// Column that stores metadata in JSON.
const COLUMN_METADATA: &[u8] = b"m";
/// Column that stores the redirect path for tombstone rows.
const COLUMN_REDIRECT: &[u8] = b"r";
/// Column that stores [`TombstoneMeta`] JSON for tombstone rows.
const COLUMN_TOMBSTONE_META: &[u8] = b"t";
/// Regex to match all non-payload columns (`m`, `r`, `t`) for metadata-only reads.
const FILTER_META: &[u8] = b"^[mrt]$";

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

/// Creates a row filter that matches a single column by exact qualifier.
fn column_filter(column: &[u8]) -> v2::RowFilter {
    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::ColumnQualifierRegexFilter(
            [b"^", column, b"$"].concat(),
        )),
    }
}

/// Creates a row filter matching the legacy tombstone format: `m` column JSON starts with
/// `{"is_redirect_tombstone":true`.
///
/// After legacy tombstones expire naturally this filter becomes dead code in both callers.
fn legacy_tombstone_filter() -> v2::RowFilter {
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
    }
}

/// Wraps `inner` so that it only matches live (non-expired) cells.
fn live_row_filter(inner: v2::RowFilter) -> v2::RowFilter {
    let now_micros = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
        * 1000;

    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Interleave(
            v2::row_filter::Interleave {
                filters: vec![
                    // Manual family: never expires.
                    v2::RowFilter {
                        filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
                            filters: vec![
                                v2::RowFilter {
                                    filter: Some(v2::row_filter::Filter::FamilyNameRegexFilter(
                                        format!("^{FAMILY_MANUAL}$"),
                                    )),
                                },
                                inner.clone(),
                            ],
                        })),
                    },
                    // GC family: only match non-expired cells.
                    v2::RowFilter {
                        filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
                            filters: vec![
                                v2::RowFilter {
                                    filter: Some(v2::row_filter::Filter::FamilyNameRegexFilter(
                                        format!("^{FAMILY_GC}$"),
                                    )),
                                },
                                v2::RowFilter {
                                    filter: Some(v2::row_filter::Filter::TimestampRangeFilter(
                                        v2::TimestampRange {
                                            start_timestamp_micros: now_micros,
                                            end_timestamp_micros: 0,
                                        },
                                    )),
                                },
                                inner,
                            ],
                        })),
                    },
                ],
            },
        )),
    }
}

/// Builds a raw row filter that matches any tombstone row, new- or legacy-format.
///
/// New format: presence of the `r` column.
/// Legacy format: `is_redirect_tombstone: true` in the `m` column JSON.
///
/// After legacy tombstones expire naturally this simplifies to just
/// `column_filter(COLUMN_REDIRECT)`.
fn tombstone_filter() -> v2::RowFilter {
    let filter = v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Interleave(
            v2::row_filter::Interleave {
                filters: vec![column_filter(COLUMN_REDIRECT), legacy_tombstone_filter()],
            },
        )),
    };
    live_row_filter(filter)
}

/// Returns a [`MutatePredicate`] that matches any tombstone row.
///
/// Mutations run only when no tombstone is present (`predicate_matched == false`).
/// Used by [`BigTableBackend::put_non_tombstone`], [`BigTableBackend::delete_non_tombstone`],
/// and [`BigTableBackend::compare_and_write`] as the `CheckAndMutateRow` predicate.
fn tombstone_predicate() -> MutatePredicate {
    MutatePredicate::Exclude(tombstone_filter())
}

/// Builds an anchored regex pattern (`^…$`) that matches `value` literally.
///
/// Uses [`regex::escape`] so that metacharacters in storage paths (`.`, `/`, etc.)
/// are treated as literal bytes.
fn exact_value_regex(value: &str) -> Vec<u8> {
    format!("^{}$", regex::escape(value)).into_bytes()
}

/// Matches tombstones whose redirect resolves to `target`.
///
/// ## Predicate Matches
///
/// Must be used with `true_mutations` and `predicate_matched == true`.
///
/// ## Details
///
/// Always includes an exact match on the `r` (redirect) column:
/// - Chain: `r` column present AND value == `target` storage path
///
/// When `target == own_id` (the caller expects a legacy identity redirect), the
/// exact match is wrapped in an Interleave with two additional fallbacks:
/// - Chain: `r` column present AND value == `b""` (empty-sentinel written before the redirect
///   column stored the path)
/// - Chain: `m` column present AND value matches `{"is_redirect_tombstone":true...}` regex
///   (legacy metadata format predating the dedicated `r` column)
fn redirect_target_filter(target: &ObjectId, own_id: &ObjectId) -> v2::RowFilter {
    let target_path = exact_value_regex(&target.as_storage_path().to_string());

    let exact_match = v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
            filters: vec![
                column_filter(COLUMN_REDIRECT),
                v2::RowFilter {
                    filter: Some(v2::row_filter::Filter::ValueRegexFilter(target_path)),
                },
            ],
        })),
    };

    if target != own_id {
        return live_row_filter(exact_match);
    }

    let empty_redirect_match = v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
            filters: vec![
                column_filter(COLUMN_REDIRECT),
                v2::RowFilter {
                    filter: Some(v2::row_filter::Filter::ValueRegexFilter(b"^$".to_vec())),
                },
            ],
        })),
    };

    // Also match legacy tombstones that resolve to the HV id:
    // - empty `r` value (written before the redirect column stored the path)
    // - legacy `m` column format (`is_redirect_tombstone: true`)
    let filter = v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Interleave(
            v2::row_filter::Interleave {
                filters: vec![exact_match, empty_redirect_match, legacy_tombstone_filter()],
            },
        )),
    };
    live_row_filter(filter)
}

/// Returns a [`MutatePredicate`] that matches tombstones whose redirect resolves to either `old` or `new`.
///
/// Mutations run only when the predicate matches (`predicate_matched == true`):
/// equivalent to `t == old || t == new`. Built as an Interleave of two
/// [`redirect_target_filter`] calls — yields cells iff at least one branch matches.
/// An absent row or non-tombstone row yields 0 cells, so `predicate_matched = false` (conflict).
fn update_predicate(old: &ObjectId, new: &ObjectId, own_id: &ObjectId) -> MutatePredicate {
    MutatePredicate::Include(v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Interleave(
            v2::row_filter::Interleave {
                filters: vec![
                    redirect_target_filter(old, own_id),
                    redirect_target_filter(new, own_id),
                ],
            },
        )),
    })
}

/// Returns a [`MutatePredicate`] that matches rows where no conflicting tombstone exists.
///
/// Mutations run only when the row is conflict-free (`predicate_matched == false`):
/// no tombstone is present, or the tombstone's redirect already points to `target`.
///
/// Built as an inverted `Condition` filter:
/// - Predicate: [`redirect_target_filter`]`(target)` — tombstone already points to `target`?
/// - True branch: `BlockAllFilter` → 0 cells (already at target, safe state).
/// - False branch: [`tombstone_filter`] → 0 cells when no tombstone exists.
///
/// Both safe states yield 0 cells, so `predicate_matched = false` in both cases.
fn optional_target_predicate(target: &ObjectId, own_id: &ObjectId) -> MutatePredicate {
    MutatePredicate::Exclude(v2::RowFilter {
        filter: Some(v2::row_filter::Filter::Condition(Box::new(
            v2::row_filter::Condition {
                predicate_filter: Some(Box::new(redirect_target_filter(target, own_id))),
                true_filter: Some(Box::new(v2::RowFilter {
                    filter: Some(v2::row_filter::Filter::BlockAllFilter(true)),
                })),
                false_filter: Some(Box::new(tombstone_filter())),
            },
        ))),
    })
}

/// The condition under which a [`BigTableBackend::check_and_mutate`] write proceeds.
///
/// Each variant pairs a row filter with the state that makes the write safe:
/// `Include` writes when the row matches; `Exclude` writes when it does not.
#[derive(Clone, Debug)]
enum MutatePredicate {
    /// Write proceeds when the filter matches the row.
    ///
    /// Mutations run in `true_mutations`; succeeds when `predicate_matched == true`.
    Include(v2::RowFilter),
    /// Write proceeds when the filter does not match the row.
    ///
    /// Mutations run in `false_mutations`; succeeds when `predicate_matched == false`.
    Exclude(v2::RowFilter),
}

/// Creates a row filter that reads all non-payload columns (`m`, `r`, `t`).
///
/// Used by metadata-only reads to avoid fetching the (potentially large) payload column
/// while still being able to detect both new- and legacy-format tombstones.
fn metadata_filter() -> v2::RowFilter {
    v2::RowFilter {
        filter: Some(v2::row_filter::Filter::ColumnQualifierRegexFilter(
            FILTER_META.to_owned(),
        )),
    }
}

fn mutation(mutation: mutation::Mutation) -> v2::Mutation {
    v2::Mutation {
        mutation: Some(mutation),
    }
}

/// Creates a `DeleteFromRow` mutation wrapped in the outer [`v2::Mutation`] envelope.
fn delete_row_mutation() -> v2::Mutation {
    mutation(mutation::Mutation::DeleteFromRow(
        mutation::DeleteFromRow {},
    ))
}

/// Builds the three mutations that write an object row: clear existing data,
/// then set the payload and metadata cells.
///
/// Used by both [`BigTableBackend::put_row`] (unconditional write) and
/// [`BigTableBackend::put_non_tombstone`] (conditional write).
fn object_mutations(
    mut metadata: Metadata,
    payload: Vec<u8>,
    now: SystemTime,
) -> Result<[v2::Mutation; 3]> {
    let (family, timestamp_micros) = match metadata.expiration_policy {
        ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
        ExpirationPolicy::TimeToLive(ttl) => (FAMILY_GC, ttl_to_micros(ttl, now)?),
        ExpirationPolicy::TimeToIdle(tti) => (FAMILY_GC, ttl_to_micros(tti, now)?),
    };

    // Record the payload size in the metadata before persisting it.
    metadata.size = Some(payload.len());

    let metadata_bytes = serde_json::to_vec(&metadata)
        .map_err(|cause| Error::serde("failed to serialize metadata", cause))?;

    Ok([
        // NB: We explicitly delete the row to clear metadata on overwrite.
        delete_row_mutation(),
        mutation(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_PAYLOAD.to_owned(),
            timestamp_micros,
            value: payload,
        })),
        mutation(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_METADATA.to_owned(),
            timestamp_micros,
            value: metadata_bytes,
        })),
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
fn tombstone_mutations(tombstone: &Tombstone, now: SystemTime) -> Result<[v2::Mutation; 3]> {
    let (family, timestamp_micros) = match tombstone.expiration_policy {
        ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
        ExpirationPolicy::TimeToLive(ttl) => (FAMILY_GC, ttl_to_micros(ttl, now)?),
        ExpirationPolicy::TimeToIdle(tti) => (FAMILY_GC, ttl_to_micros(tti, now)?),
    };

    let tombstone_meta = TombstoneMeta {
        expiration_policy: tombstone.expiration_policy,
    };

    Ok([
        delete_row_mutation(),
        mutation(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_REDIRECT.to_owned(),
            timestamp_micros,
            value: tombstone.target.as_storage_path().to_string().into_bytes(),
        })),
        mutation(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_TOMBSTONE_META.to_owned(),
            timestamp_micros,
            value: serde_json::to_vec(&tombstone_meta)
                .map_err(|cause| Error::serde("failed to serialize tombstone", cause))?,
        })),
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
        target: Vec<u8>,
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
        let mut redirect_target = Vec::new();
        let mut expire_at = None;
        let mut payload = Vec::new();

        for cell in cells {
            // NB: All cells are written with the same timestamp; last write is safe.

            // Only derive expiration from GC-family cells — manual-family cells
            // use server-assigned timestamps that don't represent expiration.
            if cell.family_name == FAMILY_GC {
                expire_at = micros_to_time(cell.timestamp_micros);
            }

            match cell.qualifier.as_slice() {
                COLUMN_REDIRECT => {
                    redirect_detected = true;
                    redirect_target = cell.value;
                }
                COLUMN_PAYLOAD => {
                    payload = cell.value;
                }
                COLUMN_TOMBSTONE_META => {
                    tombstone_meta_opt =
                        Some(serde_json::from_slice(&cell.value).map_err(|cause| {
                            Error::serde("failed to deserialize tombstone meta", cause)
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
                    } else {
                        metadata_opt =
                            Some(serde_json::from_slice(&cell.value).map_err(|cause| {
                                Error::serde("failed to deserialize metadata", cause)
                            })?);
                    }
                }
                _ => {}
            }
        }

        Ok(if redirect_detected {
            RowData::Tombstone {
                target: redirect_target,
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

/// Parses the raw `r` column bytes into a redirect target [`ObjectId`].
///
/// For tombstones with an empty `r` value, falls back to the ID of the tombstone
/// itself and emits a `bigtable.empty_redirect_read` metric so deployments can
/// track when it is safe to remove the legacy empty-value code path.
fn parse_redirect_target(redirect_path: &[u8], tombstone_id: &ObjectId) -> Result<ObjectId> {
    if redirect_path.is_empty() {
        objectstore_metrics::count!("bigtable.empty_redirect_read");
        Ok(tombstone_id.clone())
    } else {
        let redirect_str = std::str::from_utf8(redirect_path)
            .map_err(|_| Error::generic("invalid UTF-8 in redirect path"))?;
        ObjectId::from_storage_path(redirect_str)
            .ok_or_else(|| Error::generic("corrupt redirect path"))
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

        let response = retry(action, || async {
            self.bigtable.client().read_rows(request.clone()).await
        })
        .await?;
        debug_assert!(response.len() <= 1, "Expected at most one row");

        let Some((_, cells)) = response.into_iter().next() else {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        };

        let row = RowData::from_cells(cells)?;
        Ok(if row.expires_before(SystemTime::now()) {
            None
        } else {
            Some(row)
        })
    }

    async fn mutate(
        &self,
        path: Vec<u8>,
        mutations: impl Into<Vec<v2::Mutation>>,
        action: &'static str,
    ) -> Result<v2::MutateRowResponse> {
        let request = v2::MutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path,
            mutations: mutations.into(),
            ..Default::default()
        };

        let response = retry(action, || async {
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
        let mutations = object_mutations(metadata.clone(), payload, SystemTime::now())?;
        self.mutate(path, mutations, action).await
    }

    async fn put_tombstone_row(
        &self,
        path: Vec<u8>,
        tombstone: &Tombstone,
        action: &'static str,
    ) -> Result<v2::MutateRowResponse> {
        let mutations = tombstone_mutations(tombstone, SystemTime::now())?;
        self.mutate(path, mutations, action).await
    }

    /// Best-effort TTI bump for a row.
    ///
    /// If the payload isn't loaded, it will be fetched. Failures are ignored silently.
    async fn bump_tti(&self, path: Vec<u8>, row: &RowData, loaded: bool, hv_id: &ObjectId) {
        let expiration_policy = row.expiration_policy();

        match row {
            RowData::Tombstone { target, .. } => {
                let target = match parse_redirect_target(target, hv_id) {
                    Ok(target) => target,
                    Err(e) => {
                        objectstore_log::error!(!!&e, "invalid redirect target in tombstone row");
                        return;
                    }
                };

                let tombstone = Tombstone {
                    target,
                    expiration_policy,
                };
                let _ = self.put_tombstone_row(path, &tombstone, "tti-bump").await;
            }
            RowData::Object { metadata, payload } if loaded => {
                let _ = self
                    .put_row(path, metadata, payload.clone(), "tti-bump")
                    .await;
            }
            RowData::Object { metadata, .. } => {
                let payload_read = self
                    .read_row(&path, Some(column_filter(COLUMN_PAYLOAD)), "tti-bump")
                    .await;

                if let Ok(Some(RowData::Object { payload, .. })) = payload_read {
                    let _ = self.put_row(path, metadata, payload, "tti-bump").await;
                }
            }
        }
    }

    /// Executes a `CheckAndMutateRow` request.
    async fn check_and_mutate(
        &self,
        row_key: Vec<u8>,
        predicate: MutatePredicate,
        mutations: impl Into<Vec<v2::Mutation>>,
        context: &'static str,
    ) -> Result<bool> {
        let (filter, true_mutations, false_mutations, success_on_match) = match predicate {
            MutatePredicate::Include(f) => (f, mutations.into(), vec![], true),
            MutatePredicate::Exclude(f) => (f, vec![], mutations.into(), false),
        };

        let request = v2::CheckAndMutateRowRequest {
            table_name: self.table_path.clone(),
            row_key,
            predicate_filter: Some(filter),
            true_mutations,
            false_mutations,
            ..Default::default()
        };

        let future = retry(context, || async {
            self.bigtable
                .client()
                .check_and_mutate_row(request.clone())
                .await
        });

        Ok(future.await?.predicate_matched == success_on_match)
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
        objectstore_log::debug!("Writing to Bigtable backend");
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
    async fn get_object(&self, id: &ObjectId, range: Option<ByteRange>) -> Result<GetResponse> {
        match self.get_tiered_object(id, range).await? {
            TieredGet::Object(metadata, content_range, payload) => {
                Ok(Some((metadata, content_range, payload)))
            }
            TieredGet::Tombstone(_) => Err(Error::UnexpectedTombstone),
            TieredGet::NotFound => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        match self.get_tiered_metadata(id).await? {
            TieredMetadata::Object(metadata) => Ok(Some(metadata)),
            TieredMetadata::Tombstone(_) => Err(Error::UnexpectedTombstone),
            TieredMetadata::NotFound => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        self.mutate(path, [delete_row_mutation()], "delete").await?;

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
    ) -> Result<Option<Tombstone>> {
        objectstore_log::debug!("Conditional put to Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = object_mutations(metadata.clone(), payload.to_vec(), SystemTime::now())?;

        for _ in 0..CAS_RETRY_COUNT {
            let write_succeeded = self
                .check_and_mutate(
                    path.clone(),
                    tombstone_predicate(),
                    mutations.clone(),
                    "put_non_tombstone",
                )
                .await?;

            if write_succeeded {
                return Ok(None);
            }

            // A tombstone was present: read its data for the caller.
            let row = self
                .read_row(&path, Some(metadata_filter()), "put_non_tombstone")
                .await?;

            match row {
                Some(RowData::Tombstone { target, meta, .. }) => {
                    return Ok(Some(Tombstone {
                        target: parse_redirect_target(&target, id)?,
                        expiration_policy: meta.expiration_policy,
                    }));
                }
                // Race: Tombstone was replaced by an object, retry to overwrite
                Some(RowData::Object { .. }) => continue,
                // Race: Tombstone was deleted, retry to write.
                None => continue,
            }
        }

        Err(Error::generic("BigTable: race loop in put_non_tombstone"))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        range: Option<ByteRange>,
    ) -> Result<TieredGet> {
        objectstore_log::debug!("Reading from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let Some(row) = self.read_row(&path, None, "get_tiered_object").await? else {
            return Ok(TieredGet::NotFound);
        };

        if row.needs_tti_bump() {
            self.bump_tti(path.clone(), &row, true, id).await;
        }

        Ok(match row {
            RowData::Tombstone { meta, target, .. } => TieredGet::Tombstone(Tombstone {
                target: parse_redirect_target(&target, id)?,
                expiration_policy: meta.expiration_policy,
            }),
            RowData::Object { metadata, payload } => {
                let mut metadata = metadata;
                let payload = Bytes::from(payload);
                if metadata.size.is_none() {
                    metadata.size = Some(payload.len());
                }

                let (content_range, payload) = apply_range(payload, range)?;
                TieredGet::Object(metadata, content_range, crate::stream::single(payload))
            }
        })
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata> {
        objectstore_log::debug!("Reading metadata from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        // Read metadata and tombstone columns — skip the (potentially large) payload.
        // NB: `metadata.size` will only be populated if the size was added to the metadata before
        // writing to Bigtable.
        let row_opt = self
            .read_row(&path, Some(metadata_filter()), "get_tiered_metadata")
            .await?;
        let Some(row) = row_opt else {
            return Ok(TieredMetadata::NotFound);
        };

        if row.needs_tti_bump() {
            self.bump_tti(path.clone(), &row, false, id).await;
        }

        Ok(match row {
            RowData::Tombstone { meta, target, .. } => TieredMetadata::Tombstone(Tombstone {
                target: parse_redirect_target(&target, id)?,
                expiration_policy: meta.expiration_policy,
            }),
            RowData::Object { metadata, .. } => TieredMetadata::Object(metadata),
        })
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>> {
        objectstore_log::debug!("Conditional delete from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();

        for _ in 0..CAS_RETRY_COUNT {
            let write_succeeded = self
                .check_and_mutate(
                    path.clone(),
                    tombstone_predicate(),
                    [delete_row_mutation()],
                    "delete_non_tombstone",
                )
                .await?;

            if write_succeeded {
                return Ok(None);
            }

            // A tombstone was present: read its data for the caller.
            let row = self
                .read_row(&path, Some(metadata_filter()), "delete_non_tombstone")
                .await?;

            match row {
                Some(RowData::Tombstone { target, meta, .. }) => {
                    return Ok(Some(Tombstone {
                        target: parse_redirect_target(&target, id)?,
                        expiration_policy: meta.expiration_policy,
                    }));
                }
                // Race: An object replaced the tombstone, delete the new object now.
                Some(RowData::Object { .. }) => continue,
                // Race: Entry was deleted in the meanwhile, nothing left to do.
                None => return Ok(None),
            }
        }

        Err(Error::generic(
            "BigTable: race loop in delete_non_tombstone",
        ))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
    ) -> Result<bool> {
        objectstore_log::debug!("CAS put to Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let now = SystemTime::now();

        let predicate = match (current, write.target()) {
            (Some(old), Some(new)) => update_predicate(old, new, id),
            (Some(target), None) => optional_target_predicate(target, id),
            (None, Some(target)) => optional_target_predicate(target, id),
            (None, None) => tombstone_predicate(),
        };

        let mutations = match write {
            TieredWrite::Tombstone(tombstone) => tombstone_mutations(&tombstone, now)?.into(),
            TieredWrite::Object(m, p) => object_mutations(m, p.to_vec(), now)?.into(),
            TieredWrite::Delete => vec![delete_row_mutation()],
        };

        self.check_and_mutate(path, predicate, mutations, "compare_and_write")
            .await
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

/// Retries a BigTable RPC on transient errors.
async fn retry<T, F>(context: &'static str, f: impl Fn() -> F) -> Result<T>
where
    F: Future<Output = Result<T, BigTableError>> + Send,
{
    let mut retry_count = 0usize;

    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(e) if retry_count >= REQUEST_RETRY_COUNT || !is_retryable(&e) => {
                objectstore_metrics::count!("bigtable.failures", action = context);
                return Err(Error::Generic {
                    context: format!("Bigtable: `{context}` failed"),
                    cause: Some(Box::new(e)),
                });
            }
            Err(e) => {
                retry_count += 1;
                objectstore_metrics::count!("bigtable.retries", action = context);
                objectstore_log::warn!(!!&e, retry_count, context, "Retrying request");
            }
        }
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

/// Resolves an optional byte range against a payload buffer, returning the
/// applicable content range and the (potentially narrowed) payload.
///
/// When `range` is `None`, returns the full payload unchanged. Uses
/// `Bytes::slice` to avoid copying data.
fn apply_range(payload: Bytes, range: Option<ByteRange>) -> Result<(Option<ContentRange>, Bytes)> {
    let Some(byte_range) = range else {
        return Ok((None, payload));
    };

    let total = payload.len() as u64;
    let content_range = byte_range
        .resolve(total)
        .ok_or(Error::RangeNotSatisfiable { total })?;

    let start = content_range.start as usize;
    let end = content_range.end as usize + 1;
    let sliced = payload.slice(start..end);

    Ok((Some(content_range), sliced))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;
    use crate::stream;

    // NB: Most of these tests require a BigTable emulator running. This is done
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

    async fn create_object(
        backend: &BigTableBackend,
        id: &ObjectId,
        metadata: &Metadata,
        payload: &[u8],
        now: SystemTime,
    ) -> Result<()> {
        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = object_mutations(metadata.clone(), payload.to_vec(), now)?;
        backend.mutate(path, mutations, "test-setup").await?;
        Ok(())
    }

    async fn create_tombstone(
        backend: &BigTableBackend,
        id: &ObjectId,
        tombstone: &Tombstone,
        now: SystemTime,
    ) -> Result<()> {
        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = tombstone_mutations(tombstone, now)?;
        backend.mutate(path, mutations, "test-setup").await?;
        Ok(())
    }

    /// Writes a legacy-format tombstone row directly into Bigtable.
    async fn write_legacy_tombstone(
        backend: &BigTableBackend,
        id: &ObjectId,
        expiration_policy: ExpirationPolicy,
        time_expires: Option<SystemTime>,
    ) -> Result<()> {
        let meta = if expiration_policy.is_manual() {
            r#"{"is_redirect_tombstone":true}"#.to_owned()
        } else {
            let policy_json = serde_json::to_string(&expiration_policy).unwrap();
            format!(r#"{{"is_redirect_tombstone":true,"expiration_policy":{policy_json}}}"#)
        };

        let (family, timestamp_micros) = if expiration_policy.is_manual() {
            (FAMILY_MANUAL, -1)
        } else {
            let t =
                time_expires.unwrap_or(SystemTime::now() + expiration_policy.expires_in().unwrap());
            let timestamp = t
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            (FAMILY_GC, timestamp as i64 * 1000)
        };

        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = [mutation(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: family.to_owned(),
            column_qualifier: COLUMN_METADATA.to_owned(),
            timestamp_micros,
            value: meta.into_bytes(),
        }))];

        backend.mutate(path, mutations, "test-setup").await?;

        Ok(())
    }

    /// Writes a new-format tombstone row with an empty `r` value directly,
    /// simulating rows written by code before this change.
    async fn write_empty_redirect_tombstone(
        backend: &BigTableBackend,
        id: &ObjectId,
    ) -> Result<()> {
        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = [
            mutation(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: FAMILY_MANUAL.to_owned(),
                column_qualifier: COLUMN_REDIRECT.to_owned(),
                timestamp_micros: -1,
                value: b"".to_vec(), // empty — legacy format
            })),
            mutation(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: FAMILY_MANUAL.to_owned(),
                column_qualifier: COLUMN_TOMBSTONE_META.to_owned(),
                timestamp_micros: -1,
                value: b"{}".to_vec(),
            })),
        ];

        backend.mutate(path, mutations, "test-setup").await?;

        Ok(())
    }

    // --- Section 1: Object Operations ---

    /// Verifies the full roundtrip: put → get_object (payload + metadata) → get_metadata (metadata).
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

        let (obj_meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(payload, b"hello, world");
        assert_eq!(obj_meta.content_type, metadata.content_type);
        assert_eq!(obj_meta.custom, metadata.custom);

        let head_meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(head_meta.content_type, metadata.content_type);
        assert_eq!(head_meta.custom, metadata.custom);

        Ok(())
    }

    /// Verifies that absent rows return None or succeed silently for all read/delete operations.
    #[tokio::test]
    async fn test_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        assert!(backend.get_object(&id, None).await?.is_none());
        assert!(backend.get_metadata(&id).await?.is_none());
        backend.delete_object(&id).await?;

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
        create_object(&backend, &id, &first_metadata, b"hello", SystemTime::now()).await?;

        let second_metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };
        backend
            .put_object(&id, &second_metadata, stream::single("world"))
            .await?;

        let (meta, _, stream) = backend.get_object(&id, None).await?.unwrap();
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
        create_object(&backend, &id, &metadata, b"hello", SystemTime::now()).await?;
        backend.delete_object(&id).await?;

        assert!(backend.get_object(&id, None).await?.is_none());

        Ok(())
    }

    /// Verifies TTI bump via both `get_object` (loaded=true path) and `get_metadata` (loaded=false path).
    ///
    /// The bump condition is: `expire_at < now + tti - TTI_DEBOUNCE`. We write a stale
    /// timestamp just inside the bump window (still in the future, so the row is not GC'd)
    /// and confirm that a subsequent read returns a later expiry.
    #[tokio::test]
    async fn test_tti_bump() -> Result<()> {
        let backend = create_test_backend().await?;
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_hours(2 * 24);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        // Pass a backdated `now` so the written expiry is inside the bump window:
        // expire_at = past_now + tti = now - TTI_DEBOUNCE - 60s (stale but not yet expired).
        let past_now = SystemTime::now() - TTI_DEBOUNCE - Duration::from_mins(1);

        // Sub-sequence 1: get_object triggers bump (loaded=true path).
        let id1 = make_id();
        create_object(&backend, &id1, &metadata, b"hello, world", past_now).await?;

        // get_object reads the stale row, triggers bump, and returns the pre-bump metadata.
        let (pre_obj_meta, _, _) = backend.get_object(&id1, None).await?.unwrap();
        let pre_obj_expiry = pre_obj_meta.time_expires.unwrap();

        // A second get_metadata reads the freshly bumped row.
        let post_obj_meta = backend.get_metadata(&id1).await?.unwrap();
        let post_obj_expiry = post_obj_meta.time_expires.unwrap();
        assert!(
            post_obj_expiry > pre_obj_expiry,
            "bump should extend expiry"
        );

        // Sub-sequence 2: get_metadata triggers bump (loaded=false path).
        let id2 = make_id();
        create_object(&backend, &id2, &metadata, b"hello, world", past_now).await?;

        // First get_metadata sees the stale row and triggers a bump.
        let pre_meta = backend.get_metadata(&id2).await?.unwrap();
        let pre_expiry = pre_meta.time_expires.unwrap();

        // Second get_metadata reads the freshly bumped row.
        let post_meta = backend.get_metadata(&id2).await?.unwrap();
        let post_expiry = post_meta.time_expires.unwrap();
        assert!(post_expiry > pre_expiry, "bump should extend expiry");

        // Payload must be intact after the loaded=false bump (which re-fetches the payload).
        let (_, _, stream) = backend.get_object(&id2, None).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_no_bump_when_fresh() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_hours(2 * 24);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world", SystemTime::now()).await?;

        // A freshly written object has time_expires ≈ now + 2d, well outside the bump
        // window (now + 2d - 1d = now + 1d). No bump should occur.
        let first = backend.get_metadata(&id).await?.unwrap();
        let second = backend.get_metadata(&id).await?.unwrap();

        assert_eq!(
            first.time_expires.unwrap(),
            second.time_expires.unwrap(),
            "fresh TTI object must not be bumped"
        );

        Ok(())
    }

    // --- Section 2: Expiration ---

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this test. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world", SystemTime::now()).await?;

        assert!(backend.get_object(&id, None).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this test. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world", SystemTime::now()).await?;

        assert!(backend.get_object(&id, None).await?.is_none());

        Ok(())
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
            backend.get_tiered_object(&id, None).await?,
            TieredGet::NotFound
        ));
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::NotFound
        ));

        // object
        let id = make_id();
        let put_meta = Metadata {
            content_type: "text/plain".into(),
            custom: BTreeMap::from_iter([("k".into(), "v".into())]),
            ..Default::default()
        };
        create_object(&backend, &id, &put_meta, b"payload", SystemTime::now()).await?;

        let TieredGet::Object(obj_meta, _, obj_stream) =
            backend.get_tiered_object(&id, None).await?
        else {
            panic!("expected TieredGet::Object");
        };
        let obj_payload = stream::read_to_vec(obj_stream).await?;
        assert_eq!(obj_payload, b"payload");
        assert_eq!(obj_meta.content_type, put_meta.content_type);
        assert_eq!(obj_meta.custom, put_meta.custom);

        let TieredMetadata::Object(head_meta) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected TieredMetadata::Object");
        };
        assert_eq!(head_meta.content_type, put_meta.content_type);
        assert_eq!(head_meta.custom, put_meta.custom);

        // tombstone
        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let tombstone = Tombstone {
            target: lt_id.clone(),
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &hv_id, &tombstone, SystemTime::now()).await?;

        match backend.get_tiered_object(&hv_id, None).await? {
            TieredGet::Tombstone(get_t) => assert_eq!(get_t.target, lt_id),
            other => panic!("expected TieredGet::Tombstone, got {other:?}"),
        }
        match backend.get_tiered_metadata(&hv_id).await? {
            TieredMetadata::Tombstone(meta_t) => assert_eq!(meta_t.target, lt_id,),
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
            .put_non_tombstone(&id, &metadata, Bytes::from_static(b"first"))
            .await?;
        assert_eq!(result, None, "expected None on empty row");
        let (_, _, stream) = backend.get_object(&id, None).await?.unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"first");

        // object: put_non_tombstone on existing object replaces payload, returns None.
        let id = make_id();
        create_object(&backend, &id, &metadata, b"old", SystemTime::now()).await?;
        let result = backend
            .put_non_tombstone(&id, &metadata, Bytes::from_static(b"new"))
            .await?;
        assert_eq!(result, None, "expected None when overwriting object");
        let (_, _, stream) = backend.get_object(&id, None).await?.unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"new");

        // tombstone: put_non_tombstone returns Some(Tombstone) and leaves tombstone intact.
        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let tombstone = Tombstone {
            target: lt_id.clone(),
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &hv_id, &tombstone, SystemTime::now()).await?;
        let result = backend
            .put_non_tombstone(&hv_id, &metadata, Bytes::new())
            .await?;
        let returned = result.expect("expected Some(Tombstone) when row is a tombstone");
        assert_eq!(returned.target, lt_id);
        assert!(
            matches!(
                backend.get_tiered_metadata(&hv_id).await?,
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
    ///
    /// Verifies that the `r` column is correctly detected by both the `ReadRows` column
    /// filter and the `CheckAndMutate` `tombstone_predicate`.
    #[tokio::test]
    async fn test_delete_non_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        // empty
        let id = make_id();
        assert_eq!(backend.delete_non_tombstone(&id).await?, None);

        // object
        let id = make_id();
        let metadata = Metadata::default();
        create_object(&backend, &id, &metadata, b"hello, world", SystemTime::now()).await?;
        assert_eq!(backend.delete_non_tombstone(&id).await?, None);
        assert!(backend.get_object(&id, None).await?.is_none());

        // tombstone
        let id = make_id();
        let tombstone = Tombstone {
            target: id.clone(),
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &id, &tombstone, SystemTime::now()).await?;
        let tombstone = backend
            .delete_non_tombstone(&id)
            .await?
            .expect("expected Some(tombstone)");
        assert_eq!(tombstone.target, id, "tombstone target must be returned");
        assert!(
            matches!(
                backend.get_tiered_metadata(&id).await?,
                TieredMetadata::Tombstone(_)
            ),
            "tombstone must still exist after delete_non_tombstone"
        );

        Ok(())
    }

    // --- Section 4: Compare-and-Write ---

    /// Creating a tombstone on an empty row succeeds; a retry of the same CAS also succeeds.
    ///
    /// After creation, both tiered and legacy APIs reflect the tombstone.
    #[tokio::test]
    async fn test_cas_create_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let hv_id = make_id();
        let lt_id = ObjectId::random(hv_id.context().clone());
        let expiration_policy = ExpirationPolicy::TimeToLive(Duration::from_hours(1));
        let tombstone = Tombstone {
            target: lt_id.clone(),
            expiration_policy,
        };

        // First create succeeds.
        let committed = backend
            .compare_and_write(&hv_id, None, TieredWrite::Tombstone(tombstone.clone()))
            .await?;
        assert!(committed, "expected CAS success on empty row");

        // Tiered reads must see the tombstone with correct target and policy.
        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&hv_id).await? else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(t.target, lt_id, "target must round-trip via r column");
        assert_eq!(t.expiration_policy, expiration_policy);
        match backend.get_tiered_object(&hv_id, None).await? {
            TieredGet::Tombstone(t) => assert_eq!(t.target, lt_id, "round-trip via r column"),
            other => panic!("expected TieredGet::Tombstone, got {other:?}"),
        }

        // Legacy reads must error rather than leak tombstone data.
        assert!(matches!(
            backend.get_object(&hv_id, None).await,
            Err(Error::UnexpectedTombstone)
        ));
        assert!(matches!(
            backend.get_metadata(&hv_id).await,
            Err(Error::UnexpectedTombstone)
        ));

        // Idempotent retry: retry with the same target succeeds
        let second = backend
            .compare_and_write(&hv_id, None, TieredWrite::Tombstone(tombstone))
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
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &hv_id, &tombstone, SystemTime::now()).await?;

        // Wrong target: CAS fails, tombstone unchanged.
        let write = TieredWrite::Tombstone(Tombstone {
            target: new_lt_id.clone(),
            expiration_policy: ExpirationPolicy::Manual,
        });
        let swapped = backend
            .compare_and_write(&hv_id, Some(&wrong_lt_id), write.clone())
            .await?;
        assert!(!swapped, "expected CAS failure due to wrong target");
        match backend.get_tiered_metadata(&hv_id).await? {
            TieredMetadata::Tombstone(t) => assert_eq!(t.target, old_lt_id),
            other => panic!("expected tombstone, got {other:?}"),
        }

        // Correct target: CAS succeeds, target updated.
        let swapped = backend
            .compare_and_write(&hv_id, Some(&old_lt_id), write.clone())
            .await?;
        assert!(swapped, "expected CAS success with correct target");
        match backend.get_tiered_metadata(&hv_id).await? {
            TieredMetadata::Tombstone(t) => assert_eq!(t.target, new_lt_id),
            other => panic!("expected tombstone, got {other:?}"),
        }

        // Idempotent retry: same A→B swap returns true.
        let retry = backend
            .compare_and_write(&hv_id, Some(&old_lt_id), write)
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
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &id, &tombstone, SystemTime::now()).await?;

        // Wrong target: CAS fails, tombstone intact.
        let write = TieredWrite::Object(Metadata::default(), Bytes::new());
        let swapped = backend
            .compare_and_write(&id, Some(&wrong_id), write)
            .await?;
        assert!(!swapped, "expected CAS failure with wrong target");
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::Tombstone(_)
        ));

        // Correct target: CAS succeeds, row becomes an inline object.
        let payload = Bytes::from_static(b"hello inline");
        let write = TieredWrite::Object(Metadata::default(), payload.clone());
        let swapped = backend
            .compare_and_write(&id, Some(&lt_id), write.clone())
            .await?;
        assert!(swapped, "expected CAS success with correct target");
        let TieredGet::Object(_, _, stream) = backend.get_tiered_object(&id, None).await? else {
            panic!("expected inline object after swap");
        };
        assert_eq!(&stream::read_to_vec(stream).await?, payload.as_ref());

        // Idempotent retry: row is already inline (no tombstone), same CAS returns true.
        let retry = backend.compare_and_write(&id, Some(&lt_id), write).await?;
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
        let committed = backend.compare_and_write(&id, None, write).await?;
        assert!(committed, "expected CAS success on empty row");

        let TieredGet::Object(_, _, stream) = backend.get_tiered_object(&id, None).await? else {
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
            expiration_policy: ExpirationPolicy::Manual,
        };
        create_tombstone(&backend, &id, &tombstone, SystemTime::now()).await?;

        // Wrong target: fails, row preserved.
        let deleted = backend
            .compare_and_write(&id, Some(&wrong_id), TieredWrite::Delete)
            .await?;
        assert!(!deleted, "expected CAS failure with wrong target");
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::Tombstone(_)
        ));

        // Correct target: succeeds, row gone.
        let deleted = backend
            .compare_and_write(&id, Some(&lt_id), TieredWrite::Delete)
            .await?;
        assert!(deleted, "expected CAS delete success");
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::NotFound
        ));

        // Idempotent retry: row is already absent (no tombstone), same delete returns true.
        let retry = backend
            .compare_and_write(&id, Some(&lt_id), TieredWrite::Delete)
            .await?;
        assert!(retry, "idempotent retry");

        // Inline object replaced tombstone: Safe to delete since it is an idempotent operation.
        let id2 = make_id();
        let fake_lt_id = ObjectId::random(id2.context().clone());
        let metadata = Metadata::default();
        create_object(&backend, &id2, &metadata, b"data", SystemTime::now()).await?;
        let deleted = backend
            .compare_and_write(&id2, Some(&fake_lt_id), TieredWrite::Delete)
            .await?;
        assert!(deleted, "expected idempotent deletion");

        Ok(())
    }

    // --- Section 5: Legacy Tombstone Compatibility ---

    /// Legacy Manual and TTL tombstones are correctly read via the tiered APIs.
    ///
    /// Uses `Manual` expiration so `timestamp_micros = -1` (server-assigned ≈ write time)
    /// does not trigger immediate expiry.
    #[tokio::test]
    async fn test_legacy_tombstone_reads() -> Result<()> {
        let backend = create_test_backend().await?;

        // Manual policy: get_tiered_metadata returns Tombstone(Manual), get_tiered_object returns Tombstone.
        let id = make_id();
        write_legacy_tombstone(&backend, &id, ExpirationPolicy::Manual, None).await?;

        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected tombstone");
        };
        assert_eq!(t.expiration_policy, ExpirationPolicy::Manual);
        assert!(matches!(
            backend.get_tiered_object(&id, None).await?,
            TieredGet::Tombstone(_)
        ));

        // TTL policy: get_tiered_metadata returns Tombstone with the correct TTL policy.
        //
        // A future cell timestamp (now + TTL) is required so `expires_before` does not
        // immediately filter the row.
        let id = make_id();
        let ttl = Duration::from_hours(2 * 24);
        write_legacy_tombstone(&backend, &id, ExpirationPolicy::TimeToLive(ttl), None).await?;

        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(t.expiration_policy, ExpirationPolicy::TimeToLive(ttl));

        Ok(())
    }

    /// A legacy tombstone with TTI policy is upgraded to the new `r`/`t` column format on read.
    ///
    /// The bump path calls `put_tombstone_row`, which rewrites the row with `r` + `t` columns.
    /// The upgraded row has a fresh cell timestamp (≈ now + TTI), so `time_expires` increases.
    #[tokio::test]
    async fn test_legacy_tombstone_tti_upgrade() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();
        let path = id.as_storage_path().to_string().into_bytes();

        let tti = Duration::from_hours(2 * 24); // must exceed TTI_DEBOUNCE (1 day)

        // Place time_expires just inside the bump window: past `now + tti - TTI_DEBOUNCE`
        // but still in the future so `expires_before(now)` does not filter the row.
        let old_deadline = SystemTime::now() + tti - TTI_DEBOUNCE - Duration::from_mins(1);
        write_legacy_tombstone(
            &backend,
            &id,
            ExpirationPolicy::TimeToIdle(tti),
            Some(old_deadline),
        )
        .await?;

        // First read detects the stale TTI and triggers `put_tombstone_row`.
        let TieredMetadata::Tombstone(_) = backend.get_tiered_metadata(&id).await? else {
            panic!("expected tombstone");
        };

        // After the bump, the row is rewritten with a fresh timestamp (≈ now + TTI).
        let new_deadline = match backend.read_row(&path, None, "test-verify").await? {
            Some(RowData::Tombstone { time_expires, .. }) => time_expires.unwrap(),
            _ => panic!("expected tombstone row after bump"),
        };

        assert!(
            new_deadline > old_deadline,
            "TTI bump should extend tombstone expiry: {old_deadline:?} -> {new_deadline:?}"
        );

        Ok(())
    }

    /// Legacy tombstones are handled correctly by all conditional write operations.
    ///
    /// Covers: `put_non_tombstone`, `delete_non_tombstone`, CAS-delete for both the
    /// legacy-metadata format and the empty-redirect format.
    #[tokio::test]
    async fn test_legacy_tombstone_conditional_ops() -> Result<()> {
        let backend = create_test_backend().await?;

        // put_non_tombstone returns Some(target == id) for a legacy tombstone.
        let id = make_id();
        write_legacy_tombstone(&backend, &id, ExpirationPolicy::Manual, None).await?;
        let t_opt = backend
            .put_non_tombstone(&id, &Metadata::default(), Bytes::new())
            .await?;
        assert_eq!(t_opt.map(|t| t.target).as_ref(), Some(&id));

        // delete_non_tombstone returns Some(target == id) for a legacy tombstone.
        let id = make_id();
        write_legacy_tombstone(&backend, &id, ExpirationPolicy::Manual, None).await?;
        let t_opt = backend.delete_non_tombstone(&id).await?;
        assert_eq!(t_opt.map(|t| t.target).as_ref(), Some(&id));

        // CAS-delete succeeds on a legacy-metadata tombstone (target resolves to hv_id).
        let id = make_id();
        write_legacy_tombstone(&backend, &id, ExpirationPolicy::Manual, None).await?;
        let deleted = backend
            .compare_and_write(&id, Some(&id), TieredWrite::Delete)
            .await?;
        assert!(
            deleted,
            "CAS-delete must succeed on legacy-metadata tombstone"
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::NotFound
        ));

        // CAS-delete succeeds on an empty-redirect tombstone (target resolves to hv_id).
        let id = make_id();
        write_empty_redirect_tombstone(&backend, &id).await?;
        let deleted = backend
            .compare_and_write(&id, Some(&id), TieredWrite::Delete)
            .await?;
        assert!(
            deleted,
            "CAS-delete must succeed on empty-redirect tombstone"
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id).await?,
            TieredMetadata::NotFound
        ));

        Ok(())
    }

    /// An empty `r` value falls back to the HV id when resolving the tombstone target.
    #[tokio::test]
    async fn test_empty_redirect_falls_back_to_hv_id() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = make_id();

        write_empty_redirect_tombstone(&backend, &id).await?;
        match backend.get_tiered_metadata(&id).await? {
            TieredMetadata::Tombstone(t) => assert_eq!(t.target, id, "must fall back to hv_id"),
            other => panic!("expected tombstone, got {other:?}"),
        }

        Ok(())
    }

    // --- Section 6: Expired Tombstone Handling ---

    /// CAS with `current=None` must succeed when the row holds an expired
    /// tombstone. The physical row still exists but is logically gone.
    #[tokio::test]
    async fn test_cas_create_tombstone_over_expired() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let old_lt_id = ObjectId::random(id.context().clone());
        let old_tombstone = Tombstone {
            target: old_lt_id,
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
        };
        create_tombstone(&backend, &id, &old_tombstone, SystemTime::now()).await?;

        let new_lt_id = ObjectId::random(id.context().clone());
        let new_tombstone = Tombstone {
            target: new_lt_id.clone(),
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
        };
        let committed = backend
            .compare_and_write(&id, None, TieredWrite::Tombstone(new_tombstone))
            .await?;
        assert!(
            committed,
            "CAS with current=None must succeed over an expired tombstone"
        );

        let TieredMetadata::Tombstone(t) = backend.get_tiered_metadata(&id).await? else {
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
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
        };
        create_tombstone(&backend, &id, &tombstone, SystemTime::now()).await?;

        let result = backend
            .put_non_tombstone(&id, &Metadata::default(), Bytes::from_static(b"data"))
            .await?;
        assert_eq!(
            result, None,
            "put_non_tombstone must succeed (return None) over an expired tombstone"
        );

        let (_, _, stream) = backend.get_object(&id, None).await?.unwrap();
        assert_eq!(&stream::read_to_vec(stream).await?, b"data");

        Ok(())
    }

    // --- Range Request Tests ---

    async fn put_range_test_object(backend: &BigTableBackend) -> Result<ObjectId> {
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            ..Default::default()
        };
        let payload = b"Hello, range requests!";
        backend
            .put_object(&id, &metadata, stream::single(payload.as_slice()))
            .await?;
        Ok(id)
    }

    #[tokio::test]
    async fn get_object_range_bounded() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Some(ByteRange::Bounded(7, 11)))
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
            .get_object(&id, Some(ByteRange::From(7)))
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
            .get_object(&id, Some(ByteRange::Last(9)))
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

        match backend.get_object(&id, Some(ByteRange::From(100))).await {
            Err(Error::RangeNotSatisfiable { total }) => assert_eq!(total, 22),
            Ok(_) => panic!("expected RangeNotSatisfiable, got Ok"),
            Err(e) => panic!("expected RangeNotSatisfiable, got {e:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn get_object_no_range_returns_full_payload() -> Result<()> {
        let backend = create_test_backend().await?;
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend.get_object(&id, None).await?.unwrap();
        let data = stream::read_to_vec(stream).await?;
        assert_eq!(&data, b"Hello, range requests!");
        assert!(content_range.is_none());

        Ok(())
    }
}
