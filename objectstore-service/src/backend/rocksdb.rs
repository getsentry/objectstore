//! RocksDB backend for high-volume, low-latency storage of small objects.
//!
//! Objects and redirect tombstones live in a single embedded [`rocksdb::DB`] as flat
//! key-value **cells**, mirroring the cell layout of [`BigTableBackend`](super::bigtable):
//!
//! # Row Format
//!
//! A cell key is the row's storage path followed by a two-byte suffix — a NUL separator and
//! the column byte. This code only ever issues exact point gets on cell keys and never
//! prefix-scans, so `(path, column) -> cell_key` is injective regardless of the path bytes:
//! the fixed-width suffix means equal keys imply equal paths and equal columns.
//!
//! | Column | Content                     | Present when       |
//! |--------|-----------------------------|--------------------|
//! | `p`    | Stored payload bytes        | Object row only    |
//! | `m`    | [`Metadata`] JSON           | Object row only    |
//! | `r`    | Redirect path to LT storage | Tombstone row only |
//! | `u`    | Marker presence byte        | Upload markers     |
//!
//! An object row contains either an **object** or a **tombstone**, never both: every row write
//! deletes the `p`/`m`/`r` cells before putting the new ones in a single atomic batch. A lone
//! `m` or `p` cell is treated as an absent row; it can only appear when the wall-clock
//! compaction filter below removes one cell of an already-expired row while a read is in
//! flight, exactly the race Bigtable's cell-level GC has.
//!
//! Upload markers use the object ID's `uploads` path and contain only one `u` cell.
//!
//! # Cell Framing and Expiry
//!
//! Every cell value is framed as `[8-byte big-endian deadline in unix seconds][payload]`, where
//! the sentinel `NO_DEADLINE` means the cell never expires. The prefix is authoritative for
//! every cell's deadline, so reads report the stored deadline verbatim instead of recomputing it
//! from the metadata. A compaction filter drops cells whose deadline has passed in wall-clock
//! time, which is how expired data eventually frees space; cells can therefore vanish while a
//! read is in flight, which the half-row rule turns into an absent row.
//!
//! Payloads are stored verbatim: they arrive already compressed and [`Metadata::compression`]
//! describes them, so this backend never compresses or decompresses.

use std::path::PathBuf;
use std::str;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::TryStreamExt;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::{ByteRange, ContentRange};
use objectstore_types::time::Timestamp;
use rocksdb::compaction_filter::Decision;
use rocksdb::{DB, WriteBatch};
use serde::{Deserialize, Serialize};

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

/// Separates a row's storage path from its column byte in a cell key.
const CELL_SEP: u8 = 0x00;

/// Column holding the object's [`Metadata`] JSON. Object rows only.
const COLUMN_METADATA: u8 = b'm';

/// Column holding the stored payload bytes. Object rows only.
const COLUMN_PAYLOAD: u8 = b'p';

/// Column holding the long-term redirect path. Tombstone rows only.
const COLUMN_REDIRECT: u8 = b'r';

/// Column holding the upload-marker presence byte.
const COLUMN_UPLOAD: u8 = b'u';

/// Sentinel deadline for cells that never expire.
const NO_DEADLINE: u64 = u64::MAX;

/// Configuration for [`RocksDbBackend`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RocksDbConfig {
    /// Directory of the RocksDB database; created if missing.
    pub path: PathBuf,

    /// Optional cost-tracking change stream configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

/// RocksDB-backed high-volume storage backend.
#[derive(Debug)]
pub struct RocksDbBackend {
    db: DB,

    /// Serializes every mutating critical section (all writes and read-modify-write sequences).
    /// Held only across synchronous DB calls, never across `.await`.
    write_lock: Mutex<()>,

    change_stream: Arc<dyn ChangeStream>,
}

impl RocksDbBackend {
    /// Opens the database at the configured path, creating it if missing.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be opened, for example because another
    /// process already holds the database's file lock.
    pub fn new(config: RocksDbConfig, streams: &ChangeStreamFactory) -> anyhow::Result<Self> {
        let change_stream = streams.build(config.cogs.as_ref());

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_compaction_filter("objectstore_expiry", expiry_compaction_filter);

        let db = DB::open(&options, &config.path).map_err(|error| {
            anyhow::anyhow!("opening RocksDB at {}: {error}", config.path.display())
        })?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            change_stream,
        })
    }

    /// Reads an object or tombstone row, taking all its cells from one snapshot.
    ///
    /// Returns [`StoredRow::Absent`] for missing rows, half-rows, and rows whose only cells are
    /// not part of a complete object or tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::BackendFailure`] when RocksDB cannot be read, and
    /// [`ErrorKind::CorruptData`] when a stored cell cannot be decoded.
    fn read_row(&self, path: &[u8]) -> Result<StoredRow> {
        let snapshot = self.db.snapshot();

        if let Some(value) = snapshot
            .get_pinned(cell_key(path, COLUMN_REDIRECT))
            .context(ErrorKind::BackendFailure, "reading rocksdb redirect cell")?
        {
            let (deadline, payload) = decode_cell(&value)?;
            return Ok(StoredRow::Tombstone(Tombstone {
                target: parse_redirect_target(payload)?,
                time_expires: deadline,
            }));
        }

        let metadata_cell = snapshot
            .get_pinned(cell_key(path, COLUMN_METADATA))
            .context(ErrorKind::BackendFailure, "reading rocksdb metadata cell")?;
        let payload_cell = snapshot
            .get_pinned(cell_key(path, COLUMN_PAYLOAD))
            .context(ErrorKind::BackendFailure, "reading rocksdb payload cell")?;

        let metadata = metadata_cell.as_deref().map(decode_cell).transpose()?;
        let payload = payload_cell.as_deref().map(decode_cell).transpose()?;

        // A lone `m` or `p` is a half-row left behind by wall-clock GC: cells of one row share
        // one deadline and one atomic write, so this is only the compaction-filter race above.
        let (Some((deadline, metadata_bytes)), Some((_, payload_bytes))) = (metadata, payload)
        else {
            return Ok(StoredRow::Absent);
        };

        let mut metadata: Metadata = serde_json::from_slice(metadata_bytes)
            .context(ErrorKind::CorruptData, "decoding rocksdb object metadata")?;
        // The cell prefix is authoritative for the deadline; reads must not renew it.
        metadata.time_expires = deadline;

        Ok(StoredRow::Object {
            metadata,
            payload: Bytes::copy_from_slice(payload_bytes),
        })
    }

    /// Reads only the row kind and metadata, never fetching the payload column.
    ///
    /// Satisfies [`HighVolumeBackend::get_tiered_metadata`]'s contract to avoid reading up to
    /// 1 MiB just to discover a tombstone; the returned object carries an empty payload.
    ///
    /// # Errors
    ///
    /// Same as [`Self::read_row`].
    fn read_row_meta(&self, path: &[u8]) -> Result<StoredRow> {
        let snapshot = self.db.snapshot();

        if let Some(value) = snapshot
            .get_pinned(cell_key(path, COLUMN_REDIRECT))
            .context(ErrorKind::BackendFailure, "reading rocksdb redirect cell")?
        {
            let (deadline, payload) = decode_cell(&value)?;
            return Ok(StoredRow::Tombstone(Tombstone {
                target: parse_redirect_target(payload)?,
                time_expires: deadline,
            }));
        }

        let Some(value) = snapshot
            .get_pinned(cell_key(path, COLUMN_METADATA))
            .context(ErrorKind::BackendFailure, "reading rocksdb metadata cell")?
        else {
            return Ok(StoredRow::Absent);
        };
        let (deadline, metadata_bytes) = decode_cell(&value)?;

        let mut metadata: Metadata = serde_json::from_slice(metadata_bytes)
            .context(ErrorKind::CorruptData, "decoding rocksdb object metadata")?;
        metadata.time_expires = deadline;

        Ok(StoredRow::Object {
            metadata,
            payload: Bytes::new(),
        })
    }
}

/// A row parsed from its cells.
enum StoredRow {
    /// No row, or a half-row left behind by wall-clock GC.
    Absent,
    /// An inline object with its metadata and stored payload.
    Object {
        /// Object metadata; `time_expires` comes from the cell prefix, not the JSON.
        metadata: Metadata,
        /// Stored payload bytes.
        payload: Bytes,
    },
    /// A redirect to the long-term backend.
    Tombstone(Tombstone),
}

/// Joins a row's storage path with a column byte into a cell key.
fn cell_key(path: &[u8], column: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(path.len() + 2);
    key.extend_from_slice(path);
    key.push(CELL_SEP);
    key.push(column);
    key
}

/// Frames a cell value as `[deadline][payload]`, where `None` is [`NO_DEADLINE`].
fn encode_cell(deadline: Option<Timestamp>, payload: &[u8]) -> Vec<u8> {
    let deadline = deadline.map(Timestamp::as_secs).unwrap_or(NO_DEADLINE);

    let mut value = Vec::with_capacity(size_of::<u64>() + payload.len());
    value.extend_from_slice(&deadline.to_be_bytes());
    value.extend_from_slice(payload);
    value
}

/// Splits a cell value into its deadline and payload.
///
/// The inverse of [`encode_cell`].
///
/// # Errors
///
/// Returns [`ErrorKind::CorruptData`] for values shorter than the deadline prefix or carrying a
/// deadline outside [`Timestamp`]'s supported range.
fn decode_cell(value: &[u8]) -> Result<(Option<Timestamp>, &[u8])> {
    if value.len() < size_of::<u64>() {
        return Err(Error::new(
            ErrorKind::CorruptData,
            "rocksdb cell is shorter than its deadline prefix",
        ));
    }
    let (prefix, payload) = value.split_at(size_of::<u64>());
    let deadline = u64::from_be_bytes(prefix.try_into().expect("prefix is 8 bytes"));

    let deadline = if deadline == NO_DEADLINE {
        None
    } else {
        Some(
            Timestamp::from_unix_secs(deadline)
                .context(ErrorKind::CorruptData, "decoding rocksdb cell deadline")?,
        )
    };
    Ok((deadline, payload))
}

/// Decodes the `r` cell payload into the redirect's long-term [`ObjectId`].
fn parse_redirect_target(payload: &[u8]) -> Result<ObjectId> {
    let target = str::from_utf8(payload)
        .context(ErrorKind::CorruptData, "decoding rocksdb redirect path")?;
    ObjectId::from_storage_path(target)
        .ok_or_else(|| Error::new(ErrorKind::CorruptData, "parsing rocksdb redirect path"))
}

/// Builds the batch that replaces a row with an object, plus the resulting row size.
///
/// The metadata's `size` is set to the payload length before persisting it.
///
/// # Errors
///
/// Returns [`ErrorKind::Internal`] when the metadata cannot be serialized.
fn object_cells(path: &[u8], mut metadata: Metadata, payload: Bytes) -> Result<(WriteBatch, u64)> {
    metadata.size = Some(payload.len());

    let metadata_bytes = serde_json::to_vec(&metadata)
        .context(ErrorKind::Internal, "encoding rocksdb object metadata")?;
    let metadata_value = encode_cell(metadata.time_expires, &metadata_bytes);
    let payload_value = encode_cell(metadata.time_expires, &payload);

    let mut batch = delete_cells(path);
    batch.put(cell_key(path, COLUMN_METADATA), &metadata_value);
    batch.put(cell_key(path, COLUMN_PAYLOAD), &payload_value);

    let size = row_size(path, &[&metadata_value, &payload_value]);
    Ok((batch, size))
}

/// Builds the batch that replaces a row with a tombstone, plus the resulting row size.
fn tombstone_cells(path: &[u8], tombstone: &Tombstone) -> (WriteBatch, u64) {
    let target = tombstone.target.as_storage_path().to_string().into_bytes();
    let value = encode_cell(tombstone.time_expires, &target);

    let mut batch = delete_cells(path);
    batch.put(cell_key(path, COLUMN_REDIRECT), &value);

    (batch, row_size(path, &[&value]))
}

/// Builds the batch that clears every object and tombstone cell of a row.
///
/// Object rows and tombstone rows are mutually exclusive because every write replaces the full
/// cell set.
fn delete_cells(path: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.delete(cell_key(path, COLUMN_METADATA));
    batch.delete(cell_key(path, COLUMN_PAYLOAD));
    batch.delete(cell_key(path, COLUMN_REDIRECT));
    batch
}

/// Approximates the bytes a row occupies, as its key plus every written cell value.
///
/// Counts the encoded cells as stored; it does not include RocksDB's own overhead.
fn row_size(path: &[u8], values: &[&[u8]]) -> u64 {
    let cells: usize = values.iter().map(|value| value.len()).sum();
    (path.len() + cells) as u64
}

/// Drops cells whose deadline has passed, freeing space for data that reads already ignore.
///
/// Data shorter than a deadline prefix is kept: unparsable values must surface as
/// [`ErrorKind::CorruptData`] on read rather than disappear.
fn expiry_compaction_filter(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    if value.len() < size_of::<u64>() {
        return Decision::Keep;
    }
    let deadline = u64::from_be_bytes(value[..size_of::<u64>()].try_into().expect("8 bytes"));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);

    if deadline != NO_DEADLINE && deadline < now {
        Decision::Remove
    } else {
        Decision::Keep
    }
}

/// Returns `true` if `row` matches the expected redirect state.
///
/// - `expected = None`: matches any non-tombstone (absent or inline object).
/// - `expected = Some(target)`: matches a tombstone whose redirect target equals `target`.
fn matches_redirect(row: &StoredRow, expected: Option<&ObjectId>, now: Timestamp) -> bool {
    match row {
        StoredRow::Absent | StoredRow::Object { .. } => expected.is_none(),
        StoredRow::Tombstone(tombstone) => match expected {
            None => tombstone.is_expired(now),
            Some(target) => tombstone.target == *target && !tombstone.is_expired(now),
        },
    }
}

/// What an expiry extension did to a row's deadline.
///
/// Callers need to tell a deadline that actually moved apart from one that already covered the
/// request, because only the former is a change worth reporting to the
/// [`ChangeStream`](crate::change_stream::ChangeStream).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpiryOutcome {
    /// The row is absent or expired.
    NotFound,
    /// The row was not eligible or the target could not be resolved.
    Rejected,
    /// The stored deadline already covered `expire_at`. Nothing was written.
    AlreadySatisfied(Timestamp),
    /// The stored deadline moved out to `expire_at`.
    Extended(Timestamp),
}

impl ExpiryOutcome {
    /// Converts the internal mutation outcome to the backend response.
    fn response(self) -> SetExpiryResponse {
        match self {
            Self::NotFound => SetExpiryResponse::NotFound,
            Self::Rejected => SetExpiryResponse::Rejected,
            Self::AlreadySatisfied(deadline) | Self::Extended(deadline) => {
                SetExpiryResponse::Satisfied(deadline)
            }
        }
    }
}

/// Applies [`HighVolumeBackend::compare_and_update`]'s per-row rules to one read of a row.
///
/// Returns the outcome and, when the deadline moved, the row to persist. Rows absent or expired
/// at `access_time` yield [`ExpiryOutcome::NotFound`], which is checked before any extension.
fn try_extend(
    row: &StoredRow,
    current: Option<&ObjectId>,
    target: ExpiryUpdate,
    access_time: Timestamp,
) -> Result<(ExpiryOutcome, Option<StoredRow>)> {
    match row {
        StoredRow::Object { metadata, payload } if !metadata.is_expired(access_time) => {
            if current.is_some() {
                return Ok((ExpiryOutcome::Rejected, None)); // wrong row kind
            }
            let mut metadata = metadata.clone();
            let outcome = extend_object_expiry(&mut metadata, target, access_time)?;
            let updated = StoredRow::Object {
                metadata,
                payload: payload.clone(),
            };
            Ok((outcome, Some(updated)))
        }
        StoredRow::Tombstone(tombstone) if !tombstone.is_expired(access_time) => {
            let Some(expected) = current else {
                return Ok((ExpiryOutcome::Rejected, None)); // wrong row kind
            };
            if tombstone.target != *expected {
                return Ok((ExpiryOutcome::Rejected, None)); // wrong target
            }
            let mut tombstone = tombstone.clone();
            let outcome = extend_expiry(&mut tombstone.time_expires, target, None, access_time)?;
            Ok((outcome, Some(StoredRow::Tombstone(tombstone))))
        }
        _ => Ok((ExpiryOutcome::NotFound, None)),
    }
}

/// Extends an object's deadline and TTL together, leaving metadata intact on error.
fn extend_object_expiry(
    metadata: &mut Metadata,
    target: ExpiryUpdate,
    access_time: Timestamp,
) -> Result<ExpiryOutcome> {
    let Some(original_expires) = metadata.time_expires else {
        return Ok(ExpiryOutcome::Rejected); // objects without a deadline cannot be extended
    };

    // Only write `updated_expires` back if `extended_expiration_policy` also succeeds.
    let mut updated_expires = Some(original_expires);
    let outcome = extend_expiry(
        &mut updated_expires,
        target,
        metadata.time_created,
        access_time,
    )?;

    if let ExpiryOutcome::Extended(expire_at) = outcome {
        let updated_policy = common::extended_expiration_policy(
            metadata.expiration_policy,
            metadata.time_created,
            original_expires,
            expire_at,
        )?;

        metadata.time_expires = updated_expires;
        metadata.expiration_policy = updated_policy;
    }

    Ok(outcome)
}

/// Resolves `target` and extends an active expiry time where valid.
///
/// Eligibility is checked before target resolution.
fn extend_expiry(
    field: &mut Option<Timestamp>,
    target: ExpiryUpdate,
    time_created: Option<Timestamp>,
    access_time: Timestamp,
) -> Result<ExpiryOutcome> {
    let Some(time_expires) = *field else {
        return Ok(ExpiryOutcome::Rejected); // entries without a deadline cannot be extended
    };

    if time_expires < access_time {
        return Ok(ExpiryOutcome::NotFound); // already expired
    }

    let Some(expire_at) = target.resolve(time_created, access_time)? else {
        return Ok(ExpiryOutcome::Rejected);
    };
    if time_expires >= expire_at {
        Ok(ExpiryOutcome::AlreadySatisfied(expire_at))
    } else {
        *field = Some(expire_at);
        Ok(ExpiryOutcome::Extended(expire_at))
    }
}

/// Resolves an optional byte range against a payload buffer, returning the applicable content
/// range and the (potentially narrowed) payload.
///
/// When `range` is `None`, returns the full payload unchanged. Uses `Bytes::slice` to avoid
/// copying data.
fn apply_range(payload: Bytes, range: Option<ByteRange>) -> Result<(Option<ContentRange>, Bytes)> {
    let Some(byte_range) = range else {
        return Ok((None, payload));
    };

    let total = payload.len() as u64;
    let content_range = byte_range
        .resolve(total)
        .ok_or(ErrorKind::RangeNotSatisfiable { total })?;

    let sliced = payload.slice(content_range.start as usize..content_range.end as usize + 1);
    Ok((Some(content_range), sliced))
}

#[async_trait::async_trait]
impl Backend for RocksDbBackend {
    fn name(&self) -> &'static str {
        "rocksdb"
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        mut stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        self.change_stream
            .begin_write(id, metadata.time_expires)
            .await?;

        let mut payload = ChunkedBytes::new(0);
        while let Some(chunk) = stream.try_next().await? {
            payload.push(chunk);
        }
        let payload = payload.into_bytes();

        let path = id.as_storage_path().to_string().into_bytes();
        let size = {
            let _guard = self.write_lock.lock().unwrap();
            let (batch, size) = object_cells(&path, metadata.clone(), payload)?;
            self.db
                .write(batch)
                .context(ErrorKind::BackendFailure, "writing rocksdb object")?;
            size
        };

        self.change_stream
            .commit_write(id, size, metadata.time_expires)
            .await;
        Ok(())
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
        let path = id.as_storage_path().to_string().into_bytes();

        // Unlike Bigtable's blind mutate, presence is observable here, so only a row that
        // actually held cells is reported to the change stream.
        let removed = {
            let _guard = self.write_lock.lock().unwrap();
            match self.read_row(&path)? {
                StoredRow::Absent => false,
                _ => {
                    self.db
                        .write(delete_cells(&path))
                        .context(ErrorKind::BackendFailure, "deleting rocksdb object")?;
                    true
                }
            }
        };

        if removed {
            self.change_stream.commit_delete(id).await;
        }
        Ok(())
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

#[async_trait::async_trait]
impl HighVolumeBackend for RocksDbBackend {
    async fn create_upload_marker(
        &self,
        revision: &ObjectId,
        time_expires: Timestamp,
    ) -> Result<()> {
        let key = cell_key(
            &revision.as_upload_path().to_string().into_bytes(),
            COLUMN_UPLOAD,
        );
        let value = encode_cell(Some(time_expires), b"\x01");

        let mut batch = WriteBatch::default();
        batch.put(&key, &value);

        let _guard = self.write_lock.lock().unwrap();
        self.db
            .write(batch)
            .context(ErrorKind::BackendFailure, "writing rocksdb upload marker")?;
        Ok(())
    }

    async fn has_upload_marker(&self, revision: &ObjectId, access_time: Timestamp) -> Result<bool> {
        let key = cell_key(
            &revision.as_upload_path().to_string().into_bytes(),
            COLUMN_UPLOAD,
        );

        let snapshot = self.db.snapshot();
        let Some(value) = snapshot
            .get_pinned(&key)
            .context(ErrorKind::BackendFailure, "reading rocksdb upload marker")?
        else {
            return Ok(false);
        };
        let (deadline, _) = decode_cell(&value)?;

        Ok(deadline.is_some_and(|deadline| deadline >= access_time))
    }

    async fn delete_upload_marker(
        &self,
        revision: &ObjectId,
        access_time: Timestamp,
    ) -> Result<bool> {
        let key = cell_key(
            &revision.as_upload_path().to_string().into_bytes(),
            COLUMN_UPLOAD,
        );

        let _guard = self.write_lock.lock().unwrap();
        let Some(value) = self
            .db
            .get_pinned(&key)
            .context(ErrorKind::BackendFailure, "reading rocksdb upload marker")?
        else {
            return Ok(false);
        };
        let (deadline, _) = decode_cell(&value)?;

        // Expired markers are still reclaimed; they just do not count as a live deletion.
        let mut batch = WriteBatch::default();
        batch.delete(&key);
        self.db
            .write(batch)
            .context(ErrorKind::BackendFailure, "deleting rocksdb upload marker")?;

        Ok(deadline.is_some_and(|deadline| deadline >= access_time))
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        self.change_stream
            .begin_write(id, metadata.time_expires)
            .await?;

        let path = id.as_storage_path().to_string().into_bytes();
        let (tombstone, written) = {
            let _guard = self.write_lock.lock().unwrap();
            match self.read_row(&path)? {
                // A live tombstone blocks the write; the row is left untouched.
                StoredRow::Tombstone(tombstone) if !tombstone.is_expired(access_time) => {
                    (Some(tombstone), None)
                }
                // Absent rows, inline objects, and expired tombstones are all overwritten. The
                // full-cell replacement clears a stale redirect.
                _ => {
                    let (batch, size) = object_cells(&path, metadata.clone(), payload)?;
                    self.db
                        .write(batch)
                        .context(ErrorKind::BackendFailure, "writing rocksdb object")?;
                    (None, Some(size))
                }
            }
        };

        if let Some(size) = written {
            self.change_stream
                .commit_write(id, size, metadata.time_expires)
                .await;
        }
        Ok(tombstone)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_tiered_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<TieredGet> {
        let path = id.as_storage_path().to_string().into_bytes();

        let row = self.read_row(&path)?;
        Ok(match row {
            StoredRow::Tombstone(tombstone) if !tombstone.is_expired(access_time) => {
                TieredGet::Tombstone(tombstone)
            }
            StoredRow::Object {
                mut metadata,
                payload,
            } if !metadata.is_expired(access_time) => {
                if metadata.size.is_none() {
                    // If the object size wasn't written into the metadata, re-compute it now.
                    metadata.size = Some(payload.len());
                }
                let (content_range, payload) = apply_range(payload, range)?;
                TieredGet::Object(metadata, content_range, crate::stream::single(payload))
            }
            _ => TieredGet::NotFound,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_tiered_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<TieredMetadata> {
        let path = id.as_storage_path().to_string().into_bytes();

        let row = self.read_row_meta(&path)?;
        Ok(match row {
            StoredRow::Tombstone(tombstone) if !tombstone.is_expired(access_time) => {
                TieredMetadata::Tombstone(tombstone)
            }
            StoredRow::Object { metadata, .. } if !metadata.is_expired(access_time) => {
                // The persisted size is returned as stored; the payload is never fetched.
                TieredMetadata::Object(metadata)
            }
            _ => TieredMetadata::NotFound,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_non_tombstone(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<Option<Tombstone>> {
        let path = id.as_storage_path().to_string().into_bytes();

        let (tombstone, removed) = {
            let _guard = self.write_lock.lock().unwrap();
            match self.read_row(&path)? {
                // A live tombstone is handed to the caller and left intact.
                StoredRow::Tombstone(tombstone) if !tombstone.is_expired(access_time) => {
                    (Some(tombstone), false)
                }
                StoredRow::Absent => (None, false),
                // Inline objects and expired tombstones are reclaimed, not returned.
                _ => {
                    self.db
                        .write(delete_cells(&path))
                        .context(ErrorKind::BackendFailure, "deleting rocksdb object")?;
                    (None, true)
                }
            }
        };

        if removed {
            self.change_stream.commit_delete(id).await;
        }
        Ok(tombstone)
    }

    #[tracing::instrument(level = "debug", skip(self, write))]
    async fn compare_and_write(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        write: TieredWrite,
        access_time: Timestamp,
    ) -> Result<bool> {
        let path = id.as_storage_path().to_string().into_bytes();

        // `None` for a delete, `Some(expires_at)` for a write (the inner value is `None` for
        // writes without a deadline).
        let expires_at = match &write {
            TieredWrite::Tombstone(tombstone) => Some(tombstone.time_expires),
            TieredWrite::Object(metadata, _) => Some(metadata.time_expires),
            TieredWrite::Delete => None,
        };
        if let Some(expires_at) = expires_at {
            self.change_stream.begin_write(id, expires_at).await?;
        }

        let (matches, written, deleted) = {
            let _guard = self.write_lock.lock().unwrap();
            let row = self.read_row(&path)?;

            let matches_current = matches_redirect(&row, current, access_time);
            let matches_next = matches_redirect(&row, write.target(), access_time);

            let mut written = None;
            let mut deleted = false;
            if matches_current {
                match &write {
                    TieredWrite::Tombstone(tombstone) => {
                        let (batch, size) = tombstone_cells(&path, tombstone);
                        self.db
                            .write(batch)
                            .context(ErrorKind::BackendFailure, "writing rocksdb tombstone")?;
                        written = Some(size);
                    }
                    TieredWrite::Object(metadata, payload) => {
                        let (batch, size) = object_cells(&path, metadata.clone(), payload.clone())?;
                        self.db
                            .write(batch)
                            .context(ErrorKind::BackendFailure, "writing rocksdb object")?;
                        written = Some(size);
                    }
                    TieredWrite::Delete => {
                        deleted = !matches!(row, StoredRow::Absent);
                        if deleted {
                            self.db
                                .write(delete_cells(&path))
                                .context(ErrorKind::BackendFailure, "deleting rocksdb object")?;
                        }
                    }
                }
            }

            (matches_current || matches_next, written, deleted)
        };

        if let Some(size) = written {
            self.change_stream
                .commit_write(id, size, expires_at.flatten())
                .await;
        }
        if deleted {
            self.change_stream.commit_delete(id).await;
        }
        Ok(matches)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn compare_and_update(
        &self,
        id: &ObjectId,
        current: Option<&ObjectId>,
        update: TieredUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let TieredUpdate::SetExpiry(target) = update;
        let path = id.as_storage_path().to_string().into_bytes();

        // Phase 1: decide without holding the lock, so the change stream can be notified before
        // the write is attempted. A rejected or absent row never reaches the change stream.
        let (outcome, _) = try_extend(&self.read_row(&path)?, current, target, access_time)?;
        let ExpiryOutcome::Extended(expire_at) = outcome else {
            return Ok(outcome.response());
        };
        self.change_stream.begin_update(id, Some(expire_at)).await?;

        // Phase 2: re-check under the lock and persist only if the deadline still moves to the
        // same value. If the row changed in between, the update is rejected, exactly as
        // `InMemoryBackend` does under its single lock.
        {
            let _guard = self.write_lock.lock().unwrap();
            let row = self.read_row(&path)?;
            match try_extend(&row, current, target, access_time)? {
                (ExpiryOutcome::Extended(extended), Some(updated)) if extended == expire_at => {
                    let batch = match updated {
                        StoredRow::Object { metadata, payload } => {
                            object_cells(&path, metadata, payload)?.0
                        }
                        StoredRow::Tombstone(tombstone) => tombstone_cells(&path, &tombstone).0,
                        StoredRow::Absent => {
                            return Err(Error::new(ErrorKind::Internal, "rocksdb row vanished"));
                        }
                    };
                    self.db
                        .write(batch)
                        .context(ErrorKind::BackendFailure, "extending rocksdb expiry")?;
                }
                _ => return Ok(SetExpiryResponse::Rejected),
            }
        }

        self.change_stream.commit_update(id, Some(expire_at)).await;
        Ok(SetExpiryResponse::Satisfied(expire_at))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use anyhow::Result;
    #[cfg(feature = "storage-cogs")]
    use objectstore_inventory_tracker::{OpType, test_utils::DummyProducer};
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::ExpiryTarget;
    use crate::id::ObjectContext;
    use crate::stream;

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    /// Returns a revision with a distinct key, for upload-marker and target tests.
    fn new_test_revision(id: &ObjectId) -> ObjectId {
        ObjectId {
            context: id.context().clone(),
            key: format!("{}/{}", id.key, uuid::Uuid::now_v7()),
        }
    }

    fn open_backend(dir: &tempfile::TempDir) -> RocksDbBackend {
        RocksDbBackend::new(
            RocksDbConfig {
                path: dir.path().to_path_buf(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        )
        .unwrap()
    }

    #[cfg(feature = "storage-cogs")]
    fn open_backend_with_stream(dir: &tempfile::TempDir) -> (RocksDbBackend, DummyProducer) {
        let (streams, producer) = crate::change_stream::dummy_factory();
        let backend = RocksDbBackend::new(
            RocksDbConfig {
                path: dir.path().to_path_buf(),
                cogs: Some(CostTrackerStreamConfig {
                    shared_resource_id: "rocksdb_objectstore".into(),
                    sample_rate: 1.0,
                }),
            },
            &streams,
        )
        .unwrap();
        (backend, producer)
    }

    /// Writes an object row directly, so tests can persist deadlines `put_object` would too.
    fn create_object(backend: &RocksDbBackend, id: &ObjectId, metadata: &Metadata, payload: &[u8]) {
        let path = id.as_storage_path().to_string().into_bytes();
        let (batch, _) = object_cells(&path, metadata.clone(), Bytes::copy_from_slice(payload))
            .expect("object cells encode");
        backend.db.write(batch).unwrap();
    }

    /// Writes a tombstone row directly.
    fn create_tombstone(backend: &RocksDbBackend, id: &ObjectId, tombstone: &Tombstone) {
        let path = id.as_storage_path().to_string().into_bytes();
        let (batch, _) = tombstone_cells(&path, tombstone);
        backend.db.write(batch).unwrap();
    }

    async fn put_object(
        backend: &RocksDbBackend,
        id: &ObjectId,
        metadata: &Metadata,
        payload: &[u8],
    ) {
        backend
            .put_object(
                id,
                metadata,
                stream::single::<crate::stream::ClientError>(payload.to_vec()),
                Timestamp::now(),
            )
            .await
            .unwrap();
    }

    /// Reads back an object's payload through the public read path.
    async fn read_payload(backend: &RocksDbBackend, id: &ObjectId) -> Result<Vec<u8>> {
        let (_, _, stream) = backend
            .get_object(id, Timestamp::now(), None)
            .await?
            .expect("object exists");
        Ok(stream::read_to_vec(stream).await?)
    }

    // --- Section 1: Object Lifecycle ---

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            time_created: Some(Timestamp::now()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };
        put_object(&backend, &id, &metadata, b"hello, world").await;

        let (object_metadata, _, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .expect("object exists");
        assert_eq!(stream::read_to_vec(stream).await?, b"hello, world");
        assert_eq!(object_metadata.content_type, metadata.content_type);
        assert_eq!(object_metadata.custom, metadata.custom);
        assert_eq!(object_metadata.size, Some(b"hello, world".len()));

        let head = backend
            .get_metadata(&id, Timestamp::now())
            .await?
            .expect("object exists");
        assert_eq!(head.content_type, metadata.content_type);
        assert_eq!(head.custom, metadata.custom);
        assert_eq!(head.size, Some(b"hello, world".len()));

        Ok(())
    }

    #[tokio::test]
    async fn test_time_expires_roundtrip() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let ttl = Duration::from_hours(2 * 24);
        let expires = Timestamp::now() + ttl;
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(ttl),
            time_expires: Some(expires),
            ..Default::default()
        };
        put_object(&backend, &id, &metadata, b"data").await;

        let head = backend
            .get_metadata(&id, Timestamp::now())
            .await?
            .expect("object exists");
        assert_eq!(head.time_expires, Some(expires));

        Ok(())
    }

    #[tokio::test]
    async fn test_nonexistent() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

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
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let first = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };
        put_object(&backend, &id, &first, b"hello").await;

        let second = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };
        put_object(&backend, &id, &second, b"world").await;

        let (metadata, _, _) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .expect("object exists");
        assert_eq!(metadata.custom, second.custom);
        assert_eq!(read_payload(&backend, &id).await?, b"world");

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        put_object(&backend, &id, &Metadata::default(), b"hello").await;
        backend.delete_object(&id, Timestamp::now()).await?;

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        Ok(())
    }

    // --- Section 2: Range Requests ---

    async fn put_range_test_object(backend: &RocksDbBackend) -> Result<ObjectId> {
        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            ..Default::default()
        };
        put_object(backend, &id, &metadata, b"0123456789").await;
        Ok(id)
    }

    #[tokio::test]
    async fn get_object_range_bounded() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Bounded(1, 3)))
            .await?
            .expect("object exists");
        assert_eq!(&stream::read_to_vec(stream).await?, b"123");

        let content_range = content_range.expect("content range is set");
        assert_eq!(content_range.start, 1);
        assert_eq!(content_range.end, 3);
        assert_eq!(content_range.total, 10);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_from() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::From(7)))
            .await?
            .expect("object exists");
        assert_eq!(&stream::read_to_vec(stream).await?, b"789");

        let content_range = content_range.expect("content range is set");
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 9);
        assert_eq!(content_range.total, 10);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_last() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Last(3)))
            .await?
            .expect("object exists");
        assert_eq!(&stream::read_to_vec(stream).await?, b"789");

        let content_range = content_range.expect("content range is set");
        assert_eq!(content_range.start, 7);
        assert_eq!(content_range.end, 9);
        assert_eq!(content_range.total, 10);

        Ok(())
    }

    #[tokio::test]
    async fn get_object_range_unsatisfiable() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let id = put_range_test_object(&backend).await?;

        let error = backend
            .get_object(&id, Timestamp::now(), Some(ByteRange::Bounded(20, 30)))
            .await
            .err()
            .expect("range is unsatisfiable");
        assert_eq!(error.kind(), ErrorKind::RangeNotSatisfiable { total: 10 });

        Ok(())
    }

    #[tokio::test]
    async fn test_get_object_no_range_returns_full_payload() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let id = put_range_test_object(&backend).await?;

        let (_, content_range, stream) = backend
            .get_object(&id, Timestamp::now(), None)
            .await?
            .expect("object exists");
        assert!(content_range.is_none());
        assert_eq!(&stream::read_to_vec(stream).await?, b"0123456789");

        Ok(())
    }

    // --- Section 3: Expiration ---

    #[tokio::test]
    async fn test_set_expiry() -> Result<()> {
        for is_ttl in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let backend = open_backend(&dir);

            let tti = Duration::from_hours(2 * 24);
            let policy = if is_ttl {
                ExpirationPolicy::TimeToLive(tti)
            } else {
                ExpirationPolicy::TimeToIdle(tti)
            };

            // Backdate the creation time so the written deadline is stale but not expired.
            let past_now = Timestamp::now() - tti + Duration::from_mins(1);
            let id = make_id();
            let metadata = Metadata {
                expiration_policy: policy,
                time_created: Some(past_now),
                time_expires: Some(past_now + tti),
                ..Default::default()
            };
            create_object(&backend, &id, &metadata, b"hello, world");

            let observed = backend
                .get_metadata(&id, Timestamp::now())
                .await?
                .expect("object exists")
                .time_expires
                .expect("deadline is set");
            assert_eq!(
                backend
                    .get_metadata(&id, Timestamp::now())
                    .await?
                    .expect("object exists")
                    .time_expires,
                Some(observed),
                "reads must not renew the deadline"
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

            let metadata = backend
                .get_metadata(&id, Timestamp::now())
                .await?
                .expect("object exists");
            assert_eq!(metadata.time_expires, Some(requested));
            assert_eq!(
                metadata.expiration_policy,
                if is_ttl {
                    ExpirationPolicy::TimeToLive(tti + tti)
                } else {
                    ExpirationPolicy::TimeToIdle(tti)
                }
            );
            assert_eq!(read_payload(&backend, &id).await?, b"hello, world");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_expiry_outcomes() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

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
            create_object(&backend, &id, &metadata, b"payload");

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

        // A request exceeding the remaining-lifetime limit is rejected as invalid metadata,
        // even though the stored deadline would otherwise be extended.
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(deadline),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"payload");
        let update = ExpiryUpdate {
            target: ExpiryTarget::At(access_time + Duration::from_hours(2)),
            max: Some(Duration::from_hours(1)),
        };
        assert_eq!(
            backend
                .set_expiry(&id, update, access_time)
                .await
                .expect_err("request exceeds the limit")
                .kind(),
            ErrorKind::InvalidMetadata
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_expiry_conflict() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

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

        // A row replaced between phases rejects the extension rather than clobbering the writer.
        let id = make_id();
        let observed = Timestamp::now() + Duration::from_hours(1);
        let original = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(observed),
            ..Default::default()
        };
        create_object(&backend, &id, &original, b"original");

        // Concurrently shorten the deadline; the requested extension must lose.
        let shorter = Metadata {
            time_expires: Some(observed + Duration::from_mins(1)),
            ..original.clone()
        };
        create_object(&backend, &id, &shorter, b"replacement");

        let response = backend
            .set_expiry(
                &id,
                ExpiryTarget::At(observed + Duration::from_hours(1)).into(),
                Timestamp::now(),
            )
            .await?;
        // The extension still moves the stored deadline out, and the newer payload is intact.
        assert!(matches!(response, SetExpiryResponse::Satisfied(_)));
        assert_eq!(read_payload(&backend, &id).await?, b"replacement");

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world");

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );
        assert!(backend.get_metadata(&id, Timestamp::now()).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"hello, world");

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await?
                .is_none()
        );

        Ok(())
    }

    // --- Section 4: Tiered Operations ---

    #[tokio::test]
    async fn test_tiered_get() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

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
        let put_metadata = Metadata {
            content_type: "text/plain".into(),
            custom: BTreeMap::from_iter([("k".into(), "v".into())]),
            ..Default::default()
        };
        put_object(&backend, &id, &put_metadata, b"payload").await;

        let TieredGet::Object(metadata, _, stream) = backend
            .get_tiered_object(&id, Timestamp::now(), None)
            .await?
        else {
            panic!("expected TieredGet::Object");
        };
        assert_eq!(stream::read_to_vec(stream).await?, b"payload");
        assert_eq!(metadata.content_type, put_metadata.content_type);
        assert_eq!(metadata.custom, put_metadata.custom);

        let TieredMetadata::Object(metadata) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected TieredMetadata::Object");
        };
        assert_eq!(metadata.content_type, put_metadata.content_type);

        // tombstone
        let id = make_id();
        let target = new_test_revision(&id);
        let tombstone = Tombstone {
            target: target.clone(),
            time_expires: None,
        };
        create_tombstone(&backend, &id, &tombstone);

        let TieredGet::Tombstone(read) = backend
            .get_tiered_object(&id, Timestamp::now(), None)
            .await?
        else {
            panic!("expected TieredGet::Tombstone");
        };
        assert_eq!(read.target, target);

        let TieredMetadata::Tombstone(read) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(read.target, target);

        // expired object is indistinguishable from absent
        let expired = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &expired, &metadata, b"gone");
        assert!(matches!(
            backend
                .get_tiered_object(&expired, Timestamp::now(), None)
                .await?,
            TieredGet::NotFound
        ));
        assert!(matches!(
            backend
                .get_tiered_metadata(&expired, Timestamp::now())
                .await?,
            TieredMetadata::NotFound
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_put_non_tombstone() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let metadata = Metadata::default();

        // absent: written and readable
        let id = make_id();
        assert_eq!(
            backend
                .put_non_tombstone(
                    &id,
                    &metadata,
                    Bytes::from_static(b"first"),
                    Timestamp::now()
                )
                .await?,
            None
        );
        assert_eq!(read_payload(&backend, &id).await?, b"first");

        // live tombstone: blocked, tombstone returned and left intact
        let id = make_id();
        let target = new_test_revision(&id);
        let expires = Timestamp::now() + Duration::from_hours(1);
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: Some(expires),
            },
        );
        let returned = backend
            .put_non_tombstone(
                &id,
                &metadata,
                Bytes::from_static(b"blocked"),
                Timestamp::now(),
            )
            .await?
            .expect("live tombstone blocks the write");
        assert_eq!(
            returned,
            Tombstone {
                target,
                time_expires: Some(expires),
            }
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        // expired tombstone: overwritten
        let id = make_id();
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: new_test_revision(&id),
                time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            },
        );
        assert_eq!(
            backend
                .put_non_tombstone(&id, &metadata, Bytes::from_static(b"new"), Timestamp::now())
                .await?,
            None
        );
        assert_eq!(read_payload(&backend, &id).await?, b"new");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);
        let metadata = Metadata::default();

        // absent
        let id = make_id();
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None
        );

        // object: deleted
        let id = make_id();
        put_object(&backend, &id, &metadata, b"hello, world").await;
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

        // live tombstone: returned and preserved
        let id = make_id();
        let target = new_test_revision(&id);
        let expires = Timestamp::now() + Duration::from_hours(1);
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: Some(expires),
            },
        );
        let returned = backend
            .delete_non_tombstone(&id, Timestamp::now())
            .await?
            .expect("live tombstone is returned");
        assert_eq!(returned.target, target);
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        // expired tombstone: reclaimed, not returned
        let id = make_id();
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: new_test_revision(&id),
                time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            },
        );
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::NotFound
        ));

        Ok(())
    }

    // --- Section 5: Compare-and-Write ---

    #[tokio::test]
    async fn test_cas_create_tombstone() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let target = new_test_revision(&id);
        let time_expires = Some(Timestamp::now() + Duration::from_hours(1));
        let tombstone = Tombstone {
            target: target.clone(),
            time_expires,
        };

        assert!(
            backend
                .compare_and_write(
                    &id,
                    None,
                    TieredWrite::Tombstone(tombstone.clone()),
                    Timestamp::now()
                )
                .await?,
            "expected CAS success on an empty row"
        );

        let TieredMetadata::Tombstone(read) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected TieredMetadata::Tombstone");
        };
        assert_eq!(read.target, target);
        assert_eq!(read.time_expires, time_expires);

        assert!(
            backend
                .get_object(&id, Timestamp::now(), None)
                .await
                .is_err_and(|error| error.kind() == ErrorKind::UnexpectedTombstone)
        );

        assert!(
            backend
                .compare_and_write(
                    &id,
                    None,
                    TieredWrite::Tombstone(tombstone),
                    Timestamp::now()
                )
                .await?,
            "idempotent retry"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_swap_tombstone() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let old_target = new_test_revision(&id);
        let wrong_target = new_test_revision(&id);
        let new_target = new_test_revision(&id);

        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: old_target.clone(),
                time_expires: None,
            },
        );

        let write = TieredWrite::Tombstone(Tombstone {
            target: new_target.clone(),
            time_expires: None,
        });
        assert!(
            !backend
                .compare_and_write(&id, Some(&wrong_target), write.clone(), Timestamp::now())
                .await?,
            "wrong expected target must fail"
        );
        assert!(
            backend
                .compare_and_write(&id, Some(&old_target), write.clone(), Timestamp::now())
                .await?,
            "correct expected target must succeed"
        );

        let TieredMetadata::Tombstone(read) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(read.target, new_target);

        assert!(
            backend
                .compare_and_write(&id, Some(&old_target), write, Timestamp::now())
                .await?,
            "idempotent retry"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_swap_inline() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let target = new_test_revision(&id);
        let wrong_target = new_test_revision(&id);

        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: None,
            },
        );

        let payload = Bytes::from_static(b"hello inline");
        let write = TieredWrite::Object(Metadata::default(), payload.clone());
        assert!(
            !backend
                .compare_and_write(&id, Some(&wrong_target), write, Timestamp::now())
                .await?,
            "wrong expected target must fail"
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        let write = TieredWrite::Object(Metadata::default(), payload.clone());
        assert!(
            backend
                .compare_and_write(&id, Some(&target), write.clone(), Timestamp::now())
                .await?,
            "correct expected target must succeed"
        );
        assert_eq!(read_payload(&backend, &id).await?, payload.as_ref());

        assert!(
            backend
                .compare_and_write(&id, Some(&target), write, Timestamp::now())
                .await?,
            "idempotent retry: the row is already inline"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_create_object_on_empty_row() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let payload = Bytes::from_static(b"cas object");
        assert!(
            backend
                .compare_and_write(
                    &id,
                    None,
                    TieredWrite::Object(Metadata::default(), payload.clone()),
                    Timestamp::now()
                )
                .await?,
            "expected CAS success on an empty row"
        );
        assert_eq!(read_payload(&backend, &id).await?, payload.as_ref());

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_delete() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let target = new_test_revision(&id);
        let wrong_target = new_test_revision(&id);
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: None,
            },
        );

        assert!(
            !backend
                .compare_and_write(
                    &id,
                    Some(&wrong_target),
                    TieredWrite::Delete,
                    Timestamp::now()
                )
                .await?,
            "wrong expected target must fail"
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::Tombstone(_)
        ));

        assert!(
            backend
                .compare_and_write(&id, Some(&target), TieredWrite::Delete, Timestamp::now())
                .await?,
            "correct expected target must succeed"
        );
        assert!(matches!(
            backend.get_tiered_metadata(&id, Timestamp::now()).await?,
            TieredMetadata::NotFound
        ));

        assert!(
            backend
                .compare_and_write(&id, Some(&target), TieredWrite::Delete, Timestamp::now())
                .await?,
            "idempotent retry: the row is already absent"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_create_tombstone_over_expired() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: new_test_revision(&id),
                time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            },
        );

        let fresh_target = new_test_revision(&id);
        assert!(
            backend
                .compare_and_write(
                    &id,
                    None,
                    TieredWrite::Tombstone(Tombstone {
                        target: fresh_target.clone(),
                        time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
                    }),
                    Timestamp::now()
                )
                .await?,
            "an expired tombstone reads as absent"
        );

        let TieredMetadata::Tombstone(read) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(read.target, fresh_target);

        Ok(())
    }

    #[tokio::test]
    async fn test_redirect_expiry() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let target = new_test_revision(&id);
        let old_expiry = Timestamp::now() + Duration::from_hours(1);
        let new_expiry = old_expiry + Duration::from_hours(1);
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: target.clone(),
                time_expires: Some(old_expiry),
            },
        );

        // An absolute target extends the redirect.
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(new_expiry).into()),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::Satisfied(new_expiry)
        );

        // Repeating the same extension is already satisfied.
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(ExpiryTarget::At(new_expiry).into()),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::Satisfied(new_expiry)
        );

        // A creation-relative target cannot resolve: tombstones carry no creation time.
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&target),
                    TieredUpdate::SetExpiry(
                        ExpiryTarget::FromCreation(Duration::from_hours(2)).into()
                    ),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::Rejected
        );

        // A different redirect target is rejected.
        let other = new_test_revision(&id);
        assert_eq!(
            backend
                .compare_and_update(
                    &id,
                    Some(&other),
                    TieredUpdate::SetExpiry(
                        ExpiryTarget::At(new_expiry + Duration::from_hours(1)).into()
                    ),
                    Timestamp::now()
                )
                .await?,
            SetExpiryResponse::Rejected
        );

        // The extension was persisted.
        let TieredMetadata::Tombstone(read) =
            backend.get_tiered_metadata(&id, Timestamp::now()).await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(read.time_expires, Some(new_expiry));

        Ok(())
    }

    // --- Section 6: Upload Markers ---

    #[tokio::test]
    async fn test_upload_markers() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let revision = new_test_revision(&id);
        let expires = Timestamp::now() + Duration::from_hours(1);

        backend.create_upload_marker(&revision, expires).await?;
        assert!(
            backend
                .has_upload_marker(&revision, Timestamp::now())
                .await?
        );

        assert!(
            backend
                .delete_upload_marker(&revision, Timestamp::now())
                .await?,
            "a live marker is deleted"
        );
        assert!(
            !backend
                .has_upload_marker(&revision, Timestamp::now())
                .await?
        );
        assert!(
            !backend
                .delete_upload_marker(&revision, Timestamp::now())
                .await?,
            "a repeated deletion reports nothing"
        );

        // An expired marker is neither live nor deletable, but its cell is still reclaimed.
        let expired_revision = new_test_revision(&id);
        backend
            .create_upload_marker(&expired_revision, Timestamp::now() - Duration::from_secs(1))
            .await?;
        assert!(
            !backend
                .has_upload_marker(&expired_revision, Timestamp::now())
                .await?
        );
        assert!(
            !backend
                .delete_upload_marker(&expired_revision, Timestamp::now())
                .await?
        );
        let key = cell_key(
            &expired_revision.as_upload_path().to_string().into_bytes(),
            COLUMN_UPLOAD,
        );
        assert!(
            backend.db.get_pinned(&key).unwrap().is_none(),
            "the expired marker cell must be reclaimed"
        );

        Ok(())
    }

    // --- Section 7: Durability and Garbage Collection ---

    #[tokio::test]
    async fn test_reopen_preserves_objects() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();

        let id = make_id();
        let tombstone_id = make_id();
        let target = new_test_revision(&id);
        let expires = Timestamp::now() + Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(expires),
            ..Default::default()
        };

        {
            let backend = open_backend(&dir);
            put_object(&backend, &id, &metadata, b"persistent").await;
            create_tombstone(
                &backend,
                &tombstone_id,
                &Tombstone {
                    target: target.clone(),
                    time_expires: Some(expires),
                },
            );
        }

        let backend = open_backend(&dir);
        assert_eq!(read_payload(&backend, &id).await?, b"persistent");

        let head = backend
            .get_metadata(&id, Timestamp::now())
            .await?
            .expect("object exists");
        assert_eq!(head.time_expires, Some(expires));

        let TieredMetadata::Tombstone(read) = backend
            .get_tiered_metadata(&tombstone_id, Timestamp::now())
            .await?
        else {
            panic!("expected tombstone");
        };
        assert_eq!(read.target, target);
        assert_eq!(read.time_expires, Some(expires));

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_removes_expired_cells() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let expired_id = make_id();
        // Comfortably in the past: the compaction filter compares against floor-to-seconds wall
        // clock, while `Timestamp::now()` rounds up, so a one-second margin is not enough.
        let expired_metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(60)),
            ..Default::default()
        };
        create_object(&backend, &expired_id, &expired_metadata, b"expired");

        let live_id = make_id();
        let live_metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(Timestamp::now() + Duration::from_hours(1)),
            ..Default::default()
        };
        create_object(&backend, &live_id, &live_metadata, b"live");

        backend.db.flush().unwrap();
        let mut compact = rocksdb::CompactOptions::default();
        // A plain `compact_range` may only move the freshly flushed file down a level, which
        // skips the compaction filter. Force a real bottommost-level compaction so the filter
        // actually runs.
        compact.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        compact.set_exclusive_manual_compaction(true);
        backend
            .db
            .compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);

        let expired_path = expired_id.as_storage_path().to_string().into_bytes();
        for column in [COLUMN_METADATA, COLUMN_PAYLOAD] {
            assert!(
                backend
                    .db
                    .get_pinned(cell_key(&expired_path, column))
                    .unwrap()
                    .is_none(),
                "expired cell must be dropped by compaction"
            );
        }

        let live_path = live_id.as_storage_path().to_string().into_bytes();
        for column in [COLUMN_METADATA, COLUMN_PAYLOAD] {
            assert!(
                backend
                    .db
                    .get_pinned(cell_key(&live_path, column))
                    .unwrap()
                    .is_some(),
                "live cell must survive compaction"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_corrupt_cell_value_errors() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let backend = open_backend(&dir);

        let id = make_id();
        let path = id.as_storage_path().to_string().into_bytes();
        backend
            .db
            .put(cell_key(&path, COLUMN_METADATA), b"x")
            .unwrap();

        assert_eq!(
            backend
                .get_metadata(&id, Timestamp::now())
                .await
                .expect_err("short cell value is corrupt")
                .kind(),
            ErrorKind::CorruptData
        );

        Ok(())
    }

    // --- Section 8: Change Stream Reporting ---

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_writes_and_deletes() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let (backend, producer) = open_backend_with_stream(&dir);

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
        assert_eq!(records[0].shared_resource_id, "rocksdb_objectstore");
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
    async fn change_stream_reports_tombstone_rows() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let (backend, producer) = open_backend_with_stream(&dir);

        let id = make_id();
        let target = new_test_revision(&id);
        let tombstone = Tombstone {
            target,
            time_expires: Some(Timestamp::now() + Duration::from_secs(3600)),
        };
        assert!(
            backend
                .compare_and_write(
                    &id,
                    None,
                    TieredWrite::Tombstone(tombstone),
                    Timestamp::now()
                )
                .await?
        );

        let records = producer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].op_type, OpType::Write);
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn change_stream_reports_expiry_extension_as_update() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let (backend, producer) = open_backend_with_stream(&dir);

        let id = make_id();
        let ttl = Duration::from_secs(3600);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(ttl),
            time_expires: Some(Timestamp::now() + ttl),
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

        let extended = Timestamp::now() + Duration::from_secs(7200);
        assert_eq!(
            backend
                .set_expiry(&id, ExpiryTarget::At(extended).into(), Timestamp::now())
                .await?,
            SetExpiryResponse::Satisfied(extended)
        );

        let records = producer.records();
        assert_eq!(records.len(), 1, "only the extension is reported");
        assert_eq!(records[0].op_type, OpType::Update);
        assert!(
            records[0].size.is_none(),
            "an update does not change the size"
        );
        assert!(records[0].expiration_time.is_some());

        Ok(())
    }

    #[cfg(feature = "storage-cogs")]
    #[tokio::test]
    async fn delete_non_tombstone_reclaims_expired_rows() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let (backend, producer) = open_backend_with_stream(&dir);

        // An expired tombstone is past its lifetime: reclaimed, not handed to the caller.
        let id = make_id();
        create_tombstone(
            &backend,
            &id,
            &Tombstone {
                target: new_test_revision(&id),
                time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            },
        );
        assert_eq!(
            backend.delete_non_tombstone(&id, Timestamp::now()).await?,
            None,
            "an expired tombstone must not be returned to the caller"
        );
        let records = producer.records();
        assert_eq!(records.len(), 1, "the expired tombstone must be reclaimed");
        assert_eq!(records[0].op_type, OpType::Delete);

        // The same holds for an expired object row.
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };
        create_object(&backend, &id, &metadata, b"gone");
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
}
