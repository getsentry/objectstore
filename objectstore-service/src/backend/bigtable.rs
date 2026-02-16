//! BigTable backend for high-volume, low-latency storage of small objects.

use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use bigtable_rs::bigtable::{BigTableConnection, Error as BigTableError, RowCell};
use bigtable_rs::google::bigtable::v2::{self, mutation};
use futures_util::{StreamExt, TryStreamExt, stream};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use tokio::runtime::Handle;
use tonic::Code;

use crate::backend::common::{
    Backend, DeleteOutcome, DeleteResponse, GetResponse, MetadataResponse, PutResponse,
};
use crate::id::ObjectId;
use crate::{PayloadStream, ServiceError, ServiceResult};

/// Connection timeout used for the initial connection to BigQuery.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// How often to retry failed requests.
const REQUEST_RETRY_COUNT: usize = 2;

/// Column that stores the raw payload (compressed).
const COLUMN_PAYLOAD: &[u8] = b"p";
/// Column that stores metadata in JSON.
const COLUMN_METADATA: &[u8] = b"m";
/// Column family that uses timestamp-based garbage collection.
///
/// We require a GC rule on this family to automatically delete rows.
/// See: <https://cloud.google.com/bigtable/docs/gc-cell-level>
const FAMILY_GC: &str = "fg";
/// Column family that uses manual garbage collection.
const FAMILY_MANUAL: &str = "fm";

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

/// Parsed data from a BigTable row's cells.
struct RowData {
    metadata: Metadata,
    payload: Vec<u8>,
}

impl RowData {
    fn from_cells(cells: Vec<RowCell>) -> ServiceResult<Self> {
        let mut metadata = Metadata::default();
        let mut expire_at = None;
        let mut payload = Vec::new();

        for cell in cells {
            match cell.qualifier.as_slice() {
                COLUMN_PAYLOAD => {
                    payload = cell.value;
                    expire_at = micros_to_time(cell.timestamp_micros);
                }
                COLUMN_METADATA => {
                    expire_at = micros_to_time(cell.timestamp_micros);
                    metadata = serde_json::from_slice(&cell.value).map_err(|cause| {
                        ServiceError::Serde {
                            context: "failed to deserialize metadata".to_string(),
                            cause,
                        }
                    })?;
                }
                _ => {}
            }
        }

        // Both columns carry the same timestamp (set by `put_row`), so it
        // doesn't matter which cell writes `expire_at` last.
        metadata.time_expires = expire_at;

        Ok(Self { metadata, payload })
    }

    fn expires_before(&self, time: SystemTime) -> bool {
        self.metadata.time_expires.is_some_and(|ts| ts < time)
    }

    /// Returns `true` if this object's TTI deadline should be bumped.
    fn needs_tti_bump(&self) -> bool {
        matches!(
            self.metadata.expiration_policy,
            ExpirationPolicy::TimeToIdle(tti) if self.expires_before(SystemTime::now() + tti - TTI_DEBOUNCE)
        )
    }
}

impl BigTableBackend {
    pub async fn new(
        endpoint: Option<&str>,
        project_id: &str,
        instance_name: &str,
        table_name: &str,
        connections: Option<usize>,
    ) -> Result<Self> {
        let bigtable = if let Some(endpoint) = endpoint {
            BigTableConnection::new_with_emulator(
                endpoint,
                project_id,
                instance_name,
                false, // is_read_only
                Some(CONNECT_TIMEOUT),
            )?
        } else {
            let token_provider = gcp_auth::provider().await?;
            // TODO on connections: Idle connections are automatically closed in "a few minutes".
            // We need to make sure that on longer idle periods the channels are re-opened.
            let connections = connections.unwrap_or(2 * Handle::current().metrics().num_workers());

            BigTableConnection::new_with_token_provider(
                project_id,
                instance_name,
                false, // is_read_only
                connections,
                Some(CONNECT_TIMEOUT),
                token_provider.clone(),
            )?
        };

        let client = bigtable.client();

        Ok(Self {
            bigtable,
            instance_path: format!("projects/{project_id}/instances/{instance_name}"),
            table_path: client.get_full_table_name(table_name),
            table_name: table_name.to_owned(),
        })
    }

    /// Retries a BigTable RPC on transient errors.
    async fn with_retry<T, F>(&self, action: &str, f: impl Fn() -> F) -> ServiceResult<T>
    where
        F: Future<Output = Result<T, BigTableError>> + Send,
    {
        let mut retry_count = 0usize;

        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if retry_count >= REQUEST_RETRY_COUNT || !is_retryable(&e) {
                        merni::counter!("bigtable.failures": 1, "action" => action);
                        return Err(ServiceError::Generic {
                            context: format!("Bigtable: `{action}` failed"),
                            cause: Some(Box::new(e)),
                        });
                    }
                    retry_count += 1;
                    merni::counter!("bigtable.retries": 1, "action" => action);
                    tracing::warn!(retry_count, action, "Retrying request");
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
        action: &str,
    ) -> ServiceResult<Option<RowData>> {
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
        if row.metadata.expiration_policy.is_timeout() && row.expires_before(SystemTime::now()) {
            return Ok(None);
        }

        Ok(Some(row))
    }

    async fn mutate<I>(
        &self,
        path: Vec<u8>,
        mutations: I,
        action: &str,
    ) -> ServiceResult<v2::MutateRowResponse>
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
        action: &str,
    ) -> ServiceResult<v2::MutateRowResponse> {
        let now = SystemTime::now();
        let (family, timestamp_micros) = match metadata.expiration_policy {
            ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
            ExpirationPolicy::TimeToLive(ttl) => (FAMILY_GC, ttl_to_micros(ttl, now)?),
            ExpirationPolicy::TimeToIdle(tti) => (FAMILY_GC, ttl_to_micros(tti, now)?),
        };

        let mutations = [
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
                value: serde_json::to_vec(metadata).map_err(|cause| ServiceError::Serde {
                    context: "failed to serialize metadata".to_string(),
                    cause,
                })?,
            }),
        ];
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
        mut stream: PayloadStream,
    ) -> ServiceResult<PutResponse> {
        tracing::debug!("Writing to Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend_from_slice(&chunk);
        }

        self.put_row(path, metadata, payload, "put").await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> ServiceResult<GetResponse> {
        tracing::debug!("Reading from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        let Some(row) = self.read_row(&path, None, "get").await? else {
            return Ok(None);
        };

        if row.needs_tti_bump() {
            // TODO: Schedule into background persistently so this doesn't get lost on restarts
            let _ = self
                .put_row(path, &row.metadata, row.payload.clone(), "tti-bump")
                .await;
        }

        let mut metadata = row.metadata;
        metadata.size = Some(row.payload.len());

        let stream = stream::once(async { Ok(row.payload.into()) }).boxed();
        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> ServiceResult<MetadataResponse> {
        tracing::debug!("Reading metadata from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();

        // Only read the metadata column — skip the (potentially large) payload.
        // NB: `metadata.size` will not be populated since the payload is not fetched.
        let Some(row) = self
            .read_row(&path, Some(column_filter(COLUMN_METADATA)), "get_metadata")
            .await?
        else {
            return Ok(None);
        };

        // Conditional TTI bump: read the payload only when a bump is actually needed.
        if row.needs_tti_bump() {
            // Best-effort — failures here should not fail the metadata read.
            if let Ok(Some(payload_row)) = self
                .read_row(&path, Some(column_filter(COLUMN_PAYLOAD)), "tti-bump")
                .await
            {
                let _ = self
                    .put_row(path, &row.metadata, payload_row.payload, "tti-bump")
                    .await;
            }
        }

        Ok(Some(row.metadata))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse> {
        tracing::debug!("Deleting from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = [mutation::Mutation::DeleteFromRow(
            mutation::DeleteFromRow {},
        )];
        self.mutate(path, mutations, "delete").await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_non_tombstone(&self, id: &ObjectId) -> ServiceResult<DeleteOutcome> {
        tracing::debug!("Conditional delete from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let delete_mutation = v2::Mutation {
            mutation: Some(mutation::Mutation::DeleteFromRow(
                mutation::DeleteFromRow {},
            )),
        };

        // Detect tombstones by checking the metadata column for the
        // `is_redirect_tombstone` marker.  We cannot use payload-column
        // presence because `put_row` always writes a `p` cell — even for
        // tombstones (with empty bytes).
        let request = v2::CheckAndMutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path,
            predicate_filter: Some(v2::RowFilter {
                filter: Some(v2::row_filter::Filter::Chain(v2::row_filter::Chain {
                    filters: vec![
                        column_filter(COLUMN_METADATA),
                        v2::RowFilter {
                            filter: Some(v2::row_filter::Filter::ValueRegexFilter(
                                // RE2 full-match anchored to the JSON start. The field ordering of
                                // Metadata ensures `is_redirect_tombstone` is serialized first.
                                b"^\\{\"is_redirect_tombstone\":true[,}].*".to_vec(),
                            )),
                        },
                    ],
                })),
            }),
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
            Ok(DeleteOutcome::Tombstone)
        } else {
            Ok(DeleteOutcome::Deleted)
        }
    }
}

/// Converts the given TTL duration to a microsecond-precision unix timestamp.
///
/// The TTL is anchored at the provided `from` timestamp, which defaults to `SystemTime::now()`. As
/// required by BigTable, the resulting timestamp has millisecond precision, with the last digits at
/// 0.
fn ttl_to_micros(ttl: Duration, from: SystemTime) -> ServiceResult<i64> {
    let deadline = from.checked_add(ttl).ok_or_else(|| ServiceError::Generic {
        context: format!(
            "TTL duration overflow: {} plus {}s cannot be represented as SystemTime",
            humantime::format_rfc3339_seconds(from),
            ttl.as_secs()
        ),
        cause: None,
    })?;
    let millis = deadline
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| ServiceError::Generic {
            context: format!(
                "unable to get duration since UNIX_EPOCH for SystemTime {}",
                humantime::format_rfc3339_seconds(deadline)
            ),
            cause: Some(Box::new(e)),
        })?
        .as_millis();
    (millis * 1000)
        .try_into()
        .map_err(|e| ServiceError::Generic {
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

    use crate::id::ObjectContext;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    // NB: Not run most of these tests, you need to have a BigTable emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    #[test]
    fn tombstone_json_field_ordering() {
        // The `delete_non_tombstone` regex predicate assumes
        // `is_redirect_tombstone` is serialized as the first JSON field.
        let metadata = Metadata {
            is_redirect_tombstone: Some(true),
            ..Default::default()
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(
            json.starts_with(r#"{"is_redirect_tombstone":true"#),
            "is_redirect_tombstone must be the first JSON field, got: {json}"
        );
    }

    async fn create_test_backend() -> Result<BigTableBackend> {
        BigTableBackend::new(
            Some("localhost:8086"),
            "testing",
            "objectstore",
            "objectstore",
            None,
        )
        .await
    }

    fn make_stream(contents: &[u8]) -> PayloadStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    async fn read_to_vec(mut stream: PayloadStream) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }
        Ok(payload)
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = read_to_vec(stream).await?;
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
            .put_object(&id, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = read_to_vec(stream).await?;
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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
    async fn test_get_metadata_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            is_redirect_tombstone: Some(true),
            ..Default::default()
        };

        // Write a tombstone (no real payload, just metadata)
        backend.put_object(&id, &metadata, make_stream(b"")).await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.is_redirect_tombstone, Some(true));

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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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
        let payload = read_to_vec(stream).await?;
        assert_eq!(&payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_real_object() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, DeleteOutcome::Deleted);

        let get_result = backend.get_object(&id).await?;
        assert!(get_result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_tombstone() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            is_redirect_tombstone: Some(true),
            ..Default::default()
        };

        backend.put_object(&id, &metadata, make_stream(b"")).await?;

        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, DeleteOutcome::Tombstone);

        // Tombstone should still exist — delete_non_tombstone leaves it intact.
        let get_result = backend.get_metadata(&id).await?;
        assert!(
            get_result.is_some(),
            "tombstone should still exist after delete_non_tombstone"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_non_tombstone_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.delete_non_tombstone(&id).await?;
        assert_eq!(result, DeleteOutcome::Deleted);

        Ok(())
    }
}
