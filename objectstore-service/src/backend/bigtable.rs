use std::fmt;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use bigtable_rs::bigtable::BigTableConnection;
use bigtable_rs::google::bigtable::v2::{self, mutation};
use futures_util::{StreamExt, TryStreamExt, stream};
use objectstore_types::{ExpirationPolicy, Metadata};
use tokio::runtime::Handle;

use crate::backend::common::{Backend, BackendStream};
use crate::id::ObjectId;

/// Connection timeout used for the initial connection to BigQuery.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// How often to retry failed `mutate` operations
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
            // TODO on connections: Idle connections are automatically closed in “a few minutes”.
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

    async fn mutate<I>(
        &self,
        path: Vec<u8>,
        mutations: I,
        action: &str,
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

        let mut client = self.bigtable.client();

        let mut retry_count = 0;
        loop {
            let response = client
                .mutate_row(request.clone())
                .await
                .map(|res| res.into_inner());
            // TODO: Stop retrying if the object doesn't exist
            if response.is_ok() || retry_count >= REQUEST_RETRY_COUNT {
                if response.is_err() {
                    merni::counter!("bigtable.mutate_failures": 1, "action" => action);
                }
                return response.with_context(|| {
                    format!("failed mutating bigtable row performing a `{action}`")
                });
            }
            retry_count += 1;
            merni::counter!("bigtable.mutate_retry": 1, "action" => action);
            tracing::debug!(retry_count = retry_count, action, "Retrying mutate");
        }
    }

    async fn put_row(
        &self,
        path: Vec<u8>,
        metadata: &Metadata,
        payload: Vec<u8>,
        action: &str,
    ) -> Result<v2::MutateRowResponse> {
        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();
        let (family, timestamp_micros) = match metadata.expiration_policy {
            ExpirationPolicy::Manual => (FAMILY_MANUAL, -1),
            ExpirationPolicy::TimeToLive(ttl) => (
                FAMILY_GC,
                ttl_to_micros(ttl, access_time).context("TTL out of range")?,
            ),
            ExpirationPolicy::TimeToIdle(tti) => (
                FAMILY_GC,
                ttl_to_micros(tti, access_time).context("TTL out of range")?,
            ),
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
                value: serde_json::to_vec(metadata).with_context(|| "failed to encode metadata")?,
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
        mut stream: BackendStream,
    ) -> Result<()> {
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
    async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from Bigtable backend");
        let path = id.as_storage_path().to_string().into_bytes();
        let rows = v2::RowSet {
            row_keys: vec![path.clone()],
            row_ranges: vec![],
        };

        let request = v2::ReadRowsRequest {
            table_name: self.table_path.clone(),
            rows: Some(rows),
            rows_limit: 1,
            ..Default::default()
        };

        let mut client = self.bigtable.client();

        let mut retry_count = 0;
        let response = loop {
            let response = client.read_rows(request.clone()).await;
            if response.is_ok() || retry_count >= REQUEST_RETRY_COUNT {
                if response.is_err() {
                    merni::counter!("bigtable.read_failures": 1);
                }
                break response?;
            }
            retry_count += 1;
            merni::counter!("bigtable.read_retry": 1);
            tracing::warn!(retry_count = retry_count, "Retrying read");
        };
        debug_assert!(response.len() <= 1, "Expected at most one row");

        let Some((read_path, cells)) = response.into_iter().next() else {
            tracing::debug!("Object not found");
            return Ok(None);
        };

        debug_assert!(read_path == path, "Row key mismatch");
        let mut value = Vec::new();
        let mut metadata = Metadata::default();
        let mut expire_at = None;

        for cell in cells {
            match cell.qualifier.as_ref() {
                self::COLUMN_PAYLOAD => {
                    value = cell.value;
                    expire_at = micros_to_time(cell.timestamp_micros);
                    // TODO: Log if the timestamp is invalid.
                }
                self::COLUMN_METADATA => {
                    metadata = serde_json::from_slice(&cell.value)
                        .with_context(|| "failed to decode metadata")?;
                }
                _ => {
                    // TODO: Log unknown column
                }
            }
        }

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();
        metadata.size = Some(value.len());
        metadata.time_expires = expire_at;

        // Filter already expired objects but leave them to garbage collection
        if metadata.expiration_policy.is_timeout() && expire_at.is_some_and(|ts| ts < access_time) {
            tracing::debug!("Object found but past expiry");
            return Ok(None);
        }

        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy
            && expire_at.is_some_and(|ts| ts < access_time + tti - TTI_DEBOUNCE)
        {
            // TODO: Only bump if the difference in deadlines meets a minimum threshold
            // TODO: Schedule into background persistently so this doesn't get lost on restarts
            // `put_row` will internally log an error, so no need to duplicate that here
            let _ = self
                .put_row(path.clone(), &metadata, value.clone(), "tti-bump")
                .await;
        }

        let stream = stream::once(async { Ok(value.into()) }).boxed();
        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<()> {
        tracing::debug!("Deleting from Bigtable backend");

        let path = id.as_storage_path().to_string().into_bytes();
        let mutations = [mutation::Mutation::DeleteFromRow(
            mutation::DeleteFromRow {},
        )];
        self.mutate(path, mutations, "delete").await?;

        Ok(())
    }
}

/// Converts the given TTL duration to a microsecond-precision unix timestamp.
///
/// The TTL is anchored at the provided `from` timestamp, which defaults to `SystemTime::now()`. As
/// required by BigTable, the resulting timestamp has millisecond precision, with the last digits at
/// 0.
fn ttl_to_micros(ttl: Duration, from: SystemTime) -> Option<i64> {
    let deadline = from.checked_add(ttl)?;
    let millis = deadline
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_millis();
    (millis * 1000).try_into().ok()
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

    use uuid::Uuid;

    use crate::id::{Scope, Scopes};

    use super::*;

    // NB: Not run any of these tests, you need to have a BigTable emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

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

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    async fn read_to_vec(mut stream: BackendStream) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }
        Ok(payload)
    }

    fn make_key() -> ObjectId {
        ObjectId {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
            key: Uuid::new_v4().to_string(),
        }
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            time_created: Some(SystemTime::now()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&path).await?.unwrap();

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

        let path = make_key();
        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        backend.delete_object(&path).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"world"))
            .await?;

        let (meta, stream) = backend.get_object(&path).await?.unwrap();

        let payload = read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata::default();

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        backend.delete_object(&path).await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }
}
