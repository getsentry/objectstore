use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use bigtable_rs::bigtable::BigTableConnection;
use bigtable_rs::bigtable_table_admin::{BigTableTableAdminConnection, Error as AdminError};
use bigtable_rs::google::bigtable::table_admin::v2::gc_rule::Rule;
use bigtable_rs::google::bigtable::table_admin::v2::table::{TimestampGranularity, View};
use bigtable_rs::google::bigtable::table_admin::v2::{
    ColumnFamily, CreateTableRequest, GcRule, GetTableRequest, Table,
};
use bigtable_rs::google::bigtable::v2::mutation::{DeleteFromRow, Mutation, SetCell};
use bigtable_rs::google::bigtable::v2::{self, MutateRowResponse};
use futures_util::{StreamExt, TryStreamExt, stream};
use objectstore_types::{ExpirationPolicy, Metadata};
use tokio::runtime::Handle;
use tonic::Code;

use crate::ObjectPath;
use crate::backend::{Backend, BackendStream};

/// Connection timeout used for the initial connection to BigQuery.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Config for bincode encoding and decoding.
const BC_CONFIG: bincode::config::Configuration = bincode::config::standard();
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// How often to retry failed `mutate` operations
const REQUEST_RETRY_COUNT: usize = 2;

/// Column that stores the raw payload (compressed).
const COLUMN_PAYLOAD: &[u8] = b"p";
/// Column that stores metadata in bincode.
const COLUMN_METADATA: &[u8] = b"m";
/// Column family that uses timestamp-based garbage collection.
const FAMILY_GC: &str = "fg";
/// Column family that uses manual garbage collection.
const FAMILY_MANUAL: &str = "fm";

pub struct BigTableBackend {
    bigtable: BigTableConnection,
    admin: BigTableTableAdminConnection,

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
    ) -> Result<Self> {
        let bigtable;
        let admin;

        if let Some(endpoint) = endpoint {
            bigtable = BigTableConnection::new_with_emulator(
                endpoint,
                project_id,
                instance_name,
                false, // is_read_only
                Some(CONNECT_TIMEOUT),
            )?;
            admin = BigTableTableAdminConnection::new_with_emulator(
                endpoint,
                project_id,
                instance_name,
                Some(CONNECT_TIMEOUT),
            )?;
        } else {
            let token_provider = gcp_auth::provider().await?;
            // TODO on channel_size: Idle connections are automatically closed in “a few minutes”.
            // We need to make sure that on longer idle periods the channels are re-opened.
            let channel_size = 2 * Handle::current().metrics().num_workers();

            bigtable = BigTableConnection::new_with_token_provider(
                project_id,
                instance_name,
                false, // is_read_only
                channel_size,
                Some(CONNECT_TIMEOUT),
                token_provider.clone(),
            )?;
            admin = BigTableTableAdminConnection::new_with_token_provider(
                project_id,
                instance_name,
                1, // channel_size
                Some(CONNECT_TIMEOUT),
                token_provider,
            )?;
        };

        let client = bigtable.client();

        let backend = Self {
            bigtable,
            admin,

            instance_path: format!("projects/{project_id}/instances/{instance_name}"),
            table_path: client.get_full_table_name(table_name),
            table_name: table_name.to_owned(),
        };

        backend.ensure_table().await?;
        Ok(backend)
    }

    async fn ensure_table(&self) -> Result<Table> {
        let mut admin = self.admin.client();

        let get_request = GetTableRequest {
            name: self.table_path.clone(),
            view: View::Unspecified as i32,
        };

        match admin.get_table(get_request.clone()).await {
            Err(AdminError::RpcError(e)) if e.code() == Code::NotFound => (), // fall through
            Err(e) => return Err(e.into()),
            Ok(table) => return Ok(table),
        }

        let create_request = CreateTableRequest {
            parent: self.instance_path.clone(),
            table_id: self.table_name.clone(), // name without full path
            table: Some(Table {
                name: String::new(), // Must be empty during creation
                cluster_states: Default::default(),
                column_families: HashMap::from_iter([
                    (
                        FAMILY_MANUAL.to_owned(),
                        ColumnFamily {
                            gc_rule: Some(GcRule { rule: None }),
                            value_type: None,
                        },
                    ),
                    (
                        FAMILY_GC.to_owned(),
                        ColumnFamily {
                            // With automatic expiry, we set a GC rule to automatically delete rows
                            // with an age of 0. This sounds odd, but when we write rows, we write
                            // them with a future timestamp as long as a TTL is set during write. By
                            // doing this, we are effectively writing rows into the future, and they
                            // will be deleted due to TTL when their timestamp is passed.
                            // See: https://cloud.google.com/bigtable/docs/gc-cell-level
                            gc_rule: Some(GcRule {
                                rule: Some(Rule::MaxAge(Duration::from_secs(1).try_into()?)),
                            }),
                            value_type: None,
                        },
                    ),
                ]),
                granularity: TimestampGranularity::Unspecified as i32,
                restore_info: None,
                change_stream_config: None,
                deletion_protection: false,
                automated_backup_config: None,
            }),
            initial_splits: vec![],
        };

        match admin.create_table(create_request).await {
            // Race condition: table was created by another concurrent call.
            Err(AdminError::RpcError(e)) if e.code() == Code::AlreadyExists => {
                Ok(admin.get_table(get_request).await?)
            }
            Err(e) => Err(e.into()),
            Ok(created_table) => Ok(created_table),
        }
    }

    async fn mutate<I>(
        &self,
        path: Vec<u8>,
        mutations: I,
        action: &str,
    ) -> Result<MutateRowResponse>
    where
        I: IntoIterator<Item = Mutation>,
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
    ) -> Result<MutateRowResponse> {
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
            Mutation::DeleteFromRow(DeleteFromRow {}),
            Mutation::SetCell(SetCell {
                family_name: family.to_owned(),
                column_qualifier: COLUMN_PAYLOAD.to_owned(),
                timestamp_micros,
                value: payload,
            }),
            Mutation::SetCell(SetCell {
                family_name: family.to_owned(),
                column_qualifier: COLUMN_METADATA.to_owned(),
                timestamp_micros,
                // TODO: Do we really want bincode here?
                value: bincode::serde::encode_to_vec(metadata, BC_CONFIG)?,
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

    #[tracing::instrument(level = "info", fields(backend = self.name()), skip_all)]
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        mut stream: BackendStream,
    ) -> Result<()> {
        tracing::debug!("Writing to Bigtable backend");
        let path = path.to_string().into_bytes();

        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend_from_slice(&chunk);
        }

        self.put_row(path, metadata, payload, "put").await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", fields(backend = self.name()), skip_all)]
    async fn get_object(&self, path: &ObjectPath) -> Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from Bigtable backend");
        let path = path.to_string().into_bytes();
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
                    metadata = bincode::serde::decode_from_slice(&cell.value, BC_CONFIG)?.0;
                }
                _ => {
                    // TODO: Log unknown column
                }
            }
        }

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();
        metadata.size = Some(value.len());

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

    #[tracing::instrument(level = "info", fields(backend = self.name()), skip_all)]
    async fn delete_object(&self, path: &ObjectPath) -> Result<()> {
        tracing::debug!("Deleting from Bigtable backend");
        let path = path.to_string().into_bytes();
        self.mutate(path, [Mutation::DeleteFromRow(DeleteFromRow {})], "delete")
            .await?;
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

    use super::*;

    // NB: Not run any of these tests, you need to have a BigTable emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<BigTableBackend> {
        BigTableBackend::new(
            Some("localhost:8086"),
            "my-project",
            "my-instance",
            "my-table",
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

    fn make_key() -> ObjectPath {
        ObjectPath {
            usecase: "testing".into(),
            scope: "testing".into(),
            key: Uuid::new_v4().to_string(),
        }
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            size: None,
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&path).await?.unwrap();

        let payload = read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
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
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            size: None,
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            size: None,
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
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: Default::default(),
            size: None,
        };

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
            compression: None,
            custom: Default::default(),
            size: None,
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
            compression: None,
            custom: Default::default(),
            size: None,
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }
}
