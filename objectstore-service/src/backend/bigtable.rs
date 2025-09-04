use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use bigtable_rs::bigtable::BigTableConnection;
use bigtable_rs::bigtable_table_admin::BigTableTableAdminConnection;
use bigtable_rs::google::bigtable::table_admin::v2::gc_rule::Rule;
use bigtable_rs::google::bigtable::table_admin::v2::table::{TimestampGranularity, View};
use bigtable_rs::google::bigtable::table_admin::v2::{
    ColumnFamily, CreateTableRequest, GcRule, GetTableRequest, Table,
};
use bigtable_rs::google::bigtable::v2::mutation::{DeleteFromRow, Mutation, SetCell};
use bigtable_rs::google::bigtable::v2::{self, MutateRowResponse};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use objectstore_types::{ExpirationPolicy, Metadata};
use tokio::runtime::Handle;

use crate::backend::{Backend, BackendStream};

/// Connection timeout used for the initial connection to BigQuery.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Config for bincode encoding and decoding.
const BC_CONFIG: bincode::config::Configuration = bincode::config::standard();
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// Column that stores the raw payload (compressed).
const COLUMN_PAYLOAD: &[u8] = b"p";
/// Column that stores metadata in bincode.
const COLUMN_METADATA: &[u8] = b"m";
/// Column family that uses timestamp-based garbage collection.
const FAMILY_GC: &str = "fg";
/// Column family that uses manual garbage collection.
const FAMILY_MANUAL: &str = "fm";

/// Configuration for the BigTable backend.
#[derive(Debug)]
pub struct BigTableConfig {
    /// GCP project ID.
    pub project_id: String,
    /// BigTable instance name. The instance has to exist.
    pub instance_name: String,
    /// BigTable table name. Table will be auto-created.
    pub table_name: String,
}

pub struct BigTableBackend {
    config: BigTableConfig,
    bigtable: BigTableConnection,
    admin: BigTableTableAdminConnection,
    instance_path: String,
    table_path: String,
}

impl BigTableBackend {
    pub async fn new(config: BigTableConfig) -> Result<Self> {
        let tokio_runtime = Handle::current();
        let tokio_workers = tokio_runtime.metrics().num_workers();

        // TODO on channel_size: Idle connections are automatically closed in “a few minutes”. We
        // need to make sure that on longer idle period the channels are re-opened.

        // NB: Defaults to gcp_auth::provider() internally, but first checks the
        // BIGTABLE_EMULATOR_HOST environment variable for local dev & tests.
        let bigtable = BigTableConnection::new(
            &config.project_id,
            &config.instance_name,
            false,             // is_read_only
            2 * tokio_workers, // channel_size
            Some(CONNECT_TIMEOUT),
        )
        .await?;

        let client = bigtable.client();
        let instance_path = format!(
            "projects/{}/instances/{}",
            config.project_id, config.instance_name
        );
        let table_path = client.get_full_table_name(&config.table_name);

        let admin = BigTableTableAdminConnection::new(
            &config.project_id,
            &config.instance_name,
            2 * tokio_workers, // channel_size
            Some(CONNECT_TIMEOUT),
        )
        .await?;

        let backend = Self {
            config,
            bigtable,
            admin,
            instance_path,
            table_path,
        };

        backend.ensure_table().await?;
        Ok(backend)
    }
}

impl BigTableBackend {
    async fn ensure_table(&self) -> Result<Table> {
        let mut admin = self.admin.client();

        let request = GetTableRequest {
            name: self.table_path.clone(),
            view: View::Unspecified as i32,
        };

        let result = admin.get_table(request).await;
        if let Ok(table) = result {
            return Ok(table);
        }

        // With automatic expiry, we set a GC rule to automatically delete rows
        // with an age of 0. This sounds odd, but when we write rows, we write
        // them with a future timestamp as long as a TTL is set during write. By
        // doing this, we are effectively writing rows into the future, and they
        // will be deleted due to TTL when their timestamp is passed.
        // See: https://cloud.google.com/bigtable/docs/gc-cell-level
        let gc_rule = GcRule {
            rule: Some(Rule::MaxAge(Duration::from_secs(1).try_into()?)),
        };

        let request = CreateTableRequest {
            parent: self.instance_path.clone(),
            table_id: self.config.table_name.clone(), // name without full path
            table: Some(Table {
                name: self.table_path.clone(),
                cluster_states: Default::default(),
                column_families: HashMap::from_iter([
                    (
                        FAMILY_MANUAL.to_owned(),
                        ColumnFamily {
                            gc_rule: None,
                            value_type: None,
                        },
                    ),
                    (
                        FAMILY_GC.to_owned(),
                        ColumnFamily {
                            gc_rule: Some(gc_rule),
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

        let created_table = admin.create_table(request).await?;

        Ok(created_table)
    }

    async fn mutate<I>(&self, path: &str, mutations: I) -> Result<MutateRowResponse>
    where
        I: IntoIterator<Item = Mutation>,
    {
        let request = v2::MutateRowRequest {
            table_name: self.table_path.clone(),
            row_key: path.as_bytes().to_vec(),
            mutations: mutations
                .into_iter()
                .map(|m| v2::Mutation { mutation: Some(m) })
                .collect(),
            ..Default::default()
        };

        let mut client = self.bigtable.client();
        let response = client.mutate_row(request).await?;
        Ok(response.into_inner())
    }
}

impl fmt::Debug for BigTableBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigTableBackend")
            .field("config", &self.config)
            .field("connection", &format_args!("BigTableConnection {{ ... }}"))
            .field("client", &format_args!("BigTableClient {{ ... }}"))
            // .field("admin", &self.admin)
            .field("table_name", &self.table_path)
            .finish_non_exhaustive()
    }
}

impl BigTableBackend {}

#[async_trait::async_trait]
impl Backend for BigTableBackend {
    async fn put_object(
        &self,
        path: &str,
        metadata: &Metadata,
        mut stream: BackendStream,
    ) -> Result<()> {
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

        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }

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

        self.mutate(path, mutations).await?;

        Ok(())
    }

    async fn get_object(&self, path: &str) -> Result<Option<(Metadata, BackendStream)>> {
        let rows = v2::RowSet {
            row_keys: vec![path.as_bytes().to_vec()],
            row_ranges: vec![],
        };

        let request = v2::ReadRowsRequest {
            table_name: self.table_path.clone(),
            rows: Some(rows),
            rows_limit: 1,
            ..Default::default()
        };

        let mut client = self.bigtable.client();
        let response = client.read_rows(request).await?;
        debug_assert!(response.len() <= 1, "Expected at most one row");

        let Some((key, cells)) = response.into_iter().next() else {
            return Ok(None);
        };

        debug_assert!(key == path.as_bytes(), "Row key mismatch");
        let mut value = Bytes::new();
        let mut metadata = Metadata::default();
        let mut deadline = None;

        for cell in cells {
            match cell.qualifier.as_ref() {
                self::COLUMN_PAYLOAD => {
                    value = cell.value.into();
                    deadline = micros_to_time(cell.timestamp_micros);
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

        if matches!(
            metadata.expiration_policy,
            ExpirationPolicy::TimeToIdle(_) | ExpirationPolicy::TimeToLive(_)
        ) && deadline.is_some_and(|ts| ts < access_time)
        {
            // Leave the stored row to garbage collection
            return Ok(None);
        }

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy {
            // Only bump if the difference in deadlines meets a minimum threshold
            if deadline.is_some_and(|ts| ts < access_time + tti - TTI_DEBOUNCE) {
                let value = Bytes::clone(&value);
                let stream = stream::once(async { Ok(value) }).boxed();
                // TODO: Avoid the serialize roundtrip for metadata
                self.put_object(path, &metadata, stream).await?;
            }
        }

        let stream = stream::once(async { Ok(value) }).boxed();
        Ok(Some((metadata, stream)))
    }

    async fn delete_object(&self, path: &str) -> Result<()> {
        self.mutate(path, [Mutation::DeleteFromRow(DeleteFromRow {})])
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

    use super::*;

    // NB: Not run any of these tests, you need to have a BigTable emulator running and
    // `BIGTABLE_EMULATOR_HOST` set to the address where it is listening. This is done automatically
    // in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<BigTableBackend> {
        let config = BigTableConfig {
            project_id: String::from("my-project"),
            instance_name: String::from("my-instance"),
            table_name: String::from("my-table"),
        };

        BigTableBackend::new(config).await
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

    fn make_path() -> String {
        format!("usecase1/4711/{}", uuid::Uuid::new_v4())
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_path();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
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

        let path = make_path();
        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_path();
        backend.delete_object(&path).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_path();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
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

        let path = make_path();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: Default::default(),
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

        let path = make_path();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            compression: None,
            custom: Default::default(),
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

        let path = make_path();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            compression: None,
            custom: Default::default(),
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }
}
