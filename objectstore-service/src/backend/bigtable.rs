use std::fmt;
use std::time::Duration;

use anyhow::Result;
use bigtable_rs::bigtable::{BigTable as BigTableClient, BigTableConnection};
use bigtable_rs::google::bigtable::v2::mutation::{DeleteFromRow, Mutation, SetCell};
use bigtable_rs::google::bigtable::v2::{self, MutateRowResponse};
use futures_util::{StreamExt, TryStreamExt, stream};
use google_cloud_bigtable_admin_v2::client::BigtableTableAdmin;
use google_cloud_bigtable_admin_v2::model::{ColumnFamily, Table};
use objectstore_types::Metadata;
use tokio::runtime::Handle;

use crate::backend::{Backend, BackendStream};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const BC_CONFIG: bincode::config::Configuration = bincode::config::standard();

const PAYLOAD_COLUMN: &[u8] = b"p";
const METADATA_COLUMN: &[u8] = b"m";

#[derive(Debug)]
pub struct BigTableConfig {
    project_id: String,
    instance_name: String,
    table_name: String,
    column_family: String,
}

pub struct BigTableBackend {
    config: BigTableConfig,
    connection: BigTableConnection,
    client: BigTableClient,
    admin: BigtableTableAdmin,
    instance_path: String,
    table_path: String,
}

impl BigTableBackend {
    pub async fn new(config: BigTableConfig) -> Result<Self> {
        let tokio_runtime = Handle::current();
        let tokio_workers = tokio_runtime.metrics().num_workers();

        // NB: Defaults to gcp_auth::provider() internally, but first checks the
        // BIGTABLE_EMULATOR_HOST environment variable for local dev & tests.
        let connection = BigTableConnection::new(
            &config.project_id,
            &config.instance_name,
            false,             // is_read_only
            2 * tokio_workers, // channel_size
            Some(CONNECT_TIMEOUT),
        )
        .await?;

        let client = connection.client();
        let instance_path = format!(
            "projects/{}/instances/{}",
            config.project_id, config.instance_name
        );
        let table_path = client.get_full_table_name(&config.table_name);

        // TODO: Connect to emulator..?
        let admin = BigtableTableAdmin::builder().build().await?;

        let backend = Self {
            config,
            connection,
            client,
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
        let result = self
            .admin
            .get_table()
            .set_name(self.table_path.clone())
            .send()
            .await;

        if let Ok(table) = result {
            return Ok(table);
        }

        // TODO: Make automatic expiry configurable.
        //
        // With automatic expiry, we set a GC rule to automatically delete rows
        // with an age of 0. This sounds odd, but when we write rows, we write
        // them with a future timestamp as long as a TTL is set during write. By
        // doing this, we are effectively writing rows into the future, and they
        // will be deleted due to TTL when their timestamp is passed.
        //
        // let gc_rule = GcRule::new().set_max_age(Box::new(Duration::from_millis(1).try_into()?));

        let table = Table::new()
            .set_name(self.table_path.clone())
            .set_column_families(vec![(
                self.config.column_family.clone(),
                ColumnFamily::new(), //.set_gc_rule(gc_rule),
            )]);

        let created_table = self
            .admin
            .create_table()
            .set_parent(self.instance_path.clone())
            .set_table_id(self.config.table_name.clone())
            .set_table(table)
            .send()
            .await?;

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

        let response = self.client.mutate_row(request).await?;
        Ok(response.into_inner())
    }
}

impl fmt::Debug for BigTableBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigTableBackend")
            .field("config", &self.config)
            .field("connection", &format_args!("BigTableConnection {{ ... }}"))
            .field("client", &format_args!("BigTableClient {{ ... }}"))
            .field("admin", &self.admin)
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
        // TODO: Support TTL
        let timestamp_micros = -1; // Use server time

        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }

        let mutations = [
            // NB: We explicitly delete the row to clear metadata on overwrite.
            Mutation::DeleteFromRow(DeleteFromRow {}),
            Mutation::SetCell(SetCell {
                family_name: self.config.column_family.clone(),
                column_qualifier: PAYLOAD_COLUMN.to_owned(),
                timestamp_micros,
                value: payload,
            }),
            Mutation::SetCell(SetCell {
                family_name: self.config.column_family.clone(),
                column_qualifier: METADATA_COLUMN.to_owned(),
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

        let response = self.client.read_rows(request).await?;
        debug_assert!(response.len() <= 1, "Expected at most one row");

        let Some((key, cells)) = response.into_iter().next() else {
            return Ok(None);
        };

        debug_assert!(key == path.as_bytes(), "Row key mismatch");
        let mut value = Vec::new();
        let mut metadata = Metadata::default();

        for cell in cells {
            match cell.qualifier.as_ref() {
                self::PAYLOAD_COLUMN => {
                    value = cell.value;
                }
                self::METADATA_COLUMN => {
                    metadata = bincode::serde::decode_from_slice(&cell.value, BC_CONFIG)?.0;
                }
                _ => {
                    // TODO: Log unknown column
                }
            }
        }

        let stream = stream::once(async { Ok(value.into()) }).boxed();
        Ok(Some((metadata, stream)))
    }

    async fn delete_object(&self, path: &str) -> Result<()> {
        self.mutate(path, [Mutation::DeleteFromRow(DeleteFromRow {})])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // todo
}
