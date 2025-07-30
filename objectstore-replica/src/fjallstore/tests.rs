use std::sync::Arc;

use openraft::StorageError;
use openraft::testing::StoreBuilder;
use tempfile::TempDir;

use crate::TypeConfig;

use super::*;

struct FjallBuilder {}

impl StoreBuilder<TypeConfig, Arc<FjallStore>, Arc<FjallStore>, TempDir> for FjallBuilder {
    async fn build(
        &self,
    ) -> Result<(TempDir, Arc<FjallStore>, Arc<FjallStore>), StorageError<u64>> {
        let td = TempDir::new().unwrap();
        let store = Arc::new(FjallStore::new(td.path()));
        Ok((td, store.clone(), store))
    }
}

#[test]
fn passes_openraft_suite() {
    openraft::testing::Suite::test_all(FjallBuilder {}).unwrap();
}
