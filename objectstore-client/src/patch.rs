use std::collections::BTreeMap;

use objectstore_types::Metadata;
pub use objectstore_types::{PARAM_SCOPE, PARAM_USECASE};

use crate::Client;

impl Client {
    /// Creates a PATCH request to update obe
    pub fn patch<'a>(&'a self, id: &'a str) -> PatchBuilder<'a> {
        PatchBuilder {
            client: self,
            id,
            metadata: Metadata::default(),
        }
    }
}

/// A PATCH request builder.
#[derive(Debug)]
pub struct PatchBuilder<'a> {
    pub(crate) client: &'a Client,
    pub(crate) id: &'a str,
    pub(crate) metadata: Metadata,
}

impl PatchBuilder<'_> {
    /// This sets the custom metadata to the provided map.
    ///
    /// This will only add/overwrite metadata values. In other words, if a
    /// key is not present here, it will not be deleted from the server.
    pub fn set_metadata(mut self, metadata: impl Into<BTreeMap<String, String>>) -> Self {
        self.metadata.custom = metadata.into();
        self
    }

    /// Appends they `key`/`value` to the custom metadata of this object.
    ///
    /// This will only add/overwrite metadata values. In other words, if a
    /// key is not present here, it will not be deleted from the server.
    pub fn append_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.custom.insert(key.into(), value.into());
        self
    }
}

// TODO: instead of a separate `send` method, it would be nice to just implement `IntoFuture`.
// However, `IntoFuture` needs to define the resulting future as an associated type,
// and "impl trait in associated type position" is not yet stable :-(
impl PatchBuilder<'_> {
    /// Sends the built PUT request to the upstream service.
    pub async fn send(self) -> anyhow::Result<()> {
        let patch_url = format!("{}/{}", self.client.service_url, self.id);

        let _response = self
            .client
            .http
            .patch(patch_url)
            .query(&[
                (PARAM_SCOPE, self.client.scope.as_ref()),
                (PARAM_USECASE, self.client.usecase.as_ref()),
            ])
            .headers(self.metadata.to_headers("", false)?)
            .send()
            .await?;
        Ok(())
    }
}
