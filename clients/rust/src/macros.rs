macro_rules! metadata_builder_methods {
    ($metadata_field:ident) => {
        /// Sets the compression algorithm recorded in this object's metadata.
        ///
        /// For single-object uploads ([`PutBuilder`](crate::PutBuilder)), the client
        /// compresses the payload automatically before sending it.
        ///
        /// For multipart uploads ([`InitiateBuilder`](crate::InitiateBuilder)), this
        /// only records the algorithm in metadata — the caller is responsible for
        /// pre-compressing each part.
        ///
        /// Pass [`None`] to disable compression entirely (e.g. for already-compressed
        /// media formats, or when handling compression externally).
        ///
        /// By default, the compression algorithm set on this Session's Usecase is used.
        pub fn compression(mut self, compression: impl Into<Option<$crate::Compression>>) -> Self {
            self.$metadata_field.compression = compression.into();
            self
        }

        /// Sets the expiration policy of the object to be uploaded.
        ///
        /// By default, the expiration policy set on this Session's Usecase is used.
        pub fn expiration_policy(mut self, expiration_policy: $crate::ExpirationPolicy) -> Self {
            self.$metadata_field.expiration_policy = expiration_policy;
            self
        }

        /// Sets the content type of the object to be uploaded.
        ///
        /// You can use the utility function [`crate::utils::guess_mime_type`] to attempt to guess a
        /// `content_type` based on magic bytes.
        pub fn content_type(
            mut self,
            content_type: impl Into<std::borrow::Cow<'static, str>>,
        ) -> Self {
            self.$metadata_field.content_type = content_type.into();
            self
        }

        /// Sets the origin of the object, typically the IP address of the original source.
        pub fn origin(mut self, origin: impl Into<String>) -> Self {
            self.$metadata_field.origin = Some(origin.into());
            self
        }

        /// This sets the custom metadata to the provided map.
        ///
        /// It will clear any previously set metadata.
        pub fn set_metadata(
            mut self,
            metadata: impl Into<std::collections::BTreeMap<String, String>>,
        ) -> Self {
            self.$metadata_field.custom = metadata.into();
            self
        }

        /// Appends the `key`/`value` to the custom metadata of this object.
        pub fn append_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.$metadata_field.custom.insert(key.into(), value.into());
            self
        }
    };
}
