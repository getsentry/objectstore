use objectstore_service::id::ObjectId;

pub fn populate_sentry_scope(id: &ObjectId) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", id.usecase());
        s.set_extra("scope", id.scopes().as_storage_path().to_string().into());
        s.set_extra("key", id.key().into());
    });
}
