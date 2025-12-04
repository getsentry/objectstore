use objectstore_service::id::ObjectId;

pub fn populate_sentry_scope(path: &ObjectId) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", &path.usecase);
        s.set_extra("scope", path.scopes.as_storage_path().to_string().into());
        s.set_extra("key", path.key.clone().into());
    });
}
