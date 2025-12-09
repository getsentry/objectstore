use objectstore_service::id::{ObjectContext, ObjectId};

pub fn populate_sentry_context(context: &ObjectContext) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", &context.usecase);
        s.set_extra(
            "scopes",
            context.scopes.as_storage_path().to_string().into(),
        );
    });
}

pub fn populate_sentry_object_id(id: &ObjectId) {
    populate_sentry_context(id.context());
    sentry::configure_scope(|s| {
        s.set_extra("key", id.key().into());
    });
}
