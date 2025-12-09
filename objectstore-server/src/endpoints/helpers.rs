use objectstore_service::id::{ObjectContext, ObjectId};

pub fn populate_sentry_context(context: &ObjectContext) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", &context.usecase);
        for scope in &context.scopes {
            s.set_tag(&format!("scope.{}", scope.name()), scope.value());
        }
    });
}

pub fn populate_sentry_object_id(id: &ObjectId) {
    populate_sentry_context(id.context());
    sentry::configure_scope(|s| {
        s.set_extra("key", id.key().into());
    });
}
