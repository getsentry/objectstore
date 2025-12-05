//! Contains all HTTP endpoint handlers.
//!
//! Use [`routes`] to create a router with all endpoints.

use axum::Router;

use crate::state::ServiceState;

mod deprecated;
mod health;
mod helpers;
mod objects;

pub fn routes() -> Router<ServiceState> {
    let routes_v1 = Router::new()
        .merge(objects::router())
        .merge(deprecated::router());

    Router::new()
        .merge(health::router())
        .nest("/v1/", routes_v1)
}
