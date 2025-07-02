use migration::{Migrator, MigratorTrait};
use sea_orm::{Database, DatabaseConnection};

pub async fn initialize_db(database_url: &str) -> anyhow::Result<DatabaseConnection> {
    let connection = Database::connect(database_url).await?;
    Migrator::up(&connection, None).await?;

    Ok(connection)
}

pub mod prelude;

pub mod blob;
pub mod part;
