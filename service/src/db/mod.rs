use sqlx::PgPool;

pub async fn initialize_db(database_url: &str) -> anyhow::Result<PgPool> {
    let db = PgPool::connect(database_url).await?;

    sqlx::migrate!().run(&db).await?;

    Ok(db)
}
