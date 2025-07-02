use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum Part {
    Table,
    Id,
    Compression,
    #[allow(clippy::enum_variant_names)]
    PartSize,
    CompressedSize,
    SegmentId,
    SegmentOffset,
}

#[derive(DeriveIden)]
enum Blob {
    Table,
    Id,
    Parts,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Part::Table)
                    .if_not_exists()
                    .col(big_integer(Part::Id).auto_increment().primary_key())
                    .col(unsigned(Part::PartSize))
                    .col(small_unsigned(Part::Compression))
                    .col(unsigned(Part::CompressedSize))
                    .col(uuid(Part::SegmentId))
                    .col(unsigned(Part::SegmentOffset))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Blob::Table)
                    .if_not_exists()
                    .col(string(Blob::Id).primary_key())
                    .col(array(Blob::Parts, ColumnType::BigUnsigned))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Part::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Blob::Table).to_owned())
            .await
    }
}
