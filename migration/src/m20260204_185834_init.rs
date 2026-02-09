use crate::triggers::m20260204_185834_init as triggers;
use crate::{assert_not_has_tables, drop_tables};
use model::query::StopMode;
use model::query::query_state::{DesiredQueryState, QueryState, TerminationState};
use model::worker::{DesiredWorkerState, WorkerState};
use sea_orm::DbBackend;
use sea_orm_migration::prelude::{Index as MigrationIndex, Table as MigrationTable, *};
use strum::IntoEnumIterator;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(
            manager,
            LogicalSource,
            PhysicalSource,
            Sink,
            Worker,
            NetworkLink,
            Fragment,
            ActiveQuery,
            TerminatedQuery
        );

        match manager.get_database_backend() {
            DbBackend::MySql => {}
            DbBackend::Postgres => {}
            DbBackend::Sqlite => {
                manager
                    .get_connection()
                    .execute_unprepared("PRAGMA foreign_keys = ON")
                    .await
                    .expect("failed to set foreign key enforcement");

                manager
                    .get_connection()
                    .execute_unprepared("PRAGMA journal_mode = WAL")
                    .await
                    .expect("failed to set journal mode");
            }
            _ => {}
        }

        manager
            .create_table(
                MigrationTable::create()
                    .table(LogicalSource::Table)
                    .col(
                        ColumnDef::new(LogicalSource::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(LogicalSource::Schema).json().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Worker::Table)
                    .col(
                        ColumnDef::new(Worker::HostAddr)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Worker::GrpcAddr)
                            .string()
                            .not_null()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(Worker::Capacity)
                            .integer()
                            .not_null()
                            .check(Expr::col(Worker::Capacity).gte(0)),
                    )
                    .col(
                        ColumnDef::new(Worker::CurrentState)
                            .string()
                            .not_null()
                            .default(WorkerState::default().to_string()),
                    )
                    .col(
                        ColumnDef::new(Worker::DesiredState)
                            .string()
                            .not_null()
                            .default(DesiredWorkerState::default().to_string())
                            .check(
                                Expr::col(Worker::DesiredState).is_in(
                                    DesiredWorkerState::iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(PhysicalSource::Table)
                    .col(
                        ColumnDef::new(PhysicalSource::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::LogicalSource)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(PhysicalSource::HostAddr).string().not_null())
                    .col(
                        ColumnDef::new(PhysicalSource::SourceType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::SourceConfig)
                            .json()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::ParserConfig)
                            .json()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(PhysicalSource::Table, PhysicalSource::LogicalSource)
                            .to(LogicalSource::Table, LogicalSource::Name)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(PhysicalSource::Table, PhysicalSource::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Sink::Table)
                    .col(ColumnDef::new(Sink::Name).string().not_null().primary_key())
                    .col(ColumnDef::new(Sink::HostAddr).string().not_null())
                    .col(ColumnDef::new(Sink::SinkType).string().not_null())
                    .col(ColumnDef::new(Sink::Config).json().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(Sink::Table, Sink::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(NetworkLink::Table)
                    .col(
                        ColumnDef::new(NetworkLink::SourceHostAddr)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(NetworkLink::TargetHostAddr)
                            .string()
                            .not_null(),
                    )
                    .primary_key(
                        MigrationIndex::create()
                            .col(NetworkLink::SourceHostAddr)
                            .col(NetworkLink::TargetHostAddr),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(NetworkLink::Table, NetworkLink::SourceHostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(NetworkLink::Table, NetworkLink::TargetHostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(ActiveQuery::Table)
                    .col(
                        ColumnDef::new(ActiveQuery::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ActiveQuery::Statement).string().not_null())
                    .col(
                        ColumnDef::new(ActiveQuery::CurrentState)
                            .string()
                            .not_null()
                            .default(QueryState::default().to_string()),
                    )
                    .col(
                        ColumnDef::new(ActiveQuery::DesiredState)
                            .string()
                            .not_null()
                            .default(DesiredQueryState::default().to_string())
                            .check(
                                Expr::col(ActiveQuery::DesiredState).is_in(
                                    DesiredQueryState::iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .col(
                        ColumnDef::new(ActiveQuery::StartTimestamp)
                            .date_time()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ActiveQuery::StopTimestamp)
                            .date_time()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ActiveQuery::StopMode).string().null().check(
                            Expr::col(ActiveQuery::StopMode)
                                .is_null()
                                .or(Expr::col(ActiveQuery::StopMode).is_in(
                                    StopMode::iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                                )),
                        ),
                    )
                    .col(ColumnDef::new(ActiveQuery::Error).json().null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Fragment::Table)
                    .col(
                        ColumnDef::new(Fragment::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Fragment::QueryId).string().not_null())
                    .col(ColumnDef::new(Fragment::HostAddr).string().not_null())
                    .col(ColumnDef::new(Fragment::GrpcAddr).string().not_null())
                    .col(ColumnDef::new(Fragment::Plan).json().not_null())
                    .col(
                        ColumnDef::new(Fragment::UsedCapacity)
                            .integer()
                            .not_null()
                            .check(Expr::col(Fragment::UsedCapacity).gte(0)),
                    )
                    .col(ColumnDef::new(Fragment::HasSource).boolean().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(Fragment::Table, Fragment::QueryId)
                            .to(ActiveQuery::Table, ActiveQuery::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Fragment::Table, Fragment::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(TerminatedQuery::Table)
                    .col(
                        ColumnDef::new(TerminatedQuery::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TerminatedQuery::QueryId).string().not_null())
                    .col(
                        ColumnDef::new(TerminatedQuery::Statement)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TerminatedQuery::TerminationState)
                            .string()
                            .not_null()
                            .default(TerminationState::default().to_string())
                            .check(
                                Expr::col(TerminatedQuery::TerminationState).is_in(
                                    TerminationState::iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .col(
                        ColumnDef::new(TerminatedQuery::StartTimestamp)
                            .date_time()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TerminatedQuery::StopTimestamp)
                            .date_time()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TerminatedQuery::StopMode)
                            .string()
                            .null()
                            .check(Expr::col(TerminatedQuery::StopMode).is_null().or(
                                Expr::col(TerminatedQuery::StopMode).is_in(
                                    StopMode::iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                                ),
                            )),
                    )
                    .col(ColumnDef::new(TerminatedQuery::Error).json().null())
                    .to_owned(),
            )
            .await?;

        // Create triggers
        let backend = manager.get_database_backend();
        if backend == DbBackend::MySql {
            return Err(DbErr::Custom(
                "MySQL is currently not supported".to_string(),
            ));
        }
        if let Some(sql) = triggers::up(backend) {
            manager.get_connection().execute_unprepared(sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop triggers first
        let backend = manager.get_database_backend();
        if let Some(sql) = triggers::down(backend) {
            manager.get_connection().execute_unprepared(sql).await?;
        }

        drop_tables!(
            manager,
            LogicalSource,
            PhysicalSource,
            Sink,
            Worker,
            NetworkLink,
            Fragment,
            ActiveQuery,
            TerminatedQuery
        );
        Ok(())
    }
}

#[derive(DeriveIden)]
enum LogicalSource {
    Table,
    Name,
    Schema,
}

#[derive(DeriveIden)]
enum PhysicalSource {
    Table,
    Id,
    LogicalSource,
    HostAddr,
    SourceType,
    SourceConfig,
    ParserConfig,
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    Name,
    HostAddr,
    SinkType,
    Config,
}

#[derive(DeriveIden)]
enum Worker {
    Table,
    HostAddr,
    GrpcAddr,
    Capacity,
    CurrentState,
    DesiredState,
}

#[derive(DeriveIden)]
enum NetworkLink {
    Table,
    SourceHostAddr,
    TargetHostAddr,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    Id,
    QueryId,
    HostAddr,
    GrpcAddr,
    Plan,
    UsedCapacity,
    HasSource,
}

#[derive(DeriveIden)]
enum ActiveQuery {
    Table,
    Id,
    Statement,
    CurrentState,
    DesiredState,
    StartTimestamp,
    StopTimestamp,
    StopMode,
    Error,
}

#[derive(DeriveIden)]
enum TerminatedQuery {
    Table,
    Id,
    QueryId,
    Statement,
    TerminationState,
    StartTimestamp,
    StopTimestamp,
    StopMode,
    Error,
}
