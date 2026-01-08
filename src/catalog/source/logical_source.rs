use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::source::schema::Schema;
use crate::catalog::tables::{logical_sources, table};
use crate::request::Request;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteArguments;
use crate::catalog::catalog_errors::CatalogErr;

pub type LogicalSourceName = String;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    #[sqlx(json)]
    pub schema: Schema,
}

#[derive(Clone, Debug)]
pub struct CreateLogicalSource {
    pub source_name: LogicalSourceName,
    pub schema: Schema,
}
pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<(), CatalogErr>>;

#[derive(Clone, Debug)]
pub struct DropLogicalSource {
    pub source_name: Option<LogicalSourceName>,
}
pub type DropLogicalSourceRequest =
    Request<DropLogicalSource, Result<Option<LogicalSource>, CatalogErr>>;

impl ToSql for DropLogicalSource {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Delete(table::LOGICAL_SOURCES))
            .eq(logical_sources::NAME, self.source_name.clone())
            .into_parts()
    }
}

#[derive(Clone, Debug)]
pub struct GetLogicalSource {
    pub source_name: Option<LogicalSourceName>,
}
pub type GetLogicalSourceRequest =
Request<GetLogicalSource, Result<LogicalSource, CatalogErr>>;

impl ToSql for GetLogicalSource {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::LOGICAL_SOURCES))
            .eq(logical_sources::NAME, self.source_name.clone())
            .into_parts()
    }
}
