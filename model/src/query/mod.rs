pub mod fragment;
pub mod query_state;

use crate::IntoCondition;
use crate::query::fragment::{FragmentError, FragmentId};
#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use query_state::{DesiredQueryState, QueryState};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::Condition;
use sea_orm::FromJsonQueryResult;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum::Display;
use thiserror::Error;
use uuid::Uuid;

pub type QueryName = String;
pub type QueryId = i64;

#[derive(Debug, Clone, DeriveEntityModel)]
#[sea_orm(table_name = "query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub name: QueryName,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_mode: Option<StopMode>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<QueryError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "fragment::Entity")]
    Fragment,
}

impl Related<fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, Error)]
pub enum QueryError {
    #[error("Planning failed: {0}")]
    Planning(String),
    #[error("Registration failed: {0:?}")]
    Registering(HashMap<FragmentId, FragmentError>),
    #[error("Starting failed: {0:?}")]
    Starting(HashMap<FragmentId, FragmentError>),
    #[error("Execution failed: {0:?}")]
    Running(HashMap<FragmentId, FragmentError>),
}

impl From<anyhow::Error> for QueryError {
    fn from(e: anyhow::Error) -> Self {
        QueryError::Planning(e.to_string())
    }
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum StopMode {
    #[default]
    Graceful,
    Forceful,
}

impl From<StopMode> for i32 {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::Graceful => 0,
            StopMode::Forceful => 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryName,
    pub sql_statement: String,
    /// Block the request until the query reaches this state.
    /// Defaults to `QueryState::default()` (Pending), meaning no blocking.
    pub block_until: QueryState,
}

impl CreateQuery {
    pub fn new(statement: String) -> Self {
        Self {
            name: Uuid::new_v4().to_string(),
            sql_statement: statement,
            block_until: QueryState::default(),
        }
    }

    pub fn name(mut self, name: QueryName) -> Self {
        self.name = name;
        self
    }

    pub fn block_until(mut self, state: QueryState) -> Self {
        assert!(
            state != QueryState::Failed && state != QueryState::Stopped,
            "Invalid target state: {:?}",
            state
        );
        self.block_until = state;
        self
    }

    pub fn should_block(&self) -> bool {
        self.block_until != QueryState::Pending
    }
}

impl From<CreateQuery> for ActiveModel {
    fn from(req: CreateQuery) -> Self {
        Self {
            id: NotSet,
            name: Set(req.name),
            statement: Set(req.sql_statement),
            current_state: NotSet,
            desired_state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            stop_mode: NotSet,
            error: NotSet,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DropQuery {
    pub stop_mode: StopMode,
    /// Whether to block until the query is fully stopped/terminated.
    pub should_block: bool,
    pub filters: GetQuery,
}

impl DropQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_filters(mut self, filters: GetQuery) -> Self {
        self.filters = filters;
        self
    }

    pub fn stop_mode(mut self, stop_mode: StopMode) -> Self {
        self.stop_mode = stop_mode;
        self
    }

    pub fn blocking(mut self) -> Self {
        self.should_block = true;
        self
    }

    pub fn should_block(&self) -> bool {
        self.should_block
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetQuery {
    pub ids: Option<Vec<QueryId>>,
    pub name: Option<QueryName>,
    pub current_state: Option<QueryState>,
    pub desired_state: Option<DesiredQueryState>,
}

impl GetQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: QueryId) -> Self {
        self.ids = Some(vec![id]);
        self
    }

    pub fn with_ids(mut self, ids: Vec<QueryId>) -> Self {
        self.ids = Some(ids);
        self
    }

    pub fn with_name(mut self, name: QueryName) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_current_state(mut self, state: QueryState) -> Self {
        self.current_state = Some(state);
        self
    }

    pub fn with_desired_state(mut self, state: DesiredQueryState) -> Self {
        self.desired_state = Some(state);
        self
    }
}

impl IntoCondition for GetQuery {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.ids.map(|ids| Column::Id.is_in(ids)))
            .add_option(self.name.map(|v| Column::Name.eq(v)))
            .add_option(self.current_state.map(|v| Column::CurrentState.eq(v)))
            .add_option(self.desired_state.map(|v| Column::DesiredState.eq(v)))
    }
}
