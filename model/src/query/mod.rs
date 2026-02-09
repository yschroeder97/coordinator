pub mod active_query;
pub mod fragment;
pub mod query_state;
pub mod terminated_query;

use query_state::{DesiredQueryState, QueryState, TerminationState};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;
use uuid::Uuid;

pub type QueryId = String;

#[derive(
    Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, EnumIter, DeriveActiveEnum,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "stop_mode",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum StopMode {
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
    pub id: QueryId,
    pub sql_statement: String,
    /// Block the request until the query reaches this state.
    /// Defaults to `QueryState::default()` (Pending), meaning no blocking.
    pub block_until: QueryState,
}

impl CreateQuery {
    pub fn new(statement: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            sql_statement: statement,
            block_until: QueryState::default(),
        }
    }

    pub fn new_with_name(name: QueryId, statement: String) -> Self {
        Self {
            id: name,
            sql_statement: statement,
            block_until: QueryState::default(),
        }
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
}

impl From<CreateQuery> for active_query::ActiveModel {
    fn from(req: CreateQuery) -> Self {
        Self {
            id: Set(req.id),
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

#[derive(Clone, Debug)]
pub struct StopQuery {
    pub id: QueryId,
    /// Whether to block until the query is fully stopped/terminated.
    pub should_block: bool,
}

impl StopQuery {
    pub fn new(id: QueryId) -> Self {
        Self {
            id,
            should_block: false,
        }
    }

    pub fn blocking(mut self) -> Self {
        self.should_block = true;
        self
    }
}

#[derive(Clone, Debug)]
pub struct DropQuery {
    pub id: QueryId,
}

impl DropQuery {
    pub fn new(id: QueryId) -> Self {
        Self { id }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetQuery {
    pub id: Option<QueryId>,
    pub current_state: Option<QueryState>,
    pub desired_state: Option<DesiredQueryState>,
    pub termination_state: Option<TerminationState>,
}

impl GetQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: QueryId) -> Self {
        self.id = Some(id);
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

    pub fn with_termination_state(mut self, state: TerminationState) -> Self {
        self.termination_state = Some(state);
        self
    }
}

/// Represents a query from either active_query or terminated_query table
#[derive(Clone, Debug)]
pub enum Query {
    Active(active_query::Model),
    Terminated(terminated_query::Model),
}
