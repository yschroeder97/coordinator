#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::{Display, EnumString, IntoEnumIterator};

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    EnumIter,
    EnumString,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "query_state",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum QueryState {
    #[default]
    Pending, // Query was inserted into the catalog
    Planned,    // Query was successfully planned
    Registered, // Query was successfully registered on all workers
    Running,    // Query was successfully started on all workers
    Completed,  // Query completed successfully
    Stopped,    // Query was stopped from the outside
    Failed,     // Query failed
}

/// From the clients perspective, we want a query to be either Running or Stopped
#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    EnumIter,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "desired_query_state",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum DesiredQueryState {
    #[default]
    Completed,
    Stopped,
}

/// Terminal states for a query - used in terminated_queries table
#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    EnumIter,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "termination_state",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum TerminationState {
    #[default]
    Completed,
    Stopped,
    Failed,
}

impl From<String> for QueryState {
    fn from(s: String) -> Self {
        QueryState::from_str(&s).expect("Failed to parse QueryState")
    }
}

impl QueryState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryState::Completed | QueryState::Stopped | QueryState::Failed
        )
    }

    pub fn next(&self) -> Self {
        match self {
            QueryState::Pending => QueryState::Planned,
            QueryState::Planned => QueryState::Registered,
            QueryState::Registered => QueryState::Running,
            QueryState::Running => QueryState::Completed,
            _ => unreachable!("Terminal state reached"),
        }
    }

    pub fn transitions(&self) -> Vec<QueryState> {
        match self {
            QueryState::Pending => {
                vec![QueryState::Planned, QueryState::Stopped, QueryState::Failed]
            }
            QueryState::Planned => vec![
                QueryState::Registered,
                QueryState::Stopped,
                QueryState::Failed,
            ],
            QueryState::Registered => {
                vec![QueryState::Running, QueryState::Stopped, QueryState::Failed]
            }
            QueryState::Running => vec![
                QueryState::Completed,
                QueryState::Stopped,
                QueryState::Failed,
            ],
            // Terminal states have no valid next states
            QueryState::Completed | QueryState::Stopped | QueryState::Failed => vec![],
        }
    }

    pub fn invalid_transitions(&self) -> Vec<QueryState> {
        let valid = self.transitions();
        QueryState::iter()
            .filter(|s| *s != *self && !valid.contains(s))
            .collect()
    }
}
