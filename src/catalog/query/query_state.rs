#[cfg(feature = "arbitrary")]
use proptest_derive::Arbitrary;
use std::str::FromStr;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

#[cfg_attr(feature = "arbitrary", derive(Arbitrary))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, sqlx::Type, Display, EnumIter, EnumString)]
pub enum QueryState {
    Pending,    // Query was inserted into the catalog
    Planned,    // Query was successfully planned
    Registered, // Query was successfully registered on all workers
    Running,    // Query was successfully started on all workers
    Completed,  // Query completed successfully
    Stopped,    // Query was stopped from the outside
    Failed,     // Query failed
}

/// From the clients perspective, we want a query to be either Running or Stopped
#[derive(Clone, Copy, Debug, PartialEq, sqlx::Type)]
pub enum DesiredQueryState {
    Running,
    Stopped,
}

impl From<String> for QueryState {
    fn from(s: String) -> Self {
        QueryState::from_str(&s).expect("Failed to parse QueryState")
    }
}

#[derive(PartialEq)]
pub enum QueryStateTransitionType {
    Valid,
    Invalid,
}

impl QueryState {
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
