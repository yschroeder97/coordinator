use crate::query::query_state::QueryState;
use proptest::prelude::*;

pub fn arb_valid_state_path() -> impl Strategy<Value = Vec<QueryState>> {
    use QueryState::*;
    prop_oneof![
        Just(vec![Pending, Planned, Registered, Running, Completed]),
        Just(vec![Pending, Planned, Registered, Running, Stopped]),
        Just(vec![Pending, Planned, Registered, Running, Failed]),
        Just(vec![Pending, Planned, Registered, Stopped]),
        Just(vec![Pending, Planned, Registered, Failed]),
        Just(vec![Pending, Planned, Stopped]),
        Just(vec![Pending, Planned, Failed]),
        Just(vec![Pending, Stopped]),
        Just(vec![Pending, Failed]),
    ]
}
