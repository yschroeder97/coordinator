pub(crate) mod query_service;
mod query_reconciler;
mod pending_query;
mod planned_query;
mod registered_query;
mod running_query;

/*
Pending -> Planned
try_transition:
-> Input: query_id, sql_stmt
-> Does: Invoke planner to get fragments, insert fragments into DB, update query state
-> Returns: Fragments, or err: Planning, DB
on_stop:
-> Happy Path: Move query to terminated (stopped), removes fragments automatically, give back capacities.
-> Error Path: Retry on transient DB error, panic on permanent (not possible to stop the query then)
on_failed:
-> Input: Error details from the failed transition: Planning, DB error
-> Happy Path: Move query to terminated (failed), together with error, give back capacities.
-> Error Path: Same as on_stop.

Planned -> Registered

Registered -> Running

Running
 */