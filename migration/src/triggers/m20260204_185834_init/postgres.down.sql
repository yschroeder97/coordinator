DROP TRIGGER IF EXISTS release_worker_capacity ON query;
DROP FUNCTION IF EXISTS release_worker_capacity();

DROP TRIGGER IF EXISTS reserve_worker_capacity ON fragment;
DROP FUNCTION IF EXISTS reserve_worker_capacity();

DROP TRIGGER IF EXISTS validate_query_state_transition ON query;
DROP FUNCTION IF EXISTS validate_query_state_transition();
