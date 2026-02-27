DROP TRIGGER IF EXISTS derive_query_state_on_fragment_update ON fragment;
DROP FUNCTION IF EXISTS derive_query_state_on_fragment_update();

DROP TRIGGER IF EXISTS release_fragment_capacity ON fragment;
DROP FUNCTION IF EXISTS release_fragment_capacity();

DROP TRIGGER IF EXISTS acquire_worker_capacity ON fragment;
DROP FUNCTION IF EXISTS acquire_worker_capacity();

DROP TRIGGER IF EXISTS validate_query_state_transition ON query;
DROP FUNCTION IF EXISTS validate_query_state_transition();
