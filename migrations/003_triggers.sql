CREATE TRIGGER IF NOT EXISTS insert_worker_changelog
    AFTER INSERT
    ON workers
BEGIN
    INSERT INTO worker_changelog (host_name, grpc_port, capacity, current_state, desired_state)
    VALUES (NEW.host_name, NEW.grpc_port, NEW.capacity, NEW.current_state, NEW.desired_state);
END;

CREATE TRIGGER IF NOT EXISTS update_worker_changelog
    AFTER UPDATE
    ON workers
BEGIN
    INSERT INTO worker_changelog (host_name, grpc_port, capacity, current_state, desired_state)
    VALUES (NEW.host_name, NEW.grpc_port, NEW.capacity, NEW.current_state, NEW.desired_state);
END;

CREATE TRIGGER IF NOT EXISTS insert_query_changelog
    AFTER INSERT
    ON active_queries
BEGIN
    INSERT INTO query_changelog (query_id, statement, current_state, desired_state)
    VALUES (NEW.id, NEW.statement, NEW.current_state, NEW.desired_state);
END;

CREATE TRIGGER IF NOT EXISTS update_query_changelog
    AFTER UPDATE
    ON active_queries
BEGIN
    INSERT INTO query_changelog (query_id, statement, current_state, desired_state)
    VALUES (NEW.id, NEW.statement, NEW.current_state, NEW.desired_state);
END;

CREATE TRIGGER IF NOT EXISTS archive_active_query
    BEFORE DELETE
    ON active_queries
BEGIN
    SELECT CASE
               WHEN OLD.current_state NOT IN ('Completed', 'Failed', 'Stopped') THEN
                   RAISE(ABORT, 'Active query cannot be deleted; transition to Stopped, Completed, or Failed first.')
        END;

    INSERT INTO terminated_queries (id, statement, termination_state, error, stack_trace)
    VALUES (OLD.id, OLD.statement, OLD.current_state, old.error, old.stack_trace);
END;

CREATE TRIGGER release_worker_capacity
    BEFORE DELETE ON query_fragments
BEGIN
    UPDATE workers
    SET capacity = capacity + OLD.used_capacity
    WHERE host_name = OLD.host_name AND grpc_port = OLD.grpc_port;
END;
