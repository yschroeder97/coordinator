-- Query state validation trigger
CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF current_state
    ON active_query
    WHEN NEW.current_state != OLD.current_state
BEGIN
    SELECT CASE
        WHEN OLD.current_state = 'Pending' AND NEW.current_state NOT IN ('Planned', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Pending must transition to one of (Planned, Stopped, Failed)')

        WHEN OLD.current_state = 'Planned' AND NEW.current_state NOT IN ('Registered', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Planned must transition to one of (Registered, Stopped, Failed)')

        WHEN OLD.current_state = 'Registered' AND NEW.current_state NOT IN ('Running', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Registered must transition to one of (Running, Stopped, Failed)')

        WHEN OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Stopped', 'Completed', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Running must transition to one of (Stopped, Completed, Failed)')

        WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Cannot transition from a terminal state')
    END;
END;

-- Archive active query before deletion (only terminal states can be deleted)
CREATE TRIGGER IF NOT EXISTS archive_active_query
    BEFORE DELETE
    ON active_query
BEGIN
    SELECT CASE
               WHEN OLD.current_state NOT IN ('Completed', 'Failed', 'Stopped') THEN
                   RAISE(ABORT, 'Active query cannot be deleted; transition to Stopped, Completed, or Failed first.')
    END;

    INSERT INTO terminated_query (query_id, statement, termination_state, start_timestamp, stop_timestamp, stop_mode, error)
    VALUES (OLD.id, OLD.statement, OLD.current_state, COALESCE(OLD.start_timestamp, CURRENT_TIMESTAMP), COALESCE(OLD.stop_timestamp, CURRENT_TIMESTAMP), OLD.stop_mode, OLD.error);
END;

-- Auto-archive when query reaches terminal state
CREATE TRIGGER IF NOT EXISTS auto_archive_terminal_query
    AFTER UPDATE OF current_state
    ON active_query
    WHEN NEW.current_state IN ('Completed', 'Failed', 'Stopped')
BEGIN
    -- This delete will cascade to delete all fragments connected with this query
    -- This, in turn, will release the worker capacity
    DELETE FROM active_query WHERE id = NEW.id;
END;

-- Release worker capacity when fragment is deleted
CREATE TRIGGER IF NOT EXISTS release_worker_capacity
    BEFORE DELETE ON fragment
BEGIN
    UPDATE worker
    SET capacity = capacity + OLD.used_capacity
    WHERE host_addr = OLD.host_addr;
END;
