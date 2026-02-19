-- Query state validation trigger
CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF current_state
    ON query
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

-- Reserve worker capacity when a fragment is created
CREATE TRIGGER IF NOT EXISTS reserve_worker_capacity
    AFTER INSERT ON fragment
BEGIN
    UPDATE worker
    SET capacity = capacity - NEW.used_capacity
    WHERE host_addr = NEW.host_addr;
END;

-- Release all fragment capacity back to workers when query reaches terminal state
CREATE TRIGGER IF NOT EXISTS release_worker_capacity
    AFTER UPDATE OF current_state ON query
    WHEN NEW.current_state IN ('Completed', 'Stopped', 'Failed')
BEGIN
    UPDATE worker
    SET capacity = capacity + (
        SELECT COALESCE(SUM(f.used_capacity), 0)
        FROM fragment f
        WHERE f.query_id = NEW.id AND f.host_addr = worker.host_addr
    )
    WHERE host_addr IN (SELECT host_addr FROM fragment WHERE query_id = NEW.id);
END;
