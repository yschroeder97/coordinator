-- Query state validation function and trigger
CREATE OR REPLACE FUNCTION validate_query_state_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.current_state = NEW.current_state THEN
        RETURN NEW;
    END IF;

    IF OLD.current_state = 'Pending' AND NEW.current_state NOT IN ('Planned', 'Stopped', 'Failed') THEN
        RAISE EXCEPTION 'Invalid state transition: Pending must transition to one of (Planned, Stopped, Failed)';
    END IF;

    IF OLD.current_state = 'Planned' AND NEW.current_state NOT IN ('Registered', 'Stopped', 'Failed') THEN
        RAISE EXCEPTION 'Invalid state transition: Planned must transition to one of (Registered, Stopped, Failed)';
    END IF;

    IF OLD.current_state = 'Registered' AND NEW.current_state NOT IN ('Running', 'Stopped', 'Failed') THEN
        RAISE EXCEPTION 'Invalid state transition: Registered must transition to one of (Running, Stopped, Failed)';
    END IF;

    IF OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Stopped', 'Completed', 'Failed') THEN
        RAISE EXCEPTION 'Invalid state transition: Running must transition to one of (Stopped, Completed, Failed)';
    END IF;

    IF OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
        RAISE EXCEPTION 'Invalid state transition: Cannot transition from a terminal state';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_query_state_transition
    BEFORE UPDATE OF current_state
    ON query
    FOR EACH ROW
    EXECUTE FUNCTION validate_query_state_transition();

-- Reserve worker capacity when a fragment is created
CREATE OR REPLACE FUNCTION reserve_worker_capacity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE worker
    SET capacity = capacity - NEW.used_capacity
    WHERE host_addr = NEW.host_addr;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER reserve_worker_capacity
    AFTER INSERT ON fragment
    FOR EACH ROW
    EXECUTE FUNCTION reserve_worker_capacity();

-- Release all fragment capacity back to workers when query reaches terminal state
CREATE OR REPLACE FUNCTION release_worker_capacity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE worker
    SET capacity = capacity + COALESCE(sub.total, 0)
    FROM (
        SELECT host_addr, SUM(used_capacity) AS total
        FROM fragment
        WHERE query_id = NEW.id
        GROUP BY host_addr
    ) sub
    WHERE worker.host_addr = sub.host_addr;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER release_worker_capacity
    AFTER UPDATE OF current_state ON query
    FOR EACH ROW
    WHEN (NEW.current_state IN ('Completed', 'Stopped', 'Failed'))
    EXECUTE FUNCTION release_worker_capacity();
