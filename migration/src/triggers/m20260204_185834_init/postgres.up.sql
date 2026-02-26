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

CREATE OR REPLACE FUNCTION release_fragment_capacity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE worker
    SET capacity = capacity + NEW.used_capacity
    WHERE host_addr = NEW.host_addr;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER release_fragment_capacity
    AFTER UPDATE OF current_state ON fragment
    FOR EACH ROW
    WHEN (NEW.current_state IN ('Completed', 'Stopped', 'Failed')
    AND OLD.current_state NOT IN ('Completed', 'Stopped', 'Failed'))
    EXECUTE FUNCTION release_fragment_capacity();

CREATE OR REPLACE FUNCTION derive_query_state_on_fragment_update()
RETURNS TRIGGER AS $$
DECLARE
    derived TEXT;
    current TEXT;
    q_state TEXT;
BEGIN
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state = 'Failed')
            THEN 'Failed'
        WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state != 'Completed')
            THEN 'Completed'
        WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state != 'Stopped')
            THEN 'Stopped'
        WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Running', 'Started'))
            THEN 'Running'
        WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state != 'Registered')
            THEN 'Registered'
        ELSE NULL
    END INTO derived;

    IF derived IS NOT NULL THEN
        SELECT q.current_state INTO current FROM query q WHERE q.id = NEW.query_id;
        IF derived != current THEN
            UPDATE query SET current_state = derived WHERE id = NEW.query_id;
        END IF;
    END IF;

    SELECT current_state INTO q_state FROM query WHERE id = NEW.query_id;

    IF q_state IN ('Running', 'Completed', 'Stopped', 'Failed') THEN
        UPDATE query SET
            start_timestamp = COALESCE(
                (SELECT MAX(start_timestamp) FROM fragment WHERE query_id = NEW.query_id),
                start_timestamp
            ),
            stop_timestamp = CASE
                WHEN q_state IN ('Completed', 'Stopped', 'Failed')
                THEN COALESCE(
                    (SELECT MAX(stop_timestamp) FROM fragment WHERE query_id = NEW.query_id),
                    stop_timestamp
                )
                ELSE stop_timestamp
            END
        WHERE id = NEW.query_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER derive_query_state_on_fragment_update
    AFTER UPDATE OF current_state ON fragment
    FOR EACH ROW
    EXECUTE FUNCTION derive_query_state_on_fragment_update();
