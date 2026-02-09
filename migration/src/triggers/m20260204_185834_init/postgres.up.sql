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
    ON active_query
    FOR EACH ROW
    EXECUTE FUNCTION validate_query_state_transition();

-- Archive active query function and trigger
CREATE OR REPLACE FUNCTION archive_active_query()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.current_state NOT IN ('Completed', 'Failed', 'Stopped') THEN
        RAISE EXCEPTION 'Active query cannot be deleted; transition to Stopped, Completed, or Failed first.';
    END IF;

    INSERT INTO terminated_query (query_id, statement, termination_state, start_timestamp, stop_timestamp, stop_mode, error)
    VALUES (OLD.id, OLD.statement, OLD.current_state, COALESCE(OLD.start_timestamp, CURRENT_TIMESTAMP), COALESCE(OLD.stop_timestamp, CURRENT_TIMESTAMP), OLD.stop_mode, OLD.error);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER archive_active_query
    BEFORE DELETE ON active_query
    FOR EACH ROW
    EXECUTE FUNCTION archive_active_query();

-- Auto-archive terminal query function and trigger
CREATE OR REPLACE FUNCTION auto_archive_terminal_query()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.current_state IN ('Completed', 'Failed', 'Stopped') THEN
        -- This delete will cascade to delete all fragments connected with this query
        -- This, in turn, will release the worker capacity
        DELETE FROM active_query WHERE id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER auto_archive_terminal_query
    AFTER UPDATE OF current_state ON active_query
    FOR EACH ROW
    EXECUTE FUNCTION auto_archive_terminal_query();

-- Release worker capacity function and trigger
CREATE OR REPLACE FUNCTION release_worker_capacity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE worker
    SET capacity = capacity + OLD.used_capacity
    WHERE host_addr = OLD.host_addr;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER release_worker_capacity
    BEFORE DELETE ON fragment
    FOR EACH ROW
    EXECUTE FUNCTION release_worker_capacity();
