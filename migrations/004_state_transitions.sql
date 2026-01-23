-- Enforce valid state transitions for active_queries.
CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF current_state
    ON active_queries
    WHEN NEW.current_state != OLD.current_state
BEGIN
    SELECT CASE
               WHEN OLD.current_state = 'Pending' AND NEW.current_state NOT IN ('Planned', 'Stopped', 'Failed') THEN
                   RAISE(ABORT,
                         'Invalid state transition: Pending must transition to one of (Planned, Stopped, Failed)')

               WHEN OLD.current_state = 'Planned' AND NEW.current_state NOT IN ('Registered', 'Stopped', 'Failed') THEN
                   RAISE(ABORT,
                         'Invalid state transition: Planned must transition to one of (Registered, Stopped, Failed)')

               WHEN OLD.current_state = 'Registered' AND NEW.current_state NOT IN ('Running', 'Stopped', 'Failed') THEN
                   RAISE(ABORT,
                         'Invalid state transition: Registered must transition to one of (Terminating, Failed, Completed, Stopped)')

               WHEN OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Stopped', 'Completed', 'Failed') THEN
                   RAISE(ABORT,
                         'Invalid state transition: Running must transition to one of (Stopped, Completed, Failed)')

               WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
                   RAISE(ABORT, 'Invalid state transition: Cannot transition from a terminal state')
               END;
END;