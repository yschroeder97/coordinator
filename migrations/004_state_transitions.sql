-- Enforce valid state transitions for active_queries.
-- Pending -> Deploying
-- Deploying -> Running, Failed
-- Running -> Terminating, Failed, Completed, Stopped
-- Terminating -> Stopped
-- Terminal (Completed, Stopped, Failed) -> None

CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF current_state
    ON active_queries
    WHEN NEW.current_state != OLD.current_state
BEGIN
    SELECT CASE
               -- Pending can only go to Deploying
               WHEN OLD.current_state = 'Pending' AND NEW.current_state != 'Deploying' THEN
                   RAISE(ABORT, 'Invalid state transition: Pending must transition to Deploying')

               -- Deploying can only go to Running or Failed
               WHEN OLD.current_state = 'Deploying' AND NEW.current_state NOT IN ('Running', 'Failed') THEN
                   RAISE(ABORT, 'Invalid state transition: Deploying must transition to Running or Failed')

               -- Running can go to Terminating, Failed, Completed, or Stopped
               WHEN OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Terminating', 'Failed', 'Completed', 'Stopped') THEN
                   RAISE(ABORT, 'Invalid state transition: Running must transition to Terminating, Failed, Completed, or Stopped')

               -- Terminating can only go to Stopped
               WHEN OLD.current_state = 'Terminating' AND NEW.current_state NOT IN ('Stopped', 'Failed') THEN
                   RAISE(ABORT, 'Invalid state transition: Terminating must transition to Stopped')

               -- Terminal states cannot transition further
               WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
                   RAISE(ABORT, 'Invalid state transition: Cannot transition from a terminal state')
               END;
END;