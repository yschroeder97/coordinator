CREATE TABLE IF NOT EXISTS workers
(
    host_name     TEXT    NOT NULL DEFAULT '127.0.0.1',
    grpc_port     INTEGER NOT NULL DEFAULT 50051 CHECK (grpc_port BETWEEN 1 AND 65535),
    data_port     INTEGER NOT NULL DEFAULT 9090 CHECK (data_port BETWEEN 1 AND 65535),
    capacity      INTEGER NOT NULL CHECK (capacity >= 0),
    current_state TEXT    NOT NULL DEFAULT 'Pending',
    desired_state TEXT    NOT NULL DEFAULT 'Active',
    PRIMARY KEY (host_name, grpc_port),
    FOREIGN KEY (current_state) REFERENCES worker_states (state) ON DELETE CASCADE,
    FOREIGN KEY (desired_state) REFERENCES worker_states (state) ON DELETE CASCADE,
    CHECK (grpc_port != data_port),
    CHECK (desired_state IN ('Active', 'Removed'))
);

CREATE TABLE IF NOT EXISTS worker_changelog
(
    host_name     TEXT     NOT NULL,
    grpc_port     INTEGER  NOT NULL,
    timestamp     DATETIME NOT NULL DEFAULT (datetime('now', 'localtime')),
    capacity      INTEGER  NOT NULL CHECK (capacity >= 0),
    current_state TEXT     NOT NULL,
    desired_state TEXT     NOT NULL,
    FOREIGN KEY (current_state) REFERENCES worker_states (state),
    FOREIGN KEY (desired_state) REFERENCES worker_states (state)
);

CREATE TABLE IF NOT EXISTS worker_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS network_links
(
    src_host_name TEXT    NOT NULL,
    src_grpc_port INTEGER NOT NULL CHECK (src_grpc_port BETWEEN 1 AND 65535),
    dst_host_name TEXT    NOT NULL,
    dst_grpc_port INTEGER NOT NULL CHECK (dst_grpc_port BETWEEN 1 AND 65535),
    PRIMARY KEY (src_host_name, src_grpc_port, dst_host_name, dst_grpc_port),
    CHECK (src_host_name != dst_host_name)
);

CREATE TABLE IF NOT EXISTS logical_sources
(
    name   TEXT PRIMARY KEY,
    schema JSON NOT NULL CHECK (json_valid(schema) AND json_type(schema) = 'object')
);

CREATE TABLE IF NOT EXISTS physical_sources
(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_source      TEXT    NOT NULL,
    placement_host_name TEXT    NOT NULL,
    placement_grpc_port INTEGER NOT NULL,
    source_type         TEXT    NOT NULL,
    source_config       JSON    NOT NULL,
    parser_config       JSON    NOT NULL,
    FOREIGN KEY (logical_source) REFERENCES logical_sources (name) ON DELETE RESTRICT,
    FOREIGN KEY (placement_host_name, placement_grpc_port) REFERENCES workers (host_name, grpc_port) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS sinks
(
    name                TEXT PRIMARY KEY,
    placement_host_name TEXT    NOT NULL,
    placement_grpc_port INTEGER NOT NULL,
    sink_type           TEXT    NOT NULL,
    config              JSON    NOT NULL,
    FOREIGN KEY (placement_host_name, placement_grpc_port) REFERENCES workers (host_name, grpc_port) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS query_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS query_stop_modes
(
    mode TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS active_queries
(
    id            TEXT PRIMARY KEY,
    statement     TEXT NOT NULL,
    current_state TEXT NOT NULL DEFAULT 'Pending',
    desired_state TEXT NOT NULL DEFAULT 'Running',
    stop_mode     TEXT          DEFAULT NULL,
    error         TEXT          DEFAULT NULL,
    stack_trace   TEXT          DEFAULT NULL,
    FOREIGN KEY (current_state) REFERENCES query_states (state),
    FOREIGN KEY (desired_state) REFERENCES query_states (state),
    CHECK (desired_state IN ('Running', 'Stopped')),
    CHECK (stop_mode IN (NULL, 'Graceful', 'Forceful'))
);

CREATE TABLE IF NOT EXISTS terminated_queries
(
    id                TEXT,
    statement         TEXT NOT NULL,
    termination_state TEXT NOT NULL DEFAULT 'Completed',
    error             TEXT          DEFAULT NULL,
    stack_trace       TEXT          DEFAULT NULL,
    FOREIGN KEY (termination_state) REFERENCES query_states (state),
    CHECK (termination_state IN ('Stopped', 'Completed', 'Failed'))
);

CREATE TABLE IF NOT EXISTS query_changelog
(
    query_id      TEXT     NOT NULL,
    statement     TEXT     NOT NULL,
    timestamp     DATETIME NOT NULL DEFAULT (datetime('now', 'localtime')),
    current_state TEXT     NOT NULL,
    desired_state TEXT     NOT NULL,
    FOREIGN KEY (current_state) REFERENCES query_states (state),
    FOREIGN KEY (desired_state) REFERENCES query_states (state)
);

CREATE TABLE IF NOT EXISTS query_fragment_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS query_fragments
(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id            TEXT    NOT NULL,
    placement_host_name TEXT    NOT NULL,
    placement_grpc_port INTEGER NOT NULL CHECK (placement_grpc_port BETWEEN 1 AND 65535),
    current_state       TEXT    NOT NULL DEFAULT 'Pending',
    desired_state       TEXT    NOT NULL DEFAULT 'Running',
    plan                JSON    NOT NULL,
    has_source          BOOLEAN NOT NULL,
    used_capacity       INTEGER NOT NULL CHECK (used_capacity >= 0), -- Sources/sinks don't consume capacity currently, so 0 would be valid
    error               TEXT             DEFAULT NULL,
    stack_trace         TEXT             DEFAULT NULL,
    FOREIGN KEY (query_id) REFERENCES active_queries (id) ON DELETE CASCADE,
    FOREIGN KEY (placement_host_name, placement_grpc_port) REFERENCES workers (host_name, grpc_port) ON DELETE RESTRICT,
    FOREIGN KEY (current_state) REFERENCES query_fragment_states (state),
    FOREIGN KEY (desired_state) REFERENCES query_fragment_states (state),
    CHECK (desired_state IN ('Running', 'Stopped'))
);
