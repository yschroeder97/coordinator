CREATE TABLE IF NOT EXISTS workers
(
    host_name TEXT PRIMARY KEY,
    grpc_port INTEGER NOT NULL DEFAULT 50051 CHECK (grpc_port BETWEEN 1 AND 65535),
    data_port INTEGER NOT NULL DEFAULT 9090 CHECK (data_port BETWEEN 1 AND 65535),
    capacity  INTEGER NOT NULL CHECK (capacity >= 0),
    state     TEXT    NOT NULL DEFAULT 'Active',
    FOREIGN KEY (state) REFERENCES worker_states (state)
);

CREATE TABLE IF NOT EXISTS worker_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS network_links
(
    src_host_name TEXT NOT NULL,
    dst_host_name TEXT NOT NULL,
    PRIMARY KEY (src_host_name, dst_host_name),
    FOREIGN KEY (src_host_name) REFERENCES workers (host_name) ON DELETE CASCADE,
    FOREIGN KEY (dst_host_name) REFERENCES workers (host_name) ON DELETE CASCADE,
    CHECK (src_host_name != dst_host_name)
);

CREATE TABLE IF NOT EXISTS logical_sources
(
    name   TEXT PRIMARY KEY,
    schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS physical_sources
(
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_source TEXT NOT NULL,
    placement      TEXT NOT NULL,
    source_type    TEXT NOT NULL,
    source_config  JSON NOT NULL,
    parser_config  JSON NOT NULL,
    FOREIGN KEY (logical_source) REFERENCES logical_sources (name) ON DELETE RESTRICT,
    FOREIGN KEY (placement) REFERENCES workers (host_name)
);

CREATE TABLE IF NOT EXISTS sinks
(
    name      TEXT PRIMARY KEY,
    placement TEXT NOT NULL,
    sink_type TEXT NOT NULL,
    config    JSON NOT NULL,
    FOREIGN KEY (placement) REFERENCES workers (host_name) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS query_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS queries
(
    id            TEXT PRIMARY KEY,
    statement     TEXT NOT NULL,
    current_state TEXT NOT NULL DEFAULT 'Pending',
    desired_state TEXT NOT NULL DEFAULT 'Running',
    FOREIGN KEY (current_state) REFERENCES query_states (state),
    FOREIGN KEY (desired_state) REFERENCES query_states (state)
);

CREATE TABLE IF NOT EXISTS deployed_sources
(
    query_id           TEXT    NOT NULL,
    physical_source_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, physical_source_id),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (physical_source_id) REFERENCES physical_sources (id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS query_fragment_states
(
    state TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS query_fragments
(
    query_id      TEXT NOT NULL,
    host_name     TEXT NOT NULL,
    current_state TEXT NOT NULL DEFAULT 'Pending',
    desired_state TEXT NOT NULL DEFAULT 'Running',
    plan          JSON NOT NULL,
    PRIMARY KEY (query_id, host_name),
    FOREIGN KEY (query_id) REFERENCES queries (id) ON DELETE RESTRICT,
    FOREIGN KEY (host_name) REFERENCES workers (host_name) ON DELETE RESTRICT,
    FOREIGN KEY (current_state) REFERENCES query_fragment_states (state),
    FOREIGN KEY (desired_state) REFERENCES query_fragment_states (state)
);

CREATE TABLE IF NOT EXISTS deployed_sinks
(
    query_id  TEXT NOT NULL,
    sink_name TEXT NOT NULL,
    PRIMARY KEY (query_id, sink_name),
    FOREIGN KEY (query_id) REFERENCES queries (id) ON DELETE CASCADE,
    FOREIGN KEY (sink_name) REFERENCES sinks (name) ON DELETE RESTRICT
);
