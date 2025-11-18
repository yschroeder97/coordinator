CREATE TABLE IF NOT EXISTS workers
(
    host_name TEXT PRIMARY KEY,
    grpc_port INTEGER NOT NULL CHECK (grpc_port BETWEEN 0 AND 65535),
    data_port INTEGER NOT NULL CHECK (data_port BETWEEN 0 AND 65535),
    capacity  INTEGER NOT NULL CHECK (capacity >= 0)
);

CREATE TABLE IF NOT EXISTS network_links
(
    src_host_name TEXT NOT NULL,
    dst_host_name TEXT NOT NULL,
    PRIMARY KEY (src_host_name, dst_host_name),
    FOREIGN KEY (src_host_name) REFERENCES workers (host_name) ON DELETE CASCADE,
    FOREIGN KEY (dst_host_name) REFERENCES workers (host_name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS logical_sources
(
    name   TEXT PRIMARY KEY,
    schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS source_types
(
    type TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sink_types
(
    type TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS data_types
(
    type TEXT PRIMARY KEY
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
    FOREIGN KEY (placement) REFERENCES workers (host_name),
    FOREIGN KEY (source_type) REFERENCES source_types (type)
);

CREATE TABLE IF NOT EXISTS sinks
(
    name      TEXT PRIMARY KEY,
    placement TEXT NOT NULL,
    sink_type TEXT NOT NULL,
    config    JSON NOT NULL,
    FOREIGN KEY (placement) REFERENCES workers (host_name) ON DELETE RESTRICT,
    FOREIGN KEY (sink_type) REFERENCES sink_types (type)
);

CREATE TABLE IF NOT EXISTS global_query_states
(
    state TEXT PRIMARY KEY,
    tag   INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS queries
(
    id        TEXT PRIMARY KEY,
    statement TEXT NOT NULL,
    state     TEXT NOT NULL DEFAULT 'Pending',
    FOREIGN KEY (state) REFERENCES global_query_states (state)
);

CREATE TABLE IF NOT EXISTS query_physical_sources
(
    query_id           TEXT    NOT NULL,
    physical_source_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, physical_source_id),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (physical_source_id)
        REFERENCES physical_sources (id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS query_fragment_states
(
    state TEXT PRIMARY KEY,
    tag   INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS query_fragments
(
    query_id  TEXT NOT NULL,
    host_name TEXT NOT NULL,
    state     TEXT NOT NULL DEFAULT 'Pending',
    PRIMARY KEY (query_id, host_name),
    FOREIGN KEY (query_id) REFERENCES queries (id) ON DELETE RESTRICT,
    FOREIGN KEY (host_name) REFERENCES workers (host_name) ON DELETE RESTRICT,
    FOREIGN KEY (state) REFERENCES query_fragment_states (state)
);
