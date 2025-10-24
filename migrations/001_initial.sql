CREATE TABLE workers
(
    host_name TEXT PRIMARY KEY,
    grpc_port INTEGER NOT NULL,
    data_port INTEGER NOT NULL,
    num_slots INTEGER NOT NULL
);

CREATE TABLE network_links
(
    source_host_name TEXT NOT NULL,
    target_host_name TEXT NOT NULL,
    PRIMARY KEY (source_host_name, target_host_name),
    FOREIGN KEY (source_host_name) REFERENCES workers (host_name),
    FOREIGN KEY (target_host_name) REFERENCES workers (host_name)
);

CREATE TABLE logical_sources
(
    name   TEXT PRIMARY KEY,
    schema JSON NOT NULL
);

CREATE TABLE physical_sources
(
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_source TEXT NOT NULL,
    placement      TEXT NOT NULL,
    source_type    TEXT NOT NULL,
    source_config  JSON NOT NULL,
    parser_config  JSON NOT NULL,
    FOREIGN KEY (logical_source) REFERENCES logical_sources (name),
    FOREIGN KEY (placement) REFERENCES workers (host_name)
);

CREATE TABLE sinks
(
    name      TEXT PRIMARY KEY,
    placement TEXT NOT NULL,
    sink_type TEXT NOT NULL,
    config    JSON NOT NULL,
    FOREIGN KEY (placement) REFERENCES workers (host_name)
);

CREATE TABLE queries
(
    id        TEXT PRIMARY KEY,
    statement TEXT NOT NULL,
    state     TEXT NOT NULL DEFAULT 'Pending',
    sink      TEXT NOT NULL,
    FOREIGN KEY (sink) REFERENCES sinks (name)
);

CREATE TABLE query_physical_sources
(
    query_id           TEXT    NOT NULL,
    physical_source_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, physical_source_id),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (physical_source_id) REFERENCES physical_sources (id)
);

CREATE TABLE query_fragments
(
    query_id  TEXT NOT NULL,
    host_name TEXT NOT NULL,
    state     TEXT NOT NULL DEFAULT 'Pending',
    PRIMARY KEY (query_id, host_name),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (host_name) REFERENCES workers (host_name)
);
