CREATE TABLE workers
(
    host_name  VARCHAR(16) PRIMARY KEY,
    grpc_port  INTEGER NOT NULL,
    data_port  INTEGER NOT NULL,
    num_slots  INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE network_links
(
    source_host_name VARCHAR(16) NOT NULL,
    target_host_name VARCHAR(16) NOT NULL,
    PRIMARY KEY (source_host_name, target_host_name),
    FOREIGN KEY (source_host_name) REFERENCES workers (host_name),
    FOREIGN KEY (target_host_name) REFERENCES workers (host_name)
);

CREATE TABLE logical_sources
(
    name       VARCHAR(16) PRIMARY KEY,
    schema     JSON NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE physical_sources
(
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_source VARCHAR(16) NOT NULL,
    placement      VARCHAR(16) NOT NULL,
    source_type    VARCHAR(16) NOT NULL,
    source_config  JSON        NOT NULL,
    parser_config  JSON        NOT NULL,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (logical_source) REFERENCES logical_sources (name),
    FOREIGN KEY (placement) REFERENCES workers (host_name)
);

CREATE TABLE sinks
(
    name       VARCHAR(32) PRIMARY KEY,
    placement  VARCHAR(16) NOT NULL,
    sink_type  VARCHAR(16) NOT NULL,
    config     JSON        NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (placement) REFERENCES workers (host_name)
);

CREATE TABLE queries
(
    id         VARCHAR(32) PRIMARY KEY,
    statement  TEXT        NOT NULL,
    state      VARCHAR(16) NOT NULL DEFAULT 'Pending',
    sink       VARCHAR(16) NOT NULL,
    created_at DATETIME             DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME             DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sink) REFERENCES sinks (name)
);

CREATE TABLE query_physical_sources
(
    query_id           VARCHAR(32) NOT NULL,
    physical_source_id INTEGER     NOT NULL,
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_id, physical_source_id),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (physical_source_id) REFERENCES physical_sources (id)
);

CREATE TABLE query_fragments
(
    query_id   VARCHAR(32) NOT NULL,
    worker_id  VARCHAR(16) NOT NULL,
    state      VARCHAR(16) NOT NULL DEFAULT 'Pending',
    created_at DATETIME             DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME             DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_id, worker_id),
    FOREIGN KEY (query_id) REFERENCES queries (id),
    FOREIGN KEY (worker_id) REFERENCES workers (host_name)
);

CREATE INDEX idx_physical_sources_logical_source ON physical_sources (logical_source);
CREATE INDEX idx_physical_sources_placement ON physical_sources (placement);
CREATE INDEX idx_sinks_placement ON sinks (placement);
CREATE INDEX idx_queries_state ON queries (state);
CREATE INDEX idx_query_fragments_query_id ON query_fragments (query_id);
CREATE INDEX idx_query_fragments_worker_id ON query_fragments (worker_id);