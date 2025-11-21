-- Indexes for foreign key lookups
-- SQLite does not automatically create indexes on foreign key columns

-- Physical sources lookups
CREATE INDEX IF NOT EXISTS idx_physical_sources_logical
    ON physical_sources(logical_source);

CREATE INDEX IF NOT EXISTS idx_physical_sources_placement
    ON physical_sources(placement);

CREATE INDEX IF NOT EXISTS idx_physical_sources_source_type
    ON physical_sources(source_type);

-- Sinks lookups
CREATE INDEX IF NOT EXISTS idx_sinks_placement
    ON sinks(placement);

CREATE INDEX IF NOT EXISTS idx_sinks_sink_type
    ON sinks(sink_type);

-- Query fragments lookups
CREATE INDEX IF NOT EXISTS idx_query_fragments_host
    ON query_fragments(host_name);

CREATE INDEX IF NOT EXISTS idx_query_fragments_query
    ON query_fragments(query_id);

-- Query physical sources reverse lookup
CREATE INDEX IF NOT EXISTS idx_query_physical_sources_physical
    ON query_physical_sources(physical_source_id);

CREATE INDEX IF NOT EXISTS idx_query_physical_sources_query
    ON query_physical_sources(query_id);

-- Query sinks lookups
CREATE INDEX IF NOT EXISTS idx_query_sinks_sink
    ON query_sinks(sink_name);

CREATE INDEX IF NOT EXISTS idx_query_sinks_query
    ON query_sinks(query_id);

-- Query state lookup
CREATE INDEX IF NOT EXISTS idx_queries_state
    ON queries(state);
