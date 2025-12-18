// =============================================================================
// TABLE NAMES
// =============================================================================

/// Database table and column name constants
///
/// This module centralizes all table and column name references to ensure consistency
/// and make schema changes easier to manage across the codebase.
pub mod table {
    pub const WORKERS: &str = "workers";
    pub const WORKER_STATES: &str = "worker_states";
    pub const NETWORK_LINKS: &str = "network_links";
    pub const LOGICAL_SOURCES: &str = "logical_sources";
    pub const PHYSICAL_SOURCES: &str = "physical_sources";
    pub const SINKS: &str = "sinks";
    pub const QUERY_STATES: &str = "query_states";
    pub const QUERIES: &str = "queries";
    pub const QUERY_LOG: &str = "query_log";
    pub const DEPLOYED_SOURCES: &str = "deployed_sources";
    pub const QUERY_FRAGMENT_STATES: &str = "query_fragment_states";
    pub const QUERY_FRAGMENTS: &str = "query_fragments";
    pub const DEPLOYED_SINKS: &str = "deployed_sinks";
}

// =============================================================================
// COLUMN NAMES
// =============================================================================

pub mod workers {
    pub const HOST_NAME: &str = "host_name";
    pub const GRPC_PORT: &str = "grpc_port";
    pub const DATA_PORT: &str = "data_port";
    pub const CAPACITY: &str = "capacity";
    pub const CURRENT_STATE: &str = "current_state";
    pub const DESIRED_STATE: &str = "desired_state";
}

pub mod worker_states {
    pub const STATE: &str = "state";
}

pub mod network_links {
    pub const SRC_HOST_NAME: &str = "src_host_name";
    pub const SRC_GRPC_PORT: &str = "src_grpc_port";
    pub const DST_HOST_NAME: &str = "dst_host_name";
    pub const DST_GRPC_PORT: &str = "dst_grpc_port";
}

pub mod logical_sources {
    pub const NAME: &str = "name";
    pub const SCHEMA: &str = "schema";
}

pub mod physical_sources {
    pub const ID: &str = "id";
    pub const LOGICAL_SOURCE: &str = "logical_source";
    pub const PLACEMENT_HOST_NAME: &str = "placement_host_name";
    pub const PLACEMENT_GRPC_PORT: &str = "placement_grpc_port";
    pub const SOURCE_TYPE: &str = "source_type";
    pub const SOURCE_CONFIG: &str = "source_config";
    pub const PARSER_CONFIG: &str = "parser_config";
}

pub mod sinks {
    pub const NAME: &str = "name";
    pub const PLACEMENT_HOST_NAME: &str = "placement_host_name";
    pub const PLACEMENT_GRPC_PORT: &str = "placement_grpc_port";
    pub const SINK_TYPE: &str = "sink_type";
    pub const CONFIG: &str = "config";
}

pub mod query_states {
    pub const STATE: &str = "state";
}

pub mod queries {
    pub const ID: &str = "id";
    pub const STATEMENT: &str = "statement";
    pub const CURRENT_STATE: &str = "current_state";
    pub const DESIRED_STATE: &str = "desired_state";
    pub const SUBMISSION_TIMESTAMP: &str = "submission_timestamp";
}

pub mod query_log {
    pub const ID: &str = "id";
    pub const STATEMENT: &str = "statement";
    pub const TERMINATION_STATE: &str = "termination_state";
    pub const SUBMISSION_TIMESTAMP: &str = "submission_timestamp";
    pub const TERMINATION_TIMESTAMP: &str = "termination_timestamp";
    pub const ERROR: &str = "error";
}

pub mod deployed_sources {
    pub const QUERY_ID: &str = "query_id";
    pub const PHYSICAL_SOURCE_ID: &str = "physical_source_id";
}

pub mod query_fragment_states {
    pub const STATE: &str = "state";
}

pub mod query_fragments {
    pub const QUERY_ID: &str = "query_id";
    pub const HOST_NAME: &str = "host_name";
    pub const GRPC_PORT: &str = "grpc_port";
    pub const CURRENT_STATE: &str = "current_state";
    pub const DESIRED_STATE: &str = "desired_state";
    pub const PLAN: &str = "plan";
}

pub mod deployed_sinks {
    pub const QUERY_ID: &str = "query_id";
    pub const SINK_NAME: &str = "sink_name";
}
