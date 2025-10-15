INSERT INTO workers (host_name, grpc_port, data_port, num_slots) VALUES ('worker1', 9090, 8080, 4);

INSERT INTO logical_sources (name, schema) VALUES ('src', '{"ts": "UINT64"}');

INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config) VALUES ('src', 'worker1', 'File', '{"filePath": "/tmp"}','{"inputFormat": "CSV"}');

INSERT INTO sinks (name, placement, sink_type, config) VALUES ('sink', 'worker1', 'Print', '{}');
