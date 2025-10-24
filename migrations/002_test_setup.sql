-- Workers
INSERT INTO workers (host_name, grpc_port, data_port, num_slots) VALUES ('worker1', 9090, 8080, 4);
INSERT INTO workers (host_name, grpc_port, data_port, num_slots) VALUES ('worker2', 9090, 8080, 8);
INSERT INTO workers (host_name, grpc_port, data_port, num_slots) VALUES ('worker3', 9090, 8080, 2);

-- Logical Sources
INSERT INTO logical_sources (name, schema) VALUES ('src', '{"ts": "UINT64"}');
INSERT INTO logical_sources (name, schema) VALUES ('temperature_sensor', '{"timestamp": "UINT64", "sensor_id": "INT32", "temperature": "FLOAT32"}');
INSERT INTO logical_sources (name, schema) VALUES ('humidity_sensor', '{"timestamp": "UINT64", "sensor_id": "INT32", "humidity": "FLOAT32"}');

-- Physical Sources
INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
VALUES ('src', 'worker1', 'File', '{"filePath": "/tmp"}','{"inputFormat": "CSV"}');
INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
VALUES ('temperature_sensor', 'worker1', 'Tcp', '{"host": "localhost", "port": 5000}','{"inputFormat": "JSON"}');
INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
VALUES ('temperature_sensor', 'worker2', 'Tcp', '{"host": "localhost", "port": 5001}','{"inputFormat": "JSON"}');
INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
VALUES ('humidity_sensor', 'worker3', 'File', '{"filePath": "/data/humidity.csv"}','{"inputFormat": "CSV"}');

-- Sinks
INSERT INTO sinks (name, placement, sink_type, config) VALUES ('sink', 'worker1', 'Print', '{}');
INSERT INTO sinks (name, placement, sink_type, config) VALUES ('temp_output', 'worker2', 'File', '{"filePath": "/output/temp.csv"}');
INSERT INTO sinks (name, placement, sink_type, config) VALUES ('humidity_output', 'worker3', 'File', '{"filePath": "/output/humidity.csv"}');

-- Queries
INSERT INTO queries (id, statement, sink, state) VALUES ('query1', 'SELECT * FROM src INTO sink', 'sink', 'Pending');
INSERT INTO queries (id, statement, sink, state) VALUES ('query2', 'SELECT AVG(temperature) FROM temperature_sensor INTO temp_output', 'temp_output', 'Running');
INSERT INTO queries (id, statement, sink, state) VALUES ('query3', 'SELECT * FROM humidity_sensor WHERE humidity > 60 INTO humidity_output', 'humidity_output', 'Completed');
