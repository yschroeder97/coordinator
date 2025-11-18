INSERT INTO source_types (type)
VALUES ('File'),
       ('Tcp');

INSERT INTO sink_types (type)
VALUES ('File'),
       ('Print');

INSERT INTO global_query_states (state, tag)
VALUES ('Pending', 0),
       ('Running', 1),
       ('Completed', 2),
       ('Stopped', 3),
       ('Failed', 4);

INSERT INTO query_fragment_states (state, tag)
VALUES ('Registered', 0),
       ('Started', 1),
       ('Running', 2),
       ('Stopped', 3),
       ('Failed', 4);
