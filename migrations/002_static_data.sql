INSERT INTO query_states (state)
VALUES ('Pending'),
       ('Deploying'),
       ('Running'),
       ('Terminating'),
       ('Completed'),
       ('Stopped'),
       ('Failed');

INSERT INTO query_fragment_states (state)
VALUES ('Pending'),
       ('Registered'),
       ('Started'),
       ('Running'),
       ('Completed'),
       ('Stopped'),
       ('Failed');


INSERT INTO worker_states (state)
VALUES ('Pending'),
       ('Active'),
       ('Unreachable'),
       ('Removed');

INSERT INTO query_stop_modes (mode)
VALUES ('Graceful'),
       ('Forceful');
