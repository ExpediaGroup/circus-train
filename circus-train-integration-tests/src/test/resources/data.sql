SET SCHEMA housekeeping_common;

-- Old path from 2014-12-25 11:13:28.0
INSERT INTO legacy_replica_path
       (id, event_id, path_event_id, path, creation_timestamp)
VALUES (1, 'event-124', 'event-123', 'file:/foo/bar/event-123/deleteme', 1419506008000);
