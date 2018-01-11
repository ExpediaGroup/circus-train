CREATE SCHEMA IF NOT EXISTS circus_train;

CREATE TABLE IF NOT EXISTS circus_train.legacy_replica_path (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY,
  creation_timestamp BIGINT NOT NULL,
  event_id VARCHAR(250) NOT NULL,
  path_event_id VARCHAR(250) NULL,
  path VARCHAR(4000) NOT NULL,
  PRIMARY KEY (id)
);
