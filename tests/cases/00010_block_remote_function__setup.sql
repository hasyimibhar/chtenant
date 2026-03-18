CREATE DATABASE {tenant}__analytics;
CREATE TABLE {tenant}__analytics.events (id UInt32, name String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__analytics.events VALUES (1, '{tenant}_secret');
