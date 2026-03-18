CREATE DATABASE {tenant}__analytics;
CREATE TABLE {tenant}__analytics.events (id UInt32, name String, score UInt32) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__analytics.events VALUES (1, '{tenant}_a', 10), (2, '{tenant}_b', 20), (3, '{tenant}_c', 30);
