CREATE DATABASE {tenant}__analytics;
CREATE TABLE {tenant}__analytics.events (id UInt32) ENGINE = MergeTree() ORDER BY id;
