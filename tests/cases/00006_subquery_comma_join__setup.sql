CREATE DATABASE {tenant}__db1;
CREATE TABLE {tenant}__db1.t1 (id UInt32, val String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__db1.t1 VALUES (1, '{tenant}_a'), (2, '{tenant}_b'), (3, '{tenant}_c');
