CREATE DATABASE {tenant}__db1;
CREATE DATABASE {tenant}__db2;
CREATE TABLE {tenant}__db1.t1 (id UInt32, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE {tenant}__db2.t2 (id UInt32, label String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__db1.t1 VALUES (1, '{tenant}_a'), (2, '{tenant}_b');
INSERT INTO {tenant}__db2.t2 VALUES (2, '{tenant}_x'), (3, '{tenant}_y');
