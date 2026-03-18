CREATE DATABASE {tenant}__sales;
CREATE TABLE {tenant}__sales.orders (id UInt32, region String, amount UInt32) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__sales.orders VALUES (1, 'us', 100), (2, 'eu', 200), (3, 'us', 150), (4, 'eu', 50), (5, 'us', 300);
