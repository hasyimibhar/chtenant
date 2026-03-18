CREATE DATABASE {tenant}__shop;
CREATE TABLE {tenant}__shop.users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE {tenant}__shop.orders (id UInt32, user_id UInt32, amount UInt32) ENGINE = MergeTree() ORDER BY id;
INSERT INTO {tenant}__shop.users VALUES (1, '{tenant}_alice'), (2, '{tenant}_bob');
INSERT INTO {tenant}__shop.orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50);
