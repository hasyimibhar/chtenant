# chtenant

`chtenant` is a ClickHouse proxy that adds read-only multitenancy on top of ClickHouse. Tenants issue normal queries like `SELECT * FROM analytics.events`, and the proxy transparently rewrites them to tenant-scoped databases (e.g. `SELECT * FROM acme__analytics.events`). 

Supports both HTTP and native TCP protocols.

### Why?

This will be useful for building warehouse-native data product SaaS on top of ClickHouse where you want to give your customer access to their data, but at the same time minimize cost by colocating free or low-tier customer data within the same cluster.

## Quickstart

Start services:

```bash
docker compose up --build -d --wait
```

Create tenants:

```bash
curl -X POST http://localhost:8125/api/v1/tenants \
  -d '{"id": "acme", "cluster_id": "default"}'

curl -X POST http://localhost:8125/api/v1/tenants \
  -d '{"id": "globex", "cluster_id": "default"}'
```

Set up test data (directly in ClickHouse)

```bash
docker compose exec clickhouse clickhouse-client -q "CREATE DATABASE acme__analytics"
docker compose exec clickhouse clickhouse-client -q "CREATE DATABASE globex__analytics"

docker compose exec clickhouse clickhouse-client -q \
  "CREATE TABLE acme__analytics.events (id UInt32, name String) ENGINE = MergeTree() ORDER BY id"
docker compose exec clickhouse clickhouse-client -q \
  "CREATE TABLE globex__analytics.events (id UInt32, name String) ENGINE = MergeTree() ORDER BY id"

docker compose exec clickhouse clickhouse-client -q \
  "INSERT INTO acme__analytics.events VALUES (1, 'acme_event_1'), (2, 'acme_event_2')"
docker compose exec clickhouse clickhouse-client -q \
  "INSERT INTO globex__analytics.events VALUES (1, 'globex_event_1'), (2, 'globex_event_2')"
```

Query via http:

```bash
# As acme
curl -H "X-Tenant-ID: acme" \
  "http://localhost:8124/?query=SELECT+*+FROM+analytics.events+ORDER+BY+id"

# As globex
curl -H "X-Tenant-ID: globex" \
  "http://localhost:8124/?query=SELECT+*+FROM+analytics.events+ORDER+BY+id"
```

Query via native protocol, where tenant ID is the username:

```bash
# As acme
docker compose exec clickhouse clickhouse-client \
  --host host.docker.internal --port 9001 --user acme \
  --database analytics -q "SELECT * FROM events ORDER BY id"

# As globex
docker compose exec clickhouse clickhouse-client \
  --host host.docker.internal --port 9001 --user globex \
  --database analytics -q "SELECT * FROM events ORDER BY id"
```

### Manage tenants

```bash
# List tenants
curl http://localhost:8125/api/v1/tenants

# Get tenant
curl http://localhost:8125/api/v1/tenants/acme

# Disable tenant
curl -X PUT http://localhost:8125/api/v1/tenants/acme -d '{"enabled": false}'

# Delete tenant
curl -X DELETE http://localhost:8125/api/v1/tenants/acme
```
