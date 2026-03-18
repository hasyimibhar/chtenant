#!/usr/bin/env bash
#
# Multitenant integration test runner for chtenant.
#
# Test cases live in tests/cases/ as triplets:
#   <name>__setup.sql                    – DDL run directly against ClickHouse (once per tenant)
#   <name>__query.sql                    – SELECT queries run through the proxy (once per tenant)
#   <name>__reference__<tenant>.reference – expected output for that tenant
#
# Setup SQL uses {tenant} as a placeholder that gets substituted with each
# tenant ID. This lets a single setup file create per-tenant databases/tables.
#
# Tenants are discovered from reference file names and created automatically
# via the admin API before each test.
#
# Usage:
#   ./tests/run.sh                   # run all tests
#   ./tests/run.sh 00001_basic_select   # run a specific test

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CASES_DIR="$SCRIPT_DIR/cases"

# Endpoints (override via env vars if needed).
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
PROXY_URL="${PROXY_URL:-http://localhost:8124}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8125}"

passed=0
failed=0
errors=()

# ── Helpers ──────────────────────────────────────────────────────────────

die()  { echo "FATAL: $*" >&2; exit 1; }
info() { echo "--- $*"; }

# Discover unique test names from query files.
discover_tests() {
    local filter="${1:-}"
    local names=()
    for f in "$CASES_DIR"/*__query.sql; do
        [ -f "$f" ] || continue
        local name
        name="$(basename "$f" __query.sql)"
        if [ -n "$filter" ] && [ "$name" != "$filter" ]; then
            continue
        fi
        names+=("$name")
    done
    printf '%s\n' "${names[@]}" | sort
}

# Discover tenant IDs from reference files for a given test name.
discover_tenants() {
    local test_name="$1"
    for f in "$CASES_DIR"/${test_name}__reference__*.reference; do
        [ -f "$f" ] || continue
        local base
        base="$(basename "$f")"
        # Strip prefix and suffix to get tenant ID.
        base="${base#${test_name}__reference__}"
        base="${base%.reference}"
        echo "$base"
    done | sort
}

# Create a tenant via admin API (ignore if already exists).
create_tenant() {
    local tenant="$1"
    curl -sf -o /dev/null -X POST "$ADMIN_URL/api/v1/tenants" \
        -d "{\"id\": \"$tenant\", \"cluster_id\": \"default\"}" 2>/dev/null || true
}

# Delete a tenant via admin API.
delete_tenant() {
    local tenant="$1"
    curl -sf -o /dev/null -X DELETE "$ADMIN_URL/api/v1/tenants/$tenant" 2>/dev/null || true
}

# Run SQL directly against ClickHouse (one statement at a time).
ch_query() {
    local sql="$1"
    while IFS= read -r stmt; do
        stmt="$(echo "$stmt" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        [ -z "$stmt" ] && continue
        curl -sf "$CLICKHOUSE_URL" --data-binary "$stmt" || return 1
    done <<< "$sql"
}

# Run SQL through the proxy with a tenant header.
proxy_query() {
    local tenant="$1"
    local sql="$2"
    curl -sf -H "X-Tenant-ID: $tenant" "$PROXY_URL" --data-binary "$sql"
}

# Substitute {tenant} placeholder in SQL.
sub_tenant() {
    local sql="$1"
    local tenant="$2"
    echo "$sql" | sed "s/{tenant}/$tenant/g"
}

# Extract database names from setup SQL for cleanup.
extract_databases() {
    local sql="$1"
    echo "$sql" | grep -ioE 'CREATE[[:space:]]+DATABASE[[:space:]]+(IF[[:space:]]+NOT[[:space:]]+EXISTS[[:space:]]+)?[a-zA-Z0-9_]+' \
        | awk '{print $NF}'
}

# ── Test runner ──────────────────────────────────────────────────────────

run_test() {
    local test_name="$1"
    local setup_file="$CASES_DIR/${test_name}__setup.sql"
    local query_file="$CASES_DIR/${test_name}__query.sql"

    info "TEST: $test_name"

    # Discover tenants.
    local tenants
    tenants=$(discover_tenants "$test_name")
    if [ -z "$tenants" ]; then
        echo "  SKIP: no reference files found"
        return
    fi

    local setup_sql=""
    if [ -f "$setup_file" ]; then
        setup_sql="$(cat "$setup_file")"
    fi
    local query_sql
    query_sql="$(cat "$query_file")"

    # ── Setup ──

    # Clean up any leftover state from previous runs.
    cleanup "$test_name" "$setup_sql" "$tenants"

    # Create tenants.
    for tenant in $tenants; do
        create_tenant "$tenant"
    done

    # Run setup SQL per tenant (with placeholder substitution).
    if [ -n "$setup_sql" ]; then
        for tenant in $tenants; do
            local sql
            sql="$(sub_tenant "$setup_sql" "$tenant")"
            if ! ch_query "$sql"; then
                echo "  ERROR: setup failed for tenant=$tenant"
                cleanup "$test_name" "$setup_sql" "$tenants"
                return 1
            fi
        done
    fi

    # ── Run queries and compare ──

    local test_passed=true
    for tenant in $tenants; do
        local ref_file="$CASES_DIR/${test_name}__reference__${tenant}.reference"
        local expected
        expected="$(cat "$ref_file")"

        local actual
        actual="$(proxy_query "$tenant" "$query_sql" 2>&1)" || true

        if [ "$actual" = "$expected" ]; then
            echo "  PASS: tenant=$tenant"
        else
            echo "  FAIL: tenant=$tenant"
            diff --color=auto -u \
                <(echo "$expected") \
                <(echo "$actual") \
                | head -20 || true
            test_passed=false
        fi
    done

    # ── Cleanup ──

    cleanup "$test_name" "$setup_sql" "$tenants"

    if $test_passed; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
        errors+=("$test_name")
    fi
}

cleanup() {
    local test_name="$1"
    local setup_sql="$2"
    local tenants="$3"

    # Drop databases created by setup.
    if [ -n "$setup_sql" ]; then
        for tenant in $tenants; do
            local sql
            sql="$(sub_tenant "$setup_sql" "$tenant")"
            local dbs
            dbs="$(extract_databases "$sql")"
            for db in $dbs; do
                # Substitute any remaining placeholders.
                db="$(echo "$db" | sed "s/{tenant}/$tenant/g")"
                ch_query "DROP DATABASE IF EXISTS $db" 2>/dev/null || true
            done
        done
    fi

    # Delete tenants.
    for tenant in $tenants; do
        delete_tenant "$tenant"
    done
}

# ── Main ─────────────────────────────────────────────────────────────────

main() {
    local filter="${1:-}"

    # Preflight check.
    curl -sf "$CLICKHOUSE_URL" --data-binary "SELECT 1" >/dev/null 2>&1 \
        || die "ClickHouse not reachable at $CLICKHOUSE_URL"
    curl -sf "$ADMIN_URL/api/v1/tenants" >/dev/null 2>&1 \
        || die "chtenant admin API not reachable at $ADMIN_URL"

    echo "=== chtenant integration tests ==="
    echo ""

    local tests
    tests=$(discover_tests "$filter")
    if [ -z "$tests" ]; then
        die "no tests found in $CASES_DIR"
    fi

    for test_name in $tests; do
        run_test "$test_name"
        echo ""
    done

    # ── Summary ──

    echo "=== Results: $passed passed, $failed failed ==="
    if [ ${#errors[@]} -gt 0 ]; then
        echo "Failed tests:"
        for e in "${errors[@]}"; do
            echo "  - $e"
        done
        exit 1
    fi
}

main "$@"
