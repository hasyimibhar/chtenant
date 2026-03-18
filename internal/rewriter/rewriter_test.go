package rewriter

import (
	"testing"
)

func TestRewrite(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		tenantID string
		want     string
		wantErr  bool
	}{
		{
			name:     "simple select with db qualifier",
			sql:      "SELECT * FROM foo.bar",
			tenantID: "tenant1",
			want:     "SELECT * FROM tenant1__foo.bar",
		},
		{
			name:     "select without db qualifier (no rewrite needed)",
			sql:      "SELECT * FROM bar",
			tenantID: "tenant1",
			want:     "SELECT * FROM bar",
		},
		{
			name:     "select with JOIN",
			sql:      "SELECT a.id, b.name FROM foo.users a JOIN foo.orders b ON a.id = b.user_id",
			tenantID: "t1",
			want:     "SELECT a.id, b.name FROM t1__foo.users a JOIN t1__foo.orders b ON a.id = b.user_id",
		},
		{
			name:     "select with LEFT JOIN",
			sql:      "SELECT * FROM db1.table1 LEFT JOIN db2.table2 ON table1.id = table2.id",
			tenantID: "myco",
			want:     "SELECT * FROM myco__db1.table1 LEFT JOIN myco__db2.table2 ON table1.id = table2.id",
		},
		{
			name:     "subquery with db qualifier",
			sql:      "SELECT * FROM (SELECT id FROM foo.users WHERE active = 1) sub",
			tenantID: "t1",
			want:     "SELECT * FROM (SELECT id FROM t1__foo.users WHERE active = 1) sub",
		},
		{
			name:     "string literal not rewritten",
			sql:      "SELECT * FROM foo.bar WHERE name = 'FROM db.other'",
			tenantID: "t1",
			want:     "SELECT * FROM t1__foo.bar WHERE name = 'FROM db.other'",
		},
		{
			name:     "multiple databases",
			sql:      "SELECT * FROM db1.users JOIN db2.orders ON users.id = orders.uid",
			tenantID: "acme",
			want:     "SELECT * FROM acme__db1.users JOIN acme__db2.orders ON users.id = orders.uid",
		},
		{
			name:     "case insensitive FROM",
			sql:      "select * from foo.bar",
			tenantID: "t1",
			want:     "select * from t1__foo.bar",
		},
		{
			name:     "WITH CTE",
			sql:      "WITH cte AS (SELECT * FROM foo.bar) SELECT * FROM cte",
			tenantID: "t1",
			want:     "WITH cte AS (SELECT * FROM t1__foo.bar) SELECT * FROM cte",
		},
		{
			name:     "EXPLAIN allowed",
			sql:      "EXPLAIN SELECT * FROM foo.bar",
			tenantID: "t1",
			want:     "EXPLAIN SELECT * FROM t1__foo.bar",
		},
		{
			name:     "INSERT rejected",
			sql:      "INSERT INTO foo.bar VALUES (1, 2)",
			tenantID: "t1",
			wantErr:  true,
		},
		{
			name:     "DROP rejected",
			sql:      "DROP TABLE foo.bar",
			tenantID: "t1",
			wantErr:  true,
		},
		{
			name:     "already prefixed not double-prefixed",
			sql:      "SELECT * FROM t1__foo.bar",
			tenantID: "t1",
			want:     "SELECT * FROM t1__foo.bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Rewrite(tt.sql, tt.tenantID)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("\ngot:  %s\nwant: %s", got, tt.want)
			}
		})
	}
}

func TestRewriteDatabase(t *testing.T) {
	tests := []struct {
		database string
		tenantID string
		want     string
	}{
		{"mydb", "t1", "t1__mydb"},
		{"", "t1", "t1__default"},
		{"default", "t1", "t1__default"},
	}

	for _, tt := range tests {
		got := RewriteDatabase(tt.database, tt.tenantID)
		if got != tt.want {
			t.Errorf("RewriteDatabase(%q, %q) = %q, want %q", tt.database, tt.tenantID, got, tt.want)
		}
	}
}
