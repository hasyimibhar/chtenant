package tenant

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"time"
)

type Tenant struct {
	ID        string    `json:"id"`
	ClusterID string    `json:"cluster_id"`
	CreatedAt time.Time `json:"created_at"`
	Enabled   bool      `json:"enabled"`
}

type Store interface {
	Get(ctx context.Context, id string) (*Tenant, error)
	List(ctx context.Context) ([]Tenant, error)
	Create(ctx context.Context, t *Tenant) error
	Update(ctx context.Context, t *Tenant) error
	Delete(ctx context.Context, id string) error
}

var validID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// PostgresStore stores tenants in PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	s := &PostgresStore{db: db}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("migrating tenant table: %w", err)
	}
	return s, nil
}

func (s *PostgresStore) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS tenants (
			id         TEXT PRIMARY KEY,
			cluster_id TEXT NOT NULL DEFAULT 'default',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			enabled    BOOLEAN NOT NULL DEFAULT true
		)
	`)
	return err
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*Tenant, error) {
	t := &Tenant{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id, cluster_id, created_at, enabled FROM tenants WHERE id = $1`, id,
	).Scan(&t.ID, &t.ClusterID, &t.CreatedAt, &t.Enabled)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("tenant %q not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("querying tenant: %w", err)
	}
	return t, nil
}

func (s *PostgresStore) List(ctx context.Context) ([]Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, cluster_id, created_at, enabled FROM tenants ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("listing tenants: %w", err)
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.ClusterID, &t.CreatedAt, &t.Enabled); err != nil {
			return nil, fmt.Errorf("scanning tenant: %w", err)
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

func (s *PostgresStore) Create(ctx context.Context, t *Tenant) error {
	if !validID.MatchString(t.ID) {
		return fmt.Errorf("invalid tenant ID %q: must be alphanumeric (with hyphens/underscores), 1-64 chars", t.ID)
	}

	if t.ClusterID == "" {
		t.ClusterID = "default"
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	t.Enabled = true

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenants (id, cluster_id, created_at, enabled) VALUES ($1, $2, $3, $4)`,
		t.ID, t.ClusterID, t.CreatedAt, t.Enabled,
	)
	if err != nil {
		return fmt.Errorf("creating tenant: %w", err)
	}
	return nil
}

func (s *PostgresStore) Update(ctx context.Context, t *Tenant) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET cluster_id = COALESCE(NULLIF($2, ''), cluster_id), enabled = $3 WHERE id = $1`,
		t.ID, t.ClusterID, t.Enabled,
	)
	if err != nil {
		return fmt.Errorf("updating tenant: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("tenant %q not found", t.ID)
	}
	return nil
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting tenant: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("tenant %q not found", id)
	}
	return nil
}
