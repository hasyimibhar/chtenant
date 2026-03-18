package provisioner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/hasyimibhar/chtenant/internal/cluster"
)

// Provisioner manages per-tenant ClickHouse users and grants.
type Provisioner struct {
	clusters cluster.Registry
}

func New(clusters cluster.Registry) *Provisioner {
	return &Provisioner{clusters: clusters}
}

// CreateResult holds the result of provisioning a tenant.
type CreateResult struct {
	Password string
}

// Create creates a ClickHouse user for the tenant with SELECT grants
// on all tenant-prefixed databases.
func (p *Provisioner) Create(tenantID, clusterID string) (*CreateResult, error) {
	c, err := p.clusters.Get(clusterID)
	if err != nil {
		return nil, fmt.Errorf("cluster %q not found: %w", clusterID, err)
	}

	password, err := randomPassword(32)
	if err != nil {
		return nil, fmt.Errorf("generating password: %w", err)
	}

	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'", tenantID, password),
		fmt.Sprintf("GRANT SELECT ON %s__*.* TO %s", tenantID, tenantID),
	}

	for _, stmt := range stmts {
		if err := chExec(c, stmt); err != nil {
			return nil, fmt.Errorf("provisioning user: %w", err)
		}
	}

	return &CreateResult{Password: password}, nil
}

// Delete drops the ClickHouse user for the tenant.
func (p *Provisioner) Delete(tenantID, clusterID string) error {
	c, err := p.clusters.Get(clusterID)
	if err != nil {
		return fmt.Errorf("cluster %q not found: %w", clusterID, err)
	}

	if err := chExec(c, fmt.Sprintf("DROP USER IF EXISTS %s", tenantID)); err != nil {
		return fmt.Errorf("dropping user: %w", err)
	}

	return nil
}

// ResetPassword generates a new password for the tenant's ClickHouse user.
func (p *Provisioner) ResetPassword(tenantID, clusterID string) (string, error) {
	c, err := p.clusters.Get(clusterID)
	if err != nil {
		return "", fmt.Errorf("cluster %q not found: %w", clusterID, err)
	}

	password, err := randomPassword(32)
	if err != nil {
		return "", fmt.Errorf("generating password: %w", err)
	}

	stmt := fmt.Sprintf("ALTER USER %s IDENTIFIED BY '%s'", tenantID, password)
	if err := chExec(c, stmt); err != nil {
		return "", fmt.Errorf("resetting password: %w", err)
	}

	return password, nil
}

func chExec(c *cluster.Cluster, query string) error {
	u, err := url.Parse(c.HTTPEndpoint)
	if err != nil {
		return fmt.Errorf("parsing endpoint: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(query))
	if err != nil {
		return err
	}
	if c.User != "" {
		req.Header.Set("X-ClickHouse-User", c.User)
		if c.Password != "" {
			req.Header.Set("X-ClickHouse-Key", c.Password)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ClickHouse error (status %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func randomPassword(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
