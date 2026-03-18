package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/hasyimibhar/chtenant/internal/cluster"
	"github.com/hasyimibhar/chtenant/internal/rewriter"
	"github.com/hasyimibhar/chtenant/internal/tenant"
)

// HTTPProxy proxies ClickHouse HTTP protocol requests with tenant-based
// database rewriting.
type HTTPProxy struct {
	tenants  tenant.Store
	clusters cluster.Registry
	client   *http.Client
}

func NewHTTPProxy(tenants tenant.Store, clusters cluster.Registry) *HTTPProxy {
	return &HTTPProxy{
		tenants:  tenants,
		clusters: clusters,
		client:   &http.Client{},
	}
}

func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		http.Error(w, `{"error": "missing X-Tenant-ID header"}`, http.StatusBadRequest)
		return
	}

	t, err := p.tenants.Get(r.Context(), tenantID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "unknown tenant: %s"}`, tenantID), http.StatusUnauthorized)
		return
	}
	if !t.Enabled {
		http.Error(w, `{"error": "tenant is disabled"}`, http.StatusForbidden)
		return
	}

	c, err := p.clusters.Get(t.ClusterID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "cluster not found: %s"}`, t.ClusterID), http.StatusInternalServerError)
		return
	}

	// Extract query from URL param or body.
	query := r.URL.Query().Get("query")
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error": "failed to read request body"}`, http.StatusBadRequest)
			return
		}
	}

	if query == "" && len(bodyBytes) > 0 {
		query = string(bodyBytes)
		bodyBytes = nil
	}

	if query == "" {
		http.Error(w, `{"error": "no query provided"}`, http.StatusBadRequest)
		return
	}

	// Rewrite the query (also validates SELECT-only).
	rewrittenQuery, err := rewriter.Rewrite(query, tenantID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": %q}`, err.Error()), http.StatusForbidden)
		return
	}

	// Build upstream URL.
	upstreamURL, err := url.Parse(c.HTTPEndpoint)
	if err != nil {
		http.Error(w, `{"error": "invalid cluster endpoint"}`, http.StatusInternalServerError)
		return
	}

	// Copy and rewrite query params.
	params := r.URL.Query()
	params.Del("query") // We'll send query in body.

	// Rewrite database param if present.
	if db := params.Get("database"); db != "" {
		params.Set("database", rewriter.RewriteDatabase(db, tenantID))
	}

	upstreamURL.RawQuery = params.Encode()

	// Create upstream request.
	var body io.Reader
	body = strings.NewReader(rewrittenQuery)

	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL.String(), body)
	if err != nil {
		http.Error(w, `{"error": "failed to create upstream request"}`, http.StatusInternalServerError)
		return
	}

	// Set upstream credentials.
	if c.User != "" {
		upReq.Header.Set("X-ClickHouse-User", c.User)
		if c.Password != "" {
			upReq.Header.Set("X-ClickHouse-Key", c.Password)
		}
	}

	// Copy relevant headers from original request.
	for _, h := range []string{"Accept", "Accept-Encoding", "X-ClickHouse-Format"} {
		if v := r.Header.Get(h); v != "" {
			upReq.Header.Set(h, v)
		}
	}

	log.Printf("[http] tenant=%s query=%s", tenantID, truncate(rewrittenQuery, 200))

	// Forward request.
	resp, err := p.client.Do(upReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "upstream error: %s"}`, err.Error()), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers.
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *HTTPProxy) Shutdown(ctx context.Context) error {
	p.client.CloseIdleConnections()
	return nil
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
