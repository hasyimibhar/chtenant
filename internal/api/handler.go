package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/hasyimibhar/chtenant/internal/provisioner"
	"github.com/hasyimibhar/chtenant/internal/tenant"
)

type Handler struct {
	store       tenant.Store
	provisioner *provisioner.Provisioner
	mux         *http.ServeMux
}

func NewHandler(store tenant.Store, prov *provisioner.Provisioner) *Handler {
	h := &Handler{store: store, provisioner: prov, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /api/v1/tenants", h.createTenant)
	h.mux.HandleFunc("GET /api/v1/tenants", h.listTenants)
	h.mux.HandleFunc("GET /api/v1/tenants/{id}", h.getTenant)
	h.mux.HandleFunc("PUT /api/v1/tenants/{id}", h.updateTenant)
	h.mux.HandleFunc("DELETE /api/v1/tenants/{id}", h.deleteTenant)
	h.mux.HandleFunc("POST /api/v1/tenants/{id}/reset-password", h.resetPassword)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) createTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID        string `json:"id"`
		ClusterID string `json:"cluster_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	t := &tenant.Tenant{
		ID:        req.ID,
		ClusterID: req.ClusterID,
	}

	if err := h.store.Create(r.Context(), t); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}

	// Provision ClickHouse user with SELECT grants on tenant databases.
	result, err := h.provisioner.Create(t.ID, t.ClusterID)
	if err != nil {
		// Rollback tenant creation.
		h.store.Delete(r.Context(), t.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to provision ClickHouse user: " + err.Error()})
		return
	}

	// Re-fetch to get server-set fields.
	created, err := h.store.Get(r.Context(), t.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         created.ID,
		"cluster_id": created.ClusterID,
		"created_at": created.CreatedAt,
		"enabled":    created.Enabled,
		"password":   result.Password,
	})
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.store.List(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if tenants == nil {
		tenants = []tenant.Tenant{}
	}
	writeJSON(w, http.StatusOK, tenants)
}

func (h *Handler) getTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) updateTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req struct {
		ClusterID string `json:"cluster_id"`
		Enabled   *bool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	t := &tenant.Tenant{
		ID:        id,
		ClusterID: req.ClusterID,
		Enabled:   true,
	}
	if req.Enabled != nil {
		t.Enabled = *req.Enabled
	}

	if err := h.store.Update(r.Context(), t); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	updated, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (h *Handler) deleteTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Fetch tenant to get cluster ID before deletion.
	t, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	if err := h.store.Delete(r.Context(), id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	// Drop ClickHouse user (best-effort).
	h.provisioner.Delete(id, t.ClusterID)

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) resetPassword(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	t, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	password, err := h.provisioner.ResetPassword(id, t.ClusterID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to reset password: " + err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"password": password})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
