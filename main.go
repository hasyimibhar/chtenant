package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/hasyimibhar/chtenant/internal/api"
	"github.com/hasyimibhar/chtenant/internal/cluster"
	"github.com/hasyimibhar/chtenant/internal/config"
	"github.com/hasyimibhar/chtenant/internal/proxy"
	"github.com/hasyimibhar/chtenant/internal/tenant"
)

func main() {
	cfgPath := "config.yaml"
	if v := os.Getenv("CHTENANT_CONFIG"); v != "" {
		cfgPath = v
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Connect to PostgreSQL.
	db, err := sql.Open("postgres", cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping postgres: %v", err)
	}
	log.Println("connected to postgres")

	// Initialize tenant store.
	tenantStore, err := tenant.NewPostgresStore(db)
	if err != nil {
		log.Fatalf("failed to initialize tenant store: %v", err)
	}

	// Initialize cluster registry.
	clusters := make([]cluster.Cluster, len(cfg.Clusters))
	for i, c := range cfg.Clusters {
		clusters[i] = cluster.Cluster{
			ID:             c.ID,
			HTTPEndpoint:   c.HTTPEndpoint,
			NativeEndpoint: c.NativeEndpoint,
			User:           c.User,
			Password:       c.Password,
		}
	}
	clusterRegistry := cluster.NewStaticRegistry(clusters)

	// Start HTTP proxy.
	httpProxy := proxy.NewHTTPProxy(tenantStore, clusterRegistry)
	httpServer := &http.Server{
		Addr:    cfg.Proxy.HTTPAddr,
		Handler: httpProxy,
	}
	go func() {
		log.Printf("[http-proxy] listening on %s", cfg.Proxy.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[http-proxy] %v", err)
		}
	}()

	// Start native proxy.
	nativeProxy := proxy.NewNativeProxy(tenantStore, clusterRegistry)
	go func() {
		if err := nativeProxy.ListenAndServe(cfg.Proxy.NativeAddr); err != nil {
			log.Fatalf("[native-proxy] %v", err)
		}
	}()

	// Start admin API.
	adminHandler := api.NewHandler(tenantStore)
	adminServer := &http.Server{
		Addr:    cfg.Admin.Addr,
		Handler: adminHandler,
	}
	go func() {
		log.Printf("[admin] listening on %s", cfg.Admin.Addr)
		if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[admin] %v", err)
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal %v, shutting down...", sig)

	ctx := context.Background()
	httpServer.Shutdown(ctx)
	nativeProxy.Shutdown(ctx)
	adminServer.Shutdown(ctx)
	fmt.Println("bye")
}
