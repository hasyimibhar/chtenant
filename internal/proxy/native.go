package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/hasyimibhar/chtenant/internal/chproto"
	"github.com/hasyimibhar/chtenant/internal/cluster"
	"github.com/hasyimibhar/chtenant/internal/rewriter"
	"github.com/hasyimibhar/chtenant/internal/tenant"
)

// NativeProxy proxies the ClickHouse native TCP protocol with tenant-based
// database rewriting. Tenant ID is extracted from the username field in the
// client Hello packet.
//
// The proxy caps the protocol version it advertises to upstream at
// ProxyProtocolVersion (54460). This keeps the Hello handshake simple and
// avoids needing to parse all version-dependent fields (password rules,
// chunked packets, nonce, server settings, etc.).
type NativeProxy struct {
	tenants  tenant.Store
	clusters cluster.Registry
	listener net.Listener
}

func NewNativeProxy(tenants tenant.Store, clusters cluster.Registry) *NativeProxy {
	return &NativeProxy{
		tenants:  tenants,
		clusters: clusters,
	}
}

func (p *NativeProxy) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	p.listener = ln
	log.Printf("[native] listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-context.Background().Done():
				return nil
			default:
			}
			log.Printf("[native] accept error: %v", err)
			continue
		}
		go p.handleConnection(conn)
	}
}

func (p *NativeProxy) Shutdown(ctx context.Context) error {
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

func (p *NativeProxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	clientReader := chproto.NewReader(clientConn)

	// Step 1: Read packet type (should be ClientHello = 0).
	pktType, err := clientReader.UVarInt()
	if err != nil {
		log.Printf("[native] failed to read packet type: %v", err)
		return
	}
	if pktType != chproto.ClientHello {
		log.Printf("[native] expected ClientHello (0), got %d", pktType)
		return
	}

	// Step 2: Read client Hello.
	hello, err := chproto.ReadClientHello(clientReader)
	if err != nil {
		log.Printf("[native] failed to read client hello: %v", err)
		return
	}

	// Step 3: Extract tenant ID from username.
	tenantID := hello.User
	t, err := p.tenants.Get(context.Background(), tenantID)
	if err != nil {
		log.Printf("[native] unknown tenant %q: %v", tenantID, err)
		p.sendError(clientConn, 516, "AUTH_FAILED", fmt.Sprintf("unknown tenant: %s", tenantID))
		return
	}
	if !t.Enabled {
		p.sendError(clientConn, 516, "AUTH_FAILED", fmt.Sprintf("tenant %s is disabled", tenantID))
		return
	}

	// Step 4: Get cluster.
	c, err := p.clusters.Get(t.ClusterID)
	if err != nil {
		log.Printf("[native] cluster %q not found: %v", t.ClusterID, err)
		p.sendError(clientConn, 999, "INTERNAL", "cluster not found")
		return
	}

	// Step 5: Connect to upstream.
	upstreamConn, err := net.Dial("tcp", c.NativeEndpoint)
	if err != nil {
		log.Printf("[native] failed to connect to upstream %s: %v", c.NativeEndpoint, err)
		p.sendError(clientConn, 999, "INTERNAL", "failed to connect to upstream")
		return
	}
	defer upstreamConn.Close()

	// Step 6: Send rewritten Hello to upstream.
	// Cap the protocol version so the server sends a simple Hello response
	// that we know how to parse (no password rules, chunked packets, nonce, etc).
	rewrittenHello := *hello
	rewrittenHello.Database = rewriter.RewriteDatabase(hello.Database, tenantID)
	rewrittenHello.User = c.User
	rewrittenHello.Password = c.Password
	rewrittenHello.ProtocolVersion = chproto.ProxyProtocolVersion

	w := chproto.NewWriter()
	chproto.WriteClientHello(w, &rewrittenHello)
	if _, err := w.WriteTo(upstreamConn); err != nil {
		log.Printf("[native] failed to send hello to upstream: %v", err)
		return
	}

	// Step 7: Read server Hello response.
	upstreamReader := chproto.NewReader(upstreamConn)
	serverPktType, err := upstreamReader.UVarInt()
	if err != nil {
		log.Printf("[native] failed to read server response type: %v", err)
		return
	}

	if serverPktType == chproto.ServerException {
		p.relayException(upstreamReader, clientConn)
		return
	}

	if serverPktType != chproto.ServerHello {
		log.Printf("[native] expected ServerHello (0), got %d", serverPktType)
		return
	}

	serverHello, err := chproto.ReadServerHello(upstreamReader)
	if err != nil {
		log.Printf("[native] failed to read server hello: %v", err)
		return
	}

	// The effective protocol version for all subsequent packets.
	protocolVersion := uint64(chproto.ProxyProtocolVersion)

	// Forward server Hello to client, with protocol version capped.
	// This ensures the client also speaks our capped protocol version
	// (won't send chunked-protocol addendum fields, etc.).
	serverHello.ProtocolVersion = protocolVersion
	w.Reset()
	chproto.WriteServerHello(w, serverHello)
	if _, err := w.WriteTo(clientConn); err != nil {
		log.Printf("[native] failed to forward server hello: %v", err)
		return
	}

	// Step 7b: Handle client addendum.
	// At protocol 54460 >= 54458, the client sends a quota_key string.
	// At protocol 54460 < 54470, no chunked protocol fields.
	if protocolVersion >= chproto.DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
		quotaKey, err := clientReader.String() // quota_key
		if err != nil {
			log.Printf("[native] failed to read client addendum: %v", err)
			return
		}
		w.Reset()
		w.String(quotaKey)
		if _, err := w.WriteTo(upstreamConn); err != nil {
			log.Printf("[native] failed to forward client addendum: %v", err)
			return
		}
	}

	log.Printf("[native] tenant=%s connected (protocol=%d)", tenantID, protocolVersion)

	// Step 8: Bidirectional relay.
	var wg sync.WaitGroup
	wg.Add(2)

	// Client → Upstream (parse and rewrite queries).
	go func() {
		defer wg.Done()
		if err := p.relayClientToUpstream(clientReader, upstreamConn, tenantID, protocolVersion); err != nil {
			log.Printf("[native] tenant=%s client→upstream: %v", tenantID, err)
		}
		if tc, ok := upstreamConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	// Upstream → Client (raw relay).
	go func() {
		defer wg.Done()
		if _, err := io.Copy(clientConn, upstreamReader.Underlying()); err != nil {
			log.Printf("[native] tenant=%s upstream→client: %v", tenantID, err)
		}
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
	log.Printf("[native] tenant=%s disconnected", tenantID)
}

func (p *NativeProxy) relayClientToUpstream(cr *chproto.Reader, upstream io.Writer, tenantID string, revision uint64) error {
	var compressed bool // tracks compression from last query packet
	for {
		pktType, err := cr.UVarInt()
		if err != nil {
			return err
		}

		switch pktType {
		case chproto.ClientQuery:
			query, err := chproto.ReadQueryPacket(cr, revision)
			if err != nil {
				return fmt.Errorf("reading query packet: %w", err)
			}

			compressed = query.Compression != 0

			rewrittenSQL, err := rewriter.Rewrite(query.Query, tenantID)
			if err != nil {
				return fmt.Errorf("rewriting query: %w", err)
			}
			query.Query = rewrittenSQL

			log.Printf("[native] tenant=%s query=%s", tenantID, truncate(rewrittenSQL, 200))

			w := chproto.NewWriter()
			chproto.WriteQueryPacket(w, query, revision)
			if _, err := w.WriteTo(upstream); err != nil {
				return fmt.Errorf("forwarding query: %w", err)
			}

		case chproto.ClientData:
			data, err := chproto.ReadDataBlock(cr, revision, compressed)
			if err != nil {
				return fmt.Errorf("reading data block: %w", err)
			}
			if _, err := upstream.Write(data); err != nil {
				return fmt.Errorf("forwarding data block: %w", err)
			}

		case chproto.ClientPing:
			w := chproto.NewWriter()
			w.UVarInt(chproto.ClientPing)
			if _, err := w.WriteTo(upstream); err != nil {
				return fmt.Errorf("forwarding ping: %w", err)
			}

		case chproto.ClientCancel:
			w := chproto.NewWriter()
			w.UVarInt(chproto.ClientCancel)
			if _, err := w.WriteTo(upstream); err != nil {
				return fmt.Errorf("forwarding cancel: %w", err)
			}

		default:
			return fmt.Errorf("unknown client packet type: %d", pktType)
		}
	}
}

func (p *NativeProxy) sendError(conn net.Conn, code int32, name, message string) {
	w := chproto.NewWriter()
	chproto.WriteException(w, code, name, message)
	w.WriteTo(conn)
}

func (p *NativeProxy) relayException(r *chproto.Reader, clientConn net.Conn) {
	code, _ := r.Int32()
	name, _ := r.String()
	msg, _ := r.String()
	stackTrace, _ := r.String()
	hasNested, _ := r.Bool()

	w := chproto.NewWriter()
	w.UVarInt(chproto.ServerException)
	w.Int32(code)
	w.String(name)
	w.String(msg)
	w.String(stackTrace)
	w.Bool(hasNested)
	w.WriteTo(clientConn)
}
