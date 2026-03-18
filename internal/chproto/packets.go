package chproto

import (
	"encoding/binary"
	"fmt"
)

// ClientHelloPacket is the first packet sent by a ClickHouse client.
type ClientHelloPacket struct {
	ClientName      string
	VersionMajor    uint64
	VersionMinor    uint64
	ProtocolVersion uint64
	Database        string
	User            string
	Password        string
}

func ReadClientHello(r *Reader) (*ClientHelloPacket, error) {
	p := &ClientHelloPacket{}
	var err error

	if p.ClientName, err = r.String(); err != nil {
		return nil, fmt.Errorf("client hello: name: %w", err)
	}
	if p.VersionMajor, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("client hello: major: %w", err)
	}
	if p.VersionMinor, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("client hello: minor: %w", err)
	}
	if p.ProtocolVersion, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("client hello: protocol: %w", err)
	}
	if p.Database, err = r.String(); err != nil {
		return nil, fmt.Errorf("client hello: database: %w", err)
	}
	if p.User, err = r.String(); err != nil {
		return nil, fmt.Errorf("client hello: user: %w", err)
	}
	if p.Password, err = r.String(); err != nil {
		return nil, fmt.Errorf("client hello: password: %w", err)
	}
	return p, nil
}

func WriteClientHello(w *Writer, p *ClientHelloPacket) {
	w.UVarInt(ClientHello)
	w.String(p.ClientName)
	w.UVarInt(p.VersionMajor)
	w.UVarInt(p.VersionMinor)
	w.UVarInt(p.ProtocolVersion)
	w.String(p.Database)
	w.String(p.User)
	w.String(p.Password)
}

// ServerHelloPacket is the server's response to the client hello.
// The proxy caps the protocol version to ProxyProtocolVersion (54460), so the
// server Hello contains only these fields:
//   - name, major, minor, revision (always)
//   - timezone (>= 54058)
//   - display_name (>= 54372)
//   - version_patch (>= 54401)
type ServerHelloPacket struct {
	ServerName      string
	VersionMajor    uint64
	VersionMinor    uint64
	ProtocolVersion uint64
	Timezone        string
	DisplayName     string
	VersionPatch    uint64
}

func ReadServerHello(r *Reader) (*ServerHelloPacket, error) {
	p := &ServerHelloPacket{}
	var err error

	if p.ServerName, err = r.String(); err != nil {
		return nil, fmt.Errorf("server hello: name: %w", err)
	}
	if p.VersionMajor, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("server hello: major: %w", err)
	}
	if p.VersionMinor, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("server hello: minor: %w", err)
	}
	if p.ProtocolVersion, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("server hello: protocol: %w", err)
	}
	if p.Timezone, err = r.String(); err != nil {
		return nil, fmt.Errorf("server hello: timezone: %w", err)
	}
	if p.DisplayName, err = r.String(); err != nil {
		return nil, fmt.Errorf("server hello: display name: %w", err)
	}
	if p.VersionPatch, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("server hello: version patch: %w", err)
	}

	return p, nil
}

func WriteServerHello(w *Writer, p *ServerHelloPacket) {
	w.UVarInt(ServerHello)
	w.String(p.ServerName)
	w.UVarInt(p.VersionMajor)
	w.UVarInt(p.VersionMinor)
	w.UVarInt(p.ProtocolVersion)
	w.String(p.Timezone)
	w.String(p.DisplayName)
	w.UVarInt(p.VersionPatch)
}

// ReadServerHelloMinimal reads only up to the protocol version field, for cases
// where we don't control the protocol version and just need to extract it.
func ReadServerHelloMinimal(r *Reader) (uint64, error) {
	if _, err := r.String(); err != nil { // name
		return 0, err
	}
	if _, err := r.UVarInt(); err != nil { // major
		return 0, err
	}
	if _, err := r.UVarInt(); err != nil { // minor
		return 0, err
	}
	return r.UVarInt() // revision
}

// QueryPacket represents a client query packet.
type QueryPacket struct {
	QueryID     string
	ClientInfo  ClientInfo
	Settings    []Setting
	State       uint64
	Compression uint64
	Query       string
	Parameters  []Setting
}

type ClientInfo struct {
	QueryKind                  byte
	InitialUser                string
	InitialQueryID             string
	InitialAddress             string
	InitialQueryStartTime      int64
	Interface                  byte
	OSUser                     string
	ClientHostname             string
	ClientName                 string
	VersionMajor               uint64
	VersionMinor               uint64
	TCPProtocolVersion         uint64
	QuotaKey                   string
	DistributedDepth           uint64
	VersionPatch               uint64
	OpenTelemetryEnabled       bool
	OpenTelemetryTraceID       [16]byte
	OpenTelemetrySpanID        [8]byte
	OpenTelemetryTraceState    string
	OpenTelemetryTraceFlags    byte
	CollaborateWithInitiator   uint64
	CountParticipatingReplicas uint64
	NumberOfCurrentReplica     uint64
}

type Setting struct {
	Name      string
	Value     string
	Important bool
	Custom    bool
}

func ReadQueryPacket(r *Reader, revision uint64) (*QueryPacket, error) {
	p := &QueryPacket{}
	var err error

	if p.QueryID, err = r.String(); err != nil {
		return nil, fmt.Errorf("query: id: %w", err)
	}
	if err := readClientInfo(r, &p.ClientInfo, revision); err != nil {
		return nil, fmt.Errorf("query: client info: %w", err)
	}
	if p.Settings, err = readSettings(r, revision); err != nil {
		return nil, fmt.Errorf("query: settings: %w", err)
	}

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		if _, err = r.String(); err != nil {
			return nil, fmt.Errorf("query: interserver secret: %w", err)
		}
	}

	if p.State, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("query: state: %w", err)
	}
	if p.Compression, err = r.UVarInt(); err != nil {
		return nil, fmt.Errorf("query: compression: %w", err)
	}
	if p.Query, err = r.String(); err != nil {
		return nil, fmt.Errorf("query: body: %w", err)
	}

	// At protocol >= 54459, the client sends query parameters as a settings block.
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
		if p.Parameters, err = readSettings(r, revision); err != nil {
			return nil, fmt.Errorf("query: parameters: %w", err)
		}
	}

	return p, nil
}

func WriteQueryPacket(w *Writer, p *QueryPacket, revision uint64) {
	w.UVarInt(ClientQuery)
	w.String(p.QueryID)
	writeClientInfo(w, &p.ClientInfo, revision)
	writeSettings(w, p.Settings, revision)

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		w.String("")
	}

	w.UVarInt(p.State)
	w.UVarInt(p.Compression)
	w.String(p.Query)

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
		writeSettings(w, p.Parameters, revision)
	}
}

func readClientInfo(r *Reader, ci *ClientInfo, revision uint64) error {
	var err error

	if ci.QueryKind, err = r.Byte(); err != nil {
		return fmt.Errorf("query_kind: %w", err)
	}
	if ci.QueryKind == 0 {
		return nil
	}

	if ci.InitialUser, err = r.String(); err != nil {
		return fmt.Errorf("initial_user: %w", err)
	}
	if ci.InitialQueryID, err = r.String(); err != nil {
		return fmt.Errorf("initial_query_id: %w", err)
	}
	if ci.InitialAddress, err = r.String(); err != nil {
		return fmt.Errorf("initial_address: %w", err)
	}

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		if ci.InitialQueryStartTime, err = r.Int64(); err != nil {
			return fmt.Errorf("initial_query_start_time: %w", err)
		}
	}

	if ci.Interface, err = r.Byte(); err != nil {
		return fmt.Errorf("interface: %w", err)
	}

	if ci.Interface == 1 {
		if ci.OSUser, err = r.String(); err != nil {
			return fmt.Errorf("os_user: %w", err)
		}
		if ci.ClientHostname, err = r.String(); err != nil {
			return fmt.Errorf("client_hostname: %w", err)
		}
		if ci.ClientName, err = r.String(); err != nil {
			return fmt.Errorf("client_name: %w", err)
		}
		if ci.VersionMajor, err = r.UVarInt(); err != nil {
			return fmt.Errorf("version_major: %w", err)
		}
		if ci.VersionMinor, err = r.UVarInt(); err != nil {
			return fmt.Errorf("version_minor: %w", err)
		}
		if ci.TCPProtocolVersion, err = r.UVarInt(); err != nil {
			return fmt.Errorf("tcp_protocol_version: %w", err)
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY {
		if ci.QuotaKey, err = r.String(); err != nil {
			return fmt.Errorf("quota_key: %w", err)
		}
	}

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		if ci.DistributedDepth, err = r.UVarInt(); err != nil {
			return fmt.Errorf("distributed_depth: %w", err)
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if ci.VersionPatch, err = r.UVarInt(); err != nil {
			return fmt.Errorf("version_patch: %w", err)
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		hasTrace, err := r.Byte()
		if err != nil {
			return fmt.Errorf("opentelemetry flag: %w", err)
		}
		ci.OpenTelemetryEnabled = hasTrace != 0
		if ci.OpenTelemetryEnabled {
			for i := 0; i < 16; i++ {
				if ci.OpenTelemetryTraceID[i], err = r.Byte(); err != nil {
					return fmt.Errorf("trace_id[%d]: %w", i, err)
				}
			}
			for i := 0; i < 8; i++ {
				if ci.OpenTelemetrySpanID[i], err = r.Byte(); err != nil {
					return fmt.Errorf("span_id[%d]: %w", i, err)
				}
			}
			if ci.OpenTelemetryTraceState, err = r.String(); err != nil {
				return fmt.Errorf("trace_state: %w", err)
			}
			if ci.OpenTelemetryTraceFlags, err = r.Byte(); err != nil {
				return fmt.Errorf("trace_flags: %w", err)
			}
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		if ci.CollaborateWithInitiator, err = r.UVarInt(); err != nil {
			return fmt.Errorf("collaborate_with_initiator: %w", err)
		}
		if ci.CountParticipatingReplicas, err = r.UVarInt(); err != nil {
			return fmt.Errorf("count_participating_replicas: %w", err)
		}
		if ci.NumberOfCurrentReplica, err = r.UVarInt(); err != nil {
			return fmt.Errorf("number_of_current_replica: %w", err)
		}
	}

	return nil
}

func writeClientInfo(w *Writer, ci *ClientInfo, revision uint64) {
	w.Byte(ci.QueryKind)
	if ci.QueryKind == 0 {
		return
	}

	w.String(ci.InitialUser)
	w.String(ci.InitialQueryID)
	w.String(ci.InitialAddress)

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		w.Int64(ci.InitialQueryStartTime)
	}

	w.Byte(ci.Interface)

	if ci.Interface == 1 {
		w.String(ci.OSUser)
		w.String(ci.ClientHostname)
		w.String(ci.ClientName)
		w.UVarInt(ci.VersionMajor)
		w.UVarInt(ci.VersionMinor)
		w.UVarInt(ci.TCPProtocolVersion)
	}

	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY {
		w.String(ci.QuotaKey)
	}

	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		w.UVarInt(ci.DistributedDepth)
	}

	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		w.UVarInt(ci.VersionPatch)
	}

	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		if ci.OpenTelemetryEnabled {
			w.Byte(1)
			for _, b := range ci.OpenTelemetryTraceID {
				w.Byte(b)
			}
			for _, b := range ci.OpenTelemetrySpanID {
				w.Byte(b)
			}
			w.String(ci.OpenTelemetryTraceState)
			w.Byte(ci.OpenTelemetryTraceFlags)
		} else {
			w.Byte(0)
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		w.UVarInt(ci.CollaborateWithInitiator)
		w.UVarInt(ci.CountParticipatingReplicas)
		w.UVarInt(ci.NumberOfCurrentReplica)
	}
}

func readSettings(r *Reader, revision uint64) ([]Setting, error) {
	var settings []Setting

	for {
		name, err := r.String()
		if err != nil {
			return nil, fmt.Errorf("setting name: %w", err)
		}
		if name == "" {
			break
		}

		s := Setting{Name: name}

		if revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
			imp, err := r.UVarInt()
			if err != nil {
				return nil, fmt.Errorf("setting important: %w", err)
			}
			s.Important = imp != 0

			if revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
				custom, err := r.UVarInt()
				if err != nil {
					return nil, fmt.Errorf("setting custom: %w", err)
				}
				s.Custom = custom != 0
			}

			if s.Value, err = r.String(); err != nil {
				return nil, fmt.Errorf("setting value: %w", err)
			}
		}

		settings = append(settings, s)
	}

	return settings, nil
}

func writeSettings(w *Writer, settings []Setting, revision uint64) {
	for _, s := range settings {
		w.String(s.Name)

		if revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
			if s.Important {
				w.UVarInt(1)
			} else {
				w.UVarInt(0)
			}
			if revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION {
				if s.Custom {
					w.UVarInt(1)
				} else {
					w.UVarInt(0)
				}
			}
			w.String(s.Value)
		}
	}
	w.String("") // terminator
}

// ReadDataBlock reads a data block from the client and returns the raw bytes
// to forward to upstream (including the ClientData packet type prefix).
// When compression is enabled, the block content after the table name is wrapped
// in a ClickHouse compression envelope (checksum + compressed data).
func ReadDataBlock(r *Reader, revision uint64, compressed bool) ([]byte, error) {
	w := NewWriter()
	w.UVarInt(ClientData)

	tableName, err := r.String()
	if err != nil {
		return nil, fmt.Errorf("data: table name: %w", err)
	}
	w.String(tableName)

	if compressed {
		// Compressed block format:
		//   16 bytes: CityHash128 checksum
		//    1 byte:  compression method
		//    4 bytes: compressed size (LE uint32, includes method+sizes = 9 bytes)
		//    4 bytes: uncompressed size (LE uint32)
		//    N bytes: compressed data (compressed_size - 9)
		header, err := r.ReadN(16 + 1 + 4 + 4)
		if err != nil {
			return nil, fmt.Errorf("data: compressed header: %w", err)
		}
		w.Raw(header)

		compressedSize := binary.LittleEndian.Uint32(header[17:21])
		if compressedSize < 9 {
			return nil, fmt.Errorf("data: invalid compressed size: %d", compressedSize)
		}
		dataLen := int(compressedSize) - 9
		if dataLen > 0 {
			data, err := r.ReadN(dataLen)
			if err != nil {
				return nil, fmt.Errorf("data: compressed body: %w", err)
			}
			w.Raw(data)
		}
	} else {
		// Uncompressed: block info + num_cols + num_rows
		for {
			fieldNum, err := r.UVarInt()
			if err != nil {
				return nil, fmt.Errorf("data: block info field num: %w", err)
			}
			w.UVarInt(fieldNum)

			if fieldNum == 0 {
				break
			}

			switch fieldNum {
			case 1: // is_overflows
				v, err := r.Byte()
				if err != nil {
					return nil, fmt.Errorf("data: is_overflows: %w", err)
				}
				w.Byte(v)
			case 2: // bucket_num
				v, err := r.Int32()
				if err != nil {
					return nil, fmt.Errorf("data: bucket_num: %w", err)
				}
				w.Int32(v)
			default:
				return nil, fmt.Errorf("data: unknown block info field: %d", fieldNum)
			}
		}

		numCols, err := r.UVarInt()
		if err != nil {
			return nil, fmt.Errorf("data: num_columns: %w", err)
		}
		w.UVarInt(numCols)

		numRows, err := r.UVarInt()
		if err != nil {
			return nil, fmt.Errorf("data: num_rows: %w", err)
		}
		w.UVarInt(numRows)

		if numCols > 0 || numRows > 0 {
			return nil, fmt.Errorf("unexpected non-empty data block (%d cols, %d rows); only SELECT queries are supported", numCols, numRows)
		}
	}

	return w.Bytes(), nil
}

// WriteException writes a server exception packet to a writer.
func WriteException(w *Writer, code int32, name, message string) {
	w.UVarInt(ServerException)
	w.Int32(code)
	w.String(name)
	w.String(message)
	w.String("")  // stack trace
	w.Bool(false) // has nested
}
