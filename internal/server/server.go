// Package server implements the Kora TCP server.
//
// The server speaks RESP2 (the Redis wire protocol) so any Redis client
// library or redis-cli can be used to talk to it. Inline commands
// (plain text, one per line) are also accepted for testing with telnet/nc.
//
// Supported commands (redis-cli compatible):
//
//	PING [msg]              → +PONG / +msg
//	SET  key value          → +OK
//	GET  key                → $value (null bulk string if absent)
//	DEL  key [key …]        → :n  (count of keys that existed)
//	DBSIZE                  → :n  (count of live keys)
//
// Kora-specific commands:
//
//	SCAN [start] [end]      → *N flat array of alternating key/value pairs
//	                          empty start/end means "unbounded"
//	COMPACT                 → +OK  (merge WAL segments)
//	COMPACT-SST             → +OK  (merge SSTables)
package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/giova/kora-engine/internal/proto"
	"github.com/giova/kora-engine/internal/store"
)

// Server accepts TCP connections and dispatches RESP2 commands to the DB.
type Server struct {
	db *store.DB
	ln net.Listener
	wg sync.WaitGroup
}

// New creates a Server that listens on addr. Call Serve to start accepting
// connections.
func New(db *store.DB, addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{db: db, ln: ln}, nil
}

// Addr returns the address the server is listening on. Useful when the port
// was 0 (OS-assigned).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Close is called. It returns the first
// non-temporary accept error (typically net.ErrClosed after Close).
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

// Close stops the accept loop and waits for all in-flight connections to
// finish. It is safe to call from any goroutine.
func (s *Server) Close() error {
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

// handle runs the read–dispatch–write loop for a single client connection.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	bw := bufio.NewWriter(conn)
	r := proto.NewReader(conn)

	for {
		args, err := r.ReadCommand()
		if err != nil {
			return // EOF, reset, or closed — just hang up
		}
		if len(args) == 0 {
			continue // blank inline line
		}
		s.dispatch(bw, args)
		if err := bw.Flush(); err != nil {
			return
		}
	}
}

// dispatch executes one command and writes the response to bw.
func (s *Server) dispatch(w *bufio.Writer, args [][]byte) {
	cmd := strings.ToUpper(string(args[0]))

	switch cmd {
	case "PING":
		if len(args) > 1 {
			proto.WriteBulkString(w, args[1])
		} else {
			proto.WriteSimpleString(w, "PONG")
		}

	case "SET":
		if len(args) != 3 {
			proto.WriteError(w, "SET requires 2 arguments: SET key value")
			return
		}
		if err := s.db.Set(args[1], args[2]); err != nil {
			proto.WriteError(w, err.Error())
			return
		}
		proto.WriteOK(w)

	case "GET":
		if len(args) != 2 {
			proto.WriteError(w, "GET requires 1 argument: GET key")
			return
		}
		v, ok, err := s.db.Get(args[1])
		if err != nil {
			proto.WriteError(w, err.Error())
			return
		}
		if !ok {
			proto.WriteBulkString(w, nil) // null bulk string = not found
		} else {
			proto.WriteBulkString(w, v)
		}

	case "DEL":
		if len(args) < 2 {
			proto.WriteError(w, "DEL requires at least 1 argument: DEL key [key …]")
			return
		}
		var n int64
		for _, key := range args[1:] {
			_, exists, err := s.db.Get(key)
			if err != nil {
				proto.WriteError(w, err.Error())
				return
			}
			if exists {
				n++
			}
			if err := s.db.Delete(key); err != nil {
				proto.WriteError(w, err.Error())
				return
			}
		}
		proto.WriteInteger(w, n)

	case "DBSIZE":
		proto.WriteInteger(w, int64(s.db.Len()))

	case "SCAN":
		// SCAN [start] [end]
		// start/end are optional; empty string means "unbounded".
		var start, end []byte
		if len(args) >= 2 && len(args[1]) > 0 {
			start = args[1]
		}
		if len(args) >= 3 && len(args[2]) > 0 {
			end = args[2]
		}
		iter := s.db.Scan(start, end)
		var pairs [][2][]byte
		for {
			k, v, ok := iter()
			if !ok {
				break
			}
			pairs = append(pairs, [2][]byte{k, v})
		}
		proto.WriteArrayLen(w, len(pairs)*2)
		for _, p := range pairs {
			proto.WriteBulkString(w, p[0])
			proto.WriteBulkString(w, p[1])
		}

	case "COMPACT":
		if err := s.db.Compact(); err != nil {
			proto.WriteError(w, err.Error())
			return
		}
		proto.WriteOK(w)

	case "COMPACT-SST":
		if err := s.db.CompactSSTables(); err != nil {
			proto.WriteError(w, err.Error())
			return
		}
		proto.WriteSimpleString(w, fmt.Sprintf("OK sstables=%d", s.db.SSTableCount()))

	default:
		proto.WriteError(w, fmt.Sprintf("unknown command '%s'", cmd))
	}
}
