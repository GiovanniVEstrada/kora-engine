// Package client provides a Go client for the Kora TCP server.
//
// The client speaks RESP2 over a single TCP connection. All methods are safe
// to call concurrently — a mutex serialises commands over the shared socket.
//
// Usage:
//
//	c, err := client.Dial("localhost:6380")
//	if err != nil { ... }
//	defer c.Close()
//
//	if err := c.Set([]byte("key"), []byte("value")); err != nil { ... }
//	val, ok, err := c.Get([]byte("key"))
package client

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/giova/kora-engine/internal/proto"
)

// Client is a connection to a Kora server.
type Client struct {
	conn net.Conn
	bw   *bufio.Writer
	r    *proto.Reader
	mu   sync.Mutex
}

// Dial connects to a Kora server at addr (e.g. "localhost:6380").
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		bw:   bufio.NewWriter(conn),
		r:    proto.NewReader(conn),
	}, nil
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error { return c.conn.Close() }

// Ping sends a PING and expects PONG.
func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("PING")
	if err := c.bw.Flush(); err != nil {
		return err
	}
	_, err := c.r.ReadSimpleString()
	return err
}

// Set stores value under key.
func (c *Client) Set(key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("SET", key, value)
	if err := c.bw.Flush(); err != nil {
		return err
	}
	_, err := c.r.ReadSimpleString()
	return err
}

// Get returns the value for key. ok is false if the key does not exist.
func (c *Client) Get(key []byte) (value []byte, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("GET", key)
	if err := c.bw.Flush(); err != nil {
		return nil, false, err
	}
	v, err := c.r.ReadBulkString()
	if err != nil {
		return nil, false, err
	}
	if v == nil {
		return nil, false, nil
	}
	return v, true, nil
}

// Delete deletes one or more keys. Returns the number of keys that existed.
func (c *Client) Delete(keys ...[]byte) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("DEL", keys...)
	if err := c.bw.Flush(); err != nil {
		return 0, err
	}
	return c.r.ReadInteger()
}

// DBSize returns the count of live keys.
func (c *Client) DBSize() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("DBSIZE")
	if err := c.bw.Flush(); err != nil {
		return 0, err
	}
	return c.r.ReadInteger()
}

// Scan returns all live key-value pairs in the closed range [start, end].
// Pass nil for start or end to scan without that bound.
func (c *Client) Scan(start, end []byte) ([][2][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Empty bytes (not nil) signal "unbounded" to the server.
	s := start
	e := end
	if s == nil {
		s = []byte{}
	}
	if e == nil {
		e = []byte{}
	}
	c.writeCmd("SCAN", s, e)
	if err := c.bw.Flush(); err != nil {
		return nil, err
	}
	return c.r.ReadPairs()
}

// Compact merges WAL segments.
func (c *Client) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("COMPACT")
	if err := c.bw.Flush(); err != nil {
		return err
	}
	_, err := c.r.ReadSimpleString()
	return err
}

// CompactSST merges SSTables.
func (c *Client) CompactSST() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCmd("COMPACT-SST")
	if err := c.bw.Flush(); err != nil {
		return err
	}
	_, err := c.r.ReadSimpleString()
	return err
}

// writeCmd writes a RESP2 array command to the buffered writer.
// It does NOT flush — the caller flushes after.
func (c *Client) writeCmd(cmd string, args ...[]byte) {
	proto.WriteArrayLen(c.bw, 1+len(args))
	proto.WriteBulkString(c.bw, []byte(cmd))
	for _, a := range args {
		proto.WriteBulkString(c.bw, a)
	}
}

// Errorf wraps a server error string for callers that need context.
func Errorf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
