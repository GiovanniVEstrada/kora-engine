// Package proto implements the RESP2 (Redis Serialization Protocol v2) wire
// format used by the Kora TCP server.
//
// Clients can speak either RESP2 array format (used by redis-cli and all Redis
// client libraries) or inline format (plain text, useful with telnet/nc).
//
// Supported response types:
//
//	+<msg>\r\n      simple string (OK, PONG, …)
//	-ERR <msg>\r\n  error
//	:<n>\r\n        integer
//	$<n>\r\n<b>\r\n bulk string  ($-1\r\n = null / not found)
//	*<n>\r\n …      array of bulk strings
package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	typSimple  = byte('+')
	typError   = byte('-')
	typInteger = byte(':')
	typBulk    = byte('$')
	typArray   = byte('*')
)

// ---- Writers ----------------------------------------------------------------

// WriteOK writes +OK\r\n.
func WriteOK(w io.Writer) error {
	_, err := io.WriteString(w, "+OK\r\n")
	return err
}

// WriteSimpleString writes +s\r\n.
func WriteSimpleString(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

// WriteError writes -ERR msg\r\n.
func WriteError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "-ERR %s\r\n", msg)
	return err
}

// WriteInteger writes :n\r\n.
func WriteInteger(w io.Writer, n int64) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", n)
	return err
}

// WriteBulkString writes $n\r\nb\r\n. A nil slice writes a null bulk string
// ($-1\r\n), which clients interpret as "key not found".
func WriteBulkString(w io.Writer, b []byte) error {
	if b == nil {
		_, err := io.WriteString(w, "$-1\r\n")
		return err
	}
	if _, err := fmt.Fprintf(w, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

// WriteArrayLen writes *n\r\n. Follow with n bulk strings.
func WriteArrayLen(w io.Writer, n int) error {
	_, err := fmt.Fprintf(w, "*%d\r\n", n)
	return err
}

// ---- Reader -----------------------------------------------------------------

// Reader parses RESP2 messages from a stream.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps r for RESP2 reading.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadCommand reads one command from the stream.
//
// RESP2 array format (*N\r\n$len\r\nbytes\r\n…) is used by redis-cli and all
// Redis client libraries. Inline format (space-separated text on one line) is
// supported for manual testing with telnet or nc.
//
// Returns nil args (not an error) on a blank inline line.
func (r *Reader) ReadCommand() ([][]byte, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if err := r.r.UnreadByte(); err != nil {
		return nil, err
	}
	if b == typArray {
		return r.readArrayCommand()
	}
	return r.readInlineCommand()
}

func (r *Reader) readArrayCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[0] != typArray {
		return nil, fmt.Errorf("proto: expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("proto: invalid array length %q", line)
	}
	args := make([][]byte, n)
	for i := range args {
		args[i], err = r.readBulkArg()
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (r *Reader) readInlineCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, nil // blank line — caller should skip and read again
	}
	args := make([][]byte, len(parts))
	for i, p := range parts {
		args[i] = []byte(p)
	}
	return args, nil
}

// ReadSimpleString reads a + response, or returns an error if the server sent -.
func (r *Reader) ReadSimpleString() (string, error) {
	line, err := r.readLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", fmt.Errorf("proto: empty response")
	}
	switch line[0] {
	case typSimple:
		return line[1:], nil
	case typError:
		return "", fmt.Errorf("%s", line[1:])
	default:
		return "", fmt.Errorf("proto: expected simple string, got %q", line)
	}
}

// ReadBulkString reads a $ response. Returns nil if the server sent $-1 (null
// bulk string — key not found). Returns an error for - responses.
func (r *Reader) ReadBulkString() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("proto: empty response")
	}
	switch line[0] {
	case typError:
		return nil, fmt.Errorf("%s", line[1:])
	case typBulk:
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("proto: invalid bulk length %q", line)
		}
		if n < 0 {
			return nil, nil // null bulk string
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		return buf[:n], nil
	default:
		return nil, fmt.Errorf("proto: expected bulk string, got %q", line)
	}
}

// ReadInteger reads a : response. Returns an error for - responses.
func (r *Reader) ReadInteger() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	if len(line) == 0 {
		return 0, fmt.Errorf("proto: empty response")
	}
	switch line[0] {
	case typError:
		return 0, fmt.Errorf("%s", line[1:])
	case typInteger:
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("proto: invalid integer %q", line)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("proto: expected integer, got %q", line)
	}
}

// ReadPairs reads a *N\r\n array of alternating key/value bulk strings.
// Returns nil for an empty array. Used for SCAN responses.
func (r *Reader) ReadPairs() ([][2][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("proto: empty response")
	}
	switch line[0] {
	case typError:
		return nil, fmt.Errorf("%s", line[1:])
	case typArray:
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("proto: invalid array length %q", line)
		}
		if n == 0 {
			return nil, nil
		}
		if n%2 != 0 {
			return nil, fmt.Errorf("proto: expected even-length array for pairs, got %d", n)
		}
		pairs := make([][2][]byte, n/2)
		for i := range pairs {
			if pairs[i][0], err = r.readBulkArg(); err != nil {
				return nil, err
			}
			if pairs[i][1], err = r.readBulkArg(); err != nil {
				return nil, err
			}
		}
		return pairs, nil
	default:
		return nil, fmt.Errorf("proto: expected array, got %q", line)
	}
}

// readBulkArg reads a $N\r\n<data>\r\n bulk string from the stream.
func (r *Reader) readBulkArg() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != typBulk {
		return nil, fmt.Errorf("proto: expected bulk string, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("proto: invalid bulk length %q", line)
	}
	if n < 0 {
		return nil, nil
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
