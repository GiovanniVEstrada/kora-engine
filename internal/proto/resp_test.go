package proto_test

import (
	"bytes"
	"testing"

	"github.com/giova/kora-engine/internal/proto"
)

// roundTrip writes to a buffer using the writer fn, then reads back via the
// Reader, returning the raw buffer for inspection.
func roundTrip(t *testing.T, write func(w *bytes.Buffer), read func(r *proto.Reader)) {
	t.Helper()
	var buf bytes.Buffer
	write(&buf)
	r := proto.NewReader(&buf)
	read(r)
}

func TestBulkStringRoundTrip(t *testing.T) {
	roundTrip(t,
		func(w *bytes.Buffer) {
			if err := proto.WriteBulkString(w, []byte("hello")); err != nil {
				t.Fatal(err)
			}
		},
		func(r *proto.Reader) {
			got, err := r.ReadBulkString()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, []byte("hello")) {
				t.Fatalf("got %q, want hello", got)
			}
		},
	)
}

func TestNullBulkString(t *testing.T) {
	roundTrip(t,
		func(w *bytes.Buffer) {
			if err := proto.WriteBulkString(w, nil); err != nil {
				t.Fatal(err)
			}
		},
		func(r *proto.Reader) {
			got, err := r.ReadBulkString()
			if err != nil {
				t.Fatal(err)
			}
			if got != nil {
				t.Fatalf("expected nil, got %q", got)
			}
		},
	)
}

func TestBulkStringBinaryData(t *testing.T) {
	data := []byte{0, 1, 2, '\r', '\n', 0xFF}
	roundTrip(t,
		func(w *bytes.Buffer) {
			if err := proto.WriteBulkString(w, data); err != nil {
				t.Fatal(err)
			}
		},
		func(r *proto.Reader) {
			got, err := r.ReadBulkString()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("got %v, want %v", got, data)
			}
		},
	)
}

func TestIntegerRoundTrip(t *testing.T) {
	for _, n := range []int64{0, 1, -1, 9999, -9999} {
		var buf bytes.Buffer
		if err := proto.WriteInteger(&buf, n); err != nil {
			t.Fatal(err)
		}
		r := proto.NewReader(&buf)
		got, err := r.ReadInteger()
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got != n {
			t.Fatalf("n=%d: got %d", n, got)
		}
	}
}

func TestSimpleStringRoundTrip(t *testing.T) {
	roundTrip(t,
		func(w *bytes.Buffer) {
			if err := proto.WriteOK(w); err != nil {
				t.Fatal(err)
			}
		},
		func(r *proto.Reader) {
			s, err := r.ReadSimpleString()
			if err != nil {
				t.Fatal(err)
			}
			if s != "OK" {
				t.Fatalf("got %q, want OK", s)
			}
		},
	)
}

func TestErrorResponse(t *testing.T) {
	roundTrip(t,
		func(w *bytes.Buffer) {
			if err := proto.WriteError(w, "something went wrong"); err != nil {
				t.Fatal(err)
			}
		},
		func(r *proto.Reader) {
			_, err := r.ReadSimpleString()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		},
	)
}

func TestArrayCommand(t *testing.T) {
	var buf bytes.Buffer
	// Write a RESP2 array command: SET hello world
	if err := proto.WriteArrayLen(&buf, 3); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"SET", "hello", "world"} {
		if err := proto.WriteBulkString(&buf, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	r := proto.NewReader(&buf)
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "hello", "world"}
	if len(args) != len(want) {
		t.Fatalf("got %d args, want %d", len(args), len(want))
	}
	for i, w := range want {
		if !bytes.Equal(args[i], []byte(w)) {
			t.Fatalf("arg[%d]: got %q, want %q", i, args[i], w)
		}
	}
}

func TestInlineCommand(t *testing.T) {
	r := proto.NewReader(bytes.NewBufferString("SET hello world\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{[]byte("SET"), []byte("hello"), []byte("world")}
	if len(args) != len(want) {
		t.Fatalf("got %d args, want %d", len(args), len(want))
	}
	for i := range want {
		if !bytes.Equal(args[i], want[i]) {
			t.Fatalf("arg[%d]: got %q, want %q", i, args[i], want[i])
		}
	}
}

func TestPairsRoundTrip(t *testing.T) {
	pairs := [][2][]byte{
		{[]byte("apple"), []byte("red")},
		{[]byte("banana"), []byte("yellow")},
	}
	var buf bytes.Buffer
	if err := proto.WriteArrayLen(&buf, len(pairs)*2); err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		proto.WriteBulkString(&buf, p[0])
		proto.WriteBulkString(&buf, p[1])
	}

	r := proto.NewReader(&buf)
	got, err := r.ReadPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(pairs) {
		t.Fatalf("got %d pairs, want %d", len(got), len(pairs))
	}
	for i := range pairs {
		if !bytes.Equal(got[i][0], pairs[i][0]) || !bytes.Equal(got[i][1], pairs[i][1]) {
			t.Fatalf("pair[%d]: got (%q,%q), want (%q,%q)",
				i, got[i][0], got[i][1], pairs[i][0], pairs[i][1])
		}
	}
}

func TestEmptyArrayPairs(t *testing.T) {
	var buf bytes.Buffer
	proto.WriteArrayLen(&buf, 0)
	r := proto.NewReader(&buf)
	got, err := r.ReadPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}
