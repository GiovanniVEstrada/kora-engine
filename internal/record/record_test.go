package record_test

import (
	"bytes"
	"testing"

	"github.com/giova/kora-engine/internal/record"
)

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"normal", []byte("hello"), []byte("world")},
		{"empty value", []byte("key"), []byte{}},
		{"tombstone", []byte("dead"), nil},
		{"binary data", []byte{0x00, 0xFF, 0x42}, []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"large value", []byte("bigkey"), bytes.Repeat([]byte("x"), 4096)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := record.Encode(&buf, tc.key, tc.value); err != nil {
				t.Fatalf("Encode: %v", err)
			}

			rec, err := record.Decode(&buf)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if !bytes.Equal(rec.Key, tc.key) {
				t.Errorf("key: got %q, want %q", rec.Key, tc.key)
			}
			if !bytes.Equal(rec.Value, tc.value) {
				t.Errorf("value: got %q, want %q", rec.Value, tc.value)
			}
			if record.IsTombstone(rec) != (tc.value == nil) {
				t.Errorf("tombstone: got %v, want %v", record.IsTombstone(rec), tc.value == nil)
			}
			if rec.Timestamp == 0 {
				t.Error("timestamp should be non-zero")
			}
		})
	}
}

func TestCorruptedCRC(t *testing.T) {
	var buf bytes.Buffer
	if err := record.Encode(&buf, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	b := buf.Bytes()
	b[0] ^= 0xFF // flip bits in the CRC field

	_, err := record.Decode(bytes.NewReader(b))
	if err != record.ErrCorrupted {
		t.Errorf("expected ErrCorrupted, got %v", err)
	}
}

func TestTombstoneHasNilValue(t *testing.T) {
	var buf bytes.Buffer
	if err := record.Encode(&buf, []byte("gone"), nil); err != nil {
		t.Fatal(err)
	}

	rec, err := record.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Value != nil {
		t.Errorf("tombstone Value should be nil, got %q", rec.Value)
	}
	if !record.IsTombstone(rec) {
		t.Error("IsTombstone should return true")
	}
}

func TestEncodeRejectsOversizedKey(t *testing.T) {
	var buf bytes.Buffer
	// 64 KiB + 1 is cheap to allocate. The value-size guard is exercised in the
	// internal test with a lowered limit, to avoid a gigabyte allocation here.
	bigKey := make([]byte, record.MaxKeySize+1)
	if err := record.Encode(&buf, bigKey, []byte("v")); err != record.ErrKeyTooLarge {
		t.Errorf("oversized key: got %v, want ErrKeyTooLarge", err)
	}
}

// TestDecodeRejectsImplausibleLengths feeds a header claiming a gigantic key
// length. Decode must reject it as corruption *without* trying to allocate that
// much (i.e. it must not OOM).
func TestDecodeRejectsImplausibleLengths(t *testing.T) {
	// Build a header by hand: keyLen claims ~4 GiB.
	hdr := make([]byte, record.HeaderSize)
	// hdr layout: crc(4) ts(8) keyLen(4) valueLen(4). Set keyLen huge.
	hdr[12] = 0xFF
	hdr[13] = 0xFF
	hdr[14] = 0xFF
	hdr[15] = 0x00 // keyLen = 0xFFFFFF00, far above MaxKeySize
	// valueLen left 0. The CRC (hdr[0:4]) is garbage, but the length check
	// happens before CRC validation, so we expect ErrCorrupted either way.

	_, err := record.Decode(bytes.NewReader(hdr))
	if err != record.ErrCorrupted {
		t.Fatalf("got %v, want ErrCorrupted", err)
	}
}

func TestEmptyValueIsNotTombstone(t *testing.T) {
	var buf bytes.Buffer
	if err := record.Encode(&buf, []byte("k"), []byte{}); err != nil {
		t.Fatal(err)
	}

	rec, err := record.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if record.IsTombstone(rec) {
		t.Error("empty value should not be treated as tombstone")
	}
}
