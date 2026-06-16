package record

import (
	"bytes"
	"testing"
)

// TestSizeGuardsWithLoweredLimits exercises the encode/decode size guards by
// temporarily shrinking the limits, so we never have to allocate a value near
// the real 1 GiB cap to test the rejection path.
func TestSizeGuardsWithLoweredLimits(t *testing.T) {
	defer func(k, v int) { maxKeySize, maxValueSize = k, v }(maxKeySize, maxValueSize)
	maxKeySize, maxValueSize = 8, 8

	var buf bytes.Buffer
	if err := Encode(&buf, make([]byte, 9), []byte("x")); err != ErrKeyTooLarge {
		t.Errorf("oversized key: got %v, want ErrKeyTooLarge", err)
	}
	buf.Reset()
	if err := Encode(&buf, []byte("k"), make([]byte, 9)); err != ErrValueTooLarge {
		t.Errorf("oversized value: got %v, want ErrValueTooLarge", err)
	}

	// Decode must reject an on-disk value length above the limit before it
	// allocates the tail buffer.
	hdr := make([]byte, HeaderSize)
	hdr[19] = 9 // valueLen = 9 (> maxValueSize=8); keyLen = 0
	if _, err := Decode(bytes.NewReader(hdr)); err != ErrCorrupted {
		t.Errorf("decode oversized value: got %v, want ErrCorrupted", err)
	}
}
