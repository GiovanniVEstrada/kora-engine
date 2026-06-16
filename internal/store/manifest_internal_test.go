package store

import (
	"os"
	"path/filepath"
	"testing"
)

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := manifest{nextID: 7, order: []uint32{3, 5, 6}}
	if err := writeManifest(dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.nextID != in.nextID || !equalU32(out.order, in.order) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestManifestRejectsDuplicateIDs(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{nextID: 9, order: []uint32{2, 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}

func TestManifestRejectsNextIDNotAboveMax(t *testing.T) {
	dir := t.TempDir()
	// nextID must be strictly greater than the largest live id; 5 == max is bad.
	if err := writeManifest(dir, manifest{nextID: 5, order: []uint32{5}}); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}

func TestManifestRejectsBadCRC(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{nextID: 2, order: []uint32{1}}); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, manifestName)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // corrupt the last id byte
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}
