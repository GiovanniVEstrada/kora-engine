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
	in := manifest{nextID: 7, order: []uint32{3, 5, 6}, nextSSTID: 4, sstIDs: []uint32{1, 2, 3}}
	if err := writeManifest(dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.nextID != in.nextID || !equalU32(out.order, in.order) {
		t.Fatalf("segment round trip mismatch: got %+v, want %+v", out, in)
	}
	if out.nextSSTID != in.nextSSTID || !equalU32(out.sstIDs, in.sstIDs) {
		t.Fatalf("SST round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestManifestRoundTripNoSSTs(t *testing.T) {
	dir := t.TempDir()
	in := manifest{nextID: 3, order: []uint32{1, 2}, nextSSTID: 1}
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
	if out.nextSSTID != in.nextSSTID || len(out.sstIDs) != 0 {
		t.Fatalf("expected no SSTables: %+v", out)
	}
}

func TestManifestRejectsDuplicateSegIDs(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{nextID: 9, order: []uint32{2, 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}

func TestManifestRejectsDuplicateSSTIDs(t *testing.T) {
	dir := t.TempDir()
	m := manifest{nextID: 2, order: []uint32{1}, nextSSTID: 5, sstIDs: []uint32{3, 3}}
	if err := writeManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}

func TestManifestRejectsNextIDNotAboveMax(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{nextID: 5, order: []uint32{5}}); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}

func TestManifestRejectsNextSSTIDNotAboveMax(t *testing.T) {
	dir := t.TempDir()
	m := manifest{nextID: 2, order: []uint32{1}, nextSSTID: 3, sstIDs: []uint32{3}}
	if err := writeManifest(dir, m); err != nil {
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
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(dir); err != errManifestCorrupt {
		t.Fatalf("got %v, want errManifestCorrupt", err)
	}
}
