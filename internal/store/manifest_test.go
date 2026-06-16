package store_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/giova/strata-engine/internal/store"
)

// TestManifestCreatedOnOpen confirms a fresh store writes a MANIFEST.
func TestManifestCreatedOnOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err != nil {
		t.Fatalf("expected MANIFEST to exist: %v", err)
	}
}

// TestReopenViaManifestAfterCompaction exercises the full M2-hardened path:
// rollover (many segments) + compaction (merged into a fresh id) + reopen. The
// reopen must rebuild correct state purely from the manifest, even though the
// merged segment's id is *higher* than the active segment's (id order no longer
// equals recency — only the manifest order does).
func TestReopenViaManifestAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}

	val := string(make([]byte, 200))
	for round := 0; round < 40; round++ {
		for k := 0; k < 5; k++ {
			mustSet(t, db, fmt.Sprintf("k%d", k), fmt.Sprintf("%s-%d", val, round))
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for k := 0; k < 5; k++ {
		got, ok, err := db2.Get([]byte(fmt.Sprintf("k%d", k)))
		if err != nil || !ok {
			t.Fatalf("k%d: ok=%v err=%v", k, ok, err)
		}
		want := fmt.Sprintf("%s-%d", val, 39) // last round wins
		if string(got) != want {
			t.Fatalf("k%d: got %q want %q", k, got, want)
		}
	}
}

// TestLeakedSegmentIgnoredOnOpen simulates a compaction that crashed after
// writing its output file but before committing the manifest: a *.data file
// exists that the manifest doesn't list. Recovery must ignore (and clean up)
// that file rather than trusting it.
func TestLeakedSegmentIgnoredOnOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "real", "value")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Drop a bogus high-id segment file full of garbage. If recovery scanned the
	// directory instead of the manifest, this would either corrupt state or fail
	// to parse. Use a high id so a naive "max id is active" scheme would pick it.
	leaked := filepath.Join(dir, "009999.data")
	if err := os.WriteFile(leaked, []byte("not a valid record at all"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatalf("Open with leaked segment should succeed: %v", err)
	}
	defer db2.Close()

	got, ok, err := db2.Get([]byte("real"))
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("real key: got %q ok=%v err=%v", got, ok, err)
	}

	// The leaked file should have been cleaned up.
	if _, err := os.Stat(leaked); !os.IsNotExist(err) {
		t.Errorf("expected leaked segment to be removed, stat err=%v", err)
	}
}

// TestCorruptManifestRejected confirms a damaged manifest is surfaced, not
// silently worked around.
func TestCorruptManifestRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "a", "1")
	db.Close()

	// Corrupt the manifest.
	mpath := filepath.Join(dir, "MANIFEST")
	if err := os.WriteFile(mpath, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Open(dir, store.DefaultOptions()); err == nil {
		t.Fatal("expected Open to fail on a corrupt manifest")
	}
}
