package store_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/giova/kora-engine/internal/store"
)

// tinyMemOpts forces a flush after every ~8 bytes so tests exercise multiple
// SSTable files without writing megabytes. 8 bytes = one "d00"+"alive" entry.
func tinyMemOpts() store.Options {
	return store.Options{SyncOnWrite: false, MaxMemBytes: 8}
}

// TestCompactSSTablesReducesToOne verifies that after N flushes + one compact,
// there is exactly one SSTable reader and all live keys are still correct.
func TestCompactSSTablesReducesToOne(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, tinyMemOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough to produce several SSTables.
	for i := 0; i < 20; i++ {
		mustSet(t, db, fmt.Sprintf("key%02d", i), fmt.Sprintf("val%02d", i))
	}
	before := db.SSTableCount()
	if before < 2 {
		t.Fatalf("expected ≥2 SSTables before compact, got %d", before)
	}

	if err := db.CompactSSTables(); err != nil {
		t.Fatal(err)
	}
	if got := db.SSTableCount(); got != 1 {
		t.Fatalf("SSTableCount after compact: got %d, want 1", got)
	}

	// All 20 keys must survive.
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key%02d", i)
		v, ok, err := db.Get([]byte(k))
		if err != nil || !ok || string(v) != fmt.Sprintf("val%02d", i) {
			t.Fatalf("%s: got %q ok=%v err=%v", k, v, ok, err)
		}
	}
}

// TestCompactSSTablesDropsTombstones confirms that deleting a key before a
// flush and then compacting removes it entirely — the tombstone is not
// resurrected as a phantom from an older SSTable.
func TestCompactSSTablesDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, tinyMemOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write keys, force a flush, then delete some and force another flush.
	for i := 0; i < 10; i++ {
		mustSet(t, db, fmt.Sprintf("d%02d", i), "alive")
	}
	for i := 0; i < 10; i += 2 { // delete even-indexed keys
		mustDelete(t, db, fmt.Sprintf("d%02d", i))
	}
	// Force any remaining memtable data to flush by writing one more key.
	mustSet(t, db, "trigger", "flush")

	if db.SSTableCount() < 2 {
		t.Skip("not enough SSTables to test compaction — increase data or reduce MaxMemBytes")
	}

	if err := db.CompactSSTables(); err != nil {
		t.Fatal(err)
	}

	// Odd keys (live) must be present.
	for i := 1; i < 10; i += 2 {
		k := fmt.Sprintf("d%02d", i)
		v, ok, err := db.Get([]byte(k))
		if err != nil || !ok || string(v) != "alive" {
			t.Fatalf("%s: got %q ok=%v err=%v", k, v, ok, err)
		}
	}
	// Even keys (deleted) must be absent — tombstone must be gone after compact.
	for i := 0; i < 10; i += 2 {
		k := fmt.Sprintf("d%02d", i)
		_, ok, err := db.Get([]byte(k))
		if err != nil || ok {
			t.Fatalf("%s should be absent after tombstone drop: ok=%v err=%v", k, ok, err)
		}
	}
}

// TestCompactSSTablesNewestVersionWins verifies that when two SSTables contain
// different values for the same key, the newer one is kept after compaction.
func TestCompactSSTablesNewestVersionWins(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, tinyMemOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write v1 and force a flush.
	mustSet(t, db, "x", "v1")
	mustSet(t, db, "pad1", "a") // padding to push past flush threshold

	// Overwrite with v2 and force another flush.
	mustSet(t, db, "x", "v2")
	mustSet(t, db, "pad2", "b")

	if db.SSTableCount() < 2 {
		t.Skip("not enough SSTables — adjust padding")
	}

	if err := db.CompactSSTables(); err != nil {
		t.Fatal(err)
	}

	v, ok, err := db.Get([]byte("x"))
	if err != nil || !ok || string(v) != "v2" {
		t.Fatalf("x after compact: got %q ok=%v err=%v", v, ok, err)
	}
}

// TestCompactSSTablesNoopBelowTwo confirms the method is a no-op with 0 or 1
// SSTables and doesn't corrupt state.
func TestCompactSSTablesNoopBelowTwo(t *testing.T) {
	db, _ := openTemp(t)

	mustSet(t, db, "k", "v")
	// With default MaxMemBytes (4 MiB), no flush — ssReaders is empty.
	if err := db.CompactSSTables(); err != nil {
		t.Fatal(err)
	}
	v, ok, _ := db.Get([]byte("k"))
	if !ok || string(v) != "v" {
		t.Fatalf("key lost after no-op compact: %q %v", v, ok)
	}
}

// TestCompactSSTablesOracle is the heaviest M3d correctness check: random ops
// across many tiny flushes + a compaction, verified against a plain map.
func TestCompactSSTablesOracle(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, tinyMemOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	oracle := make(map[string]string)
	rng := rand.New(rand.NewSource(13))

	const ops = 3000
	keys := make([]string, 15)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
	}

	for i := 0; i < ops; i++ {
		k := keys[rng.Intn(len(keys))]
		if rng.Intn(4) == 0 {
			mustDelete(t, db, k)
			delete(oracle, k)
		} else {
			v := fmt.Sprintf("v%d", rng.Intn(500))
			mustSet(t, db, k, v)
			oracle[k] = v
		}
		if i == ops/2 {
			if err := db.CompactSSTables(); err != nil {
				t.Fatalf("CompactSSTables mid-run: %v", err)
			}
		}
	}

	// Final verification.
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		got, ok, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		want, wantOK := oracle[k]
		if ok != wantOK {
			t.Fatalf("key %q: engine ok=%v oracle ok=%v", k, ok, wantOK)
		}
		if wantOK && !bytes.Equal(got, []byte(want)) {
			t.Fatalf("key %q: engine=%q oracle=%q", k, got, want)
		}
	}
}
