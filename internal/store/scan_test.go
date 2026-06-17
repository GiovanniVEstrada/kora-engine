package store_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/giova/kora-engine/internal/store"
)

// collectScan drains the iterator into a slice for easy assertion.
func collectScan(iter func() ([]byte, []byte, bool)) [][2]string {
	var out [][2]string
	for {
		k, v, ok := iter()
		if !ok {
			break
		}
		out = append(out, [2]string{string(k), string(v)})
	}
	return out
}

// TestScanBasic writes a handful of keys and verifies that Scan returns the
// correct subset in sorted order.
func TestScanBasic(t *testing.T) {
	db, _ := openTemp(t)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		mustSet(t, db, k, "v:"+k)
	}

	got := collectScan(db.Scan([]byte("banana"), []byte("date")))
	want := [][2]string{
		{"banana", "v:banana"},
		{"cherry", "v:cherry"},
		{"date", "v:date"},
	}
	if !equalPairs(got, want) {
		t.Fatalf("Scan(banana,date): got %v, want %v", got, want)
	}
}

// TestScanNilBounds verifies open-ended scans (nil start, nil end, both nil).
func TestScanNilBounds(t *testing.T) {
	db, _ := openTemp(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		mustSet(t, db, k, k)
	}

	// nil start = from beginning
	got := collectScan(db.Scan(nil, []byte("b")))
	if !equalPairs(got, [][2]string{{"a", "a"}, {"b", "b"}}) {
		t.Fatalf("Scan(nil,b): %v", got)
	}

	// nil end = to the end
	got = collectScan(db.Scan([]byte("c"), nil))
	if !equalPairs(got, [][2]string{{"c", "c"}, {"d", "d"}}) {
		t.Fatalf("Scan(c,nil): %v", got)
	}

	// both nil = full scan
	got = collectScan(db.Scan(nil, nil))
	if len(got) != 4 {
		t.Fatalf("Scan(nil,nil): got %d pairs, want 4", len(got))
	}
}

// TestScanEmptyRange verifies that a range with no keys returns an empty iterator.
func TestScanEmptyRange(t *testing.T) {
	db, _ := openTemp(t)
	mustSet(t, db, "a", "1")
	mustSet(t, db, "z", "2")

	got := collectScan(db.Scan([]byte("m"), []byte("n")))
	if len(got) != 0 {
		t.Fatalf("expected empty scan, got %v", got)
	}
}

// TestScanSkipsTombstones verifies that deleted keys are invisible to Scan.
func TestScanSkipsTombstones(t *testing.T) {
	db, _ := openTemp(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		mustSet(t, db, k, k)
	}
	mustDelete(t, db, "b")
	mustDelete(t, db, "d")

	got := collectScan(db.Scan(nil, nil))
	if !equalPairs(got, [][2]string{{"a", "a"}, {"c", "c"}}) {
		t.Fatalf("scan after deletes: %v", got)
	}
}

// TestScanNewestVersionWins verifies that overwriting a key shows the latest value.
func TestScanNewestVersionWins(t *testing.T) {
	db, _ := openTemp(t)
	mustSet(t, db, "x", "v1")
	mustSet(t, db, "x", "v2")

	got := collectScan(db.Scan([]byte("x"), []byte("x")))
	if len(got) != 1 || got[0][1] != "v2" {
		t.Fatalf("scan overwritten key: %v", got)
	}
}

// TestScanAcrossFlushBoundary forces the memtable to flush and then verifies
// that Scan correctly merges values from both the memtable and the SSTable.
func TestScanAcrossFlushBoundary(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.Options{SyncOnWrite: false, MaxMemBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough to force several flushes.
	all := map[string]string{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		mustSet(t, db, k, v)
		all[k] = v
	}

	// Scan the full range and verify all 20 keys appear in order.
	got := collectScan(db.Scan(nil, nil))
	if len(got) != 20 {
		t.Fatalf("expected 20 results, got %d: %v", len(got), got)
	}
	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if got[i][0] != k || got[i][1] != all[k] {
			t.Fatalf("pos %d: got (%q,%q), want (%q,%q)", i, got[i][0], got[i][1], k, all[k])
		}
	}
}

// TestScanTombstoneShadowsSSTable verifies that a memtable tombstone for a key
// that exists in an SSTable causes the key to be absent from Scan results.
func TestScanTombstoneShadowsSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.Options{SyncOnWrite: false, MaxMemBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write keys that will be flushed to an SSTable.
	for i := 0; i < 10; i++ {
		mustSet(t, db, fmt.Sprintf("k%02d", i), "alive")
	}

	// Delete some keys after flush — tombstones live in the memtable.
	for i := 0; i < 10; i += 2 {
		mustDelete(t, db, fmt.Sprintf("k%02d", i))
	}

	got := collectScan(db.Scan(nil, nil))
	// Odd keys (01,03,05,07,09) must appear; even keys must not.
	if len(got) != 5 {
		t.Fatalf("expected 5 live keys, got %d: %v", len(got), got)
	}
	for _, pair := range got {
		k := pair[0]
		// All returned keys must be odd-indexed.
		digit := k[len(k)-1] - '0'
		if digit%2 == 0 {
			t.Fatalf("deleted key %q appeared in scan", k)
		}
	}
}

// TestScanSortedOrder verifies that Scan always returns keys in ascending order,
// even when values span multiple SSTables.
func TestScanSortedOrder(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.Options{SyncOnWrite: false, MaxMemBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write keys in reverse order so that out-of-order disk layout can expose bugs.
	for i := 19; i >= 0; i-- {
		mustSet(t, db, fmt.Sprintf("s%02d", i), "x")
	}

	got := collectScan(db.Scan(nil, nil))
	for i := 1; i < len(got); i++ {
		if bytes.Compare([]byte(got[i-1][0]), []byte(got[i][0])) >= 0 {
			t.Fatalf("scan not sorted at pos %d: %q >= %q", i, got[i-1][0], got[i][0])
		}
	}
}

// TestScanOracleWithFlushesAndCompaction is the heaviest correctness check:
// random ops + forced flushes + compaction, then a series of random range
// scans verified against a sorted reference map.
func TestScanOracleWithFlushesAndCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.Options{SyncOnWrite: false, MaxMemBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	oracle := make(map[string]string)
	rng := rand.New(rand.NewSource(77))
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
	}

	const ops = 2000
	for i := 0; i < ops; i++ {
		k := keys[rng.Intn(len(keys))]
		if rng.Intn(5) == 0 {
			mustDelete(t, db, k)
			delete(oracle, k)
		} else {
			v := fmt.Sprintf("v%d", rng.Intn(200))
			mustSet(t, db, k, v)
			oracle[k] = v
		}
		if i == ops/2 {
			if err := db.CompactSSTables(); err != nil {
				t.Fatalf("compact: %v", err)
			}
		}
	}

	// Run 20 random range scans and compare with oracle.
	sortedKeys := append([]string(nil), keys...)
	sort.Strings(sortedKeys)

	for trial := 0; trial < 20; trial++ {
		lo := rng.Intn(len(sortedKeys))
		hi := lo + rng.Intn(len(sortedKeys)-lo)
		start := []byte(sortedKeys[lo])
		end := []byte(sortedKeys[hi])

		// Collect expected results from oracle.
		var want [][2]string
		for _, k := range sortedKeys {
			if bytes.Compare([]byte(k), start) >= 0 && bytes.Compare([]byte(k), end) <= 0 {
				if v, ok := oracle[k]; ok {
					want = append(want, [2]string{k, v})
				}
			}
		}

		got := collectScan(db.Scan(start, end))
		if !equalPairs(got, want) {
			t.Fatalf("trial %d Scan(%q,%q):\n  got  %v\n  want %v", trial, start, end, got, want)
		}
	}
}

// --- helpers ----------------------------------------------------------------

func equalPairs(a, b [][2]string) bool {
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
