package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func tmpFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.sst")
}

// writeKV is a helper that writes a set of key-value pairs (already sorted)
// to a new SSTable and returns the path.
func writeKV(t *testing.T, pairs [][2]string) string {
	t.Helper()
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		if err := w.Set([]byte(p[0]), []byte(p[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// --- basic round-trip -------------------------------------------------------

func TestRoundTrip(t *testing.T) {
	pairs := [][2]string{
		{"apple", "fruit"},
		{"banana", "yellow"},
		{"cherry", "red"},
	}
	path := writeKV(t, pairs)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, p := range pairs {
		v, ok, err := r.Get([]byte(p[0]))
		if err != nil {
			t.Fatalf("Get(%q): %v", p[0], err)
		}
		if !ok || string(v) != p[1] {
			t.Fatalf("Get(%q): got %q %v, want %q true", p[0], v, ok, p[1])
		}
	}
}

func TestGetMiss(t *testing.T) {
	path := writeKV(t, [][2]string{{"aaa", "1"}, {"bbb", "2"}, {"ccc", "3"}})
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, k := range []string{"aab", "bbc", "ddd", "a"} {
		_, ok, err := r.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if ok {
			t.Fatalf("Get(%q): expected miss", k)
		}
	}
}

func TestGetFirstAndLast(t *testing.T) {
	pairs := [][2]string{{"a", "1"}, {"m", "2"}, {"z", "3"}}
	path := writeKV(t, pairs)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	v, ok, _ := r.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Fatalf("first key: got %q %v", v, ok)
	}
	v, ok, _ = r.Get([]byte("z"))
	if !ok || string(v) != "3" {
		t.Fatalf("last key: got %q %v", v, ok)
	}
}

// --- tombstones -------------------------------------------------------------

func TestTombstone(t *testing.T) {
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set([]byte("alive"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete([]byte("dead")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	v, ok, err := r.Get([]byte("alive"))
	if err != nil || !ok || string(v) != "yes" {
		t.Fatalf("alive: got %q %v %v", v, ok, err)
	}
	_, ok, err = r.Get([]byte("dead"))
	if err != nil || ok {
		t.Fatalf("dead (tombstone): expected miss, got ok=%v err=%v", ok, err)
	}
}

// --- iterator ---------------------------------------------------------------

func TestIteratorOrder(t *testing.T) {
	pairs := [][2]string{
		{"ant", "a"}, {"bee", "b"}, {"cat", "c"}, {"dog", "d"}, {"elk", "e"},
	}
	path := writeKV(t, pairs)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	next := r.Iterator()
	for _, p := range pairs {
		k, v, ok := next()
		if !ok {
			t.Fatalf("iterator ended early at %q", p[0])
		}
		if string(k) != p[0] || string(v) != p[1] {
			t.Fatalf("got (%q,%q), want (%q,%q)", k, v, p[0], p[1])
		}
	}
	_, _, ok := next()
	if ok {
		t.Fatal("iterator should be exhausted")
	}
}

func TestIteratorEmpty(t *testing.T) {
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, _, ok := r.Iterator()()
	if ok {
		t.Fatal("empty table iterator should return false")
	}
}

// --- index / sparse lookup (forces multiple index entries) ------------------

// TestManyKeys writes more than indexStride keys so the sparse index has
// multiple entries. Verifies that Get works for keys in every index bucket.
func TestManyKeys(t *testing.T) {
	n := indexStride*4 + 3 // 67 keys; 5 index entries
	pairs := make([][2]string, n)
	for i := 0; i < n; i++ {
		pairs[i] = [2]string{fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i)}
	}
	path := writeKV(t, pairs)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Every key must be found with the correct value.
	for _, p := range pairs {
		v, ok, err := r.Get([]byte(p[0]))
		if err != nil {
			t.Fatalf("Get(%q): %v", p[0], err)
		}
		if !ok || string(v) != p[1] {
			t.Fatalf("Get(%q): got %q %v", p[0], v, ok)
		}
	}

	// No spurious hits for keys that don't exist.
	_, ok, _ := r.Get([]byte("key9999"))
	if ok {
		t.Fatal("expected miss for key9999")
	}
}

// --- out-of-order write detection -------------------------------------------

func TestOutOfOrderError(t *testing.T) {
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Set([]byte("b"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Set([]byte("a"), []byte("2")); err == nil {
		t.Fatal("expected error for out-of-order key")
	}
}

// --- bad magic detection ----------------------------------------------------

func TestBadMagic(t *testing.T) {
	path := tmpFile(t)
	// Use footerSize bytes so the seek succeeds but magic is wrong.
	if err := os.WriteFile(path, make([]byte, footerSize), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

// --- bloom filter correctness -----------------------------------------------

// TestBloomNoFalseNegatives verifies that every key written to an SSTable is
// found after Open (the bloom filter must not produce false negatives).
func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 200
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("bloom-key-%04d", i), fmt.Sprintf("val%d", i)}
	}
	path := writeKV(t, pairs)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, p := range pairs {
		v, ok, err := r.Get([]byte(p[0]))
		if err != nil || !ok || string(v) != p[1] {
			t.Fatalf("bloom false negative for %q: got %q ok=%v err=%v", p[0], v, ok, err)
		}
	}
}

// TestBloomTombstonesIncluded verifies that a tombstone key is in the bloom
// filter. If the filter excluded tombstones a RawGet for the deleted key would
// return (nil, false, false) instead of (nil, false, true), causing the
// multi-source read path to fall through and resurrect a stale value.
func TestBloomTombstonesIncluded(t *testing.T) {
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := w.Set([]byte("zz"), []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, found, isTombstone, err := r.RawGet([]byte("gone"))
	if err != nil || found || !isTombstone {
		t.Fatalf("deleted key: found=%v isTombstone=%v err=%v; want found=false isTombstone=true", found, isTombstone, err)
	}
}

// --- oracle (randomised round-trip) -----------------------------------------

func TestOracle(t *testing.T) {
	const n = 500
	rng := rand.New(rand.NewSource(7))

	// Build a sorted list of unique keys.
	keySet := make(map[string]struct{}, n)
	for len(keySet) < n {
		k := fmt.Sprintf("k%04d", rng.Intn(n*3))
		keySet[k] = struct{}{}
	}
	keys := make([]string, 0, n)
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ref := make(map[string]string, n)
	path := tmpFile(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		v := fmt.Sprintf("v_%s", k)
		ref[k] = v
		if err := w.Set([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Every key in ref must be retrievable.
	for k, want := range ref {
		v, ok, err := r.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !ok || string(v) != want {
			t.Fatalf("Get(%q): got %q %v, want %q", k, v, ok, want)
		}
	}

	// Iterator must yield all keys in sorted order.
	next := r.Iterator()
	var got []string
	for {
		k, _, ok := next()
		if !ok {
			break
		}
		got = append(got, string(k))
	}
	if len(got) != len(keys) {
		t.Fatalf("iterator count: got %d want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("iterator pos %d: got %q want %q", i, got[i], keys[i])
		}
	}
}
