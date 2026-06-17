package store_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/giova/kora-engine/internal/store"
)

func openTemp(t *testing.T) (*store.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dir
}

// mustSet / mustDelete fail the test on a write error. In a storage engine,
// write failures matter, so tests should never silently ignore them.
func mustSet(t *testing.T, db *store.DB, key, value string) {
	t.Helper()
	if err := db.Set([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func mustDelete(t *testing.T, db *store.DB, key string) {
	t.Helper()
	if err := db.Delete([]byte(key)); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func TestSetGet(t *testing.T) {
	db, _ := openTemp(t)

	if err := db.Set([]byte("foo"), []byte("bar")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Get([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("got %q ok=%v, want bar ok=true", got, ok)
	}
}

func TestGetMissing(t *testing.T) {
	db, _ := openTemp(t)
	_, ok, err := db.Get([]byte("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected missing key to report ok=false")
	}
}

func TestOverwrite(t *testing.T) {
	db, _ := openTemp(t)
	mustSet(t, db, "k", "v1")
	mustSet(t, db, "k", "v2")

	got, _, _ := db.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("got %q, want v2", got)
	}
}

func TestDelete(t *testing.T) {
	db, _ := openTemp(t)
	mustSet(t, db, "k", "v")
	mustDelete(t, db, "k")
	_, ok, _ := db.Get([]byte("k"))
	if ok {
		t.Fatal("expected key to be gone after delete")
	}
}

func TestEmptyValueIsNotDelete(t *testing.T) {
	db, _ := openTemp(t)
	mustSet(t, db, "k", "")
	got, ok, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("empty value should still be present")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty value, got %q", got)
	}
}

// TestReopenDurability is the M1 "done when" check: write data, close, reopen,
// and confirm everything survived recovery.
func TestReopenDurability(t *testing.T) {
	dir := t.TempDir()

	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "a", "1")
	mustSet(t, db, "b", "2")
	mustSet(t, db, "a", "3") // overwrite: later wins
	mustDelete(t, db, "b")   // tombstone: should be gone after reopen
	mustSet(t, db, "c", "4")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	check := func(key, want string, wantOK bool) {
		t.Helper()
		got, ok, err := db2.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if ok != wantOK {
			t.Fatalf("key %q: ok=%v, want %v", key, ok, wantOK)
		}
		if wantOK && string(got) != want {
			t.Fatalf("key %q: got %q, want %q", key, got, want)
		}
	}
	check("a", "3", true) // overwrite survived
	check("b", "", false) // delete survived
	check("c", "4", true) // last write survived
	if db2.Len() != 2 {
		t.Fatalf("expected 2 live keys, got %d", db2.Len())
	}
}

// TestOracle runs thousands of random ops against the engine and a plain map,
// asserting they always agree — the single highest-value correctness test.
func TestOracle(t *testing.T) {
	db, dir := openTemp(t)
	oracle := make(map[string]string)

	rng := rand.New(rand.NewSource(42))
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	const ops = 5000
	for i := 0; i < ops; i++ {
		k := keys[rng.Intn(len(keys))]
		switch rng.Intn(3) {
		case 0, 1: // bias toward writes
			v := fmt.Sprintf("v%d", rng.Intn(1000))
			if err := db.Set([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			oracle[k] = v
		case 2:
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(oracle, k)
		}

		// Periodically verify the whole keyspace agrees.
		if i%500 == 0 {
			assertAgree(t, db, oracle, keys)
		}
	}
	assertAgree(t, db, oracle, keys)

	// And it must still agree after a reopen.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	assertAgree(t, db2, oracle, keys)
}

func assertAgree(t *testing.T, db *store.DB, oracle map[string]string, keys []string) {
	t.Helper()
	for _, k := range keys {
		got, ok, err := db.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		want, wantOK := oracle[k]
		if ok != wantOK {
			t.Fatalf("key %q: engine ok=%v, oracle ok=%v", k, ok, wantOK)
		}
		if wantOK && string(got) != want {
			t.Fatalf("key %q: engine=%q, oracle=%q", k, got, want)
		}
	}
}

// TestMemtableFlushAndMultiSourceRead is the M3c end-to-end test. It forces a
// memtable flush by using a tiny MaxMemBytes, then verifies:
//   - Keys written before the flush are readable from the SSTable.
//   - A delete after the flush inserts a memtable tombstone that correctly
//     shadows the SSTable value.
//   - A re-set after the delete is visible from the memtable.
//   - Len() tracks live keys correctly across flush boundaries.
func TestMemtableFlushAndMultiSourceRead(t *testing.T) {
	dir := t.TempDir()
	// 64 bytes triggers a flush after the first few writes.
	opts := store.Options{SyncOnWrite: false, MaxMemBytes: 64}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough to trigger at least one flush.
	for i := 0; i < 10; i++ {
		mustSet(t, db, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// All keys must be readable (some from memtable, some from SSTable).
	for i := 0; i < 10; i++ {
		got, ok, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatalf("Get key%d: %v", i, err)
		}
		if !ok || string(got) != fmt.Sprintf("val%d", i) {
			t.Fatalf("key%d: got %q ok=%v", i, got, ok)
		}
	}

	// Delete a key that was flushed to SSTable — memtable tombstone must win.
	mustDelete(t, db, "key0")
	_, ok, err := db.Get([]byte("key0"))
	if err != nil || ok {
		t.Fatalf("key0 after delete: ok=%v err=%v", ok, err)
	}

	// Re-set the deleted key — memtable value must win over both tombstone and SSTable.
	mustSet(t, db, "key0", "restored")
	got, ok, err := db.Get([]byte("key0"))
	if err != nil || !ok || string(got) != "restored" {
		t.Fatalf("key0 after restore: got %q ok=%v err=%v", got, ok, err)
	}

	// Len() must return 10 (all original keys live, key0 restored).
	if n := db.Len(); n != 10 {
		t.Fatalf("Len() = %d, want 10", n)
	}
}

func TestSyncOff(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.Options{SyncOnWrite: false})
	if err != nil {
		t.Fatal(err)
	}
	db.Set([]byte("k"), []byte("v"))
	got, ok, _ := db.Get([]byte("k"))
	if !ok || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	db.Close()
}
