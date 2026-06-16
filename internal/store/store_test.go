package store_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/giova/strata-engine/internal/store"
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
	db.Set([]byte("k"), []byte("v1"))
	db.Set([]byte("k"), []byte("v2"))

	got, _, _ := db.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("got %q, want v2", got)
	}
}

func TestDelete(t *testing.T) {
	db, _ := openTemp(t)
	db.Set([]byte("k"), []byte("v"))
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := db.Get([]byte("k"))
	if ok {
		t.Fatal("expected key to be gone after delete")
	}
}

func TestEmptyValueIsNotDelete(t *testing.T) {
	db, _ := openTemp(t)
	db.Set([]byte("k"), []byte{})
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
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("a"), []byte("3")) // overwrite: later wins
	db.Delete([]byte("b"))           // tombstone: should be gone after reopen
	db.Set([]byte("c"), []byte("4"))
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
