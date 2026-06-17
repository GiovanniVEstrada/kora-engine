package art

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// --- basic smoke tests ------------------------------------------------------

func TestGetMissOnEmpty(t *testing.T) {
	var tr Tree
	_, ok := tr.Get([]byte("x"))
	if ok {
		t.Fatal("expected miss on empty tree")
	}
}

func TestInsertGet(t *testing.T) {
	var tr Tree
	tr.Insert([]byte("hello"), []byte("world"))
	v, ok := tr.Get([]byte("hello"))
	if !ok || string(v.([]byte)) != "world" {
		t.Fatalf("got %q %v, want world true", v, ok)
	}
}

func TestUpdateOverwrite(t *testing.T) {
	var tr Tree
	tr.Insert([]byte("k"), []byte("v1"))
	tr.Insert([]byte("k"), []byte("v2"))
	v, ok := tr.Get([]byte("k"))
	if !ok || string(v.([]byte)) != "v2" {
		t.Fatalf("got %q %v, want v2 true", v, ok)
	}
	if tr.Len() != 1 {
		t.Fatalf("size %d, want 1", tr.Len())
	}
}

func TestDelete(t *testing.T) {
	var tr Tree
	tr.Insert([]byte("a"), []byte("1"))
	tr.Insert([]byte("b"), []byte("2"))
	if !tr.Delete([]byte("a")) {
		t.Fatal("expected true")
	}
	_, ok := tr.Get([]byte("a"))
	if ok {
		t.Fatal("key still present after delete")
	}
	v, ok := tr.Get([]byte("b"))
	if !ok || string(v.([]byte)) != "2" {
		t.Fatalf("sibling damaged: got %q %v", v, ok)
	}
}

func TestDeleteMiss(t *testing.T) {
	var tr Tree
	if tr.Delete([]byte("nope")) {
		t.Fatal("expected false on miss")
	}
}

// --- node growth tests ------------------------------------------------------

// forceNode16 inserts 5 keys that share no common prefix, forcing Node4→Node16.
func TestForceNode16(t *testing.T) {
	var tr Tree
	for i := 0; i < 5; i++ {
		key := []byte{byte(i), 'x'}
		tr.Insert(key, []byte(fmt.Sprintf("%d", i)))
	}
	for i := 0; i < 5; i++ {
		key := []byte{byte(i), 'x'}
		v, ok := tr.Get(key)
		if !ok || string(v.([]byte)) != fmt.Sprintf("%d", i) {
			t.Fatalf("key %v: got %q %v", key, v, ok)
		}
	}
}

// forceNode48 inserts 17 distinct first-byte keys → Node16→Node48.
func TestForceNode48(t *testing.T) {
	var tr Tree
	for i := 0; i < 17; i++ {
		tr.Insert([]byte{byte(i)}, []byte{byte(i)})
	}
	for i := 0; i < 17; i++ {
		v, ok := tr.Get([]byte{byte(i)})
		if !ok || v.([]byte)[0] != byte(i) {
			t.Fatalf("key %d: got %v %v", i, v, ok)
		}
	}
}

// forceNode256 inserts 49 distinct first-byte keys → Node48→Node256.
func TestForceNode256(t *testing.T) {
	var tr Tree
	for i := 0; i < 49; i++ {
		tr.Insert([]byte{byte(i)}, []byte{byte(i)})
	}
	for i := 0; i < 49; i++ {
		v, ok := tr.Get([]byte{byte(i)})
		if !ok || v.([]byte)[0] != byte(i) {
			t.Fatalf("key %d: got %v %v", i, v, ok)
		}
	}
}

// --- iterator tests ---------------------------------------------------------

func TestIteratorEmpty(t *testing.T) {
	var tr Tree
	next := tr.Iterator()
	_, _, ok := next()
	if ok {
		t.Fatal("expected empty iterator")
	}
}

func TestIteratorSorted(t *testing.T) {
	var tr Tree
	keys := []string{"banana", "apple", "cherry", "apricot", "avocado"}
	for _, k := range keys {
		tr.Insert([]byte(k), []byte(k))
	}

	var got []string
	next := tr.Iterator()
	for {
		k, _, ok2 := next()
		if !ok2 {
			break
		}
		got = append(got, string(k))
	}

	want := make([]string, len(keys))
	copy(want, keys)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %q want %q", i, got[i], want[i])
		}
	}
}

// --- oracle test (randomised) -----------------------------------------------

func TestOracle(t *testing.T) {
	const ops = 5000
	rng := rand.New(rand.NewSource(42))

	var tr Tree
	ref := make(map[string][]byte) // reference map stores []byte; ART stores any

	randKey := func() []byte {
		n := rng.Intn(12) + 1
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('a' + rng.Intn(8)) // small alphabet → lots of shared prefixes
		}
		return b
	}

	for i := 0; i < ops; i++ {
		key := randKey()
		val := []byte(fmt.Sprintf("v%d", i))

		switch rng.Intn(3) {
		case 0: // Insert
			tr.Insert(key, val)
			ref[string(key)] = val

		case 1: // Get
			v, ok := tr.Get(key)
			rv, rok := ref[string(key)]
			if ok != rok {
				t.Fatalf("op %d Get(%q): tree=%v ref=%v", i, key, ok, rok)
			}
			if ok && string(v.([]byte)) != string(rv) {
				t.Fatalf("op %d Get(%q): tree=%q ref=%q", i, key, v, rv)
			}

		case 2: // Delete
			got := tr.Delete(key)
			_, had := ref[string(key)]
			if got != had {
				t.Fatalf("op %d Delete(%q): tree=%v ref=%v", i, key, got, had)
			}
			delete(ref, string(key))
		}
	}

	// Final size check.
	if tr.Len() != len(ref) {
		t.Fatalf("final size: tree=%d ref=%d", tr.Len(), len(ref))
	}

	// Iterator must yield exactly the ref keys in sorted order.
	var wantKeys []string
	for k := range ref {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)

	var gotKeys []string
	next := tr.Iterator()
	for {
		k, _, ok := next()
		if !ok {
			break
		}
		gotKeys = append(gotKeys, string(k))
	}

	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterator count: got %d want %d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("iterator pos %d: got %q want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}
