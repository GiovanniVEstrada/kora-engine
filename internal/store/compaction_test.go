package store_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/giova/strata-engine/internal/store"
)

// smallSegOpts forces frequent rollover so tests exercise multiple segments
// without writing megabytes.
func smallSegOpts() store.Options {
	return store.Options{SyncOnWrite: false, MaxSegmentBytes: 1024}
}

func TestRollover(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 50; i++ {
		if err := db.Set([]byte(fmt.Sprintf("key%02d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	if db.SegmentCount() < 2 {
		t.Fatalf("expected rollover into multiple segments, got %d", db.SegmentCount())
	}

	// Every key must still be readable across segment boundaries.
	for i := 0; i < 50; i++ {
		got, ok, err := db.Get([]byte(fmt.Sprintf("key%02d", i)))
		if err != nil || !ok || !bytes.Equal(got, val) {
			t.Fatalf("key%02d: ok=%v err=%v len=%d", i, ok, err, len(got))
		}
	}
}

func TestCompactionReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("y"), 200)
	// Overwrite the same 5 keys many times -> lots of stale data across segments.
	for round := 0; round < 100; round++ {
		for k := 0; k < 5; k++ {
			if err := db.Set([]byte(fmt.Sprintf("k%d", k)), val); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := db.DiskUsage()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := db.DiskUsage()

	if after >= before {
		t.Fatalf("expected disk usage to drop after compaction: before=%d after=%d", before, after)
	}
	// All 5 live keys must survive with correct values.
	for k := 0; k < 5; k++ {
		got, ok, err := db.Get([]byte(fmt.Sprintf("k%d", k)))
		if err != nil || !ok || !bytes.Equal(got, val) {
			t.Fatalf("k%d after compaction: ok=%v err=%v", k, ok, err)
		}
	}
}

func TestCompactionDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}

	val := string(bytes.Repeat([]byte("z"), 200))
	for i := 0; i < 30; i++ {
		mustSet(t, db, fmt.Sprintf("d%02d", i), val)
	}
	// Delete half of them.
	for i := 0; i < 30; i += 2 {
		mustDelete(t, db, fmt.Sprintf("d%02d", i))
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	check := func(db *store.DB) {
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("d%02d", i)
			_, ok, err := db.Get([]byte(key))
			if err != nil {
				t.Fatal(err)
			}
			wantPresent := i%2 == 1
			if ok != wantPresent {
				t.Fatalf("%s: present=%v, want %v", key, ok, wantPresent)
			}
		}
	}
	check(db)
	db.Close()

	// Deleted keys must stay gone after reopen (tombstones not resurrected).
	db2, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	check(db2)
}

// TestOracleWithRolloverAndCompaction is the strongest M2 correctness check:
// random ops with frequent rollover and periodic compaction, verified against a
// map oracle, and re-verified after a reopen.
func TestOracleWithRolloverAndCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}

	oracle := make(map[string]string)
	rng := rand.New(rand.NewSource(7))
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	assert := func(db *store.DB) {
		t.Helper()
		for _, k := range keys {
			got, ok, err := db.Get([]byte(k))
			if err != nil {
				t.Fatal(err)
			}
			want, wantOK := oracle[k]
			if ok != wantOK {
				t.Fatalf("key %q: engine ok=%v oracle ok=%v", k, ok, wantOK)
			}
			if wantOK && string(got) != want {
				t.Fatalf("key %q: engine=%q oracle=%q", k, got, want)
			}
		}
	}

	for i := 0; i < 8000; i++ {
		k := keys[rng.Intn(len(keys))]
		if rng.Intn(4) == 0 {
			mustDelete(t, db, k)
			delete(oracle, k)
		} else {
			v := fmt.Sprintf("val-%d-%d", i, rng.Intn(1000))
			mustSet(t, db, k, v)
			oracle[k] = v
		}
		if i%1000 == 999 {
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			assert(db)
		}
	}
	assert(db)
	db.Close()

	db2, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	assert(db2)
}

// TestConcurrentReadsDuringCompaction asserts the safe-swap property: reads keep
// returning correct values while compaction runs. Run with -race.
func TestConcurrentReadsDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, smallSegOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := string(bytes.Repeat([]byte("c"), 100))
	for i := 0; i < 100; i++ {
		mustSet(t, db, fmt.Sprintf("key%03d", i), val)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers hammering Get.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.Intn(100)
				got, ok, err := db.Get([]byte(fmt.Sprintf("key%03d", i)))
				if err != nil {
					t.Errorf("Get error: %v", err)
					return
				}
				if !ok || string(got) != val {
					t.Errorf("key%03d: ok=%v len=%d", i, ok, len(got))
					return
				}
			}
		}()
	}

	// Compact repeatedly while reads run.
	for c := 0; c < 5; c++ {
		if err := db.Compact(); err != nil {
			t.Error(err)
			break
		}
	}
	close(stop)
	wg.Wait()
}

func TestCompactNoImmutableSegmentsIsNoop(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustSet(t, db, "a", "1")
	before := db.SegmentCount()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.SegmentCount() != before {
		t.Fatalf("compaction with no immutable segments changed segment count: %d -> %d", before, db.SegmentCount())
	}
	got, ok, _ := db.Get([]byte("a"))
	if !ok || !bytes.Equal(got, []byte("1")) {
		t.Fatal("value lost after no-op compaction")
	}
}
