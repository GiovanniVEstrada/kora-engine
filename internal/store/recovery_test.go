package store_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/giova/kora-engine/internal/store"
)

// crashWriterMain is called when the test binary is invoked with
// CRASH_WRITER=1. It opens a store, writes keys, and exits without calling
// db.Close() (simulating a crash). os.Exit bypasses all deferred calls.
func init() {
	if os.Getenv("CRASH_WRITER") != "1" {
		return
	}
	dir := os.Getenv("CRASH_DIR")
	n, _ := strconv.Atoi(os.Getenv("CRASH_KEYS"))
	if dir == "" || n == 0 {
		os.Exit(2)
	}
	maxMem, _ := strconv.ParseInt(os.Getenv("CRASH_MAXMEM"), 10, 64)
	if maxMem == 0 {
		maxMem = store.DefaultMaxMemBytes
	}

	db, err := store.Open(dir, store.Options{SyncOnWrite: true, MaxMemBytes: maxMem})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
			fmt.Fprintln(os.Stderr, "set:", err)
			os.Exit(1)
		}
	}
	// Intentionally do NOT call db.Close() — simulate crash by exiting without
	// flushing. All writes are fsynced to the WAL, so data must survive.
	os.Exit(0)
}

// TestFlushDurability verifies that SSTables survive a clean restart. After a
// close-and-reopen, all keys written before the close must be present, and the
// SSTable count must be non-zero (data was actually flushed, not just in WAL).
func TestFlushDurability(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write enough keys to trigger several flushes.
	{
		db, err := store.Open(dir, store.Options{SyncOnWrite: true, MaxMemBytes: 8})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			if err := db.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		if db.SSTableCount() == 0 {
			t.Fatal("expected at least one SSTable after 50 writes with MaxMemBytes=8")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 2: reopen and verify all keys.
	{
		db, err := store.Open(dir, store.Options{SyncOnWrite: true, MaxMemBytes: 8})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db.Close()

		if db.SSTableCount() == 0 {
			t.Fatal("SSTables did not survive restart — M4 checkpoint not working")
		}
		if db.Len() != 50 {
			t.Fatalf("Len() = %d after restart, want 50", db.Len())
		}
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("k%04d", i)
			v, ok, err := db.Get([]byte(k))
			if err != nil {
				t.Fatalf("Get(%q): %v", k, err)
			}
			if !ok {
				t.Fatalf("Get(%q): missing after restart", k)
			}
			if want := fmt.Sprintf("v%04d", i); string(v) != want {
				t.Fatalf("Get(%q): got %q, want %q", k, v, want)
			}
		}
	}
}

// TestCrashRecovery runs a subprocess that writes keys and exits without
// closing the DB (simulating kill -9 / hard crash). Verifies that all
// fsynced writes are present on the next open. Two scenarios:
//
//   - Small memtable: most writes are flushed to SSTables during the write
//     loop; a few unflushed writes are in the WAL only.
//   - Large memtable: all writes are in the WAL only (no flush triggered).
func TestCrashRecovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxMem  int64
		nKeys   int
	}{
		{"flushed_to_sst", 8, 50},
		{"wal_only", store.DefaultMaxMemBytes, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			// Run the subprocess writer.
			cmd := exec.Command(os.Args[0], "-test.run=^$") // match nothing: init() handles it
			cmd.Env = append(os.Environ(),
				"CRASH_WRITER=1",
				"CRASH_DIR="+dir,
				fmt.Sprintf("CRASH_KEYS=%d", tc.nKeys),
				fmt.Sprintf("CRASH_MAXMEM=%d", tc.maxMem),
			)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("crash writer subprocess failed: %v\noutput: %s", err, out)
			}

			// Reopen and verify all keys survived.
			db, err := store.Open(dir, store.Options{SyncOnWrite: true})
			if err != nil {
				t.Fatalf("reopen after crash: %v", err)
			}
			defer db.Close()

			for i := 0; i < tc.nKeys; i++ {
				k := fmt.Sprintf("k%04d", i)
				v, ok, err := db.Get([]byte(k))
				if err != nil {
					t.Fatalf("Get(%q): %v", k, err)
				}
				if !ok {
					t.Fatalf("Get(%q): missing after crash recovery", k)
				}
				if want := fmt.Sprintf("v%04d", i); string(v) != want {
					t.Fatalf("Get(%q): got %q, want %q", k, v, want)
				}
			}
			if got := db.Len(); got != tc.nKeys {
				t.Fatalf("Len() = %d after crash recovery, want %d", got, tc.nKeys)
			}
		})
	}
}
