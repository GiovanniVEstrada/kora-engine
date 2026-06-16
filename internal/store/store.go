// Package store implements a Bitcask-style append-only key-value store: writes
// are appended to a single immutable-on-disk log, and an in-memory "keydir"
// maps each key to the offset+length of its newest record so reads are a single
// seek.
package store

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/giova/strata-engine/internal/record"
)

// DataFileName is the single append-only log file used in M1. M2 splits this
// into multiple rolled-over segments.
const DataFileName = "data.log"

// entry is a keydir value: where the newest record for a key lives on disk.
type entry struct {
	offset int64
	length int
}

// Options configures a DB at open time.
type Options struct {
	// SyncOnWrite calls fsync after every write before returning. Safe but slow.
	// When false, writes are durable only once the OS flushes its page cache,
	// trading durability for throughput. See DESIGN.md.
	SyncOnWrite bool
}

// DefaultOptions favors durability: every write is fsynced before it is
// acknowledged, matching the project's "never lose an acknowledged write" goal.
func DefaultOptions() Options {
	return Options{SyncOnWrite: true}
}

// DB is a single-file append-only key-value store. It is safe for concurrent
// use: reads take a shared lock, writes take an exclusive lock.
type DB struct {
	mu     sync.RWMutex
	path   string
	f      *os.File // active data file, opened for append + ReadAt
	offset int64    // current end-of-file offset (next append lands here)
	keydir map[string]entry
	opts   Options
}

// Open opens (or creates) a store in dir, rebuilding the in-memory index by
// scanning the data file front to back.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, DataFileName)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	db := &DB{
		path:   path,
		f:      f,
		keydir: make(map[string]entry),
		opts:   opts,
	}

	if err := db.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return db, nil
}

// recover scans the data file from the start, rebuilding the keydir. Later
// records win; tombstones remove keys. A partial trailing record (from a write
// interrupted mid-flush) is truncated away so the append offset stays
// consistent with the file.
func (db *DB) recover() error {
	rf, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer rf.Close()

	br := bufio.NewReader(rf)
	var off int64
	for {
		rec, err := record.Decode(br)
		if err == io.EOF {
			break // clean end of log
		}
		if err == io.ErrUnexpectedEOF {
			// Trailing record was only partially written (e.g. crash mid-write).
			// Discard it and treat the log as ending here.
			if terr := db.f.Truncate(off); terr != nil {
				return terr
			}
			break
		}
		if err != nil {
			return err // ErrCorrupted mid-file or an I/O error: surface it
		}

		size := record.Size(rec)
		if record.IsTombstone(rec) {
			delete(db.keydir, string(rec.Key))
		} else {
			db.keydir[string(rec.Key)] = entry{offset: off, length: size}
		}
		off += int64(size)
	}

	db.offset = off
	return nil
}

// Set stores value under key. A nil value is normalized to an empty value so it
// is not mistaken for a delete; use Delete to remove a key.
func (db *DB) Set(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	var buf bytes.Buffer
	if err := record.Encode(&buf, key, value); err != nil {
		return err
	}
	if _, err := db.f.Write(buf.Bytes()); err != nil {
		return err
	}
	if db.opts.SyncOnWrite {
		if err := db.f.Sync(); err != nil {
			return err
		}
	}

	db.keydir[string(key)] = entry{offset: db.offset, length: buf.Len()}
	db.offset += int64(buf.Len())
	return nil
}

// Get returns the value for key. The second return is false if the key is
// absent (never set, or deleted).
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	e, ok := db.keydir[string(key)]
	db.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}

	buf := make([]byte, e.length)
	if _, err := db.f.ReadAt(buf, e.offset); err != nil {
		return nil, false, err
	}
	rec, err := record.Decode(bytes.NewReader(buf))
	if err != nil {
		return nil, false, err
	}
	return rec.Value, true, nil
}

// Delete removes key by appending a tombstone and dropping it from the keydir.
// Deleting an absent key is a no-op (still durably logged).
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var buf bytes.Buffer
	if err := record.Encode(&buf, key, nil); err != nil {
		return err
	}
	if _, err := db.f.Write(buf.Bytes()); err != nil {
		return err
	}
	if db.opts.SyncOnWrite {
		if err := db.f.Sync(); err != nil {
			return err
		}
	}

	delete(db.keydir, string(key))
	db.offset += int64(buf.Len())
	return nil
}

// Keys returns the number of live keys currently indexed.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.keydir)
}

// Close flushes and closes the underlying data file. The DB must not be used
// afterward.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.f == nil {
		return errors.New("store: already closed")
	}
	err := db.f.Close()
	db.f = nil
	return err
}
