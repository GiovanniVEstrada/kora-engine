// Package store implements a Bitcask-style append-only key-value store. Writes
// are appended to an active log segment; when it fills, it is rolled over and a
// new active segment is opened. An in-memory "keydir" maps each key to the
// {segment, offset, length} of its newest record, so reads are a single seek.
// Stale and deleted data is reclaimed by compaction (see compaction.go).
package store

import (
	"bytes"
	"errors"
	"os"
	"sync"

	"github.com/giova/strata-engine/internal/record"
)

// DefaultMaxSegmentBytes is the active-segment size at which a rollover happens.
const DefaultMaxSegmentBytes int64 = 4 << 20 // 4 MiB

// entry is a keydir value: which segment holds the newest record for a key, and
// where in that segment it lives.
type entry struct {
	fileID uint32
	offset int64
	length int
}

// Options configures a DB at open time.
type Options struct {
	// SyncOnWrite calls fsync after every write before returning. Safe but slow.
	// See DESIGN.md for the durability/throughput tradeoff.
	SyncOnWrite bool
	// MaxSegmentBytes is the active-segment size threshold for rollover. When
	// <= 0, DefaultMaxSegmentBytes is used.
	MaxSegmentBytes int64
}

// DefaultOptions favors durability: every write is fsynced before it is
// acknowledged.
func DefaultOptions() Options {
	return Options{SyncOnWrite: true, MaxSegmentBytes: DefaultMaxSegmentBytes}
}

// DB is a segmented append-only key-value store. It is safe for concurrent use:
// reads take a shared lock (held across the disk read so compaction can swap
// segments safely), writes take an exclusive lock, and only one compaction runs
// at a time.
type DB struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	segments map[uint32]*segment // all readable segments, including active
	active   *segment            // current append target (always the max id)
	keydir   map[string]entry
	nextID   uint32

	compactMu sync.Mutex // serializes compaction; held outside mu
}

// Open opens (or creates) a store in dir, rebuilding the in-memory index by
// scanning every segment from oldest to newest.
func Open(dir string, opts Options) (*DB, error) {
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:      dir,
		opts:     opts,
		segments: make(map[uint32]*segment),
		keydir:   make(map[string]entry),
	}

	if err := db.load(); err != nil {
		db.closeAll()
		return nil, err
	}
	return db, nil
}

// load discovers existing segments and rebuilds the keydir. Segments are
// scanned in ascending id order, which equals ascending recency (see
// DESIGN.md), so later records simply win and tombstones remove keys.
func (db *DB) load() error {
	ids, err := listSegmentIDs(db.dir)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		// Fresh store: create the first active segment.
		seg, err := createSegment(db.dir, 1)
		if err != nil {
			return err
		}
		db.segments[1] = seg
		db.active = seg
		db.nextID = 2
		return nil
	}

	for i, id := range ids {
		isActive := i == len(ids)-1

		var seg *segment
		if isActive {
			seg, err = createSegment(db.dir, id)
		} else {
			seg, err = openSegmentReadOnly(db.dir, id)
		}
		if err != nil {
			return err
		}
		db.segments[id] = seg

		end, serr := scanSegment(seg.path, func(rec record.Record, off int64, _ int) error {
			if record.IsTombstone(rec) {
				delete(db.keydir, string(rec.Key))
			} else {
				db.keydir[string(rec.Key)] = entry{fileID: id, offset: off, length: record.Size(rec)}
			}
			return nil
		})

		switch {
		case serr == errPartialTail:
			if !isActive {
				// Only the newest segment should ever have a partial tail.
				return errors.New("store: partial record in immutable segment " + seg.path)
			}
			if terr := seg.f.Truncate(end); terr != nil {
				return terr
			}
			seg.size = end
		case serr != nil:
			return serr
		default:
			seg.size = end
		}

		if isActive {
			db.active = seg
			db.nextID = id + 1
		}
	}
	return nil
}

// Set stores value under key. A nil value is normalized to empty so it is not
// mistaken for a delete; use Delete to remove a key.
func (db *DB) Set(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	var buf bytes.Buffer
	if err := record.Encode(&buf, key, value); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.maybeRollover(); err != nil {
		return err
	}
	off := db.active.size
	if err := db.appendActive(buf.Bytes()); err != nil {
		return err
	}
	db.keydir[string(key)] = entry{fileID: db.active.id, offset: off, length: buf.Len()}
	return nil
}

// Delete removes key by appending a tombstone and dropping it from the keydir.
func (db *DB) Delete(key []byte) error {
	var buf bytes.Buffer
	if err := record.Encode(&buf, key, nil); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.maybeRollover(); err != nil {
		return err
	}
	if err := db.appendActive(buf.Bytes()); err != nil {
		return err
	}
	delete(db.keydir, string(key))
	return nil
}

// appendActive writes b to the active segment and fsyncs if configured.
// Caller must hold db.mu.
func (db *DB) appendActive(b []byte) error {
	if err := db.active.append(b); err != nil {
		return err
	}
	if db.opts.SyncOnWrite {
		if err := db.active.sync(); err != nil {
			return err
		}
	}
	return nil
}

// maybeRollover closes the active segment and opens a new one if it has reached
// the size threshold. Caller must hold db.mu.
func (db *DB) maybeRollover() error {
	if db.active.size < db.opts.MaxSegmentBytes {
		return nil
	}
	// Make the active segment immutable: flush, drop the append handle, and
	// reopen it read-only so in-flight and future reads still work.
	old := db.active
	if err := old.sync(); err != nil {
		return err
	}
	if err := old.close(); err != nil {
		return err
	}
	ro, err := openSegmentReadOnly(db.dir, old.id)
	if err != nil {
		return err
	}
	db.segments[old.id] = ro

	na, err := createSegment(db.dir, db.nextID)
	if err != nil {
		return err
	}
	db.segments[na.id] = na
	db.active = na
	db.nextID++
	return nil
}

// Get returns the value for key. The second return is false if the key is
// absent (never set, or deleted). The read lock is held across the disk read so
// a concurrent compaction cannot close the segment out from under us.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	e, ok := db.keydir[string(key)]
	if !ok {
		return nil, false, nil
	}
	seg := db.segments[e.fileID]
	if seg == nil {
		return nil, false, errors.New("store: keydir references missing segment")
	}

	buf := make([]byte, e.length)
	if err := seg.readAt(buf, e.offset); err != nil {
		return nil, false, err
	}
	rec, err := record.Decode(bytes.NewReader(buf))
	if err != nil {
		return nil, false, err
	}
	return rec.Value, true, nil
}

// Len returns the number of live keys currently indexed.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.keydir)
}

// SegmentCount returns the number of segment files currently open.
func (db *DB) SegmentCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.segments)
}

// DiskUsage returns the total size in bytes of all segment files.
func (db *DB) DiskUsage() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, seg := range db.segments {
		total += seg.size
	}
	return total
}

// Close flushes and closes all segment files. The DB must not be used after.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeAll()
}

// closeAll closes every open segment handle. Caller must hold db.mu.
func (db *DB) closeAll() error {
	var firstErr error
	for _, seg := range db.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.segments = nil
	db.active = nil
	return firstErr
}
