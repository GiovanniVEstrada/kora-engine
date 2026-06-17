// Package store implements a log-structured key-value store.
//
// M1/M2: Bitcask model — append-only segments, in-memory keydir.
// M3c:   LSM model — writes land in an in-memory ART memtable first; when the
//        memtable exceeds MaxMemBytes it is flushed to an immutable SSTable on
//        disk and cleared. Reads check the memtable first, then SSTables newest
//        → oldest. Segments are still written on every Set/Delete and replayed
//        on Open to rebuild the memtable; they act as the durability log until
//        a proper WAL is introduced in M4.
package store

import (
	"bytes"
	"errors"
	"os"
	"sync"

	"github.com/giova/kora-engine/internal/art"
	"github.com/giova/kora-engine/internal/record"
	"github.com/giova/kora-engine/internal/sstable"
)

// DefaultMaxSegmentBytes is the active-segment size at which a rollover happens.
const DefaultMaxSegmentBytes int64 = 4 << 20 // 4 MiB

// DefaultMaxMemBytes is the memtable size at which a flush to SSTable happens.
const DefaultMaxMemBytes int64 = 4 << 20 // 4 MiB

// tombstone is stored in the memtable as the value for a deleted key.
// Using a distinct type (not nil, not []byte{}) lets us distinguish "deleted"
// from "set to empty string" and ensures a memtable tombstone correctly shadows
// a live value in an older SSTable during multi-source reads.
type tombstone struct{}

// Options configures a DB at open time.
type Options struct {
	// SyncOnWrite calls fsync after every write before returning.
	SyncOnWrite bool
	// MaxSegmentBytes is the active-segment size threshold for rollover.
	// When <= 0, DefaultMaxSegmentBytes is used.
	MaxSegmentBytes int64
	// MaxMemBytes is the memtable byte-size threshold that triggers a flush to
	// SSTable. When <= 0, DefaultMaxMemBytes is used.
	MaxMemBytes int64
}

// DefaultOptions favors durability: every write is fsynced.
func DefaultOptions() Options {
	return Options{SyncOnWrite: true}
}

// DB is a segmented, log-structured key-value store with an in-memory ART
// memtable. It is safe for concurrent use.
type DB struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	segments map[uint32]*segment
	active   *segment
	order    []uint32
	nextID   uint32

	compactMu sync.Mutex

	// M3c memtable layer.
	memtable  *art.Tree          // key → []byte (live) or tombstone{} (deleted)
	memSize   int64              // approximate bytes in the memtable
	liveKeys  int                // exact count of live (non-tombstone) keys
	ssReaders []*sstable.Reader  // open SSTables, newest first
	ssNextID  uint32             // next SSTable file id within this session
}

// Open opens (or creates) a store in dir, rebuilding the in-memory memtable by
// scanning every segment from oldest to newest.
func Open(dir string, opts Options) (*DB, error) {
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if opts.MaxMemBytes <= 0 {
		opts.MaxMemBytes = DefaultMaxMemBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:      dir,
		opts:     opts,
		segments: make(map[uint32]*segment),
		memtable: &art.Tree{},
		ssNextID: 1,
	}

	// Clean up SSTable files from a previous session — they are ephemeral in
	// M3c because we always rebuild the memtable from the durable segment log.
	db.cleanupSSTDir()

	if err := db.load(); err != nil {
		db.closeAll()
		return nil, err
	}
	return db, nil
}

// load rebuilds in-memory state at startup via the manifest.
func (db *DB) load() error {
	m, err := readManifest(db.dir)
	switch {
	case err == errNoManifest:
		return db.bootstrap()
	case err != nil:
		return err
	}
	if len(m.order) == 0 {
		return errManifestCorrupt
	}
	if err := db.openAndScan(m.order); err != nil {
		return err
	}
	db.nextID = m.nextID
	db.cleanupLeakedSegments()
	return nil
}

// bootstrap creates the manifest for a store that doesn't have one.
func (db *DB) bootstrap() error {
	ids, err := listSegmentIDs(db.dir)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		seg, err := createSegment(db.dir, 1)
		if err != nil {
			return err
		}
		db.segments[1] = seg
		db.active = seg
		db.order = []uint32{1}
		db.nextID = 2
		return db.writeManifestLocked()
	}
	if err := db.openAndScan(ids); err != nil {
		return err
	}
	db.nextID = ids[len(ids)-1] + 1
	return db.writeManifestLocked()
}

// openAndScan opens every segment in order and replays records to rebuild the
// memtable. Later records win; tombstones remove keys.
func (db *DB) openAndScan(order []uint32) error {
	for i, id := range order {
		isActive := i == len(order)-1

		var seg *segment
		var err error
		if isActive {
			seg, err = createSegment(db.dir, id)
		} else {
			seg, err = openSegmentReadOnly(db.dir, id)
		}
		if err != nil {
			return err
		}
		db.segments[id] = seg

		end, serr := scanSegment(seg.path, func(rec record.Record, _ int64, _ int) error {
			if record.IsTombstone(rec) {
				raw, wasPresent := db.memtable.Get(rec.Key)
				if wasPresent {
					if _, wasTomb := raw.(tombstone); !wasTomb {
						db.liveKeys--
					}
				}
				db.memtable.Insert(rec.Key, tombstone{})
			} else {
				raw, wasPresent := db.memtable.Get(rec.Key)
				if !wasPresent {
					db.liveKeys++
				} else if _, wasTomb := raw.(tombstone); wasTomb {
					db.liveKeys++ // key was deleted, now restored
				}
				cp := make([]byte, len(rec.Value))
				copy(cp, rec.Value)
				db.memtable.Insert(rec.Key, cp)
				db.memSize += int64(len(rec.Key) + len(rec.Value))
			}
			return nil
		})

		switch {
		case serr == errPartialTail:
			if !isActive {
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
		}
	}
	db.order = append([]uint32(nil), order...)
	return nil
}

func (db *DB) writeManifestLocked() error {
	return writeManifest(db.dir, manifest{nextID: db.nextID, order: db.order})
}

func (db *DB) cleanupLeakedSegments() {
	ids, err := listSegmentIDs(db.dir)
	if err != nil {
		return
	}
	live := make(map[uint32]bool, len(db.order))
	for _, id := range db.order {
		live[id] = true
	}
	for _, id := range ids {
		if !live[id] {
			os.Remove(segmentPath(db.dir, id))
		}
	}
}

// cleanupSSTDir removes all *.sst files written by a previous session.
func (db *DB) cleanupSSTDir() {
	entries, err := os.ReadDir(db.sstDir())
	if err != nil {
		return // directory doesn't exist yet — fine
	}
	for _, e := range entries {
		if !e.IsDir() {
			os.Remove(db.sstFilePath(0)) // path is only used for the dir prefix
			os.Remove(db.sstDir() + "/" + e.Name())
		}
	}
}

// Set stores value under key, writing to the segment log (durability) and
// updating the memtable. A nil value is normalised to empty.
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
	if err := db.appendActive(buf.Bytes()); err != nil {
		return err
	}

	if !db.isLiveKey(key) {
		db.liveKeys++
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	db.memtable.Insert(key, cp)
	db.memSize += int64(len(key) + len(value))

	if db.memSize >= db.opts.MaxMemBytes {
		return db.flushMemtable()
	}
	return nil
}

// Delete removes key by appending a tombstone and marking it deleted in the
// memtable. A tombstone in the memtable correctly shadows live values in older
// SSTables during multi-source reads.
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

	if db.isLiveKey(key) {
		db.liveKeys--
	}
	db.memtable.Insert(key, tombstone{})
	db.memSize += int64(len(key) + 1)

	if db.memSize >= db.opts.MaxMemBytes {
		return db.flushMemtable()
	}
	return nil
}

// appendActive writes b to the active segment and fsyncs if configured.
func (db *DB) appendActive(b []byte) error {
	if err := db.active.append(b); err != nil {
		return err
	}
	if db.opts.SyncOnWrite {
		return db.active.sync()
	}
	return nil
}

// maybeRollover opens a new active segment if the current one is at capacity.
func (db *DB) maybeRollover() error {
	if db.active.size < db.opts.MaxSegmentBytes {
		return nil
	}
	old := db.active
	if err := old.sync(); err != nil {
		return err
	}
	na, err := createSegment(db.dir, db.nextID)
	if err != nil {
		return err
	}
	newOrder := append(append([]uint32(nil), db.order...), na.id)
	if err := writeManifest(db.dir, manifest{nextID: db.nextID + 1, order: newOrder}); err != nil {
		na.close()
		os.Remove(segmentPath(db.dir, na.id))
		return err
	}
	db.segments[na.id] = na
	db.active = na
	db.order = newOrder
	db.nextID++
	return nil
}

// Get returns the value for key. It checks the memtable first, then SSTables
// from newest to oldest. A tombstone in any layer returns (nil, false, nil).
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Memtable check (in-memory ART — O(key length)).
	raw, ok := db.memtable.Get(key)
	if ok {
		if _, isTomb := raw.(tombstone); isTomb {
			return nil, false, nil
		}
		v := raw.([]byte)
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp, true, nil
	}

	// SSTable check, newest first. RawGet distinguishes "absent" from "tombstone"
	// so a delete in a newer SSTable correctly shadows a value in an older one.
	for _, sr := range db.ssReaders {
		val, found, isTomb, err := sr.RawGet(key)
		if err != nil {
			return nil, false, err
		}
		if isTomb {
			return nil, false, nil
		}
		if found {
			return val, true, nil
		}
	}

	return nil, false, nil
}

// isLiveKey reports whether key currently has a live value in the memtable or
// any SSTable. Caller must hold db.mu.
func (db *DB) isLiveKey(key []byte) bool {
	raw, ok := db.memtable.Get(key)
	if ok {
		_, isTomb := raw.(tombstone)
		return !isTomb
	}
	for _, sr := range db.ssReaders {
		_, found, isTomb, _ := sr.RawGet(key)
		if isTomb {
			return false
		}
		if found {
			return true
		}
	}
	return false
}

// Len returns the number of live (non-deleted) keys.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.liveKeys
}

// SSTableCount returns the number of SSTable files currently open.
func (db *DB) SSTableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.ssReaders)
}

// SegmentCount returns the number of segment files currently open.
func (db *DB) SegmentCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.segments)
}

// DiskUsage returns the total byte size of all segment files.
func (db *DB) DiskUsage() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, seg := range db.segments {
		total += seg.size
	}
	return total
}

// Close flushes and closes all segment and SSTable files.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, sr := range db.ssReaders {
		sr.Close()
	}
	db.ssReaders = nil
	return db.closeAll()
}

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
