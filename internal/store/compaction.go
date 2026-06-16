package store

import (
	"bytes"
	"os"

	"github.com/giova/strata-engine/internal/record"
)

// Compact merges all current immutable segments into a single fresh segment,
// keeping only the newest value per key and dropping tombstones. It reclaims
// space taken by overwritten and deleted records.
//
// The heavy work — reading every immutable segment and writing the merged
// output — runs without holding the write lock, so reads (and writes to the
// active segment) keep working throughout. Only the final swap takes the write
// lock briefly. Just one compaction runs at a time.
//
// Correctness rests on the invariant that ascending segment id == ascending
// recency (see DESIGN.md): the merged output reuses the *minimum* id of its
// inputs, so it stays older than the active segment and any segment created by
// a rollover while compaction was running.
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// 1. Snapshot the set of immutable segments (everything but the active one).
	db.mu.RLock()
	activeID := db.active.id
	var snap []uint32
	for id := range db.segments {
		if id != activeID {
			snap = append(snap, id)
		}
	}
	db.mu.RUnlock()

	if len(snap) == 0 {
		return nil // nothing to compact
	}
	sortIDs(snap)
	minID := snap[0]
	snapSet := make(map[uint32]bool, len(snap))
	for _, id := range snap {
		snapSet[id] = true
	}

	// 2. Merge (lock-free): scan snapshot segments oldest→newest, keeping the
	//    latest record per key. These segments are immutable, so reading them
	//    without a lock is safe.
	latest := make(map[string]record.Record)
	for _, id := range snap {
		if _, err := scanSegment(segmentPath(db.dir, id), func(rec record.Record, _ int64, _ int) error {
			latest[string(rec.Key)] = rec // later scan wins
			return nil
		}); err != nil {
			return err
		}
	}

	// 3. Write the live (non-tombstone) records to a temp file, recording where
	//    each landed. Tombstones are dropped: we merged every segment older than
	//    the active one, so a delete here has no older value left to mask.
	tmpPath := segmentPath(db.dir, minID) + ".compacting"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	newEntries := make(map[string]entry)
	var off int64
	for key, rec := range latest {
		if record.IsTombstone(rec) {
			continue
		}
		var buf bytes.Buffer
		if err := record.EncodeAt(&buf, rec.Timestamp, rec.Key, rec.Value); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		if _, err := tmp.Write(buf.Bytes()); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		newEntries[key] = entry{fileID: minID, offset: off, length: buf.Len()}
		off += int64(buf.Len())
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// 4. Swap (locked): repoint the keydir, drop the old segments, and install
	//    the merged one. Fast relative to the merge above.
	db.mu.Lock()
	defer db.mu.Unlock()

	// Only repoint keys whose newest record still lives in a snapshot segment.
	// If a key was overwritten or deleted in the active (or a post-snapshot)
	// segment while we merged, that newer write must win — leave it alone.
	for key, ne := range newEntries {
		if cur, ok := db.keydir[key]; ok && snapSet[cur.fileID] {
			db.keydir[key] = ne
		}
	}

	// Close and remove the old snapshot segment files.
	for _, id := range snap {
		if seg := db.segments[id]; seg != nil {
			seg.close()
			delete(db.segments, id)
		}
	}
	if err := os.Remove(segmentPath(db.dir, minID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tmpPath, segmentPath(db.dir, minID)); err != nil {
		return err
	}
	for _, id := range snap[1:] {
		if err := os.Remove(segmentPath(db.dir, id)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	merged, err := openSegmentReadOnly(db.dir, minID)
	if err != nil {
		return err
	}
	db.segments[minID] = merged
	return nil
}
