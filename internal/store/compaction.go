package store

import (
	"bytes"
	"os"

	"github.com/giova/kora-engine/internal/record"
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
// Crash/failure safety: the merged output is written to a brand-new segment id
// (never overwriting an existing file, which also sidesteps Windows' open-handle
// restrictions). The swap is committed by atomically replacing the manifest;
// everything before that point is invisible to recovery, and everything after
// is durable. In-memory state is mutated only once the new segment is open and
// the manifest is committed, so a failure mid-swap never leaves the keydir
// pointing at a segment that isn't present.
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// 1. Snapshot the immutable segments (everything but the active one). Only
	//    reserve a fresh id once we know there's something to compact, so a
	//    no-op call doesn't advance nextID. (A failed compaction may still
	//    advance it in memory, but that isn't persisted until the manifest
	//    commit, so it self-heals on the next Open.)
	db.mu.Lock()
	activeID := db.active.id
	var snap []uint32
	for _, id := range db.order {
		if id != activeID {
			snap = append(snap, id)
		}
	}
	if len(snap) == 0 {
		db.mu.Unlock()
		return nil // nothing to compact
	}
	newID := db.nextID
	db.nextID++
	db.mu.Unlock()

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

	// 3. Write live (non-tombstone) records to a temp file.
	//    Tombstones are dropped: we merged every segment older than the active
	//    one, so a delete here has no older value left to mask.
	finalPath := segmentPath(db.dir, newID)
	tmpPath := finalPath + ".compacting"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range latest {
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
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// 4. Swap (locked). Do every fallible step (open the merged segment, commit
	//    the manifest) before mutating the keydir / segment set, so a failure
	//    leaves the DB exactly as it was.
	db.mu.Lock()
	defer db.mu.Unlock()

	merged, err := openSegmentReadOnly(db.dir, newID)
	if err != nil {
		os.Remove(finalPath) // leaked output; manifest never referenced it
		return err
	}

	// New recency order: merged (oldest) first, then everything that was newer
	// than the snapshot (post-snapshot rollovers + the active segment).
	newOrder := []uint32{newID}
	for _, id := range db.order {
		if !snapSet[id] {
			newOrder = append(newOrder, id)
		}
	}

	// Commit point: atomically replace the manifest. Until this succeeds the
	// merged file is invisible to recovery.
	oldOrder := db.order
	db.order = newOrder
	if err := db.writeManifestLocked(); err != nil {
		db.order = oldOrder // roll back in-memory order
		merged.close()
		os.Remove(finalPath)
		return err
	}

	// Manifest committed — register the merged segment. The memtable already
	// holds all live values directly, so no memtable update is needed here.
	db.segments[newID] = merged

	// Retire the old snapshot segments (best-effort file removal — they're no
	// longer in the manifest, so a failure only leaks disk).
	for _, id := range snap {
		if seg := db.segments[id]; seg != nil {
			seg.close()
			delete(db.segments, id)
		}
		os.Remove(segmentPath(db.dir, id))
	}
	return nil
}
