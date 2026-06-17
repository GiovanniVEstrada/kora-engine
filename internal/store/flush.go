package store

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/giova/kora-engine/internal/art"
	"github.com/giova/kora-engine/internal/sstable"
)

func (db *DB) sstDir() string { return filepath.Join(db.dir, "sst") }

func (db *DB) sstFilePath(id uint32) string {
	return filepath.Join(db.sstDir(), fmt.Sprintf("%06d.sst", id))
}

// flushMemtable writes the current memtable to an SSTable file and checkpoints
// the WAL. After the flush:
//   - All prior WAL segments are superseded by the new SSTable.
//   - A fresh empty WAL segment is opened as the new active.
//   - The MANIFEST is atomically updated to record the new SSTable and the new
//     WAL segment, removing the old WAL segments from the live set.
//   - The old WAL segment files are deleted (best-effort; leaked files are
//     cleaned up on the next Open by cleanupLeakedSegments).
//
// Crash safety: the SSTable file is written before the MANIFEST is touched.
// If the process dies between writing the SSTable and committing the MANIFEST,
// the old MANIFEST is still valid; on restart, the orphaned SSTable file is
// detected by cleanupOrphanedSSTs and deleted, and the full WAL is replayed.
// If the process dies after the MANIFEST commit, the new SSTable is loaded and
// only the (empty) new WAL segment is replayed.
//
// Caller must hold db.mu (write lock).
func (db *DB) flushMemtable() error {
	if db.memtable.Len() == 0 {
		db.memSize = 0
		return nil
	}

	if err := os.MkdirAll(db.sstDir(), 0o755); err != nil {
		return err
	}

	// --- 1. Write the SSTable file -------------------------------------------

	sstID := db.ssNextID
	sstPath := db.sstFilePath(sstID)

	w, err := sstable.NewWriter(sstPath)
	if err != nil {
		return err
	}

	next := db.memtable.Iterator()
	for {
		key, val, ok := next()
		if !ok {
			break
		}
		if _, isTomb := val.(tombstone); isTomb {
			if err := w.Delete(key); err != nil {
				w.Close()
				os.Remove(sstPath)
				return err
			}
		} else {
			if err := w.Set(key, val.([]byte)); err != nil {
				w.Close()
				os.Remove(sstPath)
				return err
			}
		}
	}

	if err := w.Close(); err != nil {
		os.Remove(sstPath)
		return err
	}

	// Open the new SSTable reader before touching the MANIFEST so we can roll
	// back cleanly if Open fails.
	newReader, err := sstable.Open(sstPath)
	if err != nil {
		os.Remove(sstPath)
		return err
	}

	// --- 2. Create a new active WAL segment (WAL checkpoint boundary) ---------

	newSegID := db.nextID
	newSeg, err := createSegment(db.dir, newSegID)
	if err != nil {
		newReader.Close()
		os.Remove(sstPath)
		return err
	}

	// --- 3. Commit the MANIFEST (the atomic commit point) --------------------
	//
	// New WAL order: only the fresh empty segment.
	// New SST list: append the new SSTable (newest = highest index in the
	// oldest→newest slice stored in the manifest).

	newSSTIDs := append(db.currentSSTIDs(), sstID) // oldest → newest

	newManifest := manifest{
		nextID:    newSegID + 1,
		order:     []uint32{newSegID},
		nextSSTID: sstID + 1,
		sstIDs:    newSSTIDs,
	}
	if err := writeManifest(db.dir, newManifest); err != nil {
		newSeg.close()
		os.Remove(segmentPath(db.dir, newSegID))
		newReader.Close()
		os.Remove(sstPath)
		return err
	}

	// --- 4. Update in-memory state (post-commit, cannot fail atomically) ------

	oldOrder := db.order
	oldSegments := db.segments

	db.ssNextID = sstID + 1
	db.nextID = newSegID + 1
	db.order = []uint32{newSegID}
	db.segments = map[uint32]*segment{newSegID: newSeg}
	db.active = newSeg
	db.ssReaders = append([]*sstable.Reader{newReader}, db.ssReaders...)
	db.memtable = &art.Tree{}
	db.memSize = 0
	// liveKeys is unchanged: data moved to SSTable, not created or deleted.

	// --- 5. Delete old WAL segments (best-effort) -----------------------------

	for _, id := range oldOrder {
		if seg := oldSegments[id]; seg != nil {
			seg.close()
		}
		os.Remove(segmentPath(db.dir, id))
	}

	return nil
}
