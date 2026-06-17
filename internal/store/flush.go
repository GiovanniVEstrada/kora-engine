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

// flushMemtable writes the current memtable to an SSTable file, prepends a
// reader to db.ssReaders (newest first), and clears the memtable.
// Caller must hold db.mu (write lock).
func (db *DB) flushMemtable() error {
	if db.memtable.Len() == 0 {
		db.memSize = 0
		return nil
	}

	if err := os.MkdirAll(db.sstDir(), 0o755); err != nil {
		return err
	}

	id := db.ssNextID
	db.ssNextID++
	path := db.sstFilePath(id)

	w, err := sstable.NewWriter(path)
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
				os.Remove(path)
				return err
			}
		} else {
			if err := w.Set(key, val.([]byte)); err != nil {
				w.Close()
				os.Remove(path)
				return err
			}
		}
	}

	if err := w.Close(); err != nil {
		os.Remove(path)
		return err
	}

	r, err := sstable.Open(path)
	if err != nil {
		os.Remove(path)
		return err
	}

	// Prepend so ssReaders remains newest-first.
	db.ssReaders = append([]*sstable.Reader{r}, db.ssReaders...)

	// liveKeys is unchanged: data moved to SSTable, not created or deleted.
	db.memtable = &art.Tree{}
	db.memSize = 0
	return nil
}
