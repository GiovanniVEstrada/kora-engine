package store

import (
	"bytes"
	"container/heap"
	"os"

	"github.com/giova/kora-engine/internal/sstable"
)

// CompactSSTables merges all current SSTable files into one, keeping the
// newest version of each key and dropping tombstones. Because every SSTable
// is included in the merge, there are no older sources that a tombstone could
// be masking — so dropped tombstones cannot resurrect stale values.
//
// The swap is atomic in-memory: old readers are replaced by the new one only
// after the merged file is fully written and opened. Deleted files are removed
// best-effort; any that survive are cleaned up on the next Open.
//
// Callers typically trigger this explicitly after several flushes accumulate.
func (db *DB) CompactSSTables() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.ssReaders) < 2 {
		return nil // nothing to merge
	}

	newID := db.ssNextID
	db.ssNextID++
	newPath := db.sstFilePath(newID)

	if err := kWayMergeSSTables(db.ssReaders, newPath); err != nil {
		os.Remove(newPath)
		return err
	}

	newReader, err := sstable.Open(newPath)
	if err != nil {
		os.Remove(newPath)
		return err
	}

	// Swap: close old readers, install new one.
	old := db.ssReaders
	db.ssReaders = []*sstable.Reader{newReader}
	for _, r := range old {
		path := r.Path()
		r.Close()
		os.Remove(path) // best-effort; leaked files cleaned up on next Open
	}
	return nil
}

// --- k-way merge ------------------------------------------------------------

// mergeEntry is one slot in the min-heap used for the k-way merge.
type mergeEntry struct {
	key         []byte
	value       []byte
	isTombstone bool
	sstIdx      int // position in ssReaders: 0 = newest, higher = older
	iter        func() ([]byte, []byte, bool, bool)
}

// mergeHeap implements heap.Interface over mergeEntry pointers.
// Ordering: ascending key; for equal keys, ascending sstIdx (newer source first).
type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].sstIdx < h[j].sstIdx // newer (lower idx) wins ties
}
func (h *mergeHeap) Push(x any) { *h = append(*h, x.(*mergeEntry)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// kWayMergeSSTables merges readers (newest = index 0) into a new SSTable at
// outPath. Tombstones are dropped because all sources are merged — no older
// source exists that could hold a value the tombstone would need to suppress.
func kWayMergeSSTables(readers []*sstable.Reader, outPath string) error {
	h := make(mergeHeap, 0, len(readers))
	heap.Init(&h)

	for i, r := range readers {
		iter := r.CompactionIterator()
		if k, v, tomb, ok := iter(); ok {
			heap.Push(&h, &mergeEntry{
				key: k, value: v, isTombstone: tomb, sstIdx: i, iter: iter,
			})
		}
	}

	w, err := sstable.NewWriter(outPath)
	if err != nil {
		return err
	}

	var lastKey []byte
	for h.Len() > 0 {
		e := heap.Pop(&h).(*mergeEntry)

		if lastKey == nil || !bytes.Equal(e.key, lastKey) {
			// First (newest) occurrence of this key.
			lastKey = make([]byte, len(e.key))
			copy(lastKey, e.key)

			if !e.isTombstone {
				if err := w.Set(e.key, e.value); err != nil {
					w.Close()
					return err
				}
			}
			// Tombstones are silently dropped — full merge, no older source.
		}
		// Duplicate (older version of same key): skip.

		// Advance this source's iterator.
		if k, v, tomb, ok := e.iter(); ok {
			heap.Push(&h, &mergeEntry{
				key: k, value: v, isTombstone: tomb, sstIdx: e.sstIdx, iter: e.iter,
			})
		}
	}

	return w.Close()
}
