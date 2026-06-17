package store

import (
	"bytes"
	"container/heap"
)

// Scan returns an iterator that yields all live (key, value) pairs whose keys
// fall in the closed range [start, end] in ascending order. Pass start=nil to
// begin from the very first key; pass end=nil for an unbounded upper limit.
//
// The iterator is a snapshot: it captures the state of the memtable and all
// SSTables at the moment Scan is called. Concurrent writes and compaction are
// safe — they do not affect the returned iterator.
//
// Returns (nil, nil, false) when exhausted.
func (db *DB) Scan(start, end []byte) func() (key, value []byte, ok bool) {
	db.mu.RLock()
	results := db.collectScan(start, end)
	db.mu.RUnlock()

	i := 0
	return func() ([]byte, []byte, bool) {
		if i >= len(results) {
			return nil, nil, false
		}
		r := results[i]
		i++
		return r.key, r.value, true
	}
}

// --- k-way merge over memtable + SSTables -----------------------------------

type scanKV struct{ key, value []byte }

// scanEntry is one slot in the min-heap used for the k-way merge.
type scanEntry struct {
	key         []byte
	value       []byte
	isTombstone bool
	srcIdx      int // 0 = memtable (newest), 1..n = ssReaders[i-1]
	iter        func() ([]byte, []byte, bool, bool)
}

type scanHeap []*scanEntry

func (h scanHeap) Len() int      { return len(h) }
func (h scanHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h scanHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].srcIdx < h[j].srcIdx // lower srcIdx = newer source = wins ties
}
func (h *scanHeap) Push(x any) { *h = append(*h, x.(*scanEntry)) }
func (h *scanHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// collectScan runs the k-way merge and returns all live keys in [start, end].
// Caller must hold at least db.mu.RLock.
func (db *DB) collectScan(start, end []byte) []scanKV {
	h := make(scanHeap, 0, 1+len(db.ssReaders))
	heap.Init(&h)

	// Source 0: memtable (most recent).
	memIter := db.memtableRangeIter(start, end)
	if k, v, tomb, ok := memIter(); ok {
		heap.Push(&h, &scanEntry{key: k, value: v, isTombstone: tomb, srcIdx: 0, iter: memIter})
	}

	// Sources 1..n: SSTables, newest first.
	for i, r := range db.ssReaders {
		iter := r.ScanIterator(start, end)
		if k, v, tomb, ok := iter(); ok {
			heap.Push(&h, &scanEntry{key: k, value: v, isTombstone: tomb, srcIdx: i + 1, iter: iter})
		}
	}

	var results []scanKV
	var lastKey []byte
	for h.Len() > 0 {
		e := heap.Pop(&h).(*scanEntry)

		if lastKey == nil || !bytes.Equal(e.key, lastKey) {
			lastKey = append([]byte(nil), e.key...)
			if !e.isTombstone {
				results = append(results, scanKV{
					key:   append([]byte(nil), e.key...),
					value: append([]byte(nil), e.value...),
				})
			}
			// Tombstone: key was deleted — do not emit, but still consume
			// older versions by advancing through the heap naturally.
		}
		// Duplicate (older version of same key): skip, already handled.

		// Advance this source's iterator.
		if k, v, tomb, ok := e.iter(); ok {
			heap.Push(&h, &scanEntry{key: k, value: v, isTombstone: tomb, srcIdx: e.srcIdx, iter: e.iter})
		}
	}
	return results
}

// memtableRangeIter wraps the ART snapshot iterator, filtering to [start, end]
// and converting internal tombstone{} values to isTombstone=true.
func (db *DB) memtableRangeIter(start, end []byte) func() ([]byte, []byte, bool, bool) {
	iter := db.memtable.Iterator() // pre-collects leaves — safe after lock release
	return func() ([]byte, []byte, bool, bool) {
		for {
			k, v, ok := iter()
			if !ok {
				return nil, nil, false, false
			}
			if start != nil && bytes.Compare(k, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(k, end) > 0 {
				return nil, nil, false, false
			}
			if _, isTomb := v.(tombstone); isTomb {
				return k, nil, true, true
			}
			return k, v.([]byte), false, true
		}
	}
}
