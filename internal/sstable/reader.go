package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/giova/kora-engine/internal/bloom"
)

// ErrCorrupt is returned when the SSTable file fails a structural check.
var ErrCorrupt = errors.New("sstable: corrupt file")

// indexEntry is one entry in the in-memory sparse index.
type indexEntry struct {
	key    []byte
	offset int64 // byte offset in the data section
}

// Reader reads an SSTable. It loads the sparse index and Bloom filter into
// memory at Open and uses them to serve point lookups efficiently.
type Reader struct {
	f         *os.File
	path      string // file path, used by compaction for cleanup
	index     []indexEntry
	filter    *bloom.Filter
	dataEnd   int64  // byte offset where the data section ends (= bloom start)
	indexOff  int64  // byte offset where the index section starts (= bloom end)
	dataCount uint32 // total records in the data section
}

// Open opens the SSTable at path, reads and validates the footer, and loads
// the sparse index into memory.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &Reader{f: f, path: path}
	if err := r.loadFooterAndIndex(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// Get returns the value for key. Returns (nil, false, nil) if the key is not
// present or has a tombstone. Returns a non-nil error only on I/O or corruption.
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	v, found, _, err := r.RawGet(key)
	return v, found, err
}

// RawGet is like Get but also returns isTombstone=true when the key is present
// as a deletion marker. Callers performing multi-source lookups need this to
// distinguish "key absent from this table" from "key was deleted here" — a
// tombstone in a newer SSTable must suppress a live value in an older one.
func (r *Reader) RawGet(key []byte) (value []byte, found bool, isTombstone bool, err error) {
	// Bloom filter check: if the filter is certain the key is absent, skip I/O.
	// Tombstones are also added to the filter, so a false here means the key
	// was never written to this SSTable in any form.
	if r.filter != nil && !r.filter.Has(key) {
		return nil, false, false, nil
	}

	offset := r.seekOffset(key)

	if _, err = r.f.Seek(offset, io.SeekStart); err != nil {
		return nil, false, false, err
	}
	br := newBufReader(r.f)

	for {
		k, v, tomb, rerr := br.readRecord()
		if rerr == io.EOF {
			return nil, false, false, nil
		}
		if rerr != nil {
			return nil, false, false, rerr
		}
		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			if tomb {
				return nil, false, true, nil
			}
			return v, true, false, nil
		}
		if cmp > 0 {
			return nil, false, false, nil
		}
		if br.pos >= r.dataEnd {
			return nil, false, false, nil
		}
	}
}

// Iterator returns a function that yields every (key, value) pair in the data
// section in ascending key order. Tombstone entries are included with value==nil
// so that callers performing k-way merges can correctly suppress deleted keys.
// Returns (nil, nil, false) when exhausted.
func (r *Reader) Iterator() func() (key, value []byte, ok bool) {
	_, _ = r.f.Seek(0, io.SeekStart)
	br := newBufReader(r.f)
	done := false
	return func() ([]byte, []byte, bool) {
		if done {
			return nil, nil, false
		}
		if br.pos >= r.dataEnd {
			done = true
			return nil, nil, false
		}
		k, v, _, err := br.readRecord()
		if err != nil {
			done = true
			return nil, nil, false
		}
		return k, v, true
	}
}

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// Path returns the file path this Reader was opened from.
func (r *Reader) Path() string { return r.path }

// DataCount returns the number of records in the data section (including tombstones).
func (r *Reader) DataCount() uint32 { return r.dataCount }

// ScanIterator yields every record whose key falls in [start, end] in ascending
// key order, including tombstones so that the caller can apply multi-source
// tombstone suppression. Pass start=nil to begin from the first key; pass
// end=nil for an unbounded upper limit.
// Returns (nil, nil, false, false) when exhausted.
func (r *Reader) ScanIterator(start, end []byte) func() (key, value []byte, isTombstone bool, ok bool) {
	var offset int64
	if start != nil {
		offset = r.seekOffset(start)
	}
	if _, err := r.f.Seek(offset, io.SeekStart); err != nil {
		return func() ([]byte, []byte, bool, bool) { return nil, nil, false, false }
	}
	br := newBufReader(r.f)
	done := false
	return func() ([]byte, []byte, bool, bool) {
		for !done {
			if br.pos >= r.dataEnd {
				done = true
				break
			}
			k, v, tomb, err := br.readRecord()
			if err != nil {
				done = true
				break
			}
			if start != nil && bytes.Compare(k, start) < 0 {
				continue // skip records the sparse index overshot
			}
			if end != nil && bytes.Compare(k, end) > 0 {
				done = true
				break
			}
			return k, v, tomb, true
		}
		return nil, nil, false, false
	}
}

// CompactionIterator yields every record in the data section in ascending key
// order, including tombstone entries. The isTombstone flag must be checked by
// callers doing k-way merges so that a delete in a newer SSTable can correctly
// suppress a value in an older one.
// Returns (nil, nil, false, false) when exhausted.
func (r *Reader) CompactionIterator() func() (key, value []byte, isTombstone bool, ok bool) {
	_, _ = r.f.Seek(0, io.SeekStart)
	br := newBufReader(r.f)
	done := false
	return func() ([]byte, []byte, bool, bool) {
		if done {
			return nil, nil, false, false
		}
		if br.pos >= r.dataEnd {
			done = true
			return nil, nil, false, false
		}
		k, v, tomb, err := br.readRecord()
		if err != nil {
			done = true
			return nil, nil, false, false
		}
		return k, v, tomb, true
	}
}

// --- internal helpers -------------------------------------------------------

func (r *Reader) loadFooterAndIndex() error {
	// Seek to footer.
	if _, err := r.f.Seek(-footerSize, io.SeekEnd); err != nil {
		return fmt.Errorf("%w: cannot seek to footer", ErrCorrupt)
	}
	var fb [footerSize]byte
	if _, err := io.ReadFull(r.f, fb[:]); err != nil {
		return fmt.Errorf("%w: cannot read footer", ErrCorrupt)
	}
	indexOffset := int64(binary.BigEndian.Uint64(fb[0:8]))
	indexCount := binary.BigEndian.Uint32(fb[8:12])
	dataCount := binary.BigEndian.Uint32(fb[12:16])
	bloomSize := binary.BigEndian.Uint32(fb[16:20])
	bloomK := binary.BigEndian.Uint32(fb[20:24])
	mg := binary.BigEndian.Uint32(fb[24:28])
	if mg != magic {
		return fmt.Errorf("%w: bad magic %08x", ErrCorrupt, mg)
	}

	r.indexOff = indexOffset
	r.dataEnd = indexOffset - int64(bloomSize) // where data section ends
	r.dataCount = dataCount

	// Load the Bloom filter. bloom_offset = index_offset - bloom_size.
	if bloomSize > 0 {
		if _, err := r.f.Seek(r.dataEnd, io.SeekStart); err != nil {
			return fmt.Errorf("%w: cannot seek to bloom filter", ErrCorrupt)
		}
		bits := make([]byte, bloomSize)
		if _, err := io.ReadFull(r.f, bits); err != nil {
			return fmt.Errorf("%w: cannot read bloom filter", ErrCorrupt)
		}
		r.filter = bloom.Load(bits, bloomK)
	}

	// Load the index section.
	if _, err := r.f.Seek(indexOffset, io.SeekStart); err != nil {
		return fmt.Errorf("%w: cannot seek to index", ErrCorrupt)
	}
	r.index = make([]indexEntry, 0, indexCount)
	br := newBufReader(r.f)
	for i := uint32(0); i < indexCount; i++ {
		key, offset, err := br.readIndexEntry()
		if err != nil {
			return fmt.Errorf("%w: index entry %d: %v", ErrCorrupt, i, err)
		}
		r.index = append(r.index, indexEntry{key: key, offset: offset})
	}
	return nil
}

// seekOffset returns the file offset to start scanning for key. It binary-
// searches the sparse index for the last entry whose key <= the target.
func (r *Reader) seekOffset(key []byte) int64 {
	if len(r.index) == 0 {
		return 0
	}
	// Find the last index entry with key <= target.
	lo, hi := 0, len(r.index)-1
	result := 0
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(r.index[mid].key, key)
		if cmp <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return r.index[result].offset
}

// bufReader wraps a file with a small read buffer and tracks the current offset.
type bufReader struct {
	f   *os.File
	buf [4096]byte
	pos int64 // current logical position in file
}

func newBufReader(f *os.File) *bufReader {
	pos, _ := f.Seek(0, io.SeekCurrent)
	return &bufReader{f: f, pos: pos}
}

func (b *bufReader) read(p []byte) error {
	_, err := io.ReadFull(b.f, p)
	b.pos += int64(len(p))
	return err
}

// readRecord reads one data record and returns (key, value, isTombstone, err).
// Returns io.EOF when there are no more bytes.
func (b *bufReader) readRecord() ([]byte, []byte, bool, error) {
	hdr := b.buf[:8]
	if err := b.read(hdr); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, nil, false, io.EOF
		}
		return nil, nil, false, err
	}
	keyLen, valLen := decodeRecordHeader(hdr)

	key := make([]byte, keyLen)
	if err := b.read(key); err != nil {
		return nil, nil, false, fmt.Errorf("%w: key data", ErrCorrupt)
	}
	if valLen == tombstoneSentinel {
		return key, nil, true, nil
	}
	value := make([]byte, valLen)
	if err := b.read(value); err != nil {
		return nil, nil, false, fmt.Errorf("%w: value data", ErrCorrupt)
	}
	return key, value, false, nil
}

// readIndexEntry reads one index entry and returns (key, offset, err).
func (b *bufReader) readIndexEntry() ([]byte, int64, error) {
	var lenBuf [4]byte
	if err := b.read(lenBuf[:]); err != nil {
		return nil, 0, err
	}
	keyLen := binary.BigEndian.Uint32(lenBuf[:])
	key := make([]byte, keyLen)
	if err := b.read(key); err != nil {
		return nil, 0, err
	}
	var offBuf [8]byte
	if err := b.read(offBuf[:]); err != nil {
		return nil, 0, err
	}
	offset := int64(binary.BigEndian.Uint64(offBuf[:]))
	return key, offset, nil
}
