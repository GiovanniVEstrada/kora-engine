package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/giova/kora-engine/internal/bloom"
)

// Writer writes an SSTable to a file. Keys must be supplied in strictly
// ascending order; supplying an out-of-order key returns an error.
// Call Close to finalise the index and footer.
type Writer struct {
	f        *os.File
	offset   int64  // next write position (= bytes written so far)
	nRecords uint32 // data records written

	lastKey []byte // for ordering enforcement

	// Sparse index: accumulated in a buffer, flushed at Close.
	indexBuf   bytes.Buffer
	indexCount uint32
}

// NewWriter creates (or truncates) the file at path and returns a Writer.
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

// Set appends a live key-value record. value must not be nil; use Delete for
// tombstones. Keys must arrive in strictly ascending order.
func (w *Writer) Set(key, value []byte) error {
	if err := w.checkOrder(key); err != nil {
		return err
	}
	return w.writeRecord(key, value, false)
}

// Delete appends a tombstone record for key. Keys must arrive in strictly
// ascending order.
func (w *Writer) Delete(key []byte) error {
	if err := w.checkOrder(key); err != nil {
		return err
	}
	return w.writeRecord(key, nil, true)
}

// Close writes the bloom filter, index section, and footer, then closes the
// file. The Writer must not be used after Close returns.
func (w *Writer) Close() error {
	dataEnd := w.offset // byte offset where data section ends

	// Build bloom filter by re-scanning the data section. The data was just
	// written and is likely in the OS page cache, so this scan is cheap.
	filter, err := w.buildBloom(dataEnd)
	if err != nil {
		w.f.Close()
		return err
	}
	bloomBytes := filter.Bytes()

	// Seek to dataEnd and write bloom bytes.
	if _, err := w.f.Seek(dataEnd, io.SeekStart); err != nil {
		w.f.Close()
		return err
	}
	if _, err := w.f.Write(bloomBytes); err != nil {
		w.f.Close()
		return err
	}

	indexOffset := dataEnd + int64(len(bloomBytes))

	// Write index section.
	if _, err := w.f.Write(w.indexBuf.Bytes()); err != nil {
		w.f.Close()
		return err
	}

	// Write footer: index_offset | index_count | data_count | bloom_size | bloom_k | magic
	var footer [footerSize]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint32(footer[8:12], w.indexCount)
	binary.BigEndian.PutUint32(footer[12:16], w.nRecords)
	binary.BigEndian.PutUint32(footer[16:20], uint32(len(bloomBytes)))
	binary.BigEndian.PutUint32(footer[20:24], filter.K())
	binary.BigEndian.PutUint32(footer[24:28], magic)
	if _, err := w.f.Write(footer[:]); err != nil {
		w.f.Close()
		return err
	}

	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// buildBloom re-scans the data section (bytes 0..dataEnd) to build a
// correctly-sized Bloom filter. Both tombstone and live keys are added so that
// RawGet can use the filter to distinguish "key absent from this SSTable" from
// "key present as a tombstone".
func (w *Writer) buildBloom(dataEnd int64) (*bloom.Filter, error) {
	f := bloom.New(int(w.nRecords))
	if w.nRecords == 0 {
		return f, nil
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	br := newBufReader(w.f)
	for br.pos < dataEnd {
		k, _, _, err := br.readRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		f.Add(k)
	}
	return f, nil
}

// --- internal helpers -------------------------------------------------------

func (w *Writer) checkOrder(key []byte) error {
	if w.lastKey != nil && bytes.Compare(key, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: key %q out of order (last was %q)", key, w.lastKey)
	}
	cp := make([]byte, len(key))
	copy(cp, key)
	w.lastKey = cp
	return nil
}

func (w *Writer) writeRecord(key, value []byte, tombstone bool) error {
	// Maybe emit an index entry before writing (every indexStride records,
	// starting at record 0).
	if w.nRecords%indexStride == 0 {
		w.appendIndex(key, w.offset)
	}

	keyLen := uint32(len(key))
	var valLen uint32
	if tombstone {
		valLen = tombstoneSentinel
	} else {
		valLen = uint32(len(value))
	}

	var hdr [8]byte
	encodeRecordHeader(hdr[:], keyLen, valLen)

	n, err := w.f.Write(hdr[:])
	w.offset += int64(n)
	if err != nil {
		return err
	}
	n, err = w.f.Write(key)
	w.offset += int64(n)
	if err != nil {
		return err
	}
	if !tombstone {
		n, err = w.f.Write(value)
		w.offset += int64(n)
		if err != nil {
			return err
		}
	}

	w.nRecords++
	return nil
}

func (w *Writer) appendIndex(key []byte, offset int64) {
	keyLen := uint32(len(key))
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], keyLen)
	w.indexBuf.Write(buf[:4])
	w.indexBuf.Write(key)
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	w.indexBuf.Write(buf[:8])
	w.indexCount++
}
