// Package sstable implements an immutable Sorted String Table: a file of
// key-value records written in ascending key order, with a sparse in-memory
// index for fast point lookups and a sequential iterator for range scans and
// compaction merges.
//
// On-disk layout
//
//	[data section]
//	  record*:  key_len(4) | val_len(4) | key | value
//	            val_len == tombstoneSentinel means the key was deleted
//	[bloom section]
//	  raw bit array of the per-SSTable Bloom filter
//	[index section]
//	  entry*:   key_len(4) | key | offset(8)
//	            one entry every indexStride records; offset is the byte
//	            position of that record in the data section
//	[footer — last 28 bytes]
//	  index_offset(8) | index_count(4) | data_count(4) | bloom_size(4) | bloom_k(4) | magic(4)
//
// bloom_offset = index_offset - bloom_size (no separate field needed).
// All multi-byte integers are big-endian.
package sstable

import "encoding/binary"

const (
	// indexStride is how many data records are skipped between index entries.
	// A stride of 16 means ~1 index entry per 16 records; the reader scans at
	// most 16 records to find a key after a binary search.
	indexStride = 16

	// tombstoneSentinel is stored in val_len to signal a deleted key.
	// It matches the sentinel used by the segment record package.
	tombstoneSentinel uint32 = 0xFFFF_FFFF

	// magic identifies a valid SSTable footer.
	magic uint32 = 0x4B4F5241 // "KORA"

	footerSize = 28 // bytes: 8 + 4 + 4 + 4 + 4 + 4
)

// encodeRecordHeader writes the 8-byte record header into dst (must be len>=8).
func encodeRecordHeader(dst []byte, keyLen, valLen uint32) {
	binary.BigEndian.PutUint32(dst[0:4], keyLen)
	binary.BigEndian.PutUint32(dst[4:8], valLen)
}

// decodeRecordHeader reads the 8-byte record header from src.
func decodeRecordHeader(src []byte) (keyLen, valLen uint32) {
	return binary.BigEndian.Uint32(src[0:4]),
		binary.BigEndian.Uint32(src[4:8])
}

// recordSize returns the total byte size of a record with the given key/val lengths.
// For tombstones pass valLen == tombstoneSentinel; value bytes are not stored.
func recordSize(keyLen, valLen uint32) int64 {
	sz := int64(8) + int64(keyLen)
	if valLen != tombstoneSentinel {
		sz += int64(valLen)
	}
	return sz
}
