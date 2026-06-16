package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"
)

// TombstoneSentinel is written as value_len to signal a delete record.
// Chosen over a flag byte to keep the header fixed-width (always 20 bytes).
const TombstoneSentinel uint32 = 0xFFFFFFFF

// Size limits. These bound how much memory a single record can ask us to
// allocate, which matters in two places: Encode rejects oversized inputs
// instead of silently truncating len() to uint32, and Decode rejects
// implausible on-disk lengths *before* allocating, so a corrupt file can't
// trigger a huge allocation / OOM. Both caps sit well below math.MaxUint32, so
// there is also no overflow when summing key+value lengths.
const (
	MaxKeySize   = 1 << 16 // 64 KiB — keys are identifiers, not payloads
	MaxValueSize = 1 << 30 // 1 GiB
)

// maxKeySize / maxValueSize are the limits actually enforced by Encode/Decode.
// They default to the exported consts, but are package-level vars so tests can
// lower them to exercise the guards without allocating gigabyte-sized inputs.
var (
	maxKeySize   = MaxKeySize
	maxValueSize = MaxValueSize
)

// ErrCorrupted is returned when a record's CRC does not match its contents, or
// when its on-disk lengths exceed the size limits (a sign of corruption).
var ErrCorrupted = errors.New("record: checksum mismatch")

// ErrKeyTooLarge / ErrValueTooLarge are returned by Encode when an input
// exceeds the size limits.
var (
	ErrKeyTooLarge   = errors.New("record: key exceeds MaxKeySize")
	ErrValueTooLarge = errors.New("record: value exceeds MaxValueSize")
)

// Record is the in-memory representation of a single log entry.
// Value == nil means this is a tombstone (delete marker).
type Record struct {
	Timestamp uint64
	Key       []byte
	Value     []byte
}

func IsTombstone(r Record) bool {
	return r.Value == nil
}

// HeaderSize is the fixed-width header: crc(4) + timestamp(8) + key_len(4) + value_len(4).
const HeaderSize = 20

// Size returns the number of bytes r occupies on disk once encoded.
// A tombstone (Value == nil) and an empty value both contribute 0 value bytes,
// so this matches the encoded length in either case.
func Size(r Record) int {
	return HeaderSize + len(r.Key) + len(r.Value)
}

// Encode serializes key+value as a record and writes it to w.
// Pass value == nil to write a tombstone.
//
// Wire format (all big-endian):
//
//	[crc32 (4)] [timestamp_ms (8)] [key_len (4)] [value_len (4)] [key] [value]
//
// CRC covers everything after itself so a reader can stream-validate without
// seeking back.
//
// The timestamp is set to the current time; use EncodeAt to supply one
// explicitly (compaction does this to preserve a record's original timestamp).
func Encode(w io.Writer, key, value []byte) error {
	return EncodeAt(w, uint64(time.Now().UnixMilli()), key, value)
}

// EncodeAt is Encode with an explicit timestamp. It returns ErrKeyTooLarge or
// ErrValueTooLarge rather than silently truncating an oversized length.
func EncodeAt(w io.Writer, ts uint64, key, value []byte) error {
	tombstone := value == nil

	if len(key) > maxKeySize {
		return ErrKeyTooLarge
	}
	if !tombstone && len(value) > maxValueSize {
		return ErrValueTooLarge
	}

	var valueLen uint32
	if tombstone {
		valueLen = TombstoneSentinel
	} else {
		valueLen = uint32(len(value))
	}

	keyLen := uint32(len(key))

	var meta [16]byte
	binary.BigEndian.PutUint64(meta[0:], ts)
	binary.BigEndian.PutUint32(meta[8:], keyLen)
	binary.BigEndian.PutUint32(meta[12:], valueLen)

	h := crc32.NewIEEE()
	h.Write(meta[:])
	h.Write(key)
	if !tombstone {
		h.Write(value)
	}

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], h.Sum32())

	for _, chunk := range [][]byte{crcBuf[:], meta[:], key} {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
	if !tombstone && len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads and validates one record from r.
// Returns ErrCorrupted if the CRC does not match.
func Decode(r io.Reader) (Record, error) {
	var hdr [20]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Record{}, err
	}

	storedCRC := binary.BigEndian.Uint32(hdr[0:4])
	ts := binary.BigEndian.Uint64(hdr[4:12])
	keyLen := binary.BigEndian.Uint32(hdr[12:16])
	valueLen := binary.BigEndian.Uint32(hdr[16:20])

	tombstone := valueLen == TombstoneSentinel

	// Validate lengths against the size limits *before* allocating, so a corrupt
	// file with implausible lengths is rejected cleanly instead of triggering a
	// huge allocation.
	if keyLen > uint32(maxKeySize) {
		return Record{}, ErrCorrupted
	}
	if !tombstone && valueLen > uint32(maxValueSize) {
		return Record{}, ErrCorrupted
	}

	tailSize := int(keyLen)
	if !tombstone {
		tailSize += int(valueLen)
	}

	tail := make([]byte, tailSize)
	if _, err := io.ReadFull(r, tail); err != nil {
		return Record{}, err
	}

	h := crc32.NewIEEE()
	h.Write(hdr[4:]) // timestamp + key_len + value_len
	h.Write(tail)
	if h.Sum32() != storedCRC {
		return Record{}, ErrCorrupted
	}

	rec := Record{
		Timestamp: ts,
		Key:       tail[:keyLen],
	}
	if !tombstone {
		rec.Value = tail[keyLen:]
	}
	return rec, nil
}
