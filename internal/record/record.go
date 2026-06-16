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

// ErrCorrupted is returned when a record's CRC does not match its contents.
var ErrCorrupted = errors.New("record: checksum mismatch")

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

// Encode serializes key+value as a record and writes it to w.
// Pass value == nil to write a tombstone.
//
// Wire format (all big-endian):
//
//	[crc32 (4)] [timestamp_ms (8)] [key_len (4)] [value_len (4)] [key] [value]
//
// CRC covers everything after itself so a reader can stream-validate without
// seeking back.
func Encode(w io.Writer, key, value []byte) error {
	tombstone := value == nil

	var valueLen uint32
	if tombstone {
		valueLen = TombstoneSentinel
	} else {
		valueLen = uint32(len(value))
	}

	ts := uint64(time.Now().UnixMilli())
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
