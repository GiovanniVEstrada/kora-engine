package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// The manifest records the authoritative set of live segments, the live set of
// SSTable files, and counters for allocating new IDs. It is the single source
// of truth at startup. Updating it atomically (write temp → fsync → rename)
// makes every multi-step mutation (flush, compaction, rollover) crash-safe: the
// rename either fully happens or doesn't, so recovery always sees a consistent
// state.
//
// Two on-disk versions exist (big-endian throughout):
//
// v1 (M1/M2 — segments only):
//
//	[crc32(4)] [version=1(4)] [nextSegID(4)] [segCount(4)] [segID_0(4)] …
//
// v2 (M4 — segments + SSTables):
//
//	[crc32(4)] [version=2(4)] [nextSegID(4)] [segCount(4)] [segID_0(4)] …
//	          [nextSSTID(4)]  [sstCount(4)]  [sstID_0(4)] …
//
// v1 manifests are read transparently: nextSSTID defaults to 1 and sstIDs is
// empty, which triggers full WAL replay (same behaviour as M3).
const (
	manifestName    = "MANIFEST"
	manifestTmpName = "MANIFEST.tmp"
	manifestV1      = 1
	manifestV2      = 2
)

// errNoManifest signals that no manifest exists yet (a fresh store).
var errNoManifest = errors.New("store: no manifest")

// errManifestCorrupt signals a manifest that failed its CRC or length checks.
var errManifestCorrupt = errors.New("store: manifest corrupt")

type manifest struct {
	nextID    uint32   // next WAL segment ID to allocate
	order     []uint32 // WAL segment IDs, oldest → newest; last is active
	nextSSTID uint32   // next SSTable ID to allocate
	sstIDs    []uint32 // SSTable IDs, oldest → newest
}

// writeManifest atomically replaces the manifest in dir using v2 format.
func writeManifest(dir string, m manifest) error {
	// payload: version | nextSegID | segCount | segIDs... | nextSSTID | sstCount | sstIDs...
	size := 4 + 4 + 4 + 4*len(m.order) + 4 + 4 + 4*len(m.sstIDs)
	payload := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint32(payload[off:], manifestV2)
	off += 4
	binary.BigEndian.PutUint32(payload[off:], m.nextID)
	off += 4
	binary.BigEndian.PutUint32(payload[off:], uint32(len(m.order)))
	off += 4
	for _, id := range m.order {
		binary.BigEndian.PutUint32(payload[off:], id)
		off += 4
	}
	binary.BigEndian.PutUint32(payload[off:], m.nextSSTID)
	off += 4
	binary.BigEndian.PutUint32(payload[off:], uint32(len(m.sstIDs)))
	off += 4
	for _, id := range m.sstIDs {
		binary.BigEndian.PutUint32(payload[off:], id)
		off += 4
	}

	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[0:], crc32.ChecksumIEEE(payload))
	copy(out[4:], payload)

	tmp := filepath.Join(dir, manifestTmpName)
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(out); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, manifestName))
}

// readManifest loads the manifest from dir. Returns errNoManifest if none
// exists, or errManifestCorrupt if validation fails.
func readManifest(dir string) (manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return manifest{}, errNoManifest
		}
		return manifest{}, err
	}
	if len(b) < 16 { // crc(4) + version(4) + nextID(4) + count(4) minimum
		return manifest{}, errManifestCorrupt
	}

	storedCRC := binary.BigEndian.Uint32(b[0:4])
	payload := b[4:]
	if crc32.ChecksumIEEE(payload) != storedCRC {
		return manifest{}, errManifestCorrupt
	}

	version := binary.BigEndian.Uint32(payload[0:4])
	switch version {
	case manifestV1:
		return readManifestV1(payload)
	case manifestV2:
		return readManifestV2(payload)
	default:
		return manifest{}, errManifestCorrupt
	}
}

func readManifestV1(payload []byte) (manifest, error) {
	if len(payload) < 12 {
		return manifest{}, errManifestCorrupt
	}
	nextID := binary.BigEndian.Uint32(payload[4:8])
	count := binary.BigEndian.Uint32(payload[8:12])
	if len(payload) != 12+4*int(count) {
		return manifest{}, errManifestCorrupt
	}
	order := make([]uint32, count)
	for i := range order {
		order[i] = binary.BigEndian.Uint32(payload[12+4*i:])
	}
	if err := validateSegments(nextID, order); err != nil {
		return manifest{}, err
	}
	return manifest{nextID: nextID, order: order, nextSSTID: 1}, nil
}

func readManifestV2(payload []byte) (manifest, error) {
	// minimum: version(4) + nextSegID(4) + segCount(4) + nextSSTID(4) + sstCount(4) = 20 bytes
	if len(payload) < 20 {
		return manifest{}, errManifestCorrupt
	}
	off := 4 // skip version
	nextSegID := binary.BigEndian.Uint32(payload[off:])
	off += 4
	segCount := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4

	if len(payload) < off+4*segCount+8 { // +8 for nextSSTID + sstCount
		return manifest{}, errManifestCorrupt
	}
	order := make([]uint32, segCount)
	for i := range order {
		order[i] = binary.BigEndian.Uint32(payload[off:])
		off += 4
	}

	nextSSTID := binary.BigEndian.Uint32(payload[off:])
	off += 4
	sstCount := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4

	if len(payload) != off+4*sstCount {
		return manifest{}, errManifestCorrupt
	}
	sstIDs := make([]uint32, sstCount)
	for i := range sstIDs {
		sstIDs[i] = binary.BigEndian.Uint32(payload[off:])
		off += 4
	}

	if err := validateSegments(nextSegID, order); err != nil {
		return manifest{}, err
	}
	if err := validateSSTs(nextSSTID, sstIDs); err != nil {
		return manifest{}, err
	}

	return manifest{nextID: nextSegID, order: order, nextSSTID: nextSSTID, sstIDs: sstIDs}, nil
}

func validateSegments(nextID uint32, order []uint32) error {
	seen := make(map[uint32]bool, len(order))
	var maxID uint32
	for _, id := range order {
		if seen[id] {
			return errManifestCorrupt
		}
		seen[id] = true
		if id > maxID {
			maxID = id
		}
	}
	if len(order) > 0 && nextID <= maxID {
		return errManifestCorrupt
	}
	return nil
}

func validateSSTs(nextSSTID uint32, sstIDs []uint32) error {
	seen := make(map[uint32]bool, len(sstIDs))
	var maxID uint32
	for _, id := range sstIDs {
		if seen[id] {
			return errManifestCorrupt
		}
		seen[id] = true
		if id > maxID {
			maxID = id
		}
	}
	if len(sstIDs) > 0 && nextSSTID <= maxID {
		return errManifestCorrupt
	}
	return nil
}
