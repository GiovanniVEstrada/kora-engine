package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// The manifest records the authoritative set of live segments and their recency
// order, plus the next segment id to allocate. It is the source of truth at
// startup (the data directory may also contain leaked files from a compaction
// that crashed before committing — those are ignored). Updating it atomically
// (write temp, fsync, rename) is what makes compaction's segment swap
// crash-safe: the rename either fully happens or doesn't, so recovery always
// sees a consistent segment set.
//
// On-disk format (big-endian), mirroring the record format's CRC-first layout:
//
//	[crc32 (4)] [version (4)] [nextID (4)] [count (4)] [id_0 (4)] … [id_{count-1} (4)]
//
// The ids are listed oldest → newest; the last id is the active segment. CRC
// covers everything after itself.
const (
	manifestName    = "MANIFEST"
	manifestTmpName = "MANIFEST.tmp"
	manifestVersion = 1
)

// errNoManifest signals that no manifest exists yet (a fresh store, or one
// created by pre-manifest code that needs migrating).
var errNoManifest = errors.New("store: no manifest")

// errManifestCorrupt signals a manifest that failed its CRC or length checks.
var errManifestCorrupt = errors.New("store: manifest corrupt")

type manifest struct {
	nextID uint32
	order  []uint32 // segment ids, oldest → newest; last is active
}

// writeManifest atomically replaces the manifest in dir.
func writeManifest(dir string, m manifest) error {
	payload := make([]byte, 12+4*len(m.order))
	binary.BigEndian.PutUint32(payload[0:], manifestVersion)
	binary.BigEndian.PutUint32(payload[4:], m.nextID)
	binary.BigEndian.PutUint32(payload[8:], uint32(len(m.order)))
	for i, id := range m.order {
		binary.BigEndian.PutUint32(payload[12+4*i:], id)
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

// readManifest loads the manifest from dir. It returns errNoManifest if none
// exists, or errManifestCorrupt if it fails validation.
func readManifest(dir string) (manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return manifest{}, errNoManifest
		}
		return manifest{}, err
	}
	if len(b) < 16 { // crc(4) + version(4) + nextID(4) + count(4)
		return manifest{}, errManifestCorrupt
	}

	storedCRC := binary.BigEndian.Uint32(b[0:4])
	payload := b[4:]
	if crc32.ChecksumIEEE(payload) != storedCRC {
		return manifest{}, errManifestCorrupt
	}

	version := binary.BigEndian.Uint32(payload[0:4])
	if version != manifestVersion {
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

	// Semantic validation: a structurally valid (correct-CRC) manifest can still
	// be logically impossible. Since the manifest is the source of truth, reject
	// states that would let rollover/compaction reuse a live id or open the same
	// segment twice.
	seen := make(map[uint32]bool, len(order))
	var maxID uint32
	for _, id := range order {
		if seen[id] {
			return manifest{}, errManifestCorrupt // duplicate segment id
		}
		seen[id] = true
		if id > maxID {
			maxID = id
		}
	}
	if len(order) > 0 && nextID <= maxID {
		// nextID must be strictly greater than every live id, or the next
		// allocation would collide with an existing segment.
		return manifest{}, errManifestCorrupt
	}

	return manifest{nextID: nextID, order: order}, nil
}
