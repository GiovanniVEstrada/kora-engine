package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/giova/kora-engine/internal/record"
)

// dataExt is the suffix of a segment data file: 000001.data, 000002.data, …
const dataExt = ".data"

// errPartialTail signals that a segment ended in a partially written record
// (the symptom of a crash mid-flush). The caller decides whether to truncate
// (active segment) or treat as corruption (immutable segment).
var errPartialTail = errors.New("store: partial trailing record")

// segment is one on-disk data file. Higher id == newer (see DESIGN.md). The
// active segment is opened for append+read; immutable segments are read-only.
type segment struct {
	id   uint32
	path string
	f    *os.File
	size int64 // current end-of-file offset; next append lands here
}

func segmentName(id uint32) string {
	return fmt.Sprintf("%06d%s", id, dataExt)
}

func segmentPath(dir string, id uint32) string {
	return filepath.Join(dir, segmentName(id))
}

// createSegment opens (creating if needed) a segment for append + read.
func createSegment(dir string, id uint32) (*segment, error) {
	path := segmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{id: id, path: path, f: f, size: fi.Size()}, nil
}

// openSegmentReadOnly opens an existing segment for reading only.
func openSegmentReadOnly(dir string, id uint32) (*segment, error) {
	path := segmentPath(dir, id)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{id: id, path: path, f: f, size: fi.Size()}, nil
}

func (s *segment) append(b []byte) error {
	n, err := s.f.Write(b)
	if err != nil {
		return err
	}
	s.size += int64(n)
	return nil
}

func (s *segment) readAt(p []byte, off int64) error {
	_, err := s.f.ReadAt(p, off)
	return err
}

func (s *segment) sync() error  { return s.f.Sync() }
func (s *segment) close() error { return s.f.Close() }

// scanSegment reads every record in the file at path in order, calling fn for
// each with its starting offset and on-disk size. It returns the offset just
// past the last complete record. A partial trailing record yields
// errPartialTail with endOffset pointing at where it began.
func scanSegment(path string, fn func(rec record.Record, off int64, size int) error) (endOffset int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var off int64
	for {
		rec, derr := record.Decode(br)
		if derr == io.EOF {
			return off, nil
		}
		if derr == io.ErrUnexpectedEOF {
			return off, errPartialTail
		}
		if derr != nil {
			return off, derr
		}
		size := record.Size(rec)
		if e := fn(rec, off, size); e != nil {
			return off, e
		}
		off += int64(size)
	}
}

// listSegmentIDs returns the ids of all *.data files in dir, ascending.
func listSegmentIDs(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), dataExt) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), dataExt)
		n, perr := strconv.ParseUint(base, 10, 32)
		if perr != nil {
			continue // ignore files that aren't segment-named
		}
		ids = append(ids, uint32(n))
	}
	sortIDs(ids)
	return ids, nil
}

func sortIDs(ids []uint32) {
	// small slices; simple insertion sort avoids pulling in sort for uint32
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
