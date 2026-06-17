# Kora

A from-scratch database storage engine written in Go, built milestone by
milestone from a Bitcask-style log-structured KV store up through a full
LSM-tree with crash recovery and a network server.

## Status

- [x] M0 — Foundations & record format
- [x] M1 — Append-only KV store with in-memory hash index
- [x] M2 — Multiple segments + compaction
- [x] M3 — LSM-tree (ART memtable · SSTables · Bloom filters · range scans)
- [ ] M4 — Write-ahead log & crash recovery
- [ ] M5 — TCP server

See [MILESTONES.md](MILESTONES.md) for the full roadmap and
[DESIGN.md](DESIGN.md) for tradeoff notes.

## Getting started

```bash
go test ./...        # run the full suite
```

Try the REPL:

```bash
go run ./cmd/kora -dir ./data
> set hello world
OK
> get hello
"world"
> scan a z
"hello" -> "world"
(1 keys)
> del hello
OK
> get hello
(nil)
```

Pass `-no-sync` for faster, less durable writes. Kill the process and
re-run it against the same `-dir` — your data survives. Use `compact` to
merge segments, `compact-sst` to merge SSTables, and `stats` to watch
disk usage drop:

```bash
> stats
keys=5 segments=10 sstables=3 disk=4096 bytes
> compact
OK
> compact-sst
OK (sstables=1)
> stats
keys=5 segments=2 sstables=1 disk=512 bytes
```

## Architecture

Kora is a **log-structured merge-tree (LSM-tree)**. Writes go to a
segment WAL for durability and an in-memory sorted memtable for fast reads.
When the memtable reaches its size threshold it is flushed as an immutable
SSTable. Reads check the memtable first, then SSTables newest-to-oldest.

```
  Set / Delete                              Get / Scan
       |                                         |
       v                                         v
+------+----------+          +-----------------+-----------------+
| segment WAL     |          | 1. memtable (ART)  — O(k) lookup |
| (append-only)   |          |    tombstone{} marks deleted keys |
+------+----------+          |                                   |
       |                     | 2. SSTables  newest → oldest      |
       v                     |    a. Bloom filter → skip on miss |
+------+----------+          |    b. sparse index → seek near key|
| memtable (ART)  |          |    c. linear scan ≤ 16 records    |
| sorted, in RAM  |          +-----------+-----------------------+
+----+------------+                      |
     | memSize ≥ MaxMemBytes             | Scan: k-way merge over
     v                                   | memtable + all SSTables,
+----+--------+                          | snapshot under RLock
| flush       |
+----+--------+
     |
     v  (prepend to ssReaders — newest first)
+----+--------+   +------------+   +------------+
| SSTable  N  |   | SSTable N-1|   | SSTable N-2|  ...
| bloom+index |   | bloom+index|   | bloom+index|
+------+------+   +------+-----+   +------+-----+
       |                 |                |
       +--------+--------+----------------+
                | CompactSSTables(): k-way merge all SSTables,
                | keep newest version per key, drop tombstones,
                v atomic swap (write lock), delete old files
         +------+------+
         | SSTable out |
         | (one file)  |
         +-------------+

On Open: read MANIFEST → replay segment WAL → rebuild memtable
         delete prior-session SSTables (session-local in M3)
```

### SSTable on-disk format

```
[data]   key_len(4) | val_len(4) | key | value     (val_len=0xFFFFFFFF → tombstone)
[bloom]  raw bit array  (bloom_offset = index_offset − bloom_size)
[index]  key_len(4) | key | offset(8)               (one entry per 16 records)
[footer] index_offset(8) | index_count(4) | data_count(4)
       | bloom_size(4) | bloom_k(4) | magic(4="KORA")
```

The Bloom filter (10 bits/element, k=7, ~0.8% FPR) lets `Get` skip files
that definitely don't contain the key. Tombstone keys are in the filter so
a "deleted" key never falls through to an older SSTable's live value.

### Segment WAL format (M1/M2)

```
[crc32(4)] [timestamp_ms(8)] [key_len(4)] [val_len(4)] [key] [value]
```

Tombstone: `val_len = 0xFFFFFFFF`. The **MANIFEST** records the live segment
list in recency order and is the durable commit point for segment compaction.

## How it works

### Writing a key

```
db.Set([]byte("name"), []byte("kora"))
```

1. **Encode & WAL** — the key+value is serialized into a CRC-checked record
   (`crc32 | timestamp | key_len | val_len | key | value`) and appended to the
   active segment file. If `SyncOnWrite` is true, an `fsync` follows before the
   call returns — the write is on stable storage.

2. **Memtable update** — the key is inserted into the in-memory **Adaptive
   Radix Tree** (ART) with the raw value. The ART keeps all keys sorted at all
   times, so no sorting step is needed at flush time.

3. **Rollover check** — if the active segment exceeds `MaxSegmentBytes` (4 MiB
   default) it is sealed as immutable and a fresh segment opens. The MANIFEST
   is updated atomically so recovery always has a consistent segment list.

4. **Flush check** — if `memSize` exceeds `MaxMemBytes` (4 MiB default) the
   entire ART is iterated in key order and written to a new **SSTable** file,
   then prepended to the `ssReaders` list (newest first). The memtable is reset
   to empty and `memSize` to zero.

A `Delete` follows the same path but writes a tombstone record to the WAL and
inserts a zero-size `tombstone{}` sentinel into the ART.

### Reading a key

```
db.Get([]byte("name"))
```

1. **Memtable** — the ART is checked first (O(k), k = key length). A live
   value returns immediately. A `tombstone{}` value also returns immediately —
   as "not found" — without touching disk.

2. **SSTables, newest → oldest** — for each SSTable reader:
   - **Bloom filter**: if the filter says the key is *definitely absent*, the
     file is skipped entirely with no I/O.
   - **Sparse index**: binary-search the in-memory index for the last entry
     with key ≤ target, then seek to that file offset.
   - **Linear scan**: read forward at most 16 records until the key is found
     or passed. A tombstone record here means the key was deleted after this
     SSTable was written — stop searching, return "not found".

3. **Not found** — if all sources are exhausted the key doesn't exist.

### Range scan

```
db.Scan([]byte("a"), []byte("m"))
```

Under a read lock, a **k-way min-heap merge** runs over all sources at once:
the memtable ART iterator (pre-collected snapshot) and a `ScanIterator` per
SSTable (each uses the sparse index to seek near `start`). The heap pops keys
in ascending order; for duplicate keys across sources the newest source wins.
Tombstones suppress older versions and are not emitted. All results are
collected into a slice while the lock is held, then the lock is released and
an iterator over the slice is returned — safe to use while writes and
compaction run concurrently.

### Compaction

**Segment compaction** (`db.Compact`) merges all immutable segment files into
one, keeping the newest value per key and dropping tombstones. The merged file
is written to a fresh segment id; the MANIFEST is atomically replaced to name
it; old files are deleted. A crash at any point leaves a consistent state.

**SSTable compaction** (`db.CompactSSTables`) does the same for SSTable files
via a k-way merge. Because every SSTable is included in the merge, a tombstone
provably has no older live value to mask, so it is safe to drop it entirely.

### Recovery

On `Open`, Kora reads the MANIFEST to find the live segment list, replays
them oldest → newest to rebuild the ART memtable, then deletes any SSTable
files left from the previous session (SSTables are session-local until M4
adds a WAL checkpoint that makes them durable across restarts).

---

## Packages

| Package | Role |
|---|---|
| `internal/record` | Record encoder/decoder with CRC validation |
| `internal/art` | Adaptive Radix Tree — sorted memtable (Node4/16/48/256) |
| `internal/bloom` | Bloom filter (Kirsch-Mitzenmacher double-hashing) |
| `internal/sstable` | SSTable writer and reader (sparse index + Bloom filter) |
| `internal/store` | DB: WAL segments, memtable, flush, compaction, scan |
| `cmd/kora` | Interactive REPL |
