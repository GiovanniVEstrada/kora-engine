# StrataDB

A from-scratch database storage engine written in Go, built milestone by
milestone from a Bitcask-style log-structured KV store up through a full
LSM-tree with crash recovery and a network server.

## Status

- [x] M0 — Foundations & record format
- [x] M1 — Append-only KV store with in-memory hash index
- [x] M2 — Multiple segments + compaction
- [ ] M3 — LSM-tree (memtable -> SSTables -> Bloom filters)
- [ ] M4 — Write-ahead log & crash recovery
- [ ] M5 — TCP server

See [MILESTONES.md](MILESTONES.md) for the full roadmap and
[DESIGN.md](DESIGN.md) for tradeoff notes.

## Getting started

```bash
go test ./...        # run the suite (record round-trip, store oracle, recovery)
make test
```

Try the REPL:

```bash
go run ./cmd/strata -dir ./data
> set hello world
OK
> get hello
"world"
> del hello
OK
> get hello
(nil)
```

Set/Delete fsync by default; pass `-no-sync` for faster, less durable writes.
Kill the process and re-run it against the same `-dir` to watch your data
survive a restart. Use `compact` to merge segments and `stats` to watch disk
usage drop:

```bash
# (run with -seg 256 to force frequent rollover, then overwrite 5 keys 40×)
> stats
keys=5 segments=10 disk=2551 bytes
> compact
OK
> stats
keys=5 segments=2 disk=448 bytes
```

## Architecture (M2 — segmented Bitcask + manifest)

```
          Set / Delete                        Get
              |                                 |
              v                                 v
      +----------------+              +------------------+
      | encode record  |             | keydir lookup    |  key -> {fileID,offset,len}
      | + append       |             +--------+---------+
      +-------+--------+                      | ReadAt(offset, len)
              | fsync                         v
              v                       +-----------------------------+
      +---------------+  rollover at  |  000001.data  (immutable)   |
      | active segment|--MaxSegBytes--|  000002.data  (immutable)   |  read by id
      | 00000N.data   |-------------->|  ...                        |
      +-------+-------+               |  00000N.data  (active)      |
              |                       +--------------+--------------+
              |                                      | Compact(): merge all
              v                                      v immutable -> fresh id,
      +---------------+                       drop tombstones, then commit
      |  keydir (RAM) |  map[string]{fileID,offset,len}   by atomically
      +---------------+                                replacing the MANIFEST
              ^
              | on Open: read MANIFEST (recency-ordered segment list),
              | replay those segments oldest -> newest to rebuild keydir
              +--- MANIFEST: [crc][version][nextID][count][id0 id1 ... idN]
```

Writes append a CRC-checked record to the active segment and update the
in-memory *keydir* to point at the newest record per key. When the active
segment fills it rolls over and becomes immutable. Reads are one map lookup
plus one seek. `Compact` merges all immutable segments into one — keeping the
newest value per key, dropping tombstones, reclaiming space — while reads and
writes keep working.

A **MANIFEST** file records the live segments in recency order and is the
source of truth at startup. Compaction commits its swap by atomically replacing
the manifest, so a crash mid-compaction can never leave the index pointing at a
half-written or missing segment. See [DESIGN.md](DESIGN.md) for the tradeoffs,
including the manifest-as-commit-point design and the safe-swap rule.
