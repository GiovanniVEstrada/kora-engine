# StrataDB

A from-scratch database storage engine written in Go, built milestone by
milestone from a Bitcask-style log-structured KV store up through a full
LSM-tree with crash recovery and a network server.

## Status

- [x] M0 — Foundations & record format
- [x] M1 — Append-only KV store with in-memory hash index
- [ ] M2 — Multiple segments + compaction
- [ ] M3 — LSM-tree (memtable → SSTables → Bloom filters)
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
survive a restart.

## Architecture (M1 — Bitcask model)

```
            Set/Delete                         Get
                │                                │
                ▼                                ▼
        ┌───────────────┐               ┌───────────────┐
        │ encode record │               │ keydir lookup │  key → {offset,length}
        │  + append     │               └───────┬───────┘
        └───────┬───────┘                       │ ReadAt(offset,length)
                │ fsync                          ▼
                ▼                        ┌───────────────┐
        ┌───────────────────────────────┤  data.log     │  append-only,
        │  data.log (append-only)        │  (decode+CRC) │  read by offset
        └───────────────────────────────┴───────────────┘
                │
                │ on Open: scan front→back, rebuild keydir
                ▼
        ┌───────────────┐
        │  keydir (RAM) │  map[string]{offset,length}
        └───────────────┘
```

Writes append a CRC-checked record to a single log file and update an in-memory
index (the *keydir*) to point at the newest record per key. Reads are one map
lookup plus one seek. On startup the log is replayed front to back to rebuild
the keydir; later records win and tombstones remove keys. See
[DESIGN.md](DESIGN.md) for the tradeoffs.
