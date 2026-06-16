# StrataDB

A from-scratch database storage engine written in Go, built milestone by
milestone from a Bitcask-style log-structured KV store up through a full
LSM-tree with crash recovery and a network server.

## Status

- [x] M0 — Foundations & record format
- [ ] M1 — Append-only KV store with in-memory hash index
- [ ] M2 — Multiple segments + compaction
- [ ] M3 — LSM-tree (memtable → SSTables → Bloom filters)
- [ ] M4 — Write-ahead log & crash recovery
- [ ] M5 — TCP server

See [MILESTONES.md](MILESTONES.md) for the full roadmap and
[DESIGN.md](DESIGN.md) for tradeoff notes.

## Getting started

```bash
go test ./...
make test
```

## Architecture

*(diagram goes here once M1 is done)*
