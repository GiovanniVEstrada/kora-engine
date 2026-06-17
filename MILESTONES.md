# Kora — Build Roadmap

A from-scratch database storage engine, built in milestones. You start with a
log-structured key-value store (the Bitcask model), grow it into a full
LSM-tree, make it crash-safe with a write-ahead log, then put a server in front
of it. Every milestone is independently done and demoable, so you always have
something working — and something to show.

---

## Guiding principles

- **Each milestone ships.** No milestone depends on a later one being finished.
  At the end of every one you have a working, testable artifact.
- **Correctness before performance.** Get it right, prove it with tests, then
  make it fast. A fast database that loses data is worthless.
- **Write the design down.** Keep a `DESIGN.md` next to the code. Every time you
  make a tradeoff (sync vs async I/O, size-tiered vs leveled compaction), write
  one paragraph on why. This is half the value of the project.
- **Minimum real deliverable = M2.** A log-structured KV store with compaction
  is already a genuine database. Commit to reaching M2 before you decide whether
  to push into LSM-tree territory. That's your anti-abandonment line.

---

## Tech decisions (locked in M0)

**Language:** Go — ideal for a systems project. Expect to move a bit slower while
learning the language, but you'll get low-level control and a toolchain that's
perfectly suited to storage work.

**I/O model:** Start with simple synchronous file I/O. Async/buffered I/O is a
later optimization, not an M1 concern.

**On-disk format:** Design this on paper before coding (M0).

**Storage layout:** Single data directory; files are append-only and immutable
once rolled over. Never edit a file in place until you intentionally build a
B-tree engine (optional, much later).

---

## M0 — Foundations & setup

**Rough effort:** 1–2 days

**Goal:** Repo, tooling, and the on-disk record format defined before you write
real logic.

- [ ] Initialize repo, formatter (`gofmt`), linter (`golangci-lint`), and test runner (`go test`).
- [ ] Set up a test harness you'll actually use (`make test`).
- [ ] Design the record format on paper. A solid starting layout:
  ```
  [crc32 (4 bytes)][timestamp (8)][key_len (4)][value_len (4)][key][value]
  ```
- [ ] Decide how a delete is represented (a tombstone: e.g. a flag byte or a sentinel `value_len`).
- [ ] Write the encoder/decoder for a single record + round-trip unit tests.

**Done when:** you can encode a record to bytes and decode it back, byte-for-byte,
with passing tests.

---

## M1 — Append-only KV store with in-memory hash index

**Rough effort:** 1–2 weeks · This is the Bitcask design — it's a real architecture, not a toy

**Goal:** A working, persistent key-value store.

Concepts you'll internalize: log-structured storage, append-only writes,
in-memory indexing, serialization, the separation of index from data, basic
durability (fsync), and startup recovery.

- [ ] **Append writer:** `Set(key, value)` serializes a record and appends it to
  a single data file. Record the byte offset where it landed.
- [ ] **In-memory index (the "keydir"):** a hashmap `key → { offset, length }`.
  `Set` updates the map to point at the newest record.
- [ ] **`Get(key)`:** look up the offset in the map, seek to it in the file,
  read and decode the record, return the value.
- [ ] **`Delete(key)`:** append a tombstone record and remove the key from the index.
- [ ] **Startup recovery:** on open, scan the data file front to back and rebuild
  the in-memory index (later records win; tombstones remove keys).
- [ ] **Durability knob:** call `fsync` on write (or batch it) — and note the
  performance tradeoff in `DESIGN.md`.
- [ ] REPL/CLI to exercise `GET` / `SET` / `DEL`.

**Done when:** you can set/get/delete, kill the process, restart it, and all your
data is still there.

**Stretch:** validate CRC on read to detect corruption; make the fsync policy
configurable (every write vs. periodic).

---

## M2 — Multiple segments + compaction

**Rough effort:** 1–2 weeks · where it starts getting genuinely hard

**Goal:** Stop the log from growing forever and reclaim space from stale/deleted data.

Concepts: segment files, compaction/merge, space reclamation, garbage collection
of stale versions, and your first taste of concurrency — reads must keep working
while a merge runs.

- [ ] **Segment rollover:** when the active file passes a size threshold, close it
  (it becomes immutable) and open a new active file. Index entries now carry a
  `file_id` too.
- [ ] **Compaction/merge:** a routine that reads old segments, keeps only the
  latest value per key, drops tombstoned keys, and writes a fresh compacted
  segment. Then atomically swaps the old segments out and updates the index.
- [ ] **Safe swap:** make sure in-flight reads against a segment being deleted
  don't break (reference-count files, or swap the file set atomically).
- [ ] **Hint files** *(optional but great):* alongside each compacted segment,
  write a small file of `key → offset` so startup can rebuild the index from hint
  files instead of rescanning all data.

**Done when:** you can write far more data (with lots of overwrites/deletes) than
the live set needs, trigger compaction, watch disk usage drop, and confirm every
live key is still correct.

---

## M3 — The LSM-tree (the deep milestone)

**Rough effort:** 3–5 weeks · break it into sub-steps · this is the heart of the project

The hash-index design has two hard limits: the index must fit in RAM, and there
are no range queries (keys aren't ordered on disk). The log-structured merge-tree
fixes both. This is what LevelDB, RocksDB, and Cassandra are built on.

Concepts: sorted in-memory structures (ART), immutable sorted files (SSTables),
sparse indexing, k-way merge of sorted runs, Bloom filters, leveled vs
size-tiered compaction, and the read/write-amplification tradeoffs that define
storage-engine design.

### M3a — Sorted memtable (Adaptive Radix Tree)

Replace the hashmap keydir with an **Adaptive Radix Tree** (`internal/art`).

**Why ART over skip list / red-black tree:** ART lookup is O(k) where k is key
length, not O(log n) where n is number of keys — faster for large datasets.
More importantly, its node layout (4 / 16 / 48 / 256 children) is cache-friendly
in a way that pointer-heavy BSTs and skip lists are not. Keys in this engine are
`[]byte`, which is exactly the right fit for a trie-based structure.

**Implementation plan — three phases:**

- [x] **Phase 1 — Interface-based nodes.** Build `Node4`, `Node16`, `Node48`,
  `Node256` as structs implementing a `node` interface. This keeps node-growth
  transitions (`Node4 → Node16 → ...`) type-safe and easy to test. Interface
  dispatch overhead is negligible at this stage.
- [ ] **Phase 2 — Allocation optimization.** Profile under load. If GC pressure
  from millions of small heap nodes shows up, introduce object pools
  (`sync.Pool`) or an arena allocator for nodes. `sync.Pool` is the simpler
  first step; a page-based arena is better long-term for a database.
- [ ] **Phase 3 — `unsafe.Pointer` hot path** *(only if Phase 2 profiling
  justifies it).* Replace the interface with a shared header + `kind` byte and
  manual pointer casts. Keep casts isolated in tiny helper functions; never
  spread `unsafe` logic across the tree.

**Concurrency model:** single-writer, multi-reader. The writer holds an
exclusive lock during insert/delete; readers take a shared lock. Concurrent
writers would require lock-free node replacement (possible but significantly
harder — defer to after correctness is locked).

**Node48 note:** `Node48` is the trickiest node type. It uses a 256-byte
key→slot lookup table plus a 48-slot child array, rather than a parallel
keys[]/children[] layout like Node4/Node16. Treat it as its own mini-milestone.

- [x] **M3a done when:** `internal/art` passes a property-based oracle test
  (random insert/get/delete against a reference `map[string][]byte`), the
  iterator yields keys in sorted order, and the store's keydir is an `*art.Tree`.

### M3b — Flush to SSTable

When the memtable hits a size threshold, freeze it and write it to disk as an
immutable Sorted String Table: sorted records in blocks, plus a sparse index
(offset of every Nth key) and a footer. You no longer need every key in RAM.

- [x] Define SSTable on-disk format (block layout, sparse index, footer).
- [x] Implement flush: iterate ART in key order, write sorted blocks.
- [x] Implement SSTable reader: use sparse index to seek near a key, scan block.

### M3c — Multi-source read path

A `Get` now checks the active memtable, then any frozen memtable, then SSTables
newest → oldest, using each SSTable's sparse index to seek near the key and scan
a block.

- [x] Stack memtable + SSTable readers behind a unified `Get`.
- [x] Frozen memtable: immutable ART snapshot awaiting flush.

### M3d — SSTable compaction

Merge multiple SSTables into fewer, larger ones (size-tiered first, leveled
later). This is a k-way merge of sorted runs — keep newest version per key,
drop tombstones.

- [x] k-way merge using a min-heap over SSTable iterators.
- [x] Atomic swap of old SSTables for merged output (same manifest pattern as M2).

### M3e — Bloom filters

Add a Bloom filter per SSTable so a `Get` can skip reading a file that
definitely doesn't contain the key. Measure the read-amp improvement.

- [x] One Bloom filter per SSTable, stored in the SSTable footer.
- [x] `Get` checks filter before seeking into a file.

### Range scans

Expose `Scan(startKey, endKey)` by merging iterators over the memtable and all
SSTables. ART's in-order iterator makes the memtable side trivial.

- [x] `Scan` API on the store.
- [x] Merge-iterator over ART + SSTable iterators.

**M3 done when:** memory stays bounded regardless of dataset size, reads are
served from disk via the sparse index, range scans return sorted results, and
compaction keeps the SSTable count under control.

---

## M4 — Durability & crash recovery (write-ahead log)

**Rough effort:** ~1 week

**Goal:** Never lose an acknowledged write, even if the process dies before the
memtable is flushed.

Concepts: write-ahead logging, the durability-vs-performance tradeoff, atomicity,
checkpointing.

- [x] **WAL append:** before applying a write to the memtable, append it to a WAL
  file and `fsync`. Only then acknowledge the write.
- [x] **Recovery:** on startup, replay the WAL into a fresh memtable.
- [x] **Checkpoint:** after a memtable is safely flushed to an SSTable, discard
  (or rotate) the corresponding WAL.
- [x] **Crash test it** — this is the milestone you prove, not just claim.

**Done when:** you can `kill -9` mid-write, restart, and every acknowledged write
is present.

---

## M5 — Access layer / server

**Rough effort:** ~1 week

**Goal:** Turn the library into a database server you can talk to.

Concepts: client-server architecture, protocol/parser design, connection and
concurrency handling.

- [x] A TCP server speaking RESP2 (Redis wire protocol); `redis-cli` and any
  Redis client library work out of the box.
- [x] Handle multiple concurrent clients (one goroutine per connection).
- [x] A thin Go client library (`client/`) with typed methods for all commands.

**Done when:** you can connect over the network and run commands against your
engine from a separate process.

---

## M6+ — Advanced stretch (pick what excites you)

- Transactions + MVCC with snapshot isolation.
- A B-tree storage engine as an alternative backend — then write up the
  read/write-amplification comparison vs your LSM. Building both is a standout.
- Block compression for SSTables (e.g. Snappy/LZ4-style).
- Secondary indexes.
- Replication — leader/follower log shipping. (This is the natural bridge to a
  distributed-systems project: add Raft consensus next.)
- Observability — metrics for read/write amp, compaction stats, p99 latency.

---

## Testing strategy (do this from M1, not at the end)

- **Round-trip tests:** encode → decode returns the original record.
- **Oracle test:** run thousands of random `set`/`get`/`delete` ops against your
  engine and against a plain in-memory map; assert they always agree. This one
  test will catch most correctness bugs.
- **Property-based / fuzz testing:** fuzz the record parser with garbage bytes;
  it should never crash, only reject.
- **Crash-recovery test:** spawn the DB as a subprocess, write known data,
  `kill -9`, restart, assert durability. Automate it.
- **Benchmark harness:** measure write throughput, read throughput, and p99
  latency — before and after adding Bloom filters, before and after compaction.
  Save the numbers; they're your case-study evidence.

---

## Make it showcase-grade (low extra effort, high payoff)

- `README.md` with a clear architecture diagram and a "how it works" walkthrough.
- `DESIGN.md` documenting every real tradeoff you made and why.
- A benchmark writeup — even a short one with a couple of charts.
- A short case-study / blog post narrating the LSM-tree milestone. Explaining
  read/write amplification clearly signals you actually understand systems.

---

## Resources

- **Designing Data-Intensive Applications** — Martin Kleppmann. Chapter 3
  ("Storage and Retrieval") maps almost exactly onto M1→M3: hash indexes →
  SSTables → LSM-trees → B-trees. Your primary companion.
- **Database Internals** — Alex Petrov. Deeper on storage-engine mechanics,
  B-trees, and LSM internals.
- **The Bitcask paper** — "A Log-Structured Hash Table for Fast Key/Value Data".
  Short and readable — it is the M1/M2 design.
- **"Let's Build a Simple Database"** (cstack's db tutorial) — a SQLite-style
  clone in C, B-tree-focused; good complementary perspective.
- **LevelDB / RocksDB** source and design docs — production LSM engines worth
  reading once you've built M3.

---

## Rough timeline (solo, part-time — adjust to your pace)

| Milestone | Effort     | Cumulative result                          |
|-----------|------------|--------------------------------------------|
| M0        | 1–2 days   | Format defined, tooling ready              |
| M1        | 1–2 weeks  | A persistent KV store                      |
| M2        | 1–2 weeks  | A compacting log-structured DB *(minimum real deliverable)* |
| M3        | 3–5 weeks  | A real LSM-tree with range queries         |
| M4        | ~1 week    | Crash-safe                                 |
| M5        | ~1 week    | A networked database server                |
| M6+       | open-ended | Pick your adventure                        |
