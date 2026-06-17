# Kora — Design Notes

This document records every meaningful tradeoff made during the build. The goal
is to capture *why*, not just *what* — so future-me and future-readers understand
the reasoning, not just the result.

---

## Record format (M0)

**Format:**
```
[crc32 (4 bytes)] [timestamp_ms (8 bytes)] [key_len (4 bytes)] [value_len (4 bytes)] [key (variable)] [value (variable)]
```

**Tombstone encoding:** A delete is a normal record with `value_len = 0xFFFFFFFF`
(sentinel). Chose a sentinel over a flag byte to keep the header fixed-width,
which simplifies decoder logic — you always read exactly 20 bytes before touching
variable-length data.

**CRC placement:** CRC covers everything after itself (timestamp + lengths + key +
value). Placing it first means a reader can stream-validate without seeking back.

---

## In-memory index — the "keydir" (M1)

The keydir is a `map[string]entry` where `entry = {offset int64, length int}`,
pointing at the newest record for each key in the data file. `Set`/`Delete`
update it; `Get` looks up the entry and does a single `ReadAt` of exactly
`length` bytes, then decodes.

**Key type is `string`, not `[]byte`.** Go map keys must be comparable, and
`[]byte` isn't. Converting `string(key)` for map operations copies the bytes,
which also conveniently decouples the index from any caller-owned buffer.

**Why store `length` and not just `offset`:** a Bitcask keydir typically stores
the value size and value position. We store the *whole record's* offset and
length instead, so `Get` reads one contiguous slice and reuses the existing
record decoder (which re-validates the CRC on every read for free). Slightly
more bytes read per `Get`; much simpler and self-checking.

## Durability: fsync policy (M1)

`Set`/`Delete` call `fsync` (`File.Sync`) before returning when
`SyncOnWrite` is true (the default). This is the safe choice: an acknowledged
write is on stable storage, so a crash can't lose it — matching the project's
core principle.

**The tradeoff:** fsync is expensive (a real disk round-trip; often 1–10ms on
spinning media, less on SSD/NVMe but still far more than a buffered write).
With sync on every write, throughput is bounded by fsync latency. Setting
`SyncOnWrite: false` (`-no-sync` in the REPL) lets writes accumulate in the OS
page cache and return immediately — much faster, but a crash can lose the most
recent unflushed writes. M4's write-ahead log will give us a better point on
this curve: batch/group-commit durability without paying a full fsync per key.

## Crash-tolerant recovery (M1)

On `Open` the data file is scanned front to back to rebuild the keydir. A
**partial trailing record** — the result of a write interrupted mid-flush — is
detected as an `io.ErrUnexpectedEOF` from the decoder and the file is truncated
back to the last complete record. This keeps the append offset consistent with
the file contents, so subsequent appends don't sit on top of garbage. A CRC
mismatch *in the middle* of the log is treated as real corruption and surfaced
as an error rather than silently skipped.

---

## Segments and rollover (M2)

The single log is replaced by a series of numbered segment files
(`000001.data`, `000002.data`, ...). One segment is *active* (append target);
the rest are immutable. When the active segment reaches `MaxSegmentBytes` it is
flushed, reopened read-only, and a new active segment is created. The keydir
entry gains a `fileID` so a read knows which segment to seek into.

## Recency: from id-order to the manifest (M2)

**Recency is recorded explicitly by the `MANIFEST`, not inferred from segment
ids.** The manifest lists the live segments in order oldest -> newest; the last
is the active segment. Recovery reads the manifest and replays those segments in
that order, so later records win and tombstones remove keys.

An earlier version of M2 relied on the invariant *ascending id == ascending
recency* and reconstructed state by sorting filenames. That was abandoned for
two reasons: (1) it made compaction fragile (the merged output had to reuse the
minimum input id to stay "old," which meant overwriting an existing file — a
problem on Windows, see below); and (2) a directory scan can't distinguish a
live segment from a file leaked by a compaction that crashed before finishing.
The manifest solves both: ids are now just unique names, and recency lives in
the manifest. (The startup path still *migrates* a pre-manifest directory by
sorting ids once, then writes a manifest.)

**Manifest format** (big-endian, CRC-first like the record format):

```
[crc32 (4)] [version (4)] [nextID (4)] [count (4)] [id_0 (4)] ... [id_{count-1} (4)]
```

It is updated atomically: write `MANIFEST.tmp`, fsync, then `rename` over
`MANIFEST`. `rename` is atomic, so the manifest a reader sees is always either
the complete old one or the complete new one — never a torn write.

## Compaction (M2)

`Compact` merges **all** current immutable segments into one fresh segment,
keeping the newest value per key and dropping tombstones.

**Why merge all immutable segments at once (not a subset):** dropping a
tombstone is only safe if no *older* segment outside the merge could still hold
that key. By merging every segment older than the active one, a tombstone in
the merged set provably masks nothing that survives, so it can be discarded.
The active segment is strictly newer, so it never holds an older value to worry
about.

**Why the merged output gets a brand-new id:** because recency is in the
manifest, the merged segment doesn't need a "low" id to be treated as old — the
manifest simply lists it first. Writing to a fresh id means we never overwrite
an existing file, which removes the Windows open-handle hazard entirely and
makes the merged file invisible to recovery until the manifest names it.
(Consequence: the merged segment may exceed `MaxSegmentBytes`. Acceptable for
M2 — M3's leveled SSTable compaction addresses output sizing.)

**Preserving original timestamps:** merged records are re-encoded with
`record.EncodeAt` using each record's *original* timestamp, so the timestamp
field stays meaningful (it records when the value was written, not when it was
compacted).

## Safe swap & concurrency (M2)

Compaction's expensive phase — reading every immutable segment and writing the
merged output — runs **without the write lock**. Immutable segments never
change, so this is safe, and writes to the active segment continue throughout.
Only the final swap takes the write lock. A single `compactMu` ensures only one
compaction runs at a time.

**The commit point is the manifest rename.** The swap is ordered so that every
fallible step happens before any in-memory state changes:

1. open the merged segment (read-only);
2. atomically replace the manifest with the new segment list (the durable
   commit — on failure we roll back the in-memory order and delete the merged
   file, leaving the DB exactly as it was);
3. *only then* install the merged segment in the map and repoint keydir entries;
4. retire the old segments (best-effort file deletion — a failure here only
   leaks disk, never corrupts state).

This directly fixes the original review finding: the keydir is never repointed
to a segment that isn't open and named by a committed manifest. A crash at any
moment leaves a consistent state — either the old manifest (merge never
happened) or the new one (merge fully applied); leaked `*.data` files not named
by the manifest are deleted on the next `Open`.

**The swap's correctness rule:** a key is repointed to the merged segment
*only if its current keydir entry still lives in one of the snapshotted
segments*. If a concurrent `Set`/`Delete` moved the key to the active segment
while we were merging, that newer write must win, so we leave it alone. This is
what makes it safe to merge lock-free against a live, writable database.

**Reads during compaction:** `Get` holds the read lock across the actual disk
read (not just the keydir lookup). That way the brief write-locked swap cannot
close a segment file out from under an in-flight read. The cost is that reads
block for the duration of the swap; a future optimization is epoch/refcount-
based reclamation that avoids blocking reads at all. *(The concurrency test
validates correct values under concurrent reads + compaction; running it under
Go's `-race` detector requires a C toolchain, which isn't assumed here.)*

**Windows file semantics:** unlike POSIX, Windows won't delete or rename a file
with an open handle. Writing merged output to a fresh id (never overwriting an
open file) and committing via an atomic manifest rename means the swap no longer
depends on close-before-delete ordering for correctness — old-segment deletion
is now pure best-effort cleanup.

## Memtable data structure — ART (M3a decision)

The M1/M2 keydir is a plain `map[string]entry` — O(1) average reads/writes but
unordered, which makes range scans impossible and requires the full key set in
RAM. M3 replaces it with an **Adaptive Radix Tree** (`internal/art`).

**Why ART over skip list:** Both give O(log n) ordered access in the worst case,
but ART lookup is O(k) where k is key length — independent of dataset size.
For large datasets (millions of keys) k grows much slower than log n. The deeper
reason is cache locality: skip list and BST nodes are heap-allocated and
pointer-chased; ART's adaptive node sizes (4 / 16 / 48 / 256 children) pack
keys and child pointers together in a single cache line for small fanouts. Keys
in this engine are `[]byte`, exactly the right type for a trie-based structure.

**Why ART over red-black tree / AVL tree:** Same cache-line argument, plus ART
avoids rebalancing entirely — there is no rotation logic to get wrong.

**Implementation strategy — correctness before performance:**

1. *Interface-based nodes first.* `Node4`, `Node16`, `Node48`, `Node256` all
   implement a `node` interface. Node-growth transitions are type-safe. Interface
   dispatch adds ~1–2 ns per call — not the bottleneck at correctness stage.
2. *Allocation optimization second.* Profile under load; if GC pressure from
   small heap nodes shows up, introduce `sync.Pool` or an arena allocator.
   An arena is the better long-term choice for a database (deterministic
   reclamation, no GC scanner overhead).
3. *`unsafe.Pointer` hot path last* — only if profiling proves interfaces are
   the bottleneck after allocation is optimized. When used, casts are isolated
   in tiny helpers; `unsafe` never leaks into tree logic.

**Concurrency model (M3a):** single-writer, multi-reader with a RW mutex. The
writer holds an exclusive lock during insert/delete; readers share it. Lock-free
concurrent writes (atomic node replacement) are deferred until after correctness
is established — they interact badly with path compression and node growth.

**Node48 special case:** unlike Node4/Node16, Node48 does not store a parallel
`keys[]`/`children[]` array. It uses a 256-byte key→slot index plus a 48-slot
child array. The indirection is worth it because a 48-entry parallel array would
require SIMD search to beat the index lookup. Treat Node48 as a distinct
sub-problem during implementation.

---

## SSTable on-disk format (M3b / M3e)

An SSTable is an immutable file of key-value records written in strictly
ascending key order. It has four sections:

```
[data section]
  record*:  key_len(4) | val_len(4) | key | value
            val_len == 0xFFFFFFFF is a tombstone (no value bytes follow)
[bloom section]
  raw bit array of the per-SSTable Bloom filter
  (bloom_offset = index_offset - bloom_size; no separate field needed)
[index section]
  entry*:   key_len(4) | key | offset(8)
            one entry per indexStride (16) records; offset is the byte
            position of that record in the data section
[footer — last 28 bytes]
  index_offset(8) | index_count(4) | data_count(4)
  | bloom_size(4) | bloom_k(4) | magic(4="KORA")
```

All multi-byte integers are big-endian.

**Why a sparse index instead of a full index:** a full index requires one
in-memory entry per key, which defeats the purpose of flushing to disk.
A sparse index (one entry every 16 records) means the reader loads at most
`ceil(N/16)` entries regardless of dataset size, and scans at most 16 records
per point lookup. The tradeoff is a linear scan within each block; for the
current key sizes this is negligible.

**Why tombstones in SSTables:** when a key is deleted it must be represented
in the SSTable so that compaction can suppress older versions of the same key
that live in lower-level SSTables. Tombstones are dropped only when a
compaction covers all SSTables where that key could exist (same rule as M2's
segment compaction).

**Read path:** `Open` reads the 28-byte footer, loads the Bloom filter and
index section into memory. `Get(key)` checks the Bloom filter first (returns
immediately on a definite miss), then binary-searches the index for the last
entry with key ≤ target, seeks to that offset, and scans forward ≤
indexStride records. `Iterator` streams the data section from offset 0 to
`dataEnd` (= `index_offset − bloom_size`).

**Ordering guarantee:** the `Writer` enforces strictly ascending key order,
returning an error on any out-of-order write. This matches the precondition
required by the binary-search read path.

---

## Multi-source read path (M3c)

`Get` checks sources in recency order: memtable first, then SSTables newest →
oldest. A key found anywhere stops the search immediately.

**Tombstone representation in the memtable:** deleted keys are stored as
`type tombstone struct{}`, a distinct zero-size type. This lets the memtable
distinguish three states for any key:

- `[]byte` value → live
- `tombstone{}` → deleted (shadows any value in older SSTables)
- absent from memtable → not yet written or already flushed

Using `nil` or an empty slice would conflate "deleted" with "set to empty
value." The distinct type is zero-cost at runtime.

**`RawGet` vs `Get` on the SSTable reader:** `Get` collapses tombstone +
absent into a single `(nil, false)`. `RawGet` returns a separate `isTombstone`
flag. The multi-source read path uses `RawGet` so a tombstone in a newer
SSTable correctly short-circuits the search before reaching an older one that
might hold the last live value.

**SSTables are session-local in M3:** on `Open`, all SSTable files from prior
sessions are deleted and the memtable is rebuilt entirely from the segment log.
This defers cross-restart SSTable persistence to M4's write-ahead log +
checkpoint design. The `DefaultMaxMemBytes` is 4 MiB so existing tests
(small datasets) don't accidentally trigger flushes.

---

## SSTable compaction (M3d)

`CompactSSTables` merges all open SSTable readers into a single file via a
**k-way merge** using a min-heap:

- Each SSTable contributes a `CompactionIterator` (exposes tombstones)
- Heap ordering: ascending key; for equal keys, ascending `sstIdx` (0 =
  newest SSTable inserted first — it wins the tie)
- The first (newest) occurrence of each key is written to the output;
  subsequent occurrences of the same key are skipped as stale versions
- **Tombstones are dropped** because the merge covers every SSTable —
  there is no older source outside the merge that could hold a live value
  the tombstone would need to suppress. This is the same invariant as M2's
  segment compaction.

**Atomic swap:** the merged file is written to a fresh SSTable id before the
in-memory state is touched. Once the merged `Reader` is open, the old readers
are replaced in one assignment (`db.ssReaders = []*sstable.Reader{newReader}`),
then old files are closed and deleted best-effort. A crash before the swap
leaves the old SSTables intact; a crash after leaves an orphaned merged file
that `cleanupSSTDir` removes on the next `Open`.

---

## Bloom filters (M3e)

Each SSTable carries a Bloom filter to let `RawGet` skip I/O for keys that
are definitely absent.

**Algorithm:** Kirsch-Mitzenmacher double-hashing. A single 64-bit FNV-1a
hash is split into two 32-bit halves `(h1, h2)`; the *i*-th probe position
is `(h1 + i·h2) % m`. This is asymptotically equivalent to *k* independent
hash functions while requiring only one hash computation per key.

**Parameters:** 10 bits per element, k = 7 hash functions → theoretical
false-positive rate ≈ 0.81%. These are fixed constants; future work could
tune per-level.

**m must be byte-aligned:** `bloom.New(n)` rounds `m` up to the nearest byte
boundary so that `Load(f.Bytes(), f.K())` — which reconstructs `m` as
`len(bits)*8` — agrees exactly with the original `m`. Without this rounding,
the probe positions during `Has` would differ from those during `Add` and the
filter would produce false negatives for its own added keys.

**Tombstones are in the filter:** a deleted key is added to the Bloom filter
just like a live key. If tombstones were excluded, `RawGet` would short-circuit
on a "definitely absent" filter hit and fall through to an older SSTable,
potentially returning a stale live value — a correctness bug, not just a
performance issue.

**Writer builds the filter at `Close` time** by re-scanning the data section
(one sequential pass, already in the OS page cache from the write) with the
exact insertion count. This avoids pre-allocating memory for an unknown number
of keys and keeps the writer interface unchanged (`NewWriter(path)`).

---

## Range scans (M3 — range scans)

`Scan(start, end []byte)` returns a snapshot iterator over all live keys in
`[start, end]` in ascending order. Either bound may be `nil` (open-ended).

**Merge-iterator design:** identical to `CompactSSTables`'s k-way heap, but
instead of writing to a file the results are collected into a slice. Sources:

0. Memtable (ART `Iterator()`) — pre-collects all leaves into a slice on
   call, making it an instant snapshot safe to use after lock release
1..n. SSTable `ScanIterator(start, end)` — uses `seekOffset(start)` to jump
   near the start key via the sparse index, then streams forward

Tombstones suppress older versions of the same key but are not emitted.

**Snapshot-under-lock:** the entire k-way merge runs under `db.mu.RLock`.
Results are collected eagerly into a `[]scanKV` slice, then the lock is
released. The returned iterator function walks the slice. This makes the
iterator safe to use concurrently with writes and `CompactSSTables` — the
scan sees a consistent point-in-time view of the engine with no risk of
reading from a closed SSTable file. The memory cost is O(result size), which
is acceptable for M3; a lazy iterator with ref-counted SSTable handles is
the natural M4/M5 upgrade.

**Ordering guarantee:** the `Writer` enforces strictly ascending key order,
returning an error on any out-of-order write. This matches the precondition
required by the binary-search read path.

---

## Record size limits (M2 hardening)

`Encode` rejects keys larger than `MaxKeySize` (64 KiB) or values larger than
`MaxValueSize` (1 GiB) with a typed error, rather than silently truncating
`len()` into a `uint32`. `Decode` enforces the same caps on the on-disk lengths
*before allocating*, so a corrupted file claiming a multi-gigabyte key is
rejected as `ErrCorrupted` instead of triggering a huge allocation or OOM. Both
caps sit well below `math.MaxUint32`, so summing key+value lengths can't
overflow.
