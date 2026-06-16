# StrataDB — Design Notes

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

## Record size limits (M2 hardening)

`Encode` rejects keys larger than `MaxKeySize` (64 KiB) or values larger than
`MaxValueSize` (1 GiB) with a typed error, rather than silently truncating
`len()` into a `uint32`. `Decode` enforces the same caps on the on-disk lengths
*before allocating*, so a corrupted file claiming a multi-gigabyte key is
rejected as `ErrCorrupted` instead of triggering a huge allocation or OOM. Both
caps sit well below `math.MaxUint32`, so summing key+value lengths can't
overflow.
