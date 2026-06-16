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

## Segments and the recency invariant (M2)

The single log is replaced by a series of numbered segment files
(`000001.data`, `000002.data`, …). One segment is *active* (append target);
the rest are immutable. When the active segment reaches `MaxSegmentBytes` it is
flushed, reopened read-only, and a new active segment is created. The keydir
entry gains a `fileID` so a read knows which segment to seek into.

**The core invariant: ascending segment id == ascending recency.** A higher id
is always newer. This makes recovery trivial — scan segments in ascending id
order, exactly like the M1 single-file scan, and later records win. The active
segment always holds the maximum id (new actives are allocated from a
monotonic counter).

**Why id ordering matters even though the keydir is authoritative at runtime:**
the keydir always points at the truly newest record while the process is live,
so id order is irrelevant for online reads. But after a crash the keydir is
*gone* and must be rebuilt by scanning files. If a segment's id didn't reflect
its recency, the scan could pick a stale value as the winner. Compaction is
designed to preserve this invariant (below).

## Compaction (M2)

`Compact` merges **all** current immutable segments into one fresh segment,
keeping the newest value per key and dropping tombstones.

**Why merge all immutable segments at once (not a subset):** dropping a
tombstone is only safe if no *older* segment outside the merge could still hold
that key. By merging every segment older than the active one, a tombstone in
the merged set provably masks nothing that survives, so it can be discarded.
The active segment is strictly newer, so it never holds an older value to worry
about.

**Why the merged output reuses the minimum input id:** the merged data is older
than the active segment and older than anything a concurrent rollover creates
mid-compaction. Reusing the smallest input id keeps the merged segment below
those in id order, preserving *ascending id == ascending recency*. The active
segment keeps its max id; recovery still works unchanged. (Consequence: the
merged segment may exceed `MaxSegmentBytes`. Acceptable for M2 — M3's leveled
SSTable compaction addresses output sizing.)

**Preserving original timestamps:** merged records are re-encoded with
`record.EncodeAt` using each record's *original* timestamp, so the timestamp
field stays meaningful (it records when the value was written, not when it was
compacted).

## Safe swap & concurrency (M2)

Compaction's expensive phase — reading every immutable segment and writing the
merged output to a temp file — runs **without the write lock**. Immutable
segments never change, so this is safe, and writes to the active segment
continue throughout. Only the final swap takes the write lock: it repoints
keydir entries, closes/deletes the old segment files, renames the temp file
into place, and opens the merged segment. A single `compactMu` ensures only one
compaction runs at a time.

**The swap's correctness rule:** a key is repointed to the merged segment
*only if its current keydir entry still lives in one of the snapshotted
segments*. If a concurrent `Set`/`Delete` moved the key to the active segment
while we were merging, that newer write must win, so we leave it alone. This is
what makes it safe to merge lock-free against a live, writable database.

**Reads during compaction:** `Get` holds the read lock across the actual disk
read (not just the keydir lookup). That way the brief write-locked swap cannot
close a segment file out from under an in-flight read. The cost is that reads
block for the duration of the swap (a few file operations); a future
optimization is epoch/refcount-based reclamation that avoids blocking reads at
all. *(Note: the concurrency test validates correct values under concurrent
reads + compaction; running it under Go's `-race` detector requires a C
toolchain, which isn't assumed here.)*

**Windows file semantics:** unlike POSIX, Windows won't let you delete or rename
a file with an open handle, so the swap closes old segment handles *before*
removing/renaming their files. This ordering leaves a small window where a
failure between delete and rename is unrecoverable — noted as a known M2
limitation; M4's WAL and a proper manifest would make the swap atomic.
