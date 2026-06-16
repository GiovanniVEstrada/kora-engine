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
