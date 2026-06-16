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

*Add a new section for each tradeoff you make in M1, M2, …*
