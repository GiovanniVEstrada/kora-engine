// Package bloom implements a space-efficient probabilistic set (Bloom filter).
// It guarantees no false negatives: Has returns false only when the key was
// definitely never added. It may report false positives (returning true for a
// key that was never inserted), with a rate that depends on the number of bits
// per element and the number of hash functions.
package bloom

import "hash/fnv"

const defaultK = 7 // k=7 with 10 bits/element gives ~0.81% false-positive rate

// Filter is a Bloom filter backed by a flat bit array.
type Filter struct {
	bits []byte
	m    uint32 // total bits in the array
	k    uint32 // number of hash functions (probes per key)
}

// New returns a Filter sized for n expected insertions at ~1% false-positive
// rate (10 bits per element, 7 hash functions).
//
// m is rounded up to the nearest byte boundary so that Load(f.Bytes(), f.K())
// reconstructs an identical filter: Load derives m as len(bits)*8, which must
// equal the original m for probe positions to agree.
func New(n int) *Filter {
	if n <= 0 {
		n = 1
	}
	byteCount := (n*10 + 7) / 8 // ceil(n*10 / 8)
	m := uint32(byteCount * 8)   // round up to byte boundary
	return &Filter{
		bits: make([]byte, byteCount),
		m:    m,
		k:    defaultK,
	}
}

// Add records key as a member of the set.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.k; i++ {
		bit := (h1 + i*h2) % f.m
		f.bits[bit>>3] |= 1 << (bit & 7)
	}
}

// Has reports whether key might be in the set. A false return is definitive
// (key was never added). A true return may be a false positive.
func (f *Filter) Has(key []byte) bool {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.k; i++ {
		bit := (h1 + i*h2) % f.m
		if f.bits[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}

// Bytes returns the raw bit array for serialization. The returned slice is
// owned by the Filter; callers that need a stable copy must copy it.
func (f *Filter) Bytes() []byte { return f.bits }

// K returns the number of hash functions, which must be stored alongside
// Bytes() to allow exact reconstruction via Load.
func (f *Filter) K() uint32 { return f.k }

// Load reconstructs a Filter from a serialized bit array and its original k.
func Load(bits []byte, k uint32) *Filter {
	return &Filter{
		bits: bits,
		m:    uint32(len(bits)) * 8,
		k:    k,
	}
}

// hashes returns two independent 32-bit hashes of key. It uses a single
// 64-bit FNV-1a hash and splits it in half (Kirsch-Mitzenmacher trick): the
// i-th probe position is (h1 + i*h2) % m, which is asymptotically as good as
// k truly independent hash functions.
func hashes(key []byte) (h1, h2 uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return uint32(sum), uint32(sum >> 32)
}
