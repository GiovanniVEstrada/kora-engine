package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	const n = 1000
	f := New(n)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		f.Add(keys[i])
	}
	for _, k := range keys {
		if !f.Has(k) {
			t.Fatalf("false negative for %q", k)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	const inserted = 1000
	const probed = 10_000

	f := New(inserted)
	for i := 0; i < inserted; i++ {
		f.Add([]byte(fmt.Sprintf("present-%d", i)))
	}

	fp := 0
	for i := 0; i < probed; i++ {
		if f.Has([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / probed
	// At 10 bits/element and k=7 the theoretical FPR is ~0.81%.
	// Allow up to 3% to keep the test tolerant of hash collisions.
	if rate > 0.03 {
		t.Fatalf("false positive rate %.2f%% exceeds 3%%", rate*100)
	}
}

func TestLoadRoundtrip(t *testing.T) {
	f := New(50)
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, k := range keys {
		f.Add(k)
	}

	// Serialize and reconstruct.
	bits := make([]byte, len(f.Bytes()))
	copy(bits, f.Bytes())
	f2 := Load(bits, f.K())

	for _, k := range keys {
		if !f2.Has(k) {
			t.Fatalf("loaded filter lost key %q", k)
		}
	}
}

func TestEmptyFilter(t *testing.T) {
	f := New(0) // minimum size
	if f.Has([]byte("anything")) {
		// Possible (all bits might be 0), but for a freshly allocated filter
		// with no Adds, Has should return false because no bit is set.
		// This is always false for an empty bit array.
	}
	// The real guarantee: after Add, Has must be true.
	f.Add([]byte("x"))
	if !f.Has([]byte("x")) {
		t.Fatal("key not found after Add")
	}
}
