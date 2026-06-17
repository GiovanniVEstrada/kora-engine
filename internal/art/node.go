package art

// nodeKind identifies the concrete type behind a node interface value.
type nodeKind byte

const (
	kindNode4   nodeKind = iota
	kindNode16
	kindNode48
	kindNode256
	kindLeaf
)

// node is the common interface for all ART node types.
// Every non-leaf node stores up to N children, each addressed by a single key byte.
type node interface {
	kind() nodeKind
	// findChild returns a pointer to the child slot for byte b, or nil.
	findChild(b byte) *node
	// addChild inserts child n under byte b.
	// If the node is full it grows to the next size and returns the replacement;
	// otherwise it returns itself.
	addChild(b byte, n node) node
	// removeChild deletes the child under byte b.
	// If the node shrinks below its minimum it returns the replacement (smaller node);
	// otherwise it returns itself.
	removeChild(b byte) node
	// childCount returns the number of children currently stored.
	childCount() int
	// forEach calls fn(keyByte, child) for every non-nil child in key order.
	forEach(fn func(byte, node))
}

// leaf holds an actual key/value pair at the tip of the trie.
// value is any so the same ART can serve as a Bitcask keydir (value = entry
// struct) and as an LSM memtable (value = []byte) in later milestones.
type leaf struct {
	key   []byte
	value any
}

func (l *leaf) kind() nodeKind                    { return kindLeaf }
func (l *leaf) findChild(_ byte) *node            { return nil }
func (l *leaf) addChild(_ byte, _ node) node      { return l }
func (l *leaf) removeChild(_ byte) node           { return l }
func (l *leaf) childCount() int                   { return 0 }
func (l *leaf) forEach(_ func(byte, node))        {}

// artHeader is embedded in every inner node to hold path-compression state.
type artHeader struct {
	prefixLen uint32
	prefix    [maxPrefixLen]byte
}

// maxPrefixLen is the maximum number of prefix bytes stored inline.
// 64 bytes comfortably covers any realistic key length in this engine
// (keys are capped at 64 KiB but shared prefixes in practice are far shorter).
// Keeping it large avoids the need for lazy path-compression expansion, which
// would complicate splitNode and insertNode significantly.
const maxPrefixLen = 64

// prefixMatch returns how many bytes of h.prefix match key[depth:].
func (h *artHeader) prefixMatch(key []byte, depth int) int {
	limit := int(h.prefixLen)
	if limit > maxPrefixLen {
		limit = maxPrefixLen
	}
	i := 0
	for i < limit && depth+i < len(key) && h.prefix[i] == key[depth+i] {
		i++
	}
	return i
}
