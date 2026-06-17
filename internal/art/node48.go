package art

// node48 holds 17–48 children.
// Instead of a parallel keys[]/children[] array it uses a 256-byte index:
//   index[b] == 0   → no child for byte b
//   index[b] == s+1 → child is in slots[s]   (s is 0-based, stored as 1-based)
//
// This lets findChild do a single array lookup instead of a search, while
// keeping the children array compact (48 slots vs 256).
type node48 struct {
	artHeader
	nChildren uint8
	index     [256]uint8 // 0 = empty; 1-based slot number otherwise
	slots     [48]node
}

func newNode48() *node48 { return &node48{} }

func (n *node48) kind() nodeKind { return kindNode48 }

func (n *node48) childCount() int { return int(n.nChildren) }

func (n *node48) findChild(b byte) *node {
	s := n.index[b]
	if s == 0 {
		return nil
	}
	return &n.slots[s-1]
}

// insertAt is a helper used during node growth (no capacity check).
func (n *node48) insertAt(b byte, child node) {
	// Find the first free slot.
	var s uint8
	for s = 0; s < 48; s++ {
		if n.slots[s] == nil {
			break
		}
	}
	n.slots[s] = child
	n.index[b] = s + 1
	n.nChildren++
}

func (n *node48) addChild(b byte, child node) node {
	if int(n.nChildren) < 48 {
		n.insertAt(b, child)
		return n
	}
	// Grow to node256.
	n256 := newNode256()
	n256.artHeader = n.artHeader
	for i := 0; i < 256; i++ {
		if s := n.index[i]; s != 0 {
			n256.children[i] = n.slots[s-1]
			n256.nChildren++
		}
	}
	return n256.addChild(b, child)
}

func (n *node48) removeChild(b byte) node {
	s := n.index[b]
	if s == 0 {
		return n
	}
	n.slots[s-1] = nil
	n.index[b] = 0
	n.nChildren--
	// Shrink to node16 when 16 or fewer children remain.
	if int(n.nChildren) <= 16 {
		n16 := newNode16()
		n16.artHeader = n.artHeader
		for i := 0; i < 256; i++ {
			if si := n.index[i]; si != 0 {
				n16.addChild(byte(i), n.slots[si-1])
			}
		}
		return n16
	}
	return n
}

func (n *node48) forEach(fn func(byte, node)) {
	// Walk all 256 possible bytes in order so iteration is sorted.
	for i := 0; i < 256; i++ {
		if s := n.index[i]; s != 0 {
			fn(byte(i), n.slots[s-1])
		}
	}
}
