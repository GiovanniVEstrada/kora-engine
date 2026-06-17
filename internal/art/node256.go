package art

// node256 holds 49–256 children.
// Each of the 256 possible byte values maps directly to a child slot.
// findChild and addChild are O(1) with no search at all.
type node256 struct {
	artHeader
	nChildren uint16
	children  [256]node
}

func newNode256() *node256 { return &node256{} }

func (n *node256) kind() nodeKind { return kindNode256 }

func (n *node256) childCount() int { return int(n.nChildren) }

func (n *node256) findChild(b byte) *node {
	if n.children[b] == nil {
		return nil
	}
	return &n.children[b]
}

func (n *node256) addChild(b byte, child node) node {
	if n.children[b] == nil {
		n.nChildren++
	}
	n.children[b] = child
	return n
}

func (n *node256) removeChild(b byte) node {
	if n.children[b] != nil {
		n.children[b] = nil
		n.nChildren--
	}
	// Shrink to node48 when 48 or fewer children remain.
	if int(n.nChildren) <= 48 {
		n48 := newNode48()
		n48.artHeader = n.artHeader
		for i := 0; i < 256; i++ {
			if n.children[i] != nil {
				n48.insertAt(byte(i), n.children[i])
			}
		}
		return n48
	}
	return n
}

func (n *node256) forEach(fn func(byte, node)) {
	for i := 0; i < 256; i++ {
		if n.children[i] != nil {
			fn(byte(i), n.children[i])
		}
	}
}
