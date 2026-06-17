package art

import "sort"

// node4 holds up to 4 children. keys and children are kept in sorted order
// so forEach yields children in ascending byte order without extra work.
type node4 struct {
	artHeader
	nChildren uint8
	keys      [4]byte
	children  [4]node
}

func newNode4() *node4 { return &node4{} }

func (n *node4) kind() nodeKind { return kindNode4 }

func (n *node4) childCount() int { return int(n.nChildren) }

func (n *node4) findChild(b byte) *node {
	for i := 0; i < int(n.nChildren); i++ {
		if n.keys[i] == b {
			return &n.children[i]
		}
	}
	return nil
}

func (n *node4) addChild(b byte, child node) node {
	if int(n.nChildren) < 4 {
		// Insert in sorted position.
		pos := sort.Search(int(n.nChildren), func(i int) bool { return n.keys[i] >= b })
		copy(n.keys[pos+1:], n.keys[pos:])
		copy(n.children[pos+1:], n.children[pos:])
		n.keys[pos] = b
		n.children[pos] = child
		n.nChildren++
		return n
	}
	// Grow to node16.
	n16 := newNode16()
	n16.artHeader = n.artHeader
	n16.nChildren = n.nChildren
	copy(n16.keys[:], n.keys[:])
	copy(n16.children[:], n.children[:])
	return n16.addChild(b, child)
}

func (n *node4) removeChild(b byte) node {
	for i := 0; i < int(n.nChildren); i++ {
		if n.keys[i] == b {
			copy(n.keys[i:], n.keys[i+1:])
			copy(n.children[i:], n.children[i+1:])
			n.nChildren--
			n.children[n.nChildren] = nil
			break
		}
	}
	return n
}

func (n *node4) forEach(fn func(byte, node)) {
	for i := 0; i < int(n.nChildren); i++ {
		fn(n.keys[i], n.children[i])
	}
}
