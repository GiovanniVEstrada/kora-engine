package art

import "sort"

// node16 holds 5–16 children. Like node4, keys and children are kept sorted.
type node16 struct {
	artHeader
	nChildren uint8
	keys      [16]byte
	children  [16]node
}

func newNode16() *node16 { return &node16{} }

func (n *node16) kind() nodeKind { return kindNode16 }

func (n *node16) childCount() int { return int(n.nChildren) }

func (n *node16) findChild(b byte) *node {
	// Binary search over the sorted key array.
	cnt := int(n.nChildren)
	i := sort.Search(cnt, func(i int) bool { return n.keys[i] >= b })
	if i < cnt && n.keys[i] == b {
		return &n.children[i]
	}
	return nil
}

func (n *node16) addChild(b byte, child node) node {
	if int(n.nChildren) < 16 {
		pos := sort.Search(int(n.nChildren), func(i int) bool { return n.keys[i] >= b })
		copy(n.keys[pos+1:], n.keys[pos:])
		copy(n.children[pos+1:], n.children[pos:])
		n.keys[pos] = b
		n.children[pos] = child
		n.nChildren++
		return n
	}
	// Grow to node48.
	n48 := newNode48()
	n48.artHeader = n.artHeader
	for i := 0; i < int(n.nChildren); i++ {
		n48.insertAt(n.keys[i], n.children[i])
	}
	return n48.addChild(b, child)
}

func (n *node16) removeChild(b byte) node {
	cnt := int(n.nChildren)
	i := sort.Search(cnt, func(i int) bool { return n.keys[i] >= b })
	if i < cnt && n.keys[i] == b {
		copy(n.keys[i:], n.keys[i+1:])
		copy(n.children[i:], n.children[i+1:])
		n.nChildren--
		n.children[n.nChildren] = nil
	}
	// Shrink to node4 when only 4 children remain.
	if int(n.nChildren) <= 4 {
		n4 := newNode4()
		n4.artHeader = n.artHeader
		n4.nChildren = n.nChildren
		copy(n4.keys[:], n.keys[:n.nChildren])
		copy(n4.children[:], n.children[:n.nChildren])
		return n4
	}
	return n
}

func (n *node16) forEach(fn func(byte, node)) {
	for i := 0; i < int(n.nChildren); i++ {
		fn(n.keys[i], n.children[i])
	}
}
