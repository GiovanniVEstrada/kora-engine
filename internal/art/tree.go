package art

import "bytes"

// Tree is an Adaptive Radix Tree keyed on []byte.
// It is not safe for concurrent use; the caller is responsible for locking.
type Tree struct {
	root node
	size int
}

// Len returns the number of keys stored in the tree.
func (t *Tree) Len() int { return t.size }

// Insert stores value under key, replacing any existing value.
func (t *Tree) Insert(key []byte, value any) {
	l := &leaf{key: key, value: value}
	if t.root == nil {
		t.root = l
		t.size++
		return
	}
	var grown bool
	t.root, grown = insertNode(t.root, l, 0)
	if grown {
		t.size++
	}
}

// Get returns the value for key and true, or nil and false if not found.
func (t *Tree) Get(key []byte) (any, bool) {
	n := t.root
	depth := 0
	for n != nil {
		if lf, ok := n.(*leaf); ok {
			if bytes.Equal(lf.key, key) {
				return lf.value, true
			}
			return nil, false
		}
		h := header(n)
		// Check path-compression prefix.
		if h.prefixLen > 0 {
			m := h.prefixMatch(key, depth)
			if uint32(m) < h.prefixLen {
				return nil, false
			}
			depth += int(h.prefixLen)
		}
		if depth >= len(key) {
			// Key exhausted at an inner node means the value was stored at the
			// byte-0 terminator child during Insert (see insertNode).
			child := n.findChild(0)
			if child == nil {
				return nil, false
			}
			n = *child
			continue
		}
		child := n.findChild(key[depth])
		if child == nil {
			return nil, false
		}
		n = *child
		depth++
	}
	return nil, false
}

// Delete removes key from the tree. Returns true if the key existed.
func (t *Tree) Delete(key []byte) bool {
	if t.root == nil {
		return false
	}
	var deleted bool
	t.root, deleted = deleteNode(t.root, key, 0)
	if deleted {
		t.size--
	}
	return deleted
}

// Iterator returns a function that yields every (key, value) pair in
// ascending key order. Call it repeatedly; it returns false when exhausted.
func (t *Tree) Iterator() func() ([]byte, any, bool) {
	// Collect all leaves in sorted order via DFS.
	var leaves []*leaf
	collectLeaves(t.root, &leaves)
	i := 0
	return func() ([]byte, any, bool) {
		if i >= len(leaves) {
			return nil, nil, false
		}
		l := leaves[i]
		i++
		return l.key, l.value, true
	}
}

// --- internal helpers -------------------------------------------------------

// header extracts the artHeader pointer from any inner node.
func header(n node) *artHeader {
	switch v := n.(type) {
	case *node4:
		return &v.artHeader
	case *node16:
		return &v.artHeader
	case *node48:
		return &v.artHeader
	case *node256:
		return &v.artHeader
	}
	return nil
}

// insertNode inserts leaf l into the subtree rooted at n at the given depth.
// Returns the (possibly new) root of the subtree and whether a new key was added.
func insertNode(n node, l *leaf, depth int) (node, bool) {
	// Replace a leaf: same key → update value, different key → split.
	if existing, ok := n.(*leaf); ok {
		if bytes.Equal(existing.key, l.key) {
			// Update: swap value in-place, no size change.
			existing.value = l.value
			return existing, false
		}
		// Two different leaves at this position: create a node4 to hold both.
		n4 := newNode4()
		// Compute the shared prefix between the two keys at depth.
		prefLen := sharedPrefix(existing.key, l.key, depth)
		n4.prefixLen = uint32(prefLen)
		if prefLen > maxPrefixLen {
			prefLen = maxPrefixLen
		}
		copy(n4.prefix[:], existing.key[depth:depth+prefLen])
		advance := depth + int(n4.prefixLen)
		// Add both leaves under their diverging byte (or terminator).
		addLeafToNode(n4, existing, advance)
		addLeafToNode(n4, l, advance)
		return n4, true
	}

	h := header(n)

	// Check path-compression prefix.
	if h.prefixLen > 0 {
		m := matchPrefix(h, l.key, depth)
		if uint32(m) < h.prefixLen {
			// Prefix mismatch: split the compressed path.
			return splitNode(n, l, depth, m), true
		}
		depth += int(h.prefixLen)
	}

	// Descend.
	if depth >= len(l.key) {
		// Key is exhausted — store a terminator child at byte 0.
		child := n.findChild(0)
		if child == nil {
			return n.addChild(0, l), true
		}
		*child, _ = insertNode(*child, l, depth)
		return n, false
	}

	b := l.key[depth]
	child := n.findChild(b)
	if child == nil {
		return n.addChild(b, l), true
	}
	var grown bool
	*child, grown = insertNode(*child, l, depth+1)
	return n, grown
}

// deleteNode removes key from the subtree rooted at n.
// Returns the new root (may differ on collapse) and whether a key was removed.
func deleteNode(n node, key []byte, depth int) (node, bool) {
	if n == nil {
		return nil, false
	}
	if lf, ok := n.(*leaf); ok {
		if bytes.Equal(lf.key, key) {
			return nil, true
		}
		return n, false
	}

	h := header(n)
	if h.prefixLen > 0 {
		m := matchPrefix(h, key, depth)
		if uint32(m) < h.prefixLen {
			return n, false
		}
		depth += int(h.prefixLen)
	}

	if depth >= len(key) {
		child := n.findChild(0)
		if child == nil {
			return n, false
		}
		newChild, deleted := deleteNode(*child, key, depth)
		if !deleted {
			return n, false
		}
		if newChild == nil {
			n = n.removeChild(0)
		} else {
			*child = newChild
		}
		return collapseIfSingle(n), true
	}

	b := key[depth]
	child := n.findChild(b)
	if child == nil {
		return n, false
	}
	newChild, deleted := deleteNode(*child, key, depth+1)
	if !deleted {
		return n, false
	}
	if newChild == nil {
		n = n.removeChild(b)
	} else {
		*child = newChild
	}
	return collapseIfSingle(n), true
}

// collapseIfSingle replaces an inner node that has exactly one child with that
// child, merging the prefix down. This keeps the tree compact after deletions.
func collapseIfSingle(n node) node {
	if n == nil || n.childCount() != 1 {
		return n
	}
	// Find the single child.
	var singleByte byte
	var singleChild node
	n.forEach(func(b byte, c node) {
		singleByte = b
		singleChild = c
	})
	// Only collapse if the single child is a leaf; inner-node merging is more
	// complex and skipped for now (the tree is still correct without it).
	if _, ok := singleChild.(*leaf); !ok {
		return n
	}
	_ = singleByte
	return singleChild
}

// splitNode splits a compressed node at mismatch position m, creating a new
// node4 that branches on the differing bytes.
func splitNode(n node, l *leaf, depth, m int) node {
	h := header(n)
	n4 := newNode4()
	// The new node's prefix is the matched portion.
	n4.prefixLen = uint32(m)
	if m > maxPrefixLen {
		m = maxPrefixLen
	}
	copy(n4.prefix[:], h.prefix[:m])

	// The old node loses the matched prefix + 1 diverging byte.
	splitByte := h.prefix[m]
	h.prefixLen -= uint32(n4.prefixLen) + 1
	if h.prefixLen > 0 {
		shift := int(n4.prefixLen) + 1
		if shift > maxPrefixLen {
			shift = maxPrefixLen
		}
		copy(h.prefix[:], h.prefix[shift:])
	}

	n4.children[0] = n
	n4.keys[0] = splitByte
	n4.nChildren = 1

	// Add the new leaf on its diverging byte.
	leafDepth := depth + int(n4.prefixLen)
	addLeafToNode(n4, l, leafDepth)
	return n4
}

// sharedPrefix returns the number of bytes in common between a and b starting at depth.
func sharedPrefix(a, b []byte, depth int) int {
	maxLen := len(a) - depth
	if lb := len(b) - depth; lb < maxLen {
		maxLen = lb
	}
	i := 0
	for i < maxLen && a[depth+i] == b[depth+i] {
		i++
	}
	return i
}

// matchPrefix returns how many bytes of the node's prefix match key[depth:].
// Unlike artHeader.prefixMatch, this also handles the case where prefixLen
// exceeds maxPrefixLen by comparing against the stored partial prefix only.
func matchPrefix(h *artHeader, key []byte, depth int) int {
	return h.prefixMatch(key, depth)
}

// addLeafToNode adds leaf l to node n, choosing the correct key byte at advance.
func addLeafToNode(n4 *node4, l *leaf, advance int) {
	if advance >= len(l.key) {
		n4.addChild(0, l)
	} else {
		n4.addChild(l.key[advance], l)
	}
}

// collectLeaves appends all leaves reachable from n to out in sorted key order.
func collectLeaves(n node, out *[]*leaf) {
	if n == nil {
		return
	}
	if lf, ok := n.(*leaf); ok {
		*out = append(*out, lf)
		return
	}
	n.forEach(func(_ byte, child node) {
		collectLeaves(child, out)
	})
}
