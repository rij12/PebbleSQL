package blinktree

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

const maxKeys = 4 // For simplicity, a small branching factor

type Node struct {
	isLeaf       bool
	keys         []int
	values       [][]byte // Only for leaf nodes
	children     []*Node  // Only for internal nodes
	parent       *Node    // For backtracking
	rightSibling *Node
	mu           sync.RWMutex
}

type BlinkTree struct {
	root *Node
	mu   sync.RWMutex
}

func NewBlinkTree() *BlinkTree {
	return &BlinkTree{
		root: &Node{isLeaf: true},
	}
}

func (tree *BlinkTree) Search(key int) ([]byte, bool) {
	n := tree.root
	for {
		n.mu.RLock()
		if n.isLeaf {
			// Only leaf nodes contain key and values
			// Once we've hit a leaf nodes
			// We search the keys and return the value if found
			break
		}

		i := sort.Search(len(n.keys), func(i int) bool {
			return key < n.keys[i]
		})

		if i == len(n.children) {
			n = n.rightSibling
		} else {
			n = n.children[i]
		}
		n.mu.RUnlock()
	}
	n.mu.RUnlock()

	for i, k := range n.keys {
		if k == key {
			return n.values[i], true
		}
	}
	return nil, false
}

func (tree *BlinkTree) Insert(key int, value []byte) {
	tree.insert(tree.root, key, value)
}

func (tree *BlinkTree) insert(n *Node, key int, value []byte) {
	if n.isLeaf {
		n.mu.Lock() // <-- NODE-LEVEL LOCK
		idx := sort.SearchInts(n.keys, key)
		n.keys = append(n.keys[:idx], append([]int{key}, n.keys[idx:]...)...)
		n.values = append(n.values[:idx], append([][]byte{value}, n.values[idx:]...)...)

		if len(n.keys) > maxKeys {
			tree.split(n)
		}
		n.mu.Unlock()
		return
	}

	i := sort.Search(len(n.keys), func(i int) bool {
		return key < n.keys[i]
	})

	n.mu.RLock()
	child := n.children[i]
	n.mu.RUnlock()
	tree.insert(child, key, value)
}

func (tree *BlinkTree) split(n *Node) {

	// TODO - Need to give this more thought
	if !n.mu.TryLock() {
		log.Fatal("BlinkTree split called on node that is not locked")
	}

	mid := len(n.keys) / 2
	right := &Node{
		isLeaf:       n.isLeaf,
		keys:         append([]int(nil), n.keys[mid:]...),
		rightSibling: n.rightSibling,
		parent:       n.parent,
	}
	right.mu.Lock()
	defer right.mu.Unlock()

	if n.isLeaf {
		right.values = append([][]byte(nil), n.values[mid:]...)
		n.values = n.values[:mid]
	} else {
		right.children = append([]*Node(nil), n.children[mid+1:]...)
		for _, child := range right.children {
			child.parent = right
		}
		n.children = n.children[:mid+1]
	}

	splitKey := right.keys[0]
	n.keys = n.keys[:mid]
	n.rightSibling = right

	if tree.root == n {
		tree.root = &Node{
			keys:     []int{splitKey},
			children: []*Node{n, right},
		}
		n.parent = tree.root
		right.parent = tree.root
	} else {
		parent := n.parent
		parent.mu.Lock()
		insertPos := sort.SearchInts(parent.keys, splitKey)
		parent.keys = append(parent.keys[:insertPos], append([]int{splitKey}, parent.keys[insertPos:]...)...)
		parent.children = append(parent.children[:insertPos+1], append([]*Node{right}, parent.children[insertPos+1:]...)...)
		parent.mu.Unlock()

		if len(parent.keys) > maxKeys {
			tree.split(parent)
		}
	}
}

func (tree *BlinkTree) Delete(key int) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.delete(tree.root, key)
}

func (tree *BlinkTree) delete(n *Node, key int) {
	if !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool {
			return key < n.keys[i]
		})
		n.mu.RLock()
		child := n.children[i]
		n.mu.RUnlock()
		tree.delete(child, key)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	idx := sort.SearchInts(n.keys, key)
	if idx < len(n.keys) && n.keys[idx] == key {
		n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
		n.values = append(n.values[:idx], n.values[idx+1:]...)
	}
}

func (tree *BlinkTree) Print() {
	var levels [][]*Node
	q := []*Node{tree.root}

	for len(q) > 0 {
		var next []*Node
		levels = append(levels, q)
		for _, n := range q {
			if !n.isLeaf {
				next = append(next, n.children...)
			}
		}
		q = next
	}

	for i, level := range levels {
		fmt.Printf("Level %d:\n", i)
		for _, n := range level {
			fmt.Printf("%v ", n.keys)
		}
		fmt.Println()
	}
}
