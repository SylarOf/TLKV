package utils

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"

	"github.com/pkg/errors"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	value uint64

	keyOffset uint32
	keySize   uint16

	height uint16

	tower [maxHeight]uint32
}

// helper function
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, vs uint64) {
	atomic.StoreUint64(&n.value, vs)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

// getVs return ValueStruct stored in node
func (n *node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

type Skiplist struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func()
}

// Increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// Decrements the refcount, deallocating the SkipList when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}
	//
	s.arena = nil
}

// put node, key, value
func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}

// NewSkipList makes a new empty skiplist, with a given arena size
func NewSkipList(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	head_offset := arena.getNodeOffset(head)
	return &Skiplist{
		height:     1,
		headOffset: head_offset,
		arena:      arena,
		ref:        1,
	}
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key(if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.

func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			if !less {
				return nil, false
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			x = next
			continue
		}
		if cmp == 0 {
			if allowEqual{
				return next, true
			}
			if !less {
				return s.getNext(next, 0), false
			}
			if level >0 {
				level--
				continue
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		if level > 0 {
			level --
			continue
		}
		if !less {
			return next, false
		}
		if x == s.getHead() {
			return nil, false
		}
		return x, false

	}
}

func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		beforeNode := s.arena.getNode(before)
		next := beforeNode.getNextOffset(level)
		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			return before, next
		}
		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			return next, next
		}
		if cmp < 0 {
			return before, next
		}
		before = next
	}
}

// Put insesrts the key-value pair
func (s *Skiplist) Add(e *Entry) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increases the height. Let's defer these actions
	key, v := e.Key, ValueStruct {
		Value: e.Value,
		ExpiresAt: e.ExpiresAt,
	}

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = s.headOffset


}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will never return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

// Empty returns if the SkipList is empty
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *Skiplist) Search(key []byte) ValueStruct {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual
	if n ==nil {
		return ValueStruct{}
	}
	
	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}
	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	return vs
}



func SameKey(src []byte, dst []byte) bool
func CompareKeys(x []byte, y []byte) int
// go:linkname FastRand runtime.fastrand
func FastRand() uint32

// AssertTruef is AssertTrue with extra info
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
