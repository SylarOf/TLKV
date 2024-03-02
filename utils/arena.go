package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// Arena should be lock-free.
type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset = 0 as
	// a kind of nil pointer.
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		// Check if the current buffer has enough space.
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	if int(offset) > len(s.buf) - MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf) + int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
		// fmt.Print(len(s.buf), " ")
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize

	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// put value and length of value
func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

// put key
func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset + keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

// getKey returns byte slice at offset.
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	
	// get node offset in arena
	// unsafe.Pointer as void*, uintptr change void* to interger var
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}