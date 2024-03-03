package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"unsafe"

	"TLKV/file"
	"TLKV/utils"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}
type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}
type block struct {
	offset            int //starting address of the offset for the current block
	checksum          []byte
	entriesIndexStart int
	chklen            int
	data              []byte
	basekey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // overlap with base key
	diff    uint16 // Length of the diff
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decodes decodes the header
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// check if need allocate a new block

}

func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

// Empty returns whether it's empty
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (tb *tableBuilder) finish() []byte {

}

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	// + 1, last entry offset
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // sum64 in checksum proto
		4) // checksum length

	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry */) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize
	
	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end) + uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: Estimate the size of the SST file after organizing the builder's writes to disk.
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // indicates that current block has been serialized to memory
	return
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// need to reallocate
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // to do here can use memory allocator to improve performance
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

// calculate Checksum
func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}
