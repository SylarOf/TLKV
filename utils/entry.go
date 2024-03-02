package utils

import (
	"encoding/binary"
	"time"
)

type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value)
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	sz := binary.PutUvarint(b[0:], vs.ExpiresAt)
	n := copy(b[sz:], vs.Value)
	return uint32(sz + n)
}

func (vs *ValueStruct) DecodeValue(b []byte) {
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(b[0:])
	vs.Value = b[sz:]
}
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// Entry _ the outermost written struct

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
}
