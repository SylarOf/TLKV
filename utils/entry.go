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

// NewEntry
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// Entry
func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
		return true
	}

	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// WithTTL
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// EncodedSize is the size of the ValueStruct when encoded
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize
func (e *Entry) EstimateSize() int {
	return len(e.Key) + len(e.Value)
}