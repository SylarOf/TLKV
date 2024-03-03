package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
)

// FID according to file name to get fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}

	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// CreateSyncedFile creates a new file (using O_EXCL), errors if it already existed
func CreateSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0600)
}

// FileNameSSTable sst file name
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// openDir opens a directory for syncing
func openDir(path string) (*os.File, error) { return os.Open(path) }

// SyncDir when you create or delete a file, you have to ensure the directory
// entry for the file is synced in order to guarantee the file is visible (if the system crashes)
func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory : %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := os.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// CompareKeys checks the key without timestamp and checks the time stamp
// if keyNots is same
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp
// 8 byte for TTL
func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}
	return nil
}

// CalculateChecksum
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}

// RemoveDir
func RemoveDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		Panic(err)
	}
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// BytesToU64
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	// 获取切片的第一个元素的地址，并将其转换为 *byte 类型的指针
	ptr := unsafe.Pointer(&u32s[0])
	// 计算字节切片的长度
	length := len(u32s) * 4
	// 使用 unsafe 包的 Slice 函数将指针转换为字节切片
	return *(*[]byte)(unsafe.Pointer(&struct {
		data uintptr
		len  int
		cap  int
	}{uintptr(ptr), length, length}))
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	// 计算字节切片的长度
	length := len(b) / 4
	// 使用 unsafe 包的 Slice 函数将字节切片转换为 uint32 切片
	return *(*[]uint32)(unsafe.Pointer(&struct {
		data uintptr
		len  int
		cap  int
	}{uintptr(unsafe.Pointer(&b[0])), length, length}))
}
