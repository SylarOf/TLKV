package utils

import (
	"hash/crc32"
	"math"
	"os"
)

const (
	// MaxLevelNum
	MaxLevelNum = 7
	// DefaultValueThreshold
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename = "MANIFEST"
	ManifestRewriteFilename = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio = 10
	DefaultFileFlag = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode = 10 << 20
	// This is O_DSYNC (datasync) on platforms that support it -- see file unix.go
	datasyncFileFlag = 0x0
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
