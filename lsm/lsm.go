package lsm

// Options _
type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes
	BlockSize int
	// BloomFalsePositive is the false positive probability of bloom filter
	BloomFalsePositive float64

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // Determing the expected size ratio between levels
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
}
