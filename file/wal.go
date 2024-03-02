package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"SLKV/utils"
	"github.com/pkg/errors"
)

// WalFile _
type WalFile struct {
	lock *sync.RWMutex
	f *MmapFile
}