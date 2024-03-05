package mmap

import (
	"os"
)

func Mmap(fd *os.File, writeable bool, size int64) ([]byte, error) {
	return mmap(fd, writeable, size)
}

// Munmap unmaps a previously mapped slice
func Munmap(b []byte) error {
	return munmap(b)
}

// Madvise uses the madvise