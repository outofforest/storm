package memdev

import (
	"io"

	"github.com/pkg/errors"
)

var (
	_ io.Seeker = &MemDev{}
	_ io.Reader = &MemDev{}
	_ io.Writer = &MemDev{}
)

// MemDev simulates device io operations in memory.
type MemDev struct {
	size   int64
	offset int64
	data   []byte
}

// New returns new memdev.
func New(size int64) *MemDev {
	return &MemDev{
		size: size,
		data: make([]byte, size),
	}
}

// Seek seeks the position.
func (md *MemDev) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset = md.offset + offset
	case io.SeekEnd:
		offset = md.size + offset
	}

	if offset < 0 || offset > md.size {
		return 0, errors.Errorf("invalid offset: %d", offset)
	}

	md.offset = offset
	return offset, nil
}

// Read reads data from the memdev.
func (md *MemDev) Read(p []byte) (int, error) {
	if p == nil {
		return 0, nil
	}
	n := copy(p, md.data[md.offset:])
	md.offset += int64(n)
	return n, nil
}

// Write writes data to the memdev.
func (md *MemDev) Write(p []byte) (int, error) {
	if p == nil {
		return 0, nil
	}
	n := copy(md.data[md.offset:], p)
	md.offset += int64(n)
	return n, nil
}
