package cache

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

const alignment = 8

func TestHeaderSize(t *testing.T) {
	assert.LessOrEqual(t, uint64(unsafe.Sizeof(Header{})), uint64(CacheHeaderSize))
}

func TestHeaderSizeIsAligned(t *testing.T) {
	assert.Equal(t, 0, CacheHeaderSize%alignment)
}
