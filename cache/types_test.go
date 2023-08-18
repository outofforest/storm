package cache

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestHeaderSize(t *testing.T) {
	assert.LessOrEqual(t, uint64(unsafe.Sizeof(Header{})), uint64(CacheHeaderSize))
	assert.Greater(t, uint64(unsafe.Sizeof(Header{})), uint64(CacheHeaderSize-alignment))
}

func TestHeaderSizeIsAligned(t *testing.T) {
	assert.EqualValues(t, 0, CacheHeaderSize%alignment)
}
