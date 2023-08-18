package storm

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestBlockSizes(t *testing.T) {
	assertDiskSize[SingularityBlock](t)
	assertDiskSize[PointerBlock](t)
	assertDiskSize[DataBlock](t)

	assertCachedSize[PointerBlock](t)
	assertCachedSize[DataBlock](t)
}

func assertDiskSize[T comparable](t *testing.T) {
	var b T
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(BlockSize), "Type: %T", b)
}

func assertCachedSize[T comparable](t *testing.T) {
	var b CachedBlock[T]
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(CachedBlockSize), "Type: %T", b)
}
