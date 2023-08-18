package types

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestBlockSizes(t *testing.T) {
	assertDiskSize[SingularityBlock](t)
	assertDiskSize[PointerBlock](t)
	assertDiskSize[DataBlock](t)
}

func assertDiskSize[T comparable](t *testing.T) {
	var b T
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(BlockSize), "Type: %T", b)
}
