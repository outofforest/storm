package storm

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestBlockSizes(t *testing.T) {
	assertSize[SingularityBlock](t)
	assertSize[PointerBlock](t)
	assertSize[DataBlock](t)
}

func assertSize[T any](t *testing.T) {
	var b T
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(BlockSize), "Type: %T", b)
}
