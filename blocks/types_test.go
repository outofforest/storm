package blocks_test

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
)

func TestBlockSizes(t *testing.T) {
	assertDiskSize[singularityV0.Block](t)
	assertDiskSize[pointerV0.Block](t)
	assertDiskSize[objectlistV0.Block](t)
}

func assertDiskSize[T blocks.Block](t *testing.T) {
	var b T
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(blocks.BlockSize), "Type: %T", b)
}
