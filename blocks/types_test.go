package blocks_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
	blobV0 "github.com/outofforest/storm/blocks/blob/v0"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
)

func TestBlockSizes(t *testing.T) {
	assertDiskSize[singularityV0.Block](t)
	assertDiskSize[pointerV0.Block](t)
	assertDiskSize[objectlistV0.Block](t)
	assertDiskSize[blobV0.Block[[blocks.BlockSize]byte]](t)
}

func assertDiskSize[T blocks.Block](t *testing.T) {
	var b T
	bt := reflect.TypeOf(b)
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(blocks.BlockSize), "Type: %s/%s", bt.PkgPath(), bt.Name())
}
