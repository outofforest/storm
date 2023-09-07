package blocks_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/blob"
	"github.com/outofforest/storm/blocks/objectlist"
	"github.com/outofforest/storm/blocks/pointer"
	"github.com/outofforest/storm/blocks/singularity"
)

func TestBlockSizes(t *testing.T) {
	assertDiskSize[singularity.Block](t)
	assertDiskSize[pointer.Block](t)
	assertDiskSize[objectlist.Block](t)
	assertDiskSize[blob.Block[[blocks.BlockSize]byte]](t)
}

func assertDiskSize[T blocks.Block](t *testing.T) {
	t.Helper()

	var b T
	bt := reflect.TypeOf(b)
	assert.LessOrEqualf(t, uint64(unsafe.Sizeof(b)), uint64(blocks.BlockSize), "Type: %s/%s", bt.PkgPath(), bt.Name())
}
