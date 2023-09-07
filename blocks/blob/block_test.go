package blob

import (
	"testing"
	"unsafe"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
)

func TestMappingBlobToSlice(t *testing.T) {
	assertT := assert.New(t)

	block := &Block{}
	checksum := blocks.BlockChecksum(block)
	items := photon.SliceFromBytes[Object[item]](block.Data[:], 4)

	items[0].ObjectIDTagReminder = 1
	items[0].Object.Field1 = 2
	items[0].Object.Field2 = 0x03
	items[0].State = DefinedObjectState

	items[1].ObjectIDTagReminder = 4
	items[1].Object.Field1 = 5
	items[1].Object.Field2 = 0x06
	items[1].State = InvalidObjectState

	items[2].ObjectIDTagReminder = 7
	items[2].Object.Field1 = 8
	items[2].Object.Field2 = 0x09
	items[2].State = FreeObjectState

	items[3].ObjectIDTagReminder = 10
	items[3].Object.Field1 = 11
	items[3].Object.Field2 = 0x0c
	items[3].State = DefinedObjectState

	assertT.Equal([]byte{
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	},
		block.Data[:len(items)*int(unsafe.Sizeof(Object[item]{}))])

	assertT.NotEqual(checksum, blocks.BlockChecksum(block))
}

type item struct {
	Field1 uint64
	Field2 byte
}
