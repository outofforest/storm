package blob

import (
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
)

func TestMappingBlobToSlice(t *testing.T) {
	assertT := assert.New(t)

	block := &Block[item]{}
	checksum := blocks.BlockChecksum(block)
	items := photon.NewSliceFromBytes[Object[item]](block.Data[:])
	assertT.Len(items.V, 1365)

	items.V[0].ObjectIDTagReminder = 1
	items.V[0].Object.Field1 = 2
	items.V[0].Object.Field2 = 0x03

	items.V[1].ObjectIDTagReminder = 4
	items.V[1].Object.Field1 = 5
	items.V[1].Object.Field2 = 0x06

	items.V[2].ObjectIDTagReminder = 7
	items.V[2].Object.Field1 = 8
	items.V[2].Object.Field2 = 0x09

	items.V[3].ObjectIDTagReminder = 10
	items.V[3].Object.Field1 = 11
	items.V[3].Object.Field2 = 0x0c

	assertT.Equal([]byte{
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		block.Data[:96])

	assertT.NotEqual(checksum, blocks.BlockChecksum(block))
}

type item struct {
	Field1 uint64
	Field2 byte
}