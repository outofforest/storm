package v0

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
)

func TestChecksum(t *testing.T) {
	assertT := assert.New(t)

	pBlock := Block{}

	pBlock2 := pBlock
	pBlock2.Pointers[0].Checksum = blocks.Hash(bytes.Repeat([]byte{0x01}, blocks.HashSize))
	assertT.NotEqual(pBlock.ComputeChecksum(), pBlock2.ComputeChecksum())

	pBlock3 := pBlock2
	pBlock3.PointedBlockTypes[0] = blocks.LeafBlockType
	assertT.NotEqual(pBlock2.ComputeChecksum(), pBlock3.ComputeChecksum())

	pBlock4 := pBlock3
	pBlock4.PointedBlockTypes[1] = blocks.LeafBlockType
	assertT.NotEqual(pBlock3.ComputeChecksum(), pBlock4.ComputeChecksum())

	pBlock5 := pBlock4
	pBlock5.NUsedPointers = 2
	assertT.NotEqual(pBlock4.ComputeChecksum(), pBlock5.ComputeChecksum())

	pBlock6 := pBlock5
	pBlock6.Pointers[1].Address = 2
	assertT.NotEqual(pBlock5.ComputeChecksum(), pBlock6.ComputeChecksum())

	pBlock7 := pBlock6
	pBlock7.Pointers[2].Checksum = blocks.Hash(bytes.Repeat([]byte{0x04}, blocks.HashSize))
	assertT.NotEqual(pBlock6.ComputeChecksum(), pBlock7.ComputeChecksum())
}
