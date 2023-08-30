package v0

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/storm/blocks"
)

func TestChecksum(t *testing.T) {
	assertT := assert.New(t)

	pBlock := Block{}

	pBlock2 := pBlock
	pBlock2.Pointers[0].Checksum = 2
	assertT.NotEqual(blocks.BlockChecksum(&pBlock), blocks.BlockChecksum(&pBlock2))

	pBlock3 := pBlock2
	pBlock3.PointedBlockTypes[0] = blocks.LeafBlockType
	assertT.NotEqual(blocks.BlockChecksum(&pBlock2), blocks.BlockChecksum(&pBlock3))

	pBlock4 := pBlock3
	pBlock4.PointedBlockTypes[1] = blocks.LeafBlockType
	assertT.NotEqual(blocks.BlockChecksum(&pBlock3), blocks.BlockChecksum(&pBlock4))

	pBlock5 := pBlock4
	pBlock5.NUsedPointers = 2
	assertT.NotEqual(blocks.BlockChecksum(&pBlock4), blocks.BlockChecksum(&pBlock5))

	pBlock6 := pBlock5
	pBlock6.Pointers[1].Address = 2
	assertT.NotEqual(blocks.BlockChecksum(&pBlock5), blocks.BlockChecksum(&pBlock6))

	pBlock7 := pBlock6
	pBlock7.Pointers[2].Checksum = 4
	assertT.NotEqual(blocks.BlockChecksum(&pBlock6), blocks.BlockChecksum(&pBlock7))
}
