package v0

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
)

// TODO (wojciech): Hash data separately

// PointersPerBlock is the number of pointers in each pointer block.
const PointersPerBlock = 3048

// Pointer is a pointer to other block.
type Pointer struct {
	Checksum blocks.Hash
	Address  blocks.BlockAddress
}

// Block is the block forming tree. It contains pointers to other blocks.
type Block struct {
	NUsedPointers        uint64
	Pointers             [PointersPerBlock]Pointer
	PointedBlockVersions [PointersPerBlock]blocks.SchemaVersion
	PointedBlockTypes    [PointersPerBlock]blocks.BlockType
}

// ComputeChecksum computes checksum of the block.
func (b Block) ComputeChecksum() blocks.Hash {
	return blocks.Checksum(photon.NewFromValue(&b).B)
}
