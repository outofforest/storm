package v0

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
)

// Block is the starting block of the store. Everything starts and ends here.
type Block struct {
	SchemaVersion blocks.SchemaVersion
	Checksum      blocks.Hash
	StormID       uint64
	Revision      uint64
	NBlocks       uint64

	RootData              pointerV0.Pointer
	RootDataBlockType     blocks.BlockType
	RootDataSchemaVersion blocks.SchemaVersion

	RootObjects              pointerV0.Pointer
	RootObjectsBlockType     blocks.BlockType
	RootObjectsSchemaVersion blocks.SchemaVersion

	NextObjectID blocks.ObjectID

	// TODO (wojciech): Replace with correct (de)allocation mechanism
	LastAllocatedBlock blocks.BlockAddress
}

// ComputeChecksum computes checksum of the block.
func (b Block) ComputeChecksum() blocks.Hash {
	b.Checksum = blocks.Hash{}
	return blocks.Checksum(photon.NewFromValue(&b).B)
}
