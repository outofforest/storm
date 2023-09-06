package singularity

import (
	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/pointer"
)

// Block is the starting block of the store. Everything starts and ends here.
type Block struct {
	Checksum blocks.Hash
	StormID  uint64
	Revision uint64
	NBlocks  uint64

	RootData          pointer.Pointer
	RootDataBlockType blocks.BlockType

	RootObjects          pointer.Pointer
	RootObjectsBlockType blocks.BlockType

	NextObjectID blocks.ObjectID

	// TODO (wojciech): Replace with correct (de)allocation mechanism
	LastAllocatedBlock blocks.BlockAddress
}
