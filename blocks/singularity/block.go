package singularity

import (
	"github.com/outofforest/storm/blocks"
)

// Block is the starting block of the store. Everything starts and ends here.
type Block struct {
	Checksum     blocks.Hash
	StormID      uint64
	Revision     uint64
	NBlocks      uint64
	NextObjectID blocks.ObjectID

	RootData    blocks.Pointer
	RootObjects blocks.Pointer

	RootDataBlockType    blocks.BlockType
	RootObjectsBlockType blocks.BlockType

	// TODO (wojciech): Replace with correct (de)allocation mechanism
	LastAllocatedBlock blocks.BlockAddress
}
