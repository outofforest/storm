package v0

import (
	"crypto/sha256"

	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
)

// Block is the starting block of the store. Everything starts and ends here.
type Block struct {
	SchemaVersion  blocks.SchemaVersion
	StructChecksum blocks.Hash
	StormID        uint64
	Revision       uint64
	NBlocks        uint64

	RootData              pointerV0.Pointer
	RootDataBlockType     blocks.BlockType
	RootDataSchemaVersion blocks.SchemaVersion

	// TODO (wojciech): Replace with correct (de)allocation mechanism
	LastAllocatedBlock blocks.BlockAddress
}

// ComputeChecksums computes struct checksum of the block.
func (b Block) ComputeChecksums() (blocks.Hash, blocks.Hash, error) {
	b.StructChecksum = blocks.Hash{}
	return sha256.Sum256(photon.NewFromValue(&b).B), blocks.Hash{}, nil
}
