package v0

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
)

// ItemsPerBlock is the number of object references stored in each block.
const ItemsPerBlock = 16

// ItemState defines the state of the item.
type ItemState byte

// Item states.
const (
	FreeItemState ItemState = iota
	DefinedItemState
)

// Link links key to the object ID.
type Link struct {
	Key      [32]byte
	ObjectID blocks.ObjectID
}

// Block contains links to objects.
type Block struct {
	NUsedItems uint64
	Links      [ItemsPerBlock]Link
	Hashes     [ItemsPerBlock]uint64
	States     [ItemsPerBlock]ItemState
}

// ComputeChecksum computes checksum of the block.
func (b Block) ComputeChecksum() blocks.Hash {
	return blocks.Checksum(photon.NewFromValue(&b).B)
}
