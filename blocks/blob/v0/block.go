package v0

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
)

// Object represents an object in the blob.
type Object[T comparable] struct {
	ObjectID blocks.ObjectID
	Object   T
}

// Block contains any data.
type Block[T comparable] struct {
	Data T
}

// ComputeChecksum computes checksum of the block.
func (b Block[T]) ComputeChecksum() blocks.Hash {
	return blocks.Checksum(photon.NewFromValue(&b).B)
}
