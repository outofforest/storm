package v0

import (
	"unsafe"

	"github.com/outofforest/storm/blocks"
)

// Object represents an object in the blob.
type Object[T comparable] struct {
	ObjectID blocks.ObjectID
	Object   T
}

// Block contains any data.
type Block[T comparable] struct {
	Data [blocks.BlockSize]byte
}

// ComputeChecksum computes checksum of the block.
func (b Block[T]) ComputeChecksum() blocks.Hash {
	itemSize := int64(unsafe.Sizeof(Object[T]{}))
	return blocks.Checksum(b.Data[:blocks.BlockSize/itemSize*itemSize])
}
