package blob

import (
	"github.com/outofforest/storm/blocks"
)

// Object represents an object in the blob.
type Object[T comparable] struct {
	ObjectIDTagReminder blocks.ObjectID
	Object              T
}

// Block contains any data.
type Block[T comparable] struct {
	Data [blocks.BlockSize]byte
}
