package blob

import (
	"github.com/outofforest/storm/blocks"
)

// ObjectState defines the state of the object.
type ObjectState byte

// Object states.
const (
	FreeObjectState ObjectState = iota
	DefinedObjectState
	InvalidObjectState
)

// Object represents an object in the blob.
type Object[T comparable] struct {
	ObjectIDTagReminder uint64
	Object              T
	State               ObjectState
}

// Block contains any data.
type Block struct {
	Data [blocks.BlockSize - 8]byte

	NUsedSlots uint64
}
