package spacelist

import (
	"github.com/outofforest/storm/blocks"
)

// SplitTrigger value must be configured in a way that at least one space might be inserted after splitting.
const SplitTrigger = SpacesPerBlock * 3 / 4

// SpaceState defines the state of the slot.
type SpaceState byte

// Space states.
const (
	FreeSpaceState SpaceState = iota
	DefinedSpaceState
	InvalidSpaceState
)

// Space contains information about space.
type Space struct {
	SpaceIDTagReminder   uint64
	NextObjectID         blocks.ObjectID
	KeyStorePointer      blocks.Pointer
	ObjectStorePointer   blocks.Pointer
	KeyStoreBlockType    blocks.BlockType
	ObjectStoreBlockType blocks.BlockType
	State                SpaceState
}

// Block contains links to objects.
type Block struct {
	Spaces [SpacesPerBlock]Space

	NUsedSpaces uint16
}
