package addresslist

import (
	"github.com/outofforest/storm/blocks"
)

// SplitTrigger value must be configured in a way that at least one key might be inserted after splitting.
const SplitTrigger = SlotsPerBlock * 3 / 4

// SlotState defines the state of the slot.
type SlotState byte

// Slot states.
const (
	FreeSlotState SlotState = iota
	DefinedSlotState
	InvalidSlotState
)

// Slot contains mapping between ObjectIDTagReminder and address where data for that object exist.
type Slot struct {
	ObjectIDTagReminder uint64
	Pointer             blocks.Pointer
}

// Block contains links to objects.
type Block struct {
	Slots      [SlotsPerBlock]Slot
	SlotStates [SlotsPerBlock]SlotState
	BlockTypes [SlotsPerBlock]blocks.BlockType

	NUsedSlots uint16
}
