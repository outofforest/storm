package objectstore

import (
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/addresslist"
	"github.com/outofforest/storm/blocks/objectlist"
	"github.com/outofforest/storm/cache"
)

// TODO (wojciech): Implement deleting keys

// GetAddress returns address stored under the object ID.
func GetAddress(
	c *cache.Cache,
	origin cache.BlockOrigin,
	objectID blocks.ObjectID,
) (cache.BlockOrigin, bool, error) {
	block, tagReminder, exists, err := cache.TraceTagForReading[addresslist.Block](
		c,
		origin,
		uint64(objectID),
	)
	if !exists || err != nil {
		return cache.BlockOrigin{}, false, err
	}

	index, slotFound := findSlotForObjectID(block, tagReminder)
	if slotFound && block.SlotStates[index] == addresslist.DefinedSlotState {
		// TODO (wojciech): PointerBlock in the origin must be set, so it is incremented to not be unloaded from cache,
		// so it is required to create an abstraction for this.
		return cache.BlockOrigin{
			Pointer:   &block.Slots[index].Pointer,
			BlockType: &block.BlockTypes[index],
		}, true, nil
	}

	return cache.BlockOrigin{}, false, nil
}

// EnsureObjectID returns object ID for key. If the object ID does not exist it is created.
func EnsureObjectID(
	c *cache.Cache,
	origin cache.BlockOrigin,
	objectID blocks.ObjectID,
) (cache.BlockOrigin, error) {
	block, tagReminder, _, err := cache.TraceTagForUpdating[addresslist.Block](
		c,
		origin,
		uint64(objectID),
	)
	if err != nil {
		return cache.BlockOrigin{}, err
	}

	index, slotFound := findSlotForObjectID(block.Block.Block, tagReminder)
	if slotFound && block.Block.Block.SlotStates[index] == addresslist.DefinedSlotState {
		block.Release()
		// TODO (wojciech): PointerBlock in the origin must be set, so it is incremented to not be unloaded from cache,
		// so it is required to create an abstraction for this.
		return cache.BlockOrigin{
			Pointer:   &block.Block.Block.Slots[index].Pointer,
			BlockType: &block.Block.Block.BlockTypes[index],
		}, nil
	}

	if block.Block.Block.NUsedSlots >= addresslist.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.

		var err error
		block.Block, tagReminder, err = block.Split(func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*addresslist.Block, uint64, error)) error {
			return splitBlock(block.Block.Block, newBlockForTagReminderFunc)
		})
		if err != nil {
			return cache.BlockOrigin{}, err
		}

		index, slotFound = findSlotForObjectID(block.Block.Block, tagReminder)
	}

	if !slotFound {
		return cache.BlockOrigin{}, errors.Errorf("cannot find slot for object ID %x", objectID)
	}

	// TODO (wojciech): Set PostCommitFunc
	newBlock, err := cache.NewBlock[addresslist.Block](c)
	if err != nil {
		return cache.BlockOrigin{}, err
	}

	block.Block.Block.NUsedSlots++
	block.Block.Block.Slots[index] = addresslist.Slot{
		ObjectIDTagReminder: tagReminder,
		Pointer: blocks.Pointer{
			Address:       newBlock.Address(),
			BirthRevision: newBlock.BirthRevision(),
		},
	}
	block.Block.Block.BlockTypes[index] = blocks.LeafBlockType
	block.Block.Block.SlotStates[index] = addresslist.DefinedSlotState

	// TODO (wojciech): PointerBlock in the origin must be set, so it is incremented to not be unloaded from cache,
	// so it is required to create an abstraction for this.
	return cache.BlockOrigin{
		Pointer:   &block.Block.Block.Slots[index].Pointer,
		BlockType: &block.Block.Block.BlockTypes[index],
	}, nil
}

func findSlotForObjectID(
	block *addresslist.Block,
	tagReminder uint64,
) (uint16, bool) {
	var invalidChunkFound bool
	var invalidChunkIndex uint16

	offsetSeed := uint16(tagReminder % addresslist.SlotsPerBlock)
	for i := 0; i < addresslist.SlotsPerBlock; i++ {
		index := (offsetSeed + objectlist.AddressingOffsets[i]) % addresslist.SlotsPerBlock
		switch block.SlotStates[index] {
		case addresslist.DefinedSlotState:
			if block.Slots[index].ObjectIDTagReminder != tagReminder {
				continue
			}
		case addresslist.InvalidSlotState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
			continue
		case addresslist.FreeSlotState:
			if invalidChunkFound {
				return invalidChunkIndex, true
			}
		}
		return index, true
	}

	if invalidChunkFound {
		return invalidChunkIndex, true
	}

	return 0, false
}

func splitBlock(
	block *addresslist.Block,
	newBlockForTagReminderFunc func(oldTagReminder uint64) (*addresslist.Block, uint64, error),
) error {
	for i := uint16(0); i < addresslist.SlotsPerBlock; i++ {
		if block.SlotStates[i] != addresslist.DefinedSlotState {
			continue
		}

		newBlock, newTagReminder, err := newBlockForTagReminderFunc(block.Slots[i].ObjectIDTagReminder)
		if err != nil {
			return err
		}

		index, _ := findSlotForObjectID(newBlock, newTagReminder)

		newBlock.NUsedSlots++
		newBlock.Slots[index] = addresslist.Slot{
			ObjectIDTagReminder: newTagReminder,
			Pointer: blocks.Pointer{
				Address:       block.Slots[i].Pointer.Address,
				BirthRevision: block.Slots[i].Pointer.BirthRevision,
			},
		}
		newBlock.BlockTypes[index] = blocks.LeafBlockType
		newBlock.SlotStates[index] = addresslist.DefinedSlotState
	}

	return nil
}
