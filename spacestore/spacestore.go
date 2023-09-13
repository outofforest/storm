package spacestore

import (
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/spacelist"
	"github.com/outofforest/storm/cache"
)

// TODO (wojciech): Implement deleting keys

// GetSpace returns space.
func GetSpace(
	c *cache.Cache,
	origin cache.BlockOrigin,
	spaceID blocks.SpaceID,
) (cache.BlockOrigin, cache.BlockOrigin, bool, error) {
	block, tagReminder, exists, err := cache.TraceTagForReading[spacelist.Block](
		c,
		origin,
		uint64(spaceID),
	)
	if !exists || err != nil {
		return cache.BlockOrigin{}, cache.BlockOrigin{}, false, err
	}

	space := findSpace(block, tagReminder)
	if space == nil || space.State != spacelist.DefinedSpaceState {
		return cache.BlockOrigin{}, cache.BlockOrigin{}, false, nil
	}

	// TODO (wojciech): Set space block as a parent for keystore and objectstore

	return cache.BlockOrigin{
			Pointer:   &space.KeyStorePointer,
			BlockType: &space.KeyStoreBlockType,
		}, cache.BlockOrigin{
			Pointer:   &space.ObjectStorePointer,
			BlockType: &space.ObjectStoreBlockType,
		}, true, nil
}

// EnsureSpace returns space. If it does not exist it is created.
func EnsureSpace(
	c *cache.Cache,
	origin cache.BlockOrigin,
	spaceID blocks.SpaceID,
) (cache.BlockOrigin, cache.BlockOrigin, *cache.Trace, error) {
	block, trace, splitFunc, tagReminder, err := cache.TraceTagForUpdating[spacelist.Block](
		c,
		origin,
		nil,
		uint64(spaceID),
	)
	if err != nil {
		return cache.BlockOrigin{}, cache.BlockOrigin{}, nil, err
	}

	space := findSpace(block, tagReminder)
	if space == nil || (space.State != spacelist.DefinedSpaceState && block.NUsedSpaces >= spacelist.SplitTrigger) {
		// TODO (wojciech): Check if split makes sense, if it is the last level, then it doesn't

		var err error
		block, trace, tagReminder, err = splitFunc(func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*spacelist.Block, uint64, error)) error {
			return splitBlock(block, newBlockForTagReminderFunc)
		})
		if err != nil {
			return cache.BlockOrigin{}, cache.BlockOrigin{}, nil, err
		}
		space = findSpace(block, tagReminder)
	}
	if space == nil {
		return cache.BlockOrigin{}, cache.BlockOrigin{}, nil, errors.Errorf("cannot find slot for space ID %x", spaceID)
	}

	if space.State != spacelist.DefinedSpaceState {
		block.NUsedSpaces++
		space.State = spacelist.DefinedSpaceState
		space.SpaceIDTagReminder = tagReminder
	}

	return cache.BlockOrigin{
			Pointer:   &space.KeyStorePointer,
			BlockType: &space.KeyStoreBlockType,
		}, cache.BlockOrigin{
			Pointer:   &space.ObjectStorePointer,
			BlockType: &space.ObjectStoreBlockType,
		}, trace, nil
}

func findSpace(
	block *spacelist.Block,
	tagReminder uint64,
) *spacelist.Space {
	var invalidChunkFound bool
	var invalidChunkIndex uint16

	offsetSeed := uint16(tagReminder % spacelist.SpacesPerBlock)
	for i := 0; i < spacelist.SpacesPerBlock; i++ {
		index := (offsetSeed + spacelist.AddressingOffsets[i]) % spacelist.SpacesPerBlock
		switch block.Spaces[index].State {
		case spacelist.DefinedSpaceState:
			if block.Spaces[index].SpaceIDTagReminder == tagReminder {
				return &block.Spaces[index]
			}
		case spacelist.InvalidSpaceState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
		case spacelist.FreeSpaceState:
			if invalidChunkFound {
				return &block.Spaces[invalidChunkIndex]
			}
			return &block.Spaces[index]
		}
	}

	return nil
}

func splitBlock(
	block *spacelist.Block,
	newBlockForTagReminderFunc func(oldTagReminder uint64) (*spacelist.Block, uint64, error),
) error {
	for i := uint16(0); i < spacelist.SpacesPerBlock; i++ {
		if block.Spaces[i].State != spacelist.DefinedSpaceState {
			continue
		}

		newBlock, newTagReminder, err := newBlockForTagReminderFunc(block.Spaces[i].SpaceIDTagReminder)
		if err != nil {
			return err
		}

		space := findSpace(newBlock, newTagReminder)

		newBlock.NUsedSpaces++
		*space = block.Spaces[i]
		space.SpaceIDTagReminder = newTagReminder
	}

	return nil
}
