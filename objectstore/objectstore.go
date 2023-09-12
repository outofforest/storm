package objectstore

import (
	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/blob"
	"github.com/outofforest/storm/cache"
)

// TODO (wojciech): Implement deleting keys

// GetObject returns object by its ID.
func GetObject[T comparable](
	c *cache.Cache,
	origin cache.BlockOrigin,
	objectsPerBlock int,
	objectID blocks.ObjectID,
) (T, bool, error) {
	block, tagReminder, exists, err := cache.TraceTagForReading[blob.Block](
		c,
		origin,
		uint64(objectID),
	)
	if !exists || err != nil {
		var t T
		return t, false, err
	}

	objectSlot := findObject[T](block, objectsPerBlock, tagReminder)
	if objectSlot != nil && objectSlot.State == blob.DefinedObjectState {
		return objectSlot.Object, true, nil
	}

	var t T
	return t, false, nil
}

// SetObject sets object by its ID.
func SetObject[T comparable](
	c *cache.Cache,
	origin cache.BlockOrigin,
	parentTrace *cache.Trace,
	objectsPerBlock int,
	objectID blocks.ObjectID,
	object T,
) error {
	block, trace, splitFunc, tagReminder, err := cache.TraceTagForUpdating[blob.Block](
		c,
		origin,
		parentTrace,
		uint64(objectID),
		1,
	)
	if err != nil {
		return err
	}

	objectSlot := findObject[T](block, objectsPerBlock, tagReminder)
	if objectSlot == nil || objectSlot.State != blob.DefinedObjectState {
		objects := photon.SliceFromBytes[blob.Object[T]](block.Data[:], objectsPerBlock)
		if block.NUsedSlots >= uint64(len(objects))*3/4 {
			// TODO (wojciech): Check if split makes sense, if it is the last level, then it doesn't

			var err error
			block, trace, tagReminder, err = splitFunc(func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*blob.Block, uint64, error)) error {
				return splitBlock[T](block, objectsPerBlock, newBlockForTagReminderFunc)
			})
			if err != nil {
				return err
			}

			objectSlot = findObject[T](block, objectsPerBlock, tagReminder)
		}
	}
	if objectSlot == nil {
		return errors.Errorf("cannot find slot for object ID %x", objectID)
	}

	objectSlot.Object = object
	if objectSlot.State != blob.DefinedObjectState {
		block.NUsedSlots++
		objectSlot.ObjectIDTagReminder = tagReminder
		objectSlot.State = blob.DefinedObjectState
	}

	trace.Commit()

	return nil
}

func findObject[T comparable](
	block *blob.Block,
	objectsPerBlock int,
	tagReminder uint64,
) *blob.Object[T] {
	var invalidChunkFound bool
	var invalidChunkIndex uint64

	objects := photon.SliceFromBytes[blob.Object[T]](block.Data[:], objectsPerBlock)
	startIndex := tagReminder % uint64(len(objects))
	// TODO (wojciech): Find better open addressing scheme
	for i, j := startIndex, 0; j < len(objects); i, j = (i+1)%uint64(len(objects)), j+1 {
		switch objects[i].State {
		case blob.DefinedObjectState:
			if objects[i].ObjectIDTagReminder == tagReminder {
				return &objects[i]
			}
		case blob.InvalidObjectState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = i
			}
		case blob.FreeObjectState:
			if invalidChunkFound {
				return &objects[invalidChunkIndex]
			}
			return &objects[i]
		}
	}

	return nil
}

func splitBlock[T comparable](
	block *blob.Block,
	objectsPerBlock int,
	newBlockForTagReminderFunc func(oldTagReminder uint64) (*blob.Block, uint64, error),
) error {
	objects := photon.SliceFromBytes[blob.Object[T]](block.Data[:], objectsPerBlock)
	for i := 0; i < objectsPerBlock; i++ {
		if objects[i].State != blob.DefinedObjectState {
			continue
		}

		newBlock, newTagReminder, err := newBlockForTagReminderFunc(objects[i].ObjectIDTagReminder)
		if err != nil {
			return err
		}

		objectSlot := findObject[T](newBlock, objectsPerBlock, newTagReminder)

		newBlock.NUsedSlots++
		objectSlot.Object = objects[i].Object
		objectSlot.ObjectIDTagReminder = newTagReminder
		objectSlot.State = blob.DefinedObjectState
	}

	return nil
}
