package cache

import (
	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/pointer"
)

// TraceTagForReading starts from origin and follows pointer blocks until the final leaf block is found.
// This function returns only the final leaf block, so it can't be used to update its content.
func TraceTagForReading[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	tag uint64,
) (*T, uint64, bool, error) {
	currentOrigin := origin
	tagReminder := tag

	for {
		switch *currentOrigin.BlockType {
		case blocks.FreeBlockType:
			return nil, 0, false, nil
		case blocks.LeafBlockType:
			leafBlock, _, err := FetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return nil, 0, false, err
			}

			return leafBlock.Block, tagReminder, true, nil
		case blocks.PointerBlockType:
			nextPointerBlock, _, err := FetchBlock[pointer.Block](c, currentOrigin.Pointer)
			if err != nil {
				return nil, 0, false, err
			}

			pointerIndex := tagReminder % pointer.PointersPerBlock
			tagReminder /= pointer.PointersPerBlock

			currentOrigin.Pointer = &nextPointerBlock.Block.Pointers[pointerIndex]
			currentOrigin.BlockType = &nextPointerBlock.Block.PointedBlockTypes[pointerIndex]
		}
	}
}

// TraceTagForUpdating starts from origin and follows pointer blocks until the final leaf block is found.
// This function, when tracing, collects all the information required to store updated version of the leaf block.
func TraceTagForUpdating[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	tag uint64,
) (Trace[T], uint64, bool, error) {
	currentOrigin := origin
	pointerTrace := make([]Block[pointer.Block], 0, 11) // TODO (wojciech): Find the right size
	tagReminder := tag

	for {
		switch *currentOrigin.BlockType {
		case blocks.FreeBlockType:
			if currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
				pointerTrace = append(pointerTrace, currentOrigin.PointerBlock)
			}

			leafBlock, err := NewBlock[T](c)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}
			leafBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
				c,
				currentOrigin,
				leafBlock,
			))

			*currentOrigin.Pointer = pointer.Pointer{
				Address:       leafBlock.Address(),
				BirthRevision: c.SingularityBlock().Revision + 1,
			}
			*currentOrigin.BlockType = blocks.LeafBlockType

			return Trace[T]{
				c:             c,
				pointerBlocks: pointerTrace,
				origin:        currentOrigin,
				Block:         leafBlock,
			}, tagReminder, true, nil
		case blocks.LeafBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			leafBlock, addedToCache, err := FetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}

			tracedBlock := Trace[T]{
				c:      c,
				origin: currentOrigin,
				Block:  leafBlock,
			}

			if addedToCache {
				if currentOrigin.PointerBlock.IsValid() {
					pointerTrace = append(pointerTrace, currentOrigin.PointerBlock)
				}
				leafBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
					c,
					currentOrigin,
					leafBlock,
				))
			} else if currentOrigin.PointerBlock.IsValid() {
				// If leaf block has been already present in the cache, it means that pointer block reference was incorrectly incremented,
				// and it must be fixed now.
				currentOrigin.PointerBlock.DecrementReferences()
			}

			tracedBlock.pointerBlocks = pointerTrace
			tracedBlock.splitFunc = func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Block[T], uint64, error) {
				leafBlock.IncrementReferences() // to prevent unloading until chunks are copied

				newPointerBlock, err := NewBlock[pointer.Block](c)
				if err != nil {
					return Block[T]{}, 0, err
				}

				newPointerBlock.WithPostCommitFunc(c.newPointerBlockPostCommitFunc(
					currentOrigin,
					newPointerBlock,
				))

				newPointerBlock.IncrementReferences()

				*currentOrigin.Pointer = pointer.Pointer{
					Address:       newPointerBlock.Address(),
					BirthRevision: newPointerBlock.BirthRevision(),
				}
				*currentOrigin.BlockType = blocks.PointerBlockType

				returnedBlock, err := NewBlock[T](c)
				if err != nil {
					return Block[T]{}, 0, err
				}

				returnedPointerIndex := tagReminder % pointer.PointersPerBlock
				newPointerBlock.Block.Pointers[returnedPointerIndex] = pointer.Pointer{
					Address:       returnedBlock.Address(),
					BirthRevision: c.singularityBlock.V.Revision + 1,
				}
				newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType

				returnedBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
					c,
					BlockOrigin{
						PointerBlock: newPointerBlock,
						Pointer:      &newPointerBlock.Block.Pointers[returnedPointerIndex],
						BlockType:    &newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex],
					},
					returnedBlock,
				))

				returnedBlock.IncrementReferences() // to prevent unloading when new blocks are created as a result of split

				newBlockForTagReminderFunc := func(oldTagReminder uint64) (*T, uint64, error) {
					var newBlock Block[T]
					pointerIndex := oldTagReminder % pointer.PointersPerBlock
					if newPointerBlock.Block.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
						newPointerBlock.IncrementReferences()
						var err error
						newBlock, err = NewBlock[T](c)
						if err != nil {
							return nil, 0, err
						}

						newBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
							c,
							BlockOrigin{
								PointerBlock: newPointerBlock,
								Pointer:      &newPointerBlock.Block.Pointers[pointerIndex],
								BlockType:    &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
							},
							newBlock,
						))

						newPointerBlock.Block.Pointers[pointerIndex] = pointer.Pointer{
							Address:       newBlock.Address(),
							BirthRevision: newBlock.BirthRevision(),
						}
						newPointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
					} else {
						newPointerBlock.IncrementReferences()

						var addedToCache bool
						var err error
						newBlock, addedToCache, err = FetchBlock[T](c, &newPointerBlock.Block.Pointers[pointerIndex])
						if err != nil {
							return nil, 0, err
						}

						if addedToCache {
							newBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
								c,
								BlockOrigin{
									PointerBlock: newPointerBlock,
									Pointer:      &newPointerBlock.Block.Pointers[pointerIndex],
									BlockType:    &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
								},
								newBlock,
							))
						} else {
							newPointerBlock.DecrementReferences()
						}
					}

					c.dirtyBlock(newBlock.meta)

					return newBlock.Block, oldTagReminder / pointer.PointersPerBlock, nil
				}
				if err := splitFunc(newBlockForTagReminderFunc); err != nil {
					return Block[T]{}, 0, err
				}

				leafBlock.DecrementReferences()
				c.invalidateBlock(leafBlock.meta)

				returnedBlock.DecrementReferences()

				return returnedBlock, tagReminder / pointer.PointersPerBlock, nil
			}

			return tracedBlock, tagReminder, true, nil
		case blocks.PointerBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			nextPointerBlock, addedToCache, err := FetchBlock[pointer.Block](c, currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}

			if addedToCache {
				if currentOrigin.PointerBlock.IsValid() {
					pointerTrace = append(pointerTrace, currentOrigin.PointerBlock)
				}
				nextPointerBlock.WithPostCommitFunc(c.newPointerBlockPostCommitFunc(
					currentOrigin,
					nextPointerBlock,
				))
			} else if currentOrigin.PointerBlock.IsValid() {
				// If next pointer block has been already present in the cache, it means that previous pointer block reference was incorrectly incremented,
				// and it must e fixed now.
				currentOrigin.PointerBlock.DecrementReferences()
			}

			pointerIndex := tagReminder % pointer.PointersPerBlock
			tagReminder /= pointer.PointersPerBlock

			currentOrigin.PointerBlock = nextPointerBlock
			currentOrigin.Pointer = &nextPointerBlock.Block.Pointers[pointerIndex]
			currentOrigin.BlockType = &nextPointerBlock.Block.PointedBlockTypes[pointerIndex]
		}
	}
}

func (c *Cache) newPointerBlockPostCommitFunc(
	origin BlockOrigin,
	pointerBlock Block[pointer.Block],
) func() error {
	return func() error {
		*origin.Pointer = pointer.Pointer{
			Checksum:      blocks.BlockChecksum(pointerBlock.Block),
			Address:       pointerBlock.Address(),
			BirthRevision: pointerBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.PointerBlockType

		if origin.PointerBlock.IsValid() {
			origin.PointerBlock.DecrementReferences()
			c.dirtyBlock(origin.PointerBlock.meta)
		}

		return nil
	}
}

// TODO (wojciech): This function must receive a function computing the merkle proof hash of particular block type.
func newLeafBlockPostCommitFunc[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	leafBlock Block[T],
) func() error {
	return func() error {
		*origin.Pointer = pointer.Pointer{
			Checksum:      blocks.BlockChecksum(leafBlock.Block),
			Address:       leafBlock.Address(),
			BirthRevision: leafBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.LeafBlockType

		if origin.PointerBlock.IsValid() {
			origin.PointerBlock.DecrementReferences()
			c.dirtyBlock(origin.PointerBlock.meta)
		}

		return nil
	}
}

// Trace stores the trace of incremented pointer blocks leading to the final leaf node
type Trace[T blocks.Block] struct {
	Block Block[T]

	c             *Cache
	pointerBlocks []Block[pointer.Block]
	origin        BlockOrigin
	splitFunc     func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Block[T], uint64, error)
}

// Split replaces a leaf block with a new pointer block and triggers redistribution of existing items between new set of leaf blocks.
func (t Trace[T]) Split(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Block[T], uint64, error) {
	return t.splitFunc(splitFunc)
}

// Commit marks block as dirty, meaning that changes will be saved to the device later.
func (t Trace[T]) Commit() {
	t.c.dirtyBlock(t.Block.meta)
}

// Release is used to decrement references to pointer blocks if leaf block has not been modified.
func (t Trace[T]) Release() {
	for _, pointerBlock := range t.pointerBlocks {
		pointerBlock.DecrementReferences()
	}
}
