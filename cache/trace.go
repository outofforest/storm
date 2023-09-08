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
			leafBlock, err := FetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return nil, 0, false, err
			}

			return leafBlock.Block, tagReminder, true, nil
		case blocks.PointerBlockType:
			nextPointerBlock, err := FetchBlock[pointer.Block](c, currentOrigin.Pointer)
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
	pointerTrace := make([]*metadata, 0, 11) // TODO (wojciech): Find the right size
	tagReminder := tag

	for {
		switch *currentOrigin.BlockType {
		case blocks.FreeBlockType:
			leafBlock, err := NewBlock[T](c)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}

			leafBlock.meta.PostCommitFunc = newLeafBlockPostCommitFunc(
				c,
				currentOrigin,
				leafBlock,
			)

			*currentOrigin.Pointer = blocks.Pointer{
				Address:       leafBlock.Address(),
				BirthRevision: leafBlock.BirthRevision(),
			}
			*currentOrigin.BlockType = blocks.LeafBlockType

			return Trace[T]{
				c:             c,
				pointerBlocks: pointerTrace,
				origin:        currentOrigin,
				Block:         leafBlock,
			}, tagReminder, true, nil
		case blocks.LeafBlockType:
			leafBlock, err := FetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}

			leafBlock.meta.PostCommitFunc = newLeafBlockPostCommitFunc(
				c,
				currentOrigin,
				leafBlock,
			)

			tracedBlock := Trace[T]{
				c:             c,
				pointerBlocks: pointerTrace,
				origin:        currentOrigin,
				Block:         leafBlock,
			}

			tracedBlock.splitFunc = func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Trace[T], uint64, error) {
				leafBlock.meta.NReferences++ // to prevent unloading until chunks are copied

				newPointerBlock, err := NewBlock[pointer.Block](c)
				if err != nil {
					return Trace[T]{}, 0, err
				}

				*currentOrigin.Pointer = blocks.Pointer{
					Address:       newPointerBlock.Address(),
					BirthRevision: newPointerBlock.BirthRevision(),
				}
				*currentOrigin.BlockType = blocks.PointerBlockType

				newPointerBlock.meta.PostCommitFunc = c.newPointerBlockPostCommitFunc(
					currentOrigin,
					newPointerBlock,
				)

				for _, meta := range pointerTrace {
					meta.NReferences -= leafBlock.meta.NCommits
				}

				newPointerBlock.meta.NReferences = 1
				pointerTrace = append(pointerTrace, newPointerBlock.meta)

				returnedBlock, err := NewBlock[T](c)
				if err != nil {
					return Trace[T]{}, 0, err
				}

				returnedPointerIndex := tagReminder % pointer.PointersPerBlock
				newPointerBlock.Block.Pointers[returnedPointerIndex] = blocks.Pointer{
					Address:       returnedBlock.Address(),
					BirthRevision: returnedBlock.BirthRevision(),
				}
				newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType

				origin := BlockOrigin{
					parentBlockMeta: newPointerBlock.meta,
					Pointer:         &newPointerBlock.Block.Pointers[returnedPointerIndex],
					BlockType:       &newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex],
				}

				returnedBlock.meta.PostCommitFunc = newLeafBlockPostCommitFunc(
					c,
					origin,
					returnedBlock,
				)

				returnedBlock.meta.NReferences++ // to prevent unloading when new blocks are created as a result of split

				newBlockForTagReminderFunc := func(oldTagReminder uint64) (*T, uint64, error) {
					var newBlock Block[T]
					pointerIndex := oldTagReminder % pointer.PointersPerBlock
					if newPointerBlock.Block.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
						var err error
						newBlock, err = NewBlock[T](c)
						if err != nil {
							return nil, 0, err
						}

						newPointerBlock.Block.Pointers[pointerIndex] = blocks.Pointer{
							Address:       newBlock.Address(),
							BirthRevision: newBlock.BirthRevision(),
						}
						newPointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
					} else {
						var err error
						newBlock, err = FetchBlock[T](c, &newPointerBlock.Block.Pointers[pointerIndex])
						if err != nil {
							return nil, 0, err
						}
					}

					newBlock.meta.PostCommitFunc = newLeafBlockPostCommitFunc(
						c,
						BlockOrigin{
							parentBlockMeta: newPointerBlock.meta,
							Pointer:         &newPointerBlock.Block.Pointers[pointerIndex],
							BlockType:       &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
						},
						newBlock,
					)

					// All the pointers in the trace mut be incremented because this is new block which didn't pass
					// the standard trace flow.
					for _, meta := range pointerTrace {
						meta.NReferences++
					}
					c.dirtyBlock(newBlock.meta, 1)

					return newBlock.Block, oldTagReminder / pointer.PointersPerBlock, nil
				}
				if err := splitFunc(newBlockForTagReminderFunc); err != nil {
					return Trace[T]{}, 0, err
				}

				c.invalidateBlock(leafBlock.meta)

				returnedBlock.meta.NReferences--

				return Trace[T]{
					c:             c,
					pointerBlocks: pointerTrace,
					origin:        origin,
					Block:         returnedBlock,
				}, tagReminder / pointer.PointersPerBlock, nil
			}

			return tracedBlock, tagReminder, true, nil
		case blocks.PointerBlockType:
			pointerBlock, err := FetchBlock[pointer.Block](c, currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
			}
			pointerBlock.meta.NReferences++
			pointerBlock.meta.PostCommitFunc = c.newPointerBlockPostCommitFunc(
				currentOrigin,
				pointerBlock,
			)

			pointerTrace = append(pointerTrace, pointerBlock.meta)

			pointerIndex := tagReminder % pointer.PointersPerBlock
			tagReminder /= pointer.PointersPerBlock

			currentOrigin.parentBlockMeta = pointerBlock.meta
			currentOrigin.Pointer = &pointerBlock.Block.Pointers[pointerIndex]
			currentOrigin.BlockType = &pointerBlock.Block.PointedBlockTypes[pointerIndex]
		}
	}
}

func (c *Cache) newPointerBlockPostCommitFunc(
	origin BlockOrigin,
	pointerBlock Block[pointer.Block],
) func() error {
	return func() error {
		*origin.Pointer = blocks.Pointer{
			Checksum:      blocks.BlockChecksum(pointerBlock.Block),
			Address:       pointerBlock.Address(),
			BirthRevision: pointerBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.PointerBlockType

		if origin.parentBlockMeta != nil {
			origin.parentBlockMeta.NReferences -= pointerBlock.meta.NCommits
			c.dirtyBlock(origin.parentBlockMeta, pointerBlock.meta.NCommits)
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
		*origin.Pointer = blocks.Pointer{
			Checksum:      blocks.BlockChecksum(leafBlock.Block),
			Address:       leafBlock.Address(),
			BirthRevision: leafBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.LeafBlockType

		if origin.parentBlockMeta != nil {
			origin.parentBlockMeta.NReferences -= leafBlock.meta.NCommits
			c.dirtyBlock(origin.parentBlockMeta, leafBlock.meta.NCommits)
		}

		return nil
	}
}

// Trace stores the trace of incremented pointer blocks leading to the final leaf node
type Trace[T blocks.Block] struct {
	Block Block[T]

	c             *Cache
	pointerBlocks []*metadata
	origin        BlockOrigin
	splitFunc     func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Trace[T], uint64, error)
}

// Split replaces a leaf block with a new pointer block and triggers redistribution of existing items between new set of leaf blocks.
func (t Trace[T]) Split(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (Trace[T], uint64, error) {
	return t.splitFunc(splitFunc)
}

// Commit marks block as dirty, meaning that changes will be saved to the device later.
func (t Trace[T]) Commit() {
	t.c.dirtyBlock(t.Block.meta, 1)
}

// Release is used to decrement references to pointer blocks if leaf block has not been modified.
func (t Trace[T]) Release() {
	for _, pointerBlock := range t.pointerBlocks {
		pointerBlock.NReferences--
	}
}
