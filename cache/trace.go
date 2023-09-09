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
			leafBlock, _, err := fetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return nil, 0, false, err
			}

			return leafBlock, tagReminder, true, nil
		case blocks.PointerBlockType:
			nextPointerBlock, _, err := fetchBlock[pointer.Block](c, currentOrigin.Pointer)
			if err != nil {
				return nil, 0, false, err
			}

			pointerIndex := tagReminder % pointer.PointersPerBlock
			tagReminder /= pointer.PointersPerBlock

			currentOrigin.Pointer = &nextPointerBlock.Pointers[pointerIndex]
			currentOrigin.BlockType = &nextPointerBlock.PointedBlockTypes[pointerIndex]
		}
	}
}

// SplitFunc is a function used to split block.
type SplitFunc[T blocks.Block] func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (*T, *Trace, uint64, error)

// TraceTagForUpdating starts from origin and follows pointer blocks until the final leaf block is found.
// This function, when tracing, collects all the information required to store updated version of the leaf block.
func TraceTagForUpdating[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	parentTrace *Trace,
	tag uint64,
	nReferences uint64,
) (*T, *Trace, SplitFunc[T], uint64, error) {
	currentOrigin := origin
	var parentBlockMeta *blockMetadata
	if parentTrace != nil {
		parentBlockMeta = parentTrace.meta
	}
	pointerTrace := make([]*blockMetadata, 0, 11) // TODO (wojciech): Find the right size
	tagReminder := tag

	for {
		switch *currentOrigin.BlockType {
		case blocks.FreeBlockType:
			leafBlock, leafMeta, err := newBlock[T](c)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			leafMeta.PostCommitFunc = newLeafBlockPostCommitFunc(
				c,
				currentOrigin,
				parentBlockMeta,
				leafBlock,
				leafMeta,
			)

			leafMeta.NReferences = nReferences

			*currentOrigin.Pointer = blocks.Pointer{
				Address:       leafMeta.Address,
				BirthRevision: leafMeta.BirthRevision,
			}
			*currentOrigin.BlockType = blocks.LeafBlockType

			return leafBlock, &Trace{
				c:             c,
				meta:          leafMeta,
				pointerBlocks: pointerTrace,
				parentTrace:   parentTrace,
			}, nil, tagReminder, nil
		case blocks.LeafBlockType:
			leafBlock, leafMeta, err := fetchBlock[T](c, currentOrigin.Pointer)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			leafMeta.PostCommitFunc = newLeafBlockPostCommitFunc(
				c,
				currentOrigin,
				parentBlockMeta,
				leafBlock,
				leafMeta,
			)

			leafMeta.NReferences += nReferences

			tracedBlock := &Trace{
				c:             c,
				meta:          leafMeta,
				pointerBlocks: pointerTrace,
				parentTrace:   parentTrace,
			}

			splitFunc := func(splitFunc func(newBlockForTagReminderFunc func(oldTagReminder uint64) (*T, uint64, error)) error) (*T, *Trace, uint64, error) {
				newPointerBlock, newPointerMeta, err := newBlock[pointer.Block](c)
				if err != nil {
					return nil, nil, 0, err
				}

				*currentOrigin.Pointer = blocks.Pointer{
					Address:       newPointerMeta.Address,
					BirthRevision: newPointerMeta.BirthRevision,
				}
				*currentOrigin.BlockType = blocks.PointerBlockType

				newPointerMeta.PostCommitFunc = c.newPointerBlockPostCommitFunc(
					currentOrigin,
					parentBlockMeta,
					newPointerBlock,
					newPointerMeta,
				)

				for _, meta := range pointerTrace {
					meta.NReferences -= leafMeta.NCommits
				}

				newPointerMeta.NReferences = nReferences
				pointerTrace = append(pointerTrace, newPointerMeta)

				returnedBlock, returnedMeta, err := newBlock[T](c)
				if err != nil {
					return nil, nil, 0, err
				}

				returnedPointerIndex := tagReminder % pointer.PointersPerBlock
				newPointerBlock.Pointers[returnedPointerIndex] = blocks.Pointer{
					Address:       returnedMeta.Address,
					BirthRevision: returnedMeta.BirthRevision,
				}
				newPointerBlock.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType

				origin := BlockOrigin{
					Pointer:   &newPointerBlock.Pointers[returnedPointerIndex],
					BlockType: &newPointerBlock.PointedBlockTypes[returnedPointerIndex],
				}

				returnedMeta.PostCommitFunc = newLeafBlockPostCommitFunc(
					c,
					origin,
					newPointerMeta,
					returnedBlock,
					returnedMeta,
				)

				returnedMeta.NReferences = nReferences

				newBlockForTagReminderFunc := func(oldTagReminder uint64) (*T, uint64, error) {
					var block *T
					var blockMeta *blockMetadata
					pointerIndex := oldTagReminder % pointer.PointersPerBlock
					if newPointerBlock.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
						var err error
						block, blockMeta, err = newBlock[T](c)
						if err != nil {
							return nil, 0, err
						}

						newPointerBlock.Pointers[pointerIndex] = blocks.Pointer{
							Address:       blockMeta.Address,
							BirthRevision: blockMeta.BirthRevision,
						}
						newPointerBlock.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
					} else {
						var err error
						block, blockMeta, err = fetchBlock[T](c, &newPointerBlock.Pointers[pointerIndex])
						if err != nil {
							return nil, 0, err
						}
					}

					blockMeta.PostCommitFunc = newLeafBlockPostCommitFunc(
						c,
						BlockOrigin{
							Pointer:   &newPointerBlock.Pointers[pointerIndex],
							BlockType: &newPointerBlock.PointedBlockTypes[pointerIndex],
						},
						newPointerMeta,
						block,
						blockMeta,
					)

					if blockMeta.NCommits == 0 {
						// All the pointers in the trace mut be incremented because this is new block which didn't pass
						// the standard trace flow.
						for _, meta := range pointerTrace {
							meta.NReferences++
						}
						c.dirtyBlock(blockMeta, 1)
					}

					return block, oldTagReminder / pointer.PointersPerBlock, nil
				}
				if err := splitFunc(newBlockForTagReminderFunc); err != nil {
					return nil, nil, 0, err
				}

				c.invalidateBlock(leafMeta)

				return returnedBlock, &Trace{
					c:             c,
					meta:          returnedMeta,
					pointerBlocks: pointerTrace,
					parentTrace:   parentTrace,
				}, tagReminder / pointer.PointersPerBlock, nil
			}

			return leafBlock, tracedBlock, splitFunc, tagReminder, nil
		case blocks.PointerBlockType:
			pointerBlock, pointerBlockMeta, err := fetchBlock[pointer.Block](c, currentOrigin.Pointer)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			pointerBlockMeta.NReferences += nReferences
			pointerBlockMeta.PostCommitFunc = c.newPointerBlockPostCommitFunc(
				currentOrigin,
				parentBlockMeta,
				pointerBlock,
				pointerBlockMeta,
			)

			pointerTrace = append(pointerTrace, pointerBlockMeta)

			pointerIndex := tagReminder % pointer.PointersPerBlock
			tagReminder /= pointer.PointersPerBlock

			parentBlockMeta = pointerBlockMeta
			currentOrigin.Pointer = &pointerBlock.Pointers[pointerIndex]
			currentOrigin.BlockType = &pointerBlock.PointedBlockTypes[pointerIndex]
		}
	}
}

func (c *Cache) newPointerBlockPostCommitFunc(
	origin BlockOrigin,
	parentBlockMeta *blockMetadata,
	pointerBlock *pointer.Block,
	pointerBlockMeta *blockMetadata,
) func() error {
	return func() error {
		*origin.Pointer = blocks.Pointer{
			Checksum:      blocks.BlockChecksum(pointerBlock),
			Address:       pointerBlockMeta.Address,
			BirthRevision: pointerBlockMeta.BirthRevision,
		}
		*origin.BlockType = blocks.PointerBlockType

		if parentBlockMeta != nil {
			parentBlockMeta.NReferences -= pointerBlockMeta.NCommits
			c.dirtyBlock(parentBlockMeta, pointerBlockMeta.NCommits)
		}

		return nil
	}
}

// TODO (wojciech): This function must receive a function computing the merkle proof hash of particular block type.
func newLeafBlockPostCommitFunc[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	parentBlockMeta *blockMetadata,
	leafBlock *T,
	leafBlockMeta *blockMetadata,
) func() error {
	return func() error {
		*origin.Pointer = blocks.Pointer{
			Checksum:      blocks.BlockChecksum(leafBlock),
			Address:       leafBlockMeta.Address,
			BirthRevision: leafBlockMeta.BirthRevision,
		}
		*origin.BlockType = blocks.LeafBlockType

		if parentBlockMeta != nil {
			parentBlockMeta.NReferences -= leafBlockMeta.NCommits
			c.dirtyBlock(parentBlockMeta, leafBlockMeta.NCommits)
		}

		return nil
	}
}

// Trace stores the trace of incremented pointer blocks leading to the final leaf node
type Trace struct {
	c             *Cache
	meta          *blockMetadata
	pointerBlocks []*blockMetadata
	parentTrace   *Trace
}

// Commit marks block as dirty, meaning that changes will be saved to the device later.
func (t Trace) Commit() {
	t.meta.NReferences--
	t.c.dirtyBlock(t.meta, 1)
}

// Release is used to decrement references to pointer blocks if leaf block has not been modified.
func (t Trace) Release() {
	t.meta.NReferences--
	for _, pointerBlock := range t.pointerBlocks {
		pointerBlock.NReferences--
	}
	if t.parentTrace != nil {
		t.parentTrace.Release()
	}
}
