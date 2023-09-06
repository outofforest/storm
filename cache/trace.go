package cache

import (
	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
)

// TraceTag starts from origin and follows pointer blocks until the final leaf block is found.
func TraceTag[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	forUpdate bool,
	tag uint64,
) (Trace[T], uint64, bool, func() (Block[pointerV0.Block], error), error) {
	currentOrigin := origin
	pointerTrace := make([]Block[pointerV0.Block], 0, 11) // TODO (wojciech): Find the right size
	tagReminder := tag

	var pointerIndex uint64
	for {
		switch *currentOrigin.BlockType {
		case blocks.FreeBlockType:
			if forUpdate {
				if currentOrigin.PointerBlock.IsValid() {
					currentOrigin.PointerBlock.IncrementReferences()
					pointerTrace = append(pointerTrace, currentOrigin.PointerBlock)
				}

				leafBlock, err := NewBlock[T](c)
				if err != nil {
					return Trace[T]{}, 0, false, nil, err
				}
				leafBlock.WithPostCommitFunc(NewLeafBlockPostCommitFunc(
					c,
					currentOrigin,
					leafBlock,
				))

				*currentOrigin.Pointer = pointerV0.Pointer{
					Address:       leafBlock.Address(),
					BirthRevision: c.SingularityBlock().Revision + 1,
				}
				*currentOrigin.BlockType = blocks.LeafBlockType
				*currentOrigin.BlockSchemaVersion = blocks.ObjectListV0

				return Trace[T]{
					PointerBlocks: pointerTrace,
					Origin:        currentOrigin,
					Block:         leafBlock,
				}, tagReminder, true, nil, nil
			}
			return Trace[T]{}, 0, false, nil, nil
		case blocks.LeafBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if forUpdate && currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			leafBlock, addedToCache, err := FetchBlock[T](c, *currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, nil, err
			}

			tracedBlock := Trace[T]{
				Origin: currentOrigin,
				Block:  leafBlock,
			}

			if forUpdate {
				if addedToCache {
					if currentOrigin.PointerBlock.IsValid() {
						pointerTrace = append(pointerTrace, currentOrigin.PointerBlock)
					}
					leafBlock.WithPostCommitFunc(NewLeafBlockPostCommitFunc(
						c,
						currentOrigin,
						leafBlock,
					))
				} else if currentOrigin.PointerBlock.IsValid() {
					// If leaf block has been already present in the cache, it means that pointer block reference was incorrectly incremented,
					// and it must e fixed now.
					currentOrigin.PointerBlock.DecrementReferences()
				}

				tracedBlock.PointerBlocks = pointerTrace
			}

			splitFunc := func() (Block[pointerV0.Block], error) {
				newPointerBlock, err := NewBlock[pointerV0.Block](c)
				if err != nil {
					return Block[pointerV0.Block]{}, err
				}

				newPointerBlock.WithPostCommitFunc(c.newPointerBlockPostCommitFunc(
					currentOrigin,
					newPointerBlock,
				))

				*currentOrigin.Pointer = pointerV0.Pointer{
					Address:       newPointerBlock.Address(),
					BirthRevision: newPointerBlock.BirthRevision(),
				}
				*currentOrigin.BlockType = blocks.PointerBlockType
				*currentOrigin.BlockSchemaVersion = blocks.PointerV0

				return newPointerBlock, nil
			}

			return tracedBlock, tagReminder, true, splitFunc, nil
		case blocks.PointerBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if forUpdate && currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			nextPointerBlock, addedToCache, err := FetchBlock[pointerV0.Block](c, *currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, nil, err
			}

			if forUpdate {
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
			}

			pointerIndex = tagReminder % pointerV0.PointersPerBlock
			tagReminder /= pointerV0.PointersPerBlock

			currentOrigin.PointerBlock = nextPointerBlock
			currentOrigin.Pointer = &nextPointerBlock.Block.Pointers[pointerIndex]
			currentOrigin.BlockType = &nextPointerBlock.Block.PointedBlockTypes[pointerIndex]
			currentOrigin.BlockSchemaVersion = &nextPointerBlock.Block.PointedBlockVersions[pointerIndex]
		}
	}
}

func (c *Cache) newPointerBlockPostCommitFunc(
	origin BlockOrigin,
	pointerBlock Block[pointerV0.Block],
) func() error {
	return func() error {
		*origin.Pointer = pointerV0.Pointer{
			Checksum:      blocks.BlockChecksum(pointerBlock.Block),
			Address:       pointerBlock.Address(),
			BirthRevision: pointerBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.PointerBlockType
		*origin.BlockSchemaVersion = blocks.PointerV0

		if origin.PointerBlock.IsValid() {
			origin.PointerBlock.DecrementReferences()
			if err := DirtyBlock(c, origin.PointerBlock); err != nil {
				return err
			}
		}

		return nil
	}
}

// NewLeafBlockPostCommitFunc returns new function to be used as post commit function for leaf nodes.
// TODO (wojciech): This function must receive a function computing the merkle proof hash of particular block type.
func NewLeafBlockPostCommitFunc[T blocks.Block](
	c *Cache,
	origin BlockOrigin,
	leafBlock Block[T],
) func() error {
	return func() error {
		*origin.Pointer = pointerV0.Pointer{
			Checksum:      blocks.BlockChecksum(leafBlock.Block),
			Address:       leafBlock.Address(),
			BirthRevision: leafBlock.BirthRevision(),
		}
		*origin.BlockType = blocks.LeafBlockType
		*origin.BlockSchemaVersion = blocks.ObjectListV0

		if origin.PointerBlock.IsValid() {
			origin.PointerBlock.DecrementReferences()
			if err := DirtyBlock(c, origin.PointerBlock); err != nil {
				return err
			}
		}

		return nil
	}
}
