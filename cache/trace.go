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
) (Trace[T], uint64, bool, error) {
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
					return Trace[T]{}, 0, false, err
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
				}, tagReminder, true, nil
			}
			return Trace[T]{}, 0, false, nil
		case blocks.LeafBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if forUpdate && currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			leafBlock, addedToCache, err := FetchBlock[T](c, *currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
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
					// and it must be fixed now.
					currentOrigin.PointerBlock.DecrementReferences()
				}

				tracedBlock.PointerBlocks = pointerTrace
				tracedBlock.splitFunc = func(splitFunc func(newPointerBlock Block[pointerV0.Block]) error) (Block[T], uint64, error) {
					leafBlock.IncrementReferences() // to prevent unloading until chunks are copied

					newPointerBlock, err := NewBlock[pointerV0.Block](c)
					if err != nil {
						return Block[T]{}, 0, err
					}

					newPointerBlock.WithPostCommitFunc(c.newPointerBlockPostCommitFunc(
						currentOrigin,
						newPointerBlock,
					))

					newPointerBlock.IncrementReferences()

					*currentOrigin.Pointer = pointerV0.Pointer{
						Address:       newPointerBlock.Address(),
						BirthRevision: newPointerBlock.BirthRevision(),
					}
					*currentOrigin.BlockType = blocks.PointerBlockType
					*currentOrigin.BlockSchemaVersion = blocks.PointerV0

					returnedBlock, err := NewBlock[T](c)
					if err != nil {
						return Block[T]{}, 0, err
					}

					returnedPointerIndex := tagReminder % pointerV0.PointersPerBlock
					newPointerBlock.Block.Pointers[returnedPointerIndex] = pointerV0.Pointer{
						Address:       returnedBlock.Address(),
						BirthRevision: c.singularityBlock.V.Revision + 1,
					}
					newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType
					newPointerBlock.Block.PointedBlockVersions[returnedPointerIndex] = blocks.ObjectListV0

					returnedBlock.WithPostCommitFunc(NewLeafBlockPostCommitFunc(
						c,
						BlockOrigin{
							PointerBlock:       newPointerBlock,
							Pointer:            &newPointerBlock.Block.Pointers[returnedPointerIndex],
							BlockType:          &newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex],
							BlockSchemaVersion: &newPointerBlock.Block.PointedBlockVersions[returnedPointerIndex],
						},
						returnedBlock,
					))

					returnedBlock.IncrementReferences() // to prevent unloading when new blocks are created as a result of split

					if err := splitFunc(newPointerBlock); err != nil {
						return Block[T]{}, 0, err
					}

					leafBlock.DecrementReferences()
					InvalidateBlock(c, leafBlock)

					returnedBlock.DecrementReferences()

					return returnedBlock, tagReminder / pointerV0.PointersPerBlock, nil
				}
			}

			return tracedBlock, tagReminder, true, nil
		case blocks.PointerBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if forUpdate && currentOrigin.PointerBlock.IsValid() {
				currentOrigin.PointerBlock.IncrementReferences()
			}

			nextPointerBlock, addedToCache, err := FetchBlock[pointerV0.Block](c, *currentOrigin.Pointer)
			if err != nil {
				return Trace[T]{}, 0, false, err
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

// Trace stores the trace of incremented pointer blocks leading to the final leaf node
type Trace[T blocks.Block] struct {
	PointerBlocks []Block[pointerV0.Block]
	Origin        BlockOrigin
	Block         Block[T]

	splitFunc func(splitFunc func(newPointerBlock Block[pointerV0.Block]) error) (Block[T], uint64, error)
}

// Split replaces a leaf block with a new pointer block and triggers redistribution of existing items between new set of leaf blocks.
func (t Trace[T]) Split(splitFunc func(newPointerBlock Block[pointerV0.Block]) error) (Block[T], uint64, error) {
	return t.splitFunc(splitFunc)
}
