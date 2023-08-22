package storm

import (
	"github.com/cespare/xxhash/v2"

	"github.com/outofforest/storm/blocks"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
)

// Storm represents the storm storage engine.
type Storm struct {
	c *cache.Cache
}

// New returns new storm store.
func New(dev persistence.Dev, cacheSize int64) (*Storm, error) {
	store, err := persistence.OpenStore(dev)
	if err != nil {
		return nil, err
	}
	c, err := cache.New(store, cacheSize)
	if err != nil {
		return nil, err
	}
	return &Storm{
		c: c,
	}, nil
}

// Get gets value for a key from the store.
func (s *Storm) Get(key [32]byte) (blocks.ObjectID, bool, error) {
	sBlock := s.c.SingularityBlock()
	dataBlockPath, exists, err := lookupByKey[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		false,
		key,
	)
	if !exists || err != nil {
		return 0, false, err
	}

	dataBlock := dataBlockPath.Leaf.Block

	if dataBlock.States[0] == objectlistV0.FreeItemState {
		return 0, false, nil
	}

	if hash := xxhash.Sum64(key[:]); dataBlock.Hashes[0] != hash {
		return 0, false, nil
	}

	link := dataBlock.Links[0]

	if link.Key != key {
		return 0, false, nil
	}

	return link.ObjectID, true, nil
}

// Set sets value for a key in the store.
func (s *Storm) Set(key [32]byte, objectID blocks.ObjectID) error {
	// TODO (wojciech): Implement block splitting

	sBlock := s.c.SingularityBlock()
	dataBlockPath, _, err := lookupByKey[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		true,
		key,
	)
	if err != nil {
		return err
	}

	// TODO (wojciech): Find correct record index

	dataBlockPath.Leaf.Block.NUsedItems = 1
	dataBlockPath.Leaf.Block.States[0] = objectlistV0.DefinedItemState
	dataBlockPath.Leaf.Block.Hashes[0] = xxhash.Sum64(key[:])
	dataBlockPath.Leaf.Block.Links[0] = objectlistV0.Link{
		Key:      key,
		ObjectID: objectID,
	}

	_, err = dataBlockPath.Commit()
	return err
}

// Delete deletes key from the store.
func (s *Storm) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

// Commit commits cached changes to the device.
func (s *Storm) Commit() error {
	return s.c.Commit()
}

type hop struct {
	Index uint64
	Block cache.CachedBlock[pointerV0.Block]
}

type keyPath[T blocks.Block] struct {
	rootPointer            *pointerV0.Pointer
	rootBlockType          *blocks.BlockType
	rootBlockSchemaVersion *blocks.SchemaVersion
	leafSchemaVersion      blocks.SchemaVersion
	hops                   []hop
	Leaf                   cache.CachedBlock[T]
}

func lookupByKey[T blocks.Block](
	c *cache.Cache,
	rootPointer *pointerV0.Pointer,
	rootBlockType *blocks.BlockType,
	rootBlockSchemaVersion *blocks.SchemaVersion,
	leafSchemaVersion blocks.SchemaVersion,
	createIfMissing bool,
	key [32]byte,
) (keyPath[T], bool, error) {
	currentPointer := *rootPointer
	currentPointedBlockType := *rootBlockType

	hash := xxhash.Sum64(key[:])
	hopAddressing := hash
	hops := make([]hop, 0, 11)

	for {
		switch currentPointedBlockType {
		case blocks.FreeBlockType:
			if createIfMissing {
				return keyPath[T]{
					rootPointer:            rootPointer,
					rootBlockType:          rootBlockType,
					rootBlockSchemaVersion: rootBlockSchemaVersion,
					leafSchemaVersion:      leafSchemaVersion,
					hops:                   hops,
					Leaf:                   cache.NewBlock[T](c),
				}, true, nil
			}
			return keyPath[T]{}, false, nil
		case blocks.LeafBlockType:
			leafBlock, err := cache.FetchBlock[T](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, false, err
			}
			return keyPath[T]{
				rootPointer:            rootPointer,
				rootBlockType:          rootBlockType,
				rootBlockSchemaVersion: rootBlockSchemaVersion,
				leafSchemaVersion:      leafSchemaVersion,
				hops:                   hops,
				Leaf:                   leafBlock,
			}, true, nil
		case blocks.PointerBlockType:
			pointerBlock, err := cache.FetchBlock[pointerV0.Block](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, false, err
			}

			pointerIndex := hopAddressing & (pointerV0.PointersPerBlock - 1)
			hopAddressing >>= blocks.PointersPerBlockShift

			currentPointedBlockType = pointerBlock.Block.PointedBlockTypes[pointerIndex]
			currentPointer = pointerBlock.Block.Pointers[pointerIndex]

			hops = append(hops, hop{
				Block: pointerBlock,
				Index: pointerIndex,
			})
		}
	}
}

func (kp keyPath[T]) Commit() (cache.CachedBlock[T], error) {
	leaf, err := kp.Leaf.Commit()
	if err != nil {
		return cache.CachedBlock[T]{}, err
	}
	address, err := leaf.Address()
	if err != nil {
		return cache.CachedBlock[T]{}, err
	}
	checksum := leaf.Block.ComputeChecksum()

	if nHops := len(kp.hops); nHops > 0 {
		lastHop := kp.hops[nHops-1]
		if lastHop.Block.Block.PointedBlockTypes[lastHop.Index] == blocks.FreeBlockType {
			lastHop.Block.Block.NUsedPointers++
			lastHop.Block.Block.PointedBlockTypes[lastHop.Index] = blocks.LeafBlockType
			lastHop.Block.Block.PointedBlockVersions[lastHop.Index] = kp.leafSchemaVersion
		}
		for i := nHops - 1; i >= 0; i-- {
			hop := kp.hops[i]
			hop.Block.Block.Pointers[hop.Index] = pointerV0.Pointer{
				Address:  address,
				Checksum: checksum,
			}
			pointerBlock, err := hop.Block.Commit()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			address, err = pointerBlock.Address()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			checksum = leaf.Block.ComputeChecksum()
		}
	}

	if *kp.rootBlockType == blocks.FreeBlockType {
		*kp.rootBlockType = blocks.LeafBlockType
		*kp.rootBlockSchemaVersion = kp.leafSchemaVersion
	}
	*kp.rootPointer = pointerV0.Pointer{
		Checksum: checksum,
		Address:  address,
	}

	return leaf, nil
}
